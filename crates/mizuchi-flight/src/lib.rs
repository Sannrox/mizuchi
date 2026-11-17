//! Arrow Flight workers: heartbeat, task retry, shuffle fetch, and fencing.
//!
//! TCP is opened only when the embedder sets `allow_network`. Shuffle
//! partitions are served as Flight `DoGet` streams. A worker that cannot be
//! reached is marked dead and the coordinator retries the task on a live
//! peer; a worker that answers with a refusal (a SQL error, say) stays live
//! and the error goes back to the caller. A later successful heartbeat
//! revives a dead worker.
//! When Flight fetch fails and a spill directory is set, the reader falls
//! back to the local spill file.
//!
//! A coordinator with [`RemoteCluster::with_leadership`] sends its lease's
//! fencing token on every assignment ([`FENCING_HEADER`]). A worker refuses
//! a token lower than the highest it has seen, and re-checks it before a
//! result or shuffle write is published, so a superseded coordinator's work
//! never lands after a standby takes over.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightClient, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt, stream};
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status, Streaming};

use mizuchi_core::batch::Batch;
use mizuchi_core::error::{Error, Result};
use mizuchi_exec::memory::{read_spill, write_spill};
use mizuchi_exec::{DirLeaseStore, Fence, LeaderStatus, Leadership, Session, StageCommits};

const HEARTBEAT_STALE: Duration = Duration::from_secs(2);

/// How long a worker waits on a peer for one shuffle partition before it
/// reports the fetch as failed, so the coordinator can fall back.
const PEER_FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Request metadata carrying the coordinator's fencing token.
pub const FENCING_HEADER: &str = "x-mizuchi-fencing-token";

struct WorkerState {
    /// One session per leadership generation (`None` when unfenced), so a
    /// table a superseded generation registers late is invisible to the
    /// generation that replaced it. Older generations are dropped as the
    /// fence advances.
    sessions: Mutex<HashMap<Option<u64>, Arc<Mutex<Session>>>>,
    shuffle: Mutex<HashMap<String, Vec<Batch>>>,
    spill_dir: Option<PathBuf>,
    fence: Fence,
}

impl WorkerState {
    /// Runs `work` on `token`'s generation session, creating it on first use.
    /// The map is locked only to find the session, so a superseded
    /// generation's long query never blocks its successor's work.
    fn with_session<T>(
        &self,
        token: Option<u64>,
        work: impl FnOnce(&mut Session) -> T,
    ) -> std::result::Result<T, Status> {
        let session = {
            let mut sessions = self.sessions.lock().map_err(|_| lock_err())?;
            sessions
                .entry(token)
                .or_insert_with(|| {
                    let session = Session::new();
                    Arc::new(Mutex::new(match &self.spill_dir {
                        Some(dir) => session.with_spill_dir(dir.clone()),
                        None => session,
                    }))
                })
                .clone()
        };
        let mut session = session.lock().map_err(|_| lock_err())?;
        Ok(work(&mut session))
    }

    /// Drops the sessions, shuffle partitions and this worker's spill files
    /// of generations older than `token`. A superseded generation's late
    /// writes are already refused by the fence, so nothing re-creates them.
    fn retire_before(&self, token: Option<u64>) {
        let Some(token) = token else {
            return;
        };
        if let Ok(mut sessions) = self.sessions.lock() {
            sessions.retain(|generation, _| generation.is_some_and(|g| g >= token));
        }
        if let Ok(mut shuffle) = self.shuffle.lock() {
            shuffle.retain(|key, _| {
                let current = key_generation(key) >= token;
                if !current && let Some(dir) = &self.spill_dir {
                    let _ = std::fs::remove_file(dir.join(format!("{key}.arrow")));
                }
                current
            });
        }
    }
}

/// Flight server that owns one worker's session and shuffle files.
pub struct FlightWorker {
    state: Arc<WorkerState>,
}

impl FlightWorker {
    /// With `lease_dir`, the worker checks every token against the lease
    /// directory's current token as well as its own high-water mark.
    fn new(spill_dir: Option<PathBuf>, lease_dir: Option<PathBuf>) -> Result<Self> {
        let fence = match lease_dir {
            Some(dir) => Fence::with_authority(DirLeaseStore::new(dir)?),
            None => Fence::default(),
        };
        Ok(Self {
            state: Arc::new(WorkerState {
                sessions: Mutex::new(HashMap::new()),
                shuffle: Mutex::new(HashMap::new()),
                spill_dir,
                fence,
            }),
        })
    }
}

/// Bind a Flight worker. Fails closed without a network grant. With
/// `lease_dir` (the coordinators' lease directory, ADR 0016) the worker's
/// fence survives a restart.
pub fn serve_worker(
    bind: SocketAddr,
    allow_network: bool,
    spill_dir: Option<PathBuf>,
    lease_dir: Option<PathBuf>,
) -> Result<()> {
    if !allow_network {
        return Err(Error::Io(
            "Flight worker requires SessionConfig.allow_network".into(),
        ));
    }
    serve_until(bind, spill_dir, lease_dir, std::future::pending())
}

/// Serves a worker on `bind` until `shutdown` completes.
fn serve_until(
    bind: SocketAddr,
    spill_dir: Option<PathBuf>,
    lease_dir: Option<PathBuf>,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<()> {
    let listener = std::net::TcpListener::bind(bind).map_err(|e| Error::Io(e.to_string()))?;
    ReadyWorker::prepare(listener, spill_dir, lease_dir)?.serve(shutdown)
}

/// A worker whose fallible setup is done: the runtime, the worker state, and
/// the listener it serves on. Only serving remains, so a caller that builds
/// one knows the worker will accept connections on the bound address.
struct ReadyWorker {
    rt: tokio::runtime::Runtime,
    listener: tokio::net::TcpListener,
    worker: FlightWorker,
}

impl ReadyWorker {
    fn prepare(
        listener: std::net::TcpListener,
        spill_dir: Option<PathBuf>,
        lease_dir: Option<PathBuf>,
    ) -> Result<Self> {
        let io = |e: std::io::Error| Error::Io(e.to_string());
        let worker = FlightWorker::new(spill_dir, lease_dir)?;
        let rt = tokio::runtime::Runtime::new().map_err(io)?;
        listener.set_nonblocking(true).map_err(io)?;
        let listener = {
            let _ctx = rt.enter();
            tokio::net::TcpListener::from_std(listener).map_err(io)?
        };
        Ok(Self {
            rt,
            listener,
            worker,
        })
    }

    fn serve(self, shutdown: impl std::future::Future<Output = ()>) -> Result<()> {
        let Self {
            rt,
            listener,
            worker,
        } = self;
        rt.block_on(async move {
            Server::builder()
                .add_service(FlightServiceServer::new(worker))
                .serve_with_incoming_shutdown(TcpIncoming::from(listener), shutdown)
                .await
                .map_err(|e| Error::Io(e.to_string()))
        })
    }
}

/// Bind `127.0.0.1:0` and serve a worker on a background thread. Fails
/// closed without a grant. The worker serves on the listener bound here, so
/// no other process can take the port, and setup errors (spill or lease
/// directory, runtime) return here instead of from the thread.
pub fn spawn_worker(
    allow_network: bool,
    spill_dir: Option<PathBuf>,
    lease_dir: Option<PathBuf>,
) -> Result<(SocketAddr, JoinHandle<Result<()>>)> {
    if !allow_network {
        return Err(Error::Io(
            "Flight worker requires SessionConfig.allow_network".into(),
        ));
    }
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| Error::Io(e.to_string()))?;
    let addr = listener
        .local_addr()
        .map_err(|e| Error::Io(e.to_string()))?;
    let ready = ReadyWorker::prepare(listener, spill_dir, lease_dir)?;
    let handle = std::thread::spawn(move || ready.serve(std::future::pending()));
    Ok((addr, handle))
}

fn encode_batches(batches: &[Batch]) -> Result<Vec<u8>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, schema.as_ref())
            .map_err(|e| Error::Io(e.to_string()))?;
        for b in batches {
            w.write(b.as_arrow())
                .map_err(|e| Error::Io(e.to_string()))?;
        }
        w.finish().map_err(|e| Error::Io(e.to_string()))?;
    }
    Ok(buf)
}

/// A table for `register`: its schema, then its batches. With no batches it
/// is a schema-only stream, so an empty table still registers.
fn encode_table(schema: &SchemaRef, batches: &[Batch]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut w =
        StreamWriter::try_new(&mut buf, schema.as_ref()).map_err(|e| Error::Io(e.to_string()))?;
    for b in batches {
        w.write(b.as_arrow())
            .map_err(|e| Error::Io(e.to_string()))?;
    }
    w.finish().map_err(|e| Error::Io(e.to_string()))?;
    drop(w);
    Ok(buf)
}

/// Schema and batches of an [`encode_table`] payload; `None` for an empty one.
fn decode_table(bytes: &[u8]) -> Result<Option<(SchemaRef, Vec<Batch>)>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let r = StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| Error::Io(e.to_string()))?;
    let schema = r.schema();
    let batches = r
        .map(|b| Batch::from_arrow(b.map_err(|e| Error::Io(e.to_string()))?))
        .collect::<Result<_>>()?;
    Ok(Some((schema, batches)))
}

fn decode_batches(bytes: &[u8]) -> Result<Vec<Batch>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let r = StreamReader::try_new(std::io::Cursor::new(bytes.to_vec()), None)
        .map_err(|e| Error::Io(e.to_string()))?;
    r.map(|b| Batch::from_arrow(b.map_err(|e| Error::Io(e.to_string()))?))
        .collect()
}

fn lock_err() -> Status {
    Status::internal("worker lock poisoned")
}

/// The fencing token on a request, if any. A malformed token is refused
/// rather than treated as absent.
fn fencing_token(
    metadata: &tonic::metadata::MetadataMap,
) -> std::result::Result<Option<u64>, Status> {
    metadata
        .get(FENCING_HEADER)
        .map(|value| {
            value
                .to_str()
                .ok()
                .and_then(|token| token.parse::<u64>().ok())
                .ok_or_else(|| Status::invalid_argument("fencing token"))
        })
        .transpose()
}

fn fenced(error: Error) -> Status {
    Status::failed_precondition(error.to_string())
}

/// An engine error for this request. The worker is fine, so the coordinator
/// returns it to the caller instead of retrying on a peer.
fn refused(error: Error) -> Status {
    Status::invalid_argument(error.to_string())
}

/// The generation a shuffle partition name belongs to; 0 when unfenced.
fn key_generation(key: &str) -> u64 {
    key.rsplit_once(".g")
        .and_then(|(_, generation)| generation.parse().ok())
        .unwrap_or(0)
}

/// A shuffle partition's name for one leadership generation. Workers share
/// the spill directory, so a superseded generation writes beside the
/// current one instead of over it, and each generation reads only its own.
fn generation_key(key: &str, token: Option<u64>) -> String {
    match token {
        Some(token) => format!("{key}.g{token}"),
        None => key.to_string(),
    }
}

fn record_batches(batches: &[Batch]) -> Vec<RecordBatch> {
    batches.iter().map(|b| b.as_arrow().clone()).collect()
}

#[tonic::async_trait]
impl FlightService for FlightWorker {
    type HandshakeStream = BoxStream<'static, std::result::Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, std::result::Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, std::result::Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, std::result::Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, std::result::Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, std::result::Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, std::result::Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        let token = fencing_token(request.metadata())?;
        self.state.fence.admit(token).map_err(fenced)?;
        let key = String::from_utf8(request.into_inner().ticket.to_vec())
            .map_err(|_| Status::invalid_argument("ticket"))?;
        let key = generation_key(&key, token);
        let batches = {
            let sh = self.state.shuffle.lock().map_err(|_| lock_err())?;
            sh.get(&key).cloned()
        };
        let batches = match (batches, self.state.spill_dir.clone()) {
            (Some(b), _) => b,
            (None, Some(dir)) => {
                blocking(move || {
                    if !dir.join(format!("{key}.arrow")).exists() {
                        return Err(Status::not_found(key));
                    }
                    read_shuffle_spill(&dir, &key).map_err(|e| Status::internal(e.to_string()))
                })
                .await?
            }
            (None, None) => return Err(Status::not_found(key)),
        };
        // Data read after a successor raised the fence is not returned.
        self.state.fence.still_current(token).map_err(fenced)?;
        let rbs = record_batches(&batches);
        let stream = FlightDataEncoderBuilder::new()
            .build(stream::iter(rbs.into_iter().map(Ok)))
            .map_err(|e| Status::internal(e.to_string()));
        Ok(Response::new(stream.boxed()))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        let token = fencing_token(request.metadata())?;
        let action = request.into_inner();
        // Every action but a heartbeat is an assignment and passes the fence.
        if action.r#type != "heartbeat" {
            self.state.fence.admit(token).map_err(fenced)?;
            self.state.retire_before(token);
        }
        let state = Arc::clone(&self.state);
        let body = action.body;
        // Engine work blocks, so it runs on the blocking pool; the runtime
        // threads stay free for heartbeats and peer fetches meanwhile.
        let body = match action.r#type.as_str() {
            "heartbeat" => Vec::new(),
            "register" => blocking(move || register_table(&state, &body, token)).await?,
            "execute" => blocking(move || execute_sql(&state, &body, token)).await?,
            "execute_slow" => {
                // Yields the runtime while it waits, so other assignments
                // (a successor's, say) are served meanwhile.
                tokio::time::sleep(Duration::from_millis(400)).await;
                blocking(move || execute_sql(&state, &body, token)).await?
            }
            "put_shuffle" => blocking(move || put_shuffle(&state, &body, token)).await?,
            "fetch_shuffle" => fetch_remote(state, &body, token).await?,
            other => return Err(Status::invalid_argument(format!("unknown action {other}"))),
        };
        Ok(Response::new(
            stream::once(async move { Ok(arrow_flight::Result { body: body.into() }) }).boxed(),
        ))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
}

fn empty_schema() -> SchemaRef {
    Arc::new(arrow::datatypes::Schema::empty())
}

fn register_table(
    state: &WorkerState,
    body: &[u8],
    token: Option<u64>,
) -> std::result::Result<Vec<u8>, Status> {
    let (name, rest) = split_name(body)?;
    let (schema, batches) = decode_table(rest)
        .map_err(|e| Status::invalid_argument(e.to_string()))?
        .ok_or_else(|| Status::invalid_argument("table payload without a schema"))?;
    // The table lands in this generation's own session, and only while the
    // generation is current; a late one is invisible to its successor.
    state
        .with_session(token, |session| {
            state.fence.publish(token, || {
                session.register(&name, schema, batches).map_err(refused)
            })
        })?
        .map_err(fenced)??;
    Ok(Vec::new())
}

/// Runs `body` and publishes the result only if `token` is still current.
fn execute_sql(
    state: &WorkerState,
    body: &[u8],
    token: Option<u64>,
) -> std::result::Result<Vec<u8>, Status> {
    let sql = std::str::from_utf8(body).map_err(|_| Status::invalid_argument("sql"))?;
    // A query with no rows still answers with its columns, as one empty batch,
    // so a later stage can read its output as a table.
    let out = state.with_session(token, |session| -> Result<Vec<Batch>> {
        let out = session.execute_sql(sql)?;
        if out.is_empty() {
            return Ok(vec![Batch::empty(
                session.logical_plan(sql)?.schema().clone(),
            )?]);
        }
        Ok(out)
    })?;
    // Fenced first: a superseded generation's query reports the fence, not
    // whatever went wrong after its session was retired.
    state.fence.still_current(token).map_err(fenced)?;
    let out = out.map_err(refused)?;
    encode_batches(&out).map_err(|e| Status::internal(e.to_string()))
}

fn put_shuffle(
    state: &WorkerState,
    body: &[u8],
    token: Option<u64>,
) -> std::result::Result<Vec<u8>, Status> {
    let (key, rest) = split_name(body)?;
    let batches = decode_batches(rest).map_err(|e| Status::internal(e.to_string()))?;
    let key = generation_key(&key, token);
    // Under the fence, so a successor cannot take over between the check and
    // the write and then have its own shuffle data replaced on this worker.
    state
        .fence
        .publish(token, || {
            // The spill copy backs the coordinator's fallback, so a write
            // that fails fails the put. An empty partition is still written,
            // so the fallback can tell it from a missing one.
            if let Some(dir) = &state.spill_dir {
                write_shuffle_spill(dir, &key, &batches)
                    .map_err(|e| Status::internal(format!("shuffle spill: {e}")))?;
            }
            state
                .shuffle
                .lock()
                .map_err(|_| lock_err())
                .map(|mut shuffle| shuffle.insert(key, batches))
        })
        .map_err(fenced)??;
    Ok(Vec::new())
}

/// Writes `batches` as the spill file of shuffle partition `key` under `dir`.
/// The file appears complete or not at all: it is written beside its final
/// name, synced, then renamed.
fn write_shuffle_spill(dir: &std::path::Path, key: &str, batches: &[Batch]) -> Result<()> {
    std::fs::create_dir_all(dir).map_err(|e| Error::Io(e.to_string()))?;
    let schema = batches.first().map_or_else(empty_schema, Batch::schema);
    let path = dir.join(format!("{key}.arrow"));
    let partial = dir.join(format!(
        ".{key}.arrow.partial-{}-{}",
        std::process::id(),
        PARTIAL_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let written = write_spill(&partial, schema, batches).and_then(|()| {
        std::fs::File::open(&partial)
            .and_then(|f| f.sync_all())
            .and_then(|()| std::fs::rename(&partial, &path))
            .map_err(|e| Error::Io(e.to_string()))
    });
    if written.is_err() {
        let _ = std::fs::remove_file(&partial);
    }
    written
}

static PARTIAL_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Shuffle partition `key` from the spill directory `dir`. A missing file is
/// an error; zero-row batches are dropped, as a Flight fetch would.
fn read_shuffle_spill(dir: &std::path::Path, key: &str) -> Result<Vec<Batch>> {
    Ok(
        read_spill(&dir.join(format!("{key}.arrow")), empty_schema())?
            .into_iter()
            .filter(|b| b.num_rows() > 0)
            .collect(),
    )
}

/// Fetches shuffle partition `owner`/`key` from its owner for this reader.
/// The call is awaited on the runtime, bounded by [`PEER_FETCH_TIMEOUT`], so
/// a slow or hung owner never holds a runtime thread.
async fn fetch_remote(
    state: Arc<WorkerState>,
    body: &[u8],
    token: Option<u64>,
) -> std::result::Result<Vec<u8>, Status> {
    let (owner, key) = split_name(body)?;
    let key = std::str::from_utf8(key).map_err(|_| Status::invalid_argument("key"))?;
    // The owner checks the same token the reader was assigned under.
    let batches = tokio::time::timeout(PEER_FETCH_TIMEOUT, fetch_shuffle_async(&owner, key, token))
        .await
        .map_err(|_| Status::deadline_exceeded(format!("shuffle fetch from {owner}")))?
        .map_err(|e| match Error::from(e) {
            // The owner's fence refusal reaches the coordinator as such.
            e @ Error::Fenced(_) => fenced(e),
            e => Status::internal(e.to_string()),
        })?;
    blocking(move || {
        state.fence.still_current(token).map_err(fenced)?;
        encode_batches(&batches).map_err(|e| Status::internal(e.to_string()))
    })
    .await
}

/// Runs blocking `work` on the runtime's blocking pool and awaits it.
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> std::result::Result<T, Status> + Send + 'static,
) -> std::result::Result<T, Status> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|e| Status::internal(format!("worker task: {e}")))?
}

fn split_name(body: &[u8]) -> std::result::Result<(String, &[u8]), Status> {
    let n = body.first().copied().unwrap_or(0) as usize;
    if body.len() < 1 + n {
        return Err(Status::invalid_argument("name"));
    }
    let name = std::str::from_utf8(&body[1..1 + n])
        .map_err(|_| Status::invalid_argument("name"))?
        .to_string();
    Ok((name, &body[1 + n..]))
}

/// Frames `name` ahead of `payload` behind a one-byte length, the format
/// `split_name` reads. A name longer than 255 bytes cannot be framed and is
/// refused rather than misframing the payload.
fn prefix_name(name: &str, payload: &[u8]) -> Result<Vec<u8>> {
    let bytes = name.as_bytes();
    let len = u8::try_from(bytes.len()).map_err(|_| {
        Error::Plan(format!(
            "name of {} bytes exceeds the 255-byte limit",
            bytes.len()
        ))
    })?;
    let mut out = Vec::with_capacity(1 + bytes.len() + payload.len());
    out.push(len);
    out.extend_from_slice(bytes);
    out.extend_from_slice(payload);
    Ok(out)
}

struct Peer {
    addr: String,
    last_ok: Mutex<Instant>,
    seen: Mutex<bool>,
    dead: Mutex<bool>,
}

/// One stage of a resumable batch query: SQL over the tables registered
/// so far, whose output is registered as `output` for the stages after it.
#[derive(Clone, Debug)]
pub struct Stage {
    pub sql: String,
    pub output: String,
}

/// Coordinator that talks to Flight workers.
#[derive(Clone)]
pub struct RemoteCluster {
    peers: Vec<Arc<Peer>>,
    spill_dir: Option<PathBuf>,
    leadership: Option<Arc<Leadership>>,
    /// Staged queries share the workers' table namespace, so one runs at a time.
    staged: Arc<Mutex<()>>,
}

impl RemoteCluster {
    /// Connect to `addrs`. Fails closed without a network grant.
    pub fn connect(
        addrs: Vec<String>,
        allow_network: bool,
        spill_dir: Option<PathBuf>,
    ) -> Result<Self> {
        if !allow_network {
            return Err(Error::Io(
                "Flight cluster requires SessionConfig.allow_network".into(),
            ));
        }
        if addrs.len() < 2 {
            return Err(Error::Plan(
                "Flight cluster needs at least two workers".into(),
            ));
        }
        Ok(Self {
            peers: addrs
                .into_iter()
                .map(|addr| {
                    Arc::new(Peer {
                        addr,
                        last_ok: Mutex::new(Instant::now() - HEARTBEAT_STALE),
                        seen: Mutex::new(false),
                        dead: Mutex::new(false),
                    })
                })
                .collect(),
            spill_dir,
            leadership: None,
            staged: Arc::new(Mutex::new(())),
        })
    }

    /// Fences every assignment with `leadership`'s lease. Each assignment
    /// renews the lease first; one that finds it lost fails with
    /// [`Error::Fenced`] instead of reaching a worker.
    pub fn with_leadership(mut self, leadership: Arc<Leadership>) -> Self {
        self.leadership = Some(leadership);
        self
    }

    /// Leadership, lease age and fencing token, when fenced.
    pub fn leader_status(&self) -> Option<LeaderStatus> {
        self.leadership
            .as_ref()
            .map(|leadership| leadership.status())
    }

    /// The token for the next assignment, renewing the lease.
    fn token(&self) -> Result<Option<u64>> {
        self.leadership
            .as_ref()
            .map(|leadership| leadership.assignment_token())
            .transpose()
    }

    /// Worker addresses in claim order.
    pub fn worker_addrs(&self) -> Vec<String> {
        self.peers.iter().map(|p| p.addr.clone()).collect()
    }

    /// Ping every worker, dead ones included. An answer marks the worker
    /// live again; no answer marks a worker seen before dead.
    pub fn heartbeat(&self) -> usize {
        for peer in &self.peers {
            match action(&peer.addr, "heartbeat", &[], None) {
                Ok(_) => revive(peer),
                Err(_) => mark_dead_if_seen(peer),
            }
        }
        self.live_workers()
    }

    /// Live worker count (not marked dead).
    pub fn live_workers(&self) -> usize {
        self.peers
            .iter()
            .filter(|p| !is_dead(p) && fresh(p))
            .count()
    }

    /// Broadcast a table to every live worker.
    /// A refusal (a bad table, a fence) is returned at once; an unreachable
    /// worker is marked dead and skipped.
    pub fn register(&self, name: &str, schema: SchemaRef, batches: Vec<Batch>) -> Result<()> {
        let payload = prefix_name(name, &encode_table(&schema, &batches)?)?;
        let mut last = None;
        for peer in &self.peers {
            if is_dead(peer) {
                continue;
            }
            match action(&peer.addr, "register", &payload, self.token()?) {
                Ok(_) => touch(peer),
                Err(CallError::Refused(e)) => return Err(e),
                Err(CallError::Transport(e)) => {
                    mark_dead(peer);
                    last = Some(e);
                }
            }
        }
        if self.live_workers() == 0 {
            return Err(last.unwrap_or_else(|| Error::Io("no live workers".into())));
        }
        self.token()?;
        Ok(())
    }

    /// Run `sql` on a live worker. Retries on a peer when a worker cannot be
    /// reached; a worker's refusal (a SQL or plan error) is returned as is.
    pub fn execute_sql(&self, sql: &str) -> Result<Vec<Batch>> {
        self.execute_kind("execute", sql)
    }

    /// Same as [`Self::execute_sql`] but the worker sleeps before running.
    /// Used to prove a mid-query kill still retries.
    pub fn execute_sql_slow(&self, sql: &str) -> Result<Vec<Batch>> {
        self.execute_kind("execute_slow", sql)
    }

    fn execute_kind(&self, kind: &str, sql: &str) -> Result<Vec<Batch>> {
        let mut last = Error::Io("no live workers".into());
        for peer in self.live_peers() {
            match action(&peer.addr, kind, sql.as_bytes(), self.token()?) {
                Ok(body) => {
                    touch(&peer);
                    // A result that arrives after the lease was lost is not
                    // exposed: the caller gets the fence instead.
                    self.token()?;
                    return decode_batches(&body);
                }
                // A refusal, the fence included, would repeat on every peer.
                Err(CallError::Refused(e)) => return Err(e),
                Err(CallError::Transport(e)) => {
                    mark_dead(&peer);
                    last = e;
                }
            }
        }
        Err(last)
    }

    /// Runs `stages` as the batch query `query_id` and returns the last
    /// stage's output. Each stage's output is committed to the shared spill
    /// directory while this coordinator holds its lease; a coordinator that
    /// takes over runs the same id and reuses every committed stage instead
    /// of re-running it (#162). A committed stage whose output is missing or
    /// altered fails the query cleanly, never with a wrong result.
    pub fn run_staged(&self, query_id: &str, stages: &[Stage]) -> Result<Vec<Batch>> {
        let spill = self
            .spill_dir
            .as_ref()
            .ok_or_else(|| Error::Plan("staged queries need a shared spill directory".into()))?;
        // Intermediate tables live in the workers' shared namespace; a second
        // staged query must not replace one between this query's stages.
        let _one_at_a_time = self.staged.lock().unwrap_or_else(|e| e.into_inner());
        // Each leadership generation commits to its own records and adopts
        // only what earlier generations committed before it sealed the query.
        let generation = self.token()?.unwrap_or(0);
        let commits = StageCommits::open(spill, query_id, generation)?;
        let mut output = Vec::new();
        for (index, stage) in stages.iter().enumerate() {
            // What the stage computes; a changed stage is never reused.
            let definition = format!("{}\n{}", stage.output, stage.sql);
            output = match commits.committed(index, &definition)? {
                Some(committed) => committed,
                None => {
                    let ran = self.execute_sql(&stage.sql)?;
                    commits.commit(index, &definition, &ran)?;
                    // This generation's first commit of the stage counts.
                    commits.committed(index, &definition)?.ok_or_else(|| {
                        Error::Io(format!("stage {index} of {query_id} did not commit"))
                    })?
                }
            };
            if index + 1 < stages.len()
                && let Some(first) = output.first()
            {
                self.register(&stage.output, first.schema(), output.clone())?;
            }
        }
        // A result is exposed only to a coordinator that still holds the lease,
        // also when every stage was already committed.
        self.token()?;
        Ok(output)
    }

    /// Store shuffle batches on `owner` and fetch them over Flight from `reader`.
    /// Falls back to the shared spill file when the owner is unreachable.
    pub fn shuffle_round_trip(
        &self,
        owner: usize,
        reader: usize,
        key: &str,
        batches: &[Batch],
    ) -> Result<Vec<Batch>> {
        let peers = &self.peers;
        let owner = peers
            .get(owner)
            .ok_or_else(|| Error::Plan("unknown shuffle owner".into()))?;
        let reader = peers
            .get(reader)
            .ok_or_else(|| Error::Plan("unknown shuffle reader".into()))?;
        let payload = prefix_name(key, &encode_batches(batches)?)?;
        action(&owner.addr, "put_shuffle", &payload, self.token()?)?;
        touch(owner);
        let ticket = prefix_name(&owner.addr, key.as_bytes())?;
        let token = self.token()?;
        let fetched = match action(&reader.addr, "fetch_shuffle", &ticket, token) {
            Ok(body) => {
                touch(reader);
                decode_batches(&body)
            }
            Err(CallError::Refused(e @ Error::Fenced(_))) => Err(e),
            Err(_) => {
                // The fallback reads shared storage directly, so it re-checks
                // leadership first and reads only this generation's file.
                let token = self.token()?;
                match &self.spill_dir {
                    Some(dir) => read_shuffle_spill(dir, &generation_key(key, token)),
                    None => fetch_shuffle(&owner.addr, key, token).map_err(Error::from),
                }
            }
        };
        // Shuffle data read after the lease was lost is not exposed either.
        let batches = fetched?;
        self.token()?;
        Ok(batches)
    }

    fn live_peers(&self) -> Vec<Arc<Peer>> {
        self.peers.iter().filter(|p| !is_dead(p)).cloned().collect()
    }
}

fn is_dead(peer: &Peer) -> bool {
    *peer.dead.lock().unwrap_or_else(|e| e.into_inner())
}

fn fresh(peer: &Peer) -> bool {
    peer.last_ok
        .lock()
        .map(|t| t.elapsed() < HEARTBEAT_STALE)
        .unwrap_or(true)
}

fn touch(peer: &Peer) {
    if let Ok(mut t) = peer.last_ok.lock() {
        *t = Instant::now();
    }
    if let Ok(mut s) = peer.seen.lock() {
        *s = true;
    }
}

fn mark_dead(peer: &Peer) {
    if let Ok(mut d) = peer.dead.lock() {
        *d = true;
    }
}

fn revive(peer: &Peer) {
    touch(peer);
    if let Ok(mut d) = peer.dead.lock() {
        *d = false;
    }
}

fn mark_dead_if_seen(peer: &Peer) {
    let seen = *peer.seen.lock().unwrap_or_else(|e| e.into_inner());
    if seen {
        mark_dead(peer);
    }
}

fn rt() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new().map_err(|e| Error::Io(e.to_string()))
}

/// Why a call to a worker failed.
#[derive(Debug)]
enum CallError {
    /// No answer: connect failure, timeout, or a dropped connection. The
    /// worker may be gone.
    Transport(Error),
    /// The worker answered and refused the request. It is live.
    Refused(Error),
}

impl From<CallError> for Error {
    fn from(error: CallError) -> Self {
        match error {
            CallError::Transport(e) | CallError::Refused(e) => e,
        }
    }
}

type CallResult<T> = std::result::Result<T, CallError>;

fn action(addr: &str, kind: &str, body: &[u8], token: Option<u64>) -> CallResult<Vec<u8>> {
    let rt = rt().map_err(CallError::Transport)?;
    rt.block_on(async {
        let mut client = connect(addr, token).await.map_err(CallError::Transport)?;
        let mut stream = client
            .do_action(Action {
                r#type: kind.into(),
                body: body.to_vec().into(),
            })
            .await
            .map_err(flight_error)?;
        let msg = stream
            .try_next()
            .await
            .map_err(flight_error)?
            .ok_or_else(|| CallError::Transport(Error::Io("empty action result".into())))?;
        Ok(msg.to_vec())
    })
}

fn fetch_shuffle(owner: &str, key: &str, token: Option<u64>) -> CallResult<Vec<Batch>> {
    rt().map_err(CallError::Transport)?
        .block_on(fetch_shuffle_async(owner, key, token))
}

async fn fetch_shuffle_async(owner: &str, key: &str, token: Option<u64>) -> CallResult<Vec<Batch>> {
    let mut client = connect(owner, token).await.map_err(CallError::Transport)?;
    let stream = client
        .do_get(Ticket {
            ticket: key.as_bytes().to_vec().into(),
        })
        .await
        .map_err(flight_error)?;
    let rbs: Vec<RecordBatch> = stream.try_collect().await.map_err(flight_error)?;
    rbs.into_iter()
        .map(Batch::from_arrow)
        .collect::<Result<_>>()
        .map_err(CallError::Refused)
}

/// Sorts a failed call into a worker's refusal or a transport failure. The
/// codes a worker sends for a request it rejects are refusals; anything else
/// (unavailable, deadline, a dropped stream, an internal fault) is transport.
fn flight_error(error: arrow_flight::error::FlightError) -> CallError {
    use tonic::Code;
    let arrow_flight::error::FlightError::Tonic(status) = &error else {
        return CallError::Transport(Error::Io(error.to_string()));
    };
    let message = status.message();
    match status.code() {
        Code::FailedPrecondition if message.starts_with("fenced: ") => CallError::Refused(
            Error::Fenced(message.trim_start_matches("fenced: ").to_string()),
        ),
        Code::InvalidArgument
        | Code::NotFound
        | Code::FailedPrecondition
        | Code::AlreadyExists
        | Code::OutOfRange
        | Code::Unimplemented
        | Code::PermissionDenied => CallError::Refused(remote_error(message)),
        _ => CallError::Transport(Error::Io(error.to_string())),
    }
}

/// The engine error a worker refused with, from its display text; a message
/// with no known prefix is reported as I/O from the worker.
fn remote_error(message: &str) -> Error {
    if message == "cancelled" {
        return Error::Cancelled;
    }
    type Kind = fn(String) -> Error;
    let kinds: [(&str, Kind); 7] = [
        ("sql parse: ", Error::Parse),
        ("plan: ", Error::Plan),
        ("expression eval: ", Error::Eval),
        ("memory: ", Error::Memory),
        ("invalid batch: ", Error::InvalidBatch),
        ("unsupported Arrow type: ", Error::UnsupportedType),
        ("io: ", Error::Io),
    ];
    for (prefix, kind) in kinds {
        if let Some(rest) = message.strip_prefix(prefix) {
            return kind(rest.to_string());
        }
    }
    Error::Io(format!("worker refused the request: {message}"))
}

async fn connect(addr: &str, token: Option<u64>) -> Result<FlightClient> {
    let uri = if addr.starts_with("http") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    };
    let channel = Channel::from_shared(uri)
        .map_err(|e| Error::Io(e.to_string()))?
        .connect()
        .await
        .map_err(|e| Error::Io(e.to_string()))?;
    let mut client = FlightClient::new(channel);
    if let Some(token) = token {
        client
            .add_header(FENCING_HEADER, &token.to_string())
            .map_err(|e| Error::Io(e.to_string()))?;
    }
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shuffle_names_carry_their_generation() {
        assert_eq!(generation_key("q1-p0", Some(7)), "q1-p0.g7");
        assert_eq!(generation_key("q1-p0", None), "q1-p0");
        assert_eq!(key_generation("q1-p0.g7"), 7);
        assert_eq!(key_generation("q1.g2-p0.g12"), 12);
        assert_eq!(key_generation("q1-p0"), 0);
    }

    fn numbers(n: i64) -> (SchemaRef, Vec<Batch>) {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::Int64Array::from_iter_values(0..n))],
        )
        .unwrap();
        (schema, vec![batch])
    }

    fn rows(out: &[Batch]) -> usize {
        out.iter().map(Batch::num_rows).sum()
    }

    fn two_workers() -> RemoteCluster {
        let (a, _) = spawn_worker(true, None, None).unwrap();
        let (b, _) = spawn_worker(true, None, None).unwrap();
        let cluster =
            RemoteCluster::connect(vec![a.to_string(), b.to_string()], true, None).unwrap();
        assert_eq!(cluster.heartbeat(), 2);
        cluster
    }

    #[test]
    fn a_sql_error_goes_to_the_caller_and_leaves_workers_live() {
        let cluster = two_workers();
        let (schema, batches) = numbers(4);
        cluster.register("t", schema, batches).unwrap();
        let err = cluster.execute_sql("SELECT missing FROM t").unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
        let err = cluster.register("", numbers(1).0, Vec::new()).unwrap_err();
        assert!(!matches!(err, Error::Fenced(_)), "{err:?}");
        assert_eq!(cluster.live_workers(), 2);
        let out = cluster.execute_sql("SELECT a FROM t WHERE a > 1").unwrap();
        assert_eq!(rows(&out), 2);
        assert_eq!(cluster.live_workers(), 2);
    }

    #[test]
    fn a_table_name_over_255_bytes_is_refused_and_255_round_trips() {
        let cluster = two_workers();
        let (schema, batches) = numbers(3);
        let err = cluster
            .register(&"n".repeat(256), schema.clone(), batches.clone())
            .unwrap_err();
        assert!(
            matches!(&err, Error::Plan(m) if m.contains("255-byte limit")),
            "{err:?}"
        );
        let name = "n".repeat(255);
        cluster.register(&name, schema, batches).unwrap();
        let out = cluster
            .execute_sql(&format!("SELECT a FROM {name}"))
            .unwrap();
        assert_eq!(rows(&out), 3);
        assert_eq!(cluster.live_workers(), 2);
    }

    #[test]
    fn an_empty_table_registers_and_leaves_workers_live() {
        let cluster = two_workers();
        let (schema, _) = numbers(0);
        cluster.register("empty", schema, Vec::new()).unwrap();
        assert_eq!(cluster.live_workers(), 2);
        let out = cluster.execute_sql("SELECT a FROM empty").unwrap();
        assert_eq!(rows(&out), 0);
        assert_eq!(out[0].schema().field(0).name(), "a");
    }

    /// Serves a worker on `addr` on a thread until the returned sender fires.
    fn stoppable_worker(
        addr: SocketAddr,
    ) -> (tokio::sync::oneshot::Sender<()>, JoinHandle<Result<()>>) {
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let handle = std::thread::spawn(move || {
            serve_until(addr, None, None, async {
                let _ = stopped.await;
            })
        });
        let start = Instant::now();
        while std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(20)).is_err() {
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "worker did not bind"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        (stop, handle)
    }

    #[test]
    fn a_heartbeat_revives_a_restarted_worker() {
        let addr = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let (stop, handle) = stoppable_worker(addr);
        let (other, _) = spawn_worker(true, None, None).unwrap();
        let cluster =
            RemoteCluster::connect(vec![addr.to_string(), other.to_string()], true, None).unwrap();
        assert_eq!(cluster.heartbeat(), 2);
        stop.send(()).unwrap();
        handle.join().unwrap().unwrap();
        assert_eq!(cluster.heartbeat(), 1, "a stopped worker is marked dead");
        let (stop, handle) = stoppable_worker(addr);
        assert_eq!(cluster.heartbeat(), 2, "the restarted worker is live again");
        let (schema, batches) = numbers(3);
        cluster.register("t", schema, batches).unwrap();
        assert_eq!(rows(&cluster.execute_sql("SELECT a FROM t").unwrap()), 3);
        stop.send(()).unwrap();
        handle.join().unwrap().unwrap();
    }

    /// A worker served on a runtime with only `threads` worker threads, so a
    /// handler that blocks a runtime thread starves the rest. The returned
    /// sender stops it; the state lets a test hold its locks.
    fn narrow_worker(
        threads: usize,
    ) -> (
        SocketAddr,
        Arc<WorkerState>,
        tokio::sync::oneshot::Sender<()>,
        JoinHandle<()>,
    ) {
        let addr = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let worker = FlightWorker::new(None, None).unwrap();
        let state = Arc::clone(&worker.state);
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(threads)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(
                Server::builder()
                    .add_service(FlightServiceServer::new(worker))
                    .serve_with_shutdown(addr, async {
                        let _ = stopped.await;
                    }),
            )
            .unwrap();
        });
        let start = Instant::now();
        while std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(20)).is_err() {
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "worker did not bind"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        (addr, state, stop, handle)
    }

    /// Sends `count` copies of one action to `addr` at once, each on its own
    /// connection, and waits at most `limit` for all of them.
    async fn concurrent_actions(
        addr: SocketAddr,
        kind: &'static str,
        body: Vec<u8>,
        count: usize,
    ) -> Vec<CallResult<Vec<u8>>> {
        let calls = (0..count).map(|_| {
            let body = body.clone();
            tokio::spawn(async move { action_async(&addr.to_string(), kind, &body, None).await })
        });
        futures::future::join_all(calls)
            .await
            .into_iter()
            .map(|joined| joined.unwrap())
            .collect()
    }

    async fn action_async(
        addr: &str,
        kind: &str,
        body: &[u8],
        token: Option<u64>,
    ) -> CallResult<Vec<u8>> {
        let mut client = connect(addr, token).await.map_err(CallError::Transport)?;
        let mut stream = client
            .do_action(Action {
                r#type: kind.into(),
                body: body.to_vec().into(),
            })
            .await
            .map_err(flight_error)?;
        let msg = stream
            .try_next()
            .await
            .map_err(flight_error)?
            .ok_or_else(|| CallError::Transport(Error::Io("empty action result".into())))?;
        Ok(msg.to_vec())
    }

    #[test]
    fn more_peer_fetches_than_runtime_threads_all_complete() {
        const THREADS: usize = 2;
        let (addr, _state, stop, handle) = narrow_worker(THREADS);
        let (_, batches) = numbers(3);
        let put = prefix_name("p", &encode_batches(&batches).unwrap()).unwrap();
        action(&addr.to_string(), "put_shuffle", &put, None).unwrap();
        // Each fetch asks this same worker for the partition, so every fetch
        // needs a free runtime thread to serve the owner's side.
        let ticket = prefix_name(&addr.to_string(), b"p").unwrap();
        let rt = rt().unwrap();
        let out = rt.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(10),
                concurrent_actions(addr, "fetch_shuffle", ticket, THREADS + 1),
            )
            .await
        });
        let _ = stop.send(());
        let out = out.expect("peer fetches starved the worker runtime");
        handle.join().unwrap();
        for body in out {
            assert_eq!(rows(&decode_batches(&body.unwrap()).unwrap()), 3);
        }
    }

    #[test]
    fn a_heartbeat_is_answered_while_queries_wait_on_the_session() {
        const THREADS: usize = 2;
        let (addr, state, stop, handle) = narrow_worker(THREADS);
        let (schema, batches) = numbers(3);
        let table = prefix_name("t", &encode_table(&schema, &batches).unwrap()).unwrap();
        action(&addr.to_string(), "register", &table, None).unwrap();
        let session = state.sessions.lock().unwrap().get(&None).cloned().unwrap();
        let held = session.lock().unwrap();
        let rt = rt().unwrap();
        // More queries than runtime threads, all blocked on the held session.
        let queries = rt.spawn(concurrent_actions(
            addr,
            "execute",
            b"SELECT a FROM t".to_vec(),
            THREADS + 1,
        ));
        std::thread::sleep(Duration::from_millis(200));
        let beat = rt.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(5),
                action_async(&addr.to_string(), "heartbeat", &[], None),
            )
            .await
        });
        drop(held);
        let out = rt.block_on(queries).unwrap();
        let _ = stop.send(());
        handle.join().unwrap();
        beat.expect("heartbeat waited behind blocked queries")
            .unwrap();
        for body in out {
            assert_eq!(rows(&decode_batches(&body.unwrap()).unwrap()), 3);
        }
    }

    /// A unique spill directory for one test.
    fn spill_dir(tag: &str) -> PathBuf {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "mizuchi-flight-{tag}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// An address nothing listens on, so a call to it fails as transport.
    fn unreachable_addr() -> String {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .to_string()
    }

    #[cfg(unix)]
    #[test]
    fn a_shuffle_put_fails_when_its_spill_cannot_be_written() {
        use std::os::unix::fs::PermissionsExt;
        let dir = spill_dir("ro");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
        // Permission bits do not bind a privileged user; nothing to prove then.
        let writable = std::fs::write(dir.join("probe"), b"x").is_ok();
        let (owner, _) = spawn_worker(true, Some(dir.clone()), None).unwrap();
        let (reader, _) = spawn_worker(true, Some(dir.clone()), None).unwrap();
        let cluster =
            RemoteCluster::connect(vec![owner.to_string(), reader.to_string()], true, None)
                .unwrap();
        let (_, batches) = numbers(3);
        let out = cluster.shuffle_round_trip(0, 1, "ro-part", &batches);
        let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
        let _ = std::fs::remove_dir_all(&dir);
        if !writable {
            let err = out.unwrap_err();
            assert!(err.to_string().contains("shuffle spill"), "{err}");
        }
    }

    #[test]
    fn a_missing_shuffle_spill_is_an_error_not_an_empty_partition() {
        let dir = spill_dir("missing");
        let (owner, _) = spawn_worker(true, Some(dir.clone()), None).unwrap();
        let (reader, _) = spawn_worker(true, Some(dir.clone()), None).unwrap();
        let cluster =
            RemoteCluster::connect(vec![owner.to_string(), reader.to_string()], true, None)
                .unwrap();
        let (_, batches) = numbers(3);
        cluster
            .shuffle_round_trip(0, 1, "gone-part", &batches)
            .unwrap();
        let spilled = read_shuffle_spill(&dir, "gone-part").map(|b| rows(&b));
        std::fs::remove_file(dir.join("gone-part.arrow")).unwrap();
        let missing = read_shuffle_spill(&dir, "gone-part");
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(spilled.unwrap(), 3);
        let err = missing.unwrap_err();
        assert!(err.to_string().contains("missing"), "{err}");
    }

    #[test]
    fn an_empty_shuffle_partition_round_trips_through_the_spill_fallback() {
        let dir = spill_dir("empty");
        let (owner, _) = spawn_worker(true, Some(dir.clone()), None).unwrap();
        // The reader cannot be reached, so the coordinator reads the spill.
        let cluster = RemoteCluster::connect(
            vec![owner.to_string(), unreachable_addr()],
            true,
            Some(dir.clone()),
        )
        .unwrap();
        let empty = cluster.shuffle_round_trip(0, 1, "empty-part", &[]);
        let (_, batches) = numbers(2);
        let full = cluster.shuffle_round_trip(0, 1, "full-part", &batches);
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(rows(&empty.unwrap()), 0);
        assert_eq!(rows(&full.unwrap()), 2);
    }

    #[test]
    fn a_worker_that_cannot_start_fails_spawn() {
        // A lease directory path that is a regular file cannot be created,
        // so the worker never serves; spawn_worker must say so.
        let dir = spill_dir("not-a-dir");
        let file = dir.join("file");
        std::fs::write(&file, b"x").unwrap();
        let res = spawn_worker(true, None, Some(file.join("lease")));
        let _ = std::fs::remove_dir_all(&dir);
        assert!(
            matches!(res, Err(Error::Io(_))),
            "{:?}",
            res.map(|(a, _)| a)
        );
    }

    #[test]
    fn serve_and_connect_require_network_grant() {
        let err = serve_worker("127.0.0.1:0".parse().unwrap(), false, None, None).unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err:?}");
        let err = spawn_worker(false, None, None).unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err:?}");
        let err = match RemoteCluster::connect(
            vec!["127.0.0.1:1".into(), "127.0.0.1:2".into()],
            false,
            None,
        ) {
            Ok(_) => panic!("connect must fail without a network grant"),
            Err(e) => e,
        };
        assert!(matches!(err, Error::Io(_)), "{err:?}");
    }
}
