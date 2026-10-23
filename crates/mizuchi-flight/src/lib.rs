//! Arrow Flight workers: heartbeat, task retry, shuffle fetch, and fencing.
//!
//! TCP is opened only when the embedder sets `allow_network`. Shuffle
//! partitions are served as Flight `DoGet` streams. A missed heartbeat
//! marks the worker dead; the coordinator retries the task on a live peer.
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
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status, Streaming};

use mizuchi_core::batch::Batch;
use mizuchi_core::error::{Error, Result};
use mizuchi_exec::memory::{read_spill, write_spill};
use mizuchi_exec::{DirLeaseStore, Fence, LeaderStatus, Leadership, Session};

const HEARTBEAT_STALE: Duration = Duration::from_secs(2);

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
    let worker = FlightWorker::new(spill_dir, lease_dir)?;
    let rt = tokio::runtime::Runtime::new().map_err(|e| Error::Io(e.to_string()))?;
    rt.block_on(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(worker))
            .serve(bind)
            .await
            .map_err(|e| Error::Io(e.to_string()))
    })
}

/// Bind `127.0.0.1:0` on a background thread. Fails closed without a grant.
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
    drop(listener);
    let handle = std::thread::spawn(move || serve_worker(addr, true, spill_dir, lease_dir));
    for _ in 0..50 {
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(20)).is_ok() {
            return Ok((addr, handle));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
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
        let batches = match batches {
            Some(b) => b,
            None => match &self.state.spill_dir {
                Some(dir) => {
                    let path = dir.join(format!("{key}.arrow"));
                    if path.exists() {
                        read_spill(&path, empty_schema())
                            .map_err(|e| Status::internal(e.to_string()))?
                    } else {
                        return Err(Status::not_found(key));
                    }
                }
                None => return Err(Status::not_found(key)),
            },
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
        let state = &self.state;
        let body = match action.r#type.as_str() {
            "heartbeat" => Vec::new(),
            "register" => register_table(state, &action.body, token)?,
            "execute" => execute_sql(state, &action.body, token)?,
            "execute_slow" => {
                // Yields the runtime while it waits, so other assignments
                // (a successor's, say) are served meanwhile.
                tokio::time::sleep(Duration::from_millis(400)).await;
                execute_sql(state, &action.body, token)?
            }
            "put_shuffle" => put_shuffle(state, &action.body, token)?,
            "fetch_shuffle" => fetch_remote(state, &action.body, token)?,
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
    let batches = decode_batches(rest).map_err(|e| Status::internal(e.to_string()))?;
    if batches.is_empty() {
        return Err(Status::invalid_argument("empty table"));
    }
    let schema = batches[0].schema();
    // The table lands in this generation's own session, and only while the
    // generation is current; a late one is invisible to its successor.
    state
        .with_session(token, |session| {
            state.fence.publish(token, || {
                session
                    .register(&name, schema, batches)
                    .map_err(|e| Status::internal(e.to_string()))
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
    let out = state.with_session(token, |session| session.execute_sql(sql))?;
    // Fenced first: a superseded generation's query reports the fence, not
    // whatever went wrong after its session was retired.
    state.fence.still_current(token).map_err(fenced)?;
    let out = out.map_err(|e| Status::internal(e.to_string()))?;
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
            if let (Some(dir), Some(schema)) =
                (&state.spill_dir, batches.first().map(Batch::schema))
            {
                let _ = std::fs::create_dir_all(dir);
                let path = dir.join(format!("{key}.arrow"));
                let _ = write_spill(&path, schema, &batches);
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

fn fetch_remote(
    state: &WorkerState,
    body: &[u8],
    token: Option<u64>,
) -> std::result::Result<Vec<u8>, Status> {
    let (owner, key) = split_name(body)?;
    let key = std::str::from_utf8(key)
        .map_err(|_| Status::invalid_argument("key"))?
        .to_string();
    // The Flight handler already sits on a Tokio runtime. A nested
    // `Runtime::new().block_on` panics, so the client call runs on its own thread.
    // The owner checks the same token the reader was assigned under.
    let batches = std::thread::spawn(move || fetch_shuffle(&owner, &key, token))
        .join()
        .map_err(|_| Status::internal("shuffle fetch thread"))?
        .map_err(|e| match e {
            // The owner's fence refusal reaches the coordinator as such.
            Error::Fenced(_) => fenced(e),
            e => Status::internal(e.to_string()),
        })?;
    state.fence.still_current(token).map_err(fenced)?;
    encode_batches(&batches).map_err(|e| Status::internal(e.to_string()))
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

fn prefix_name(name: &str, payload: &[u8]) -> Vec<u8> {
    let bytes = name.as_bytes();
    let mut out = Vec::with_capacity(1 + bytes.len() + payload.len());
    out.push(u8::try_from(bytes.len()).unwrap_or(255));
    out.extend_from_slice(bytes);
    out.extend_from_slice(payload);
    out
}

struct Peer {
    addr: String,
    last_ok: Mutex<Instant>,
    seen: Mutex<bool>,
    dead: Mutex<bool>,
}

/// Coordinator that talks to Flight workers.
#[derive(Clone)]
pub struct RemoteCluster {
    peers: Vec<Arc<Peer>>,
    spill_dir: Option<PathBuf>,
    leadership: Option<Arc<Leadership>>,
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

    /// Ping every worker and mark failures dead.
    pub fn heartbeat(&self) -> usize {
        for peer in &self.peers {
            if is_dead(peer) {
                continue;
            }
            match action(&peer.addr, "heartbeat", &[], None) {
                Ok(_) => touch(peer),
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
    pub fn register(&self, name: &str, schema: SchemaRef, batches: Vec<Batch>) -> Result<()> {
        let _ = schema;
        let payload = prefix_name(name, &encode_batches(&batches)?);
        let mut last = None;
        for peer in &self.peers {
            if is_dead(peer) {
                continue;
            }
            match action(&peer.addr, "register", &payload, self.token()?) {
                Ok(_) => touch(peer),
                Err(e @ Error::Fenced(_)) => return Err(e),
                Err(e) => {
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

    /// Run `sql` on a live worker. Retries on a peer after a failure.
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
                // A fenced coordinator stops; retrying on a peer would not help.
                Err(e @ Error::Fenced(_)) => return Err(e),
                Err(e) => {
                    mark_dead(&peer);
                    last = e;
                }
            }
        }
        Err(last)
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
        let payload = prefix_name(key, &encode_batches(batches)?);
        action(&owner.addr, "put_shuffle", &payload, self.token()?)?;
        touch(owner);
        let ticket = prefix_name(&owner.addr, key.as_bytes());
        let token = self.token()?;
        let fetched = match action(&reader.addr, "fetch_shuffle", &ticket, token) {
            Ok(body) => {
                touch(reader);
                decode_batches(&body)
            }
            Err(e @ Error::Fenced(_)) => Err(e),
            Err(_) => {
                // The fallback reads shared storage directly, so it re-checks
                // leadership first and reads only this generation's file.
                let token = self.token()?;
                match &self.spill_dir {
                    Some(dir) => read_spill(
                        &dir.join(format!("{}.arrow", generation_key(key, token))),
                        empty_schema(),
                    ),
                    None => fetch_shuffle(&owner.addr, key, token),
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

fn mark_dead_if_seen(peer: &Peer) {
    let seen = *peer.seen.lock().unwrap_or_else(|e| e.into_inner());
    if seen {
        mark_dead(peer);
    }
}

fn rt() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new().map_err(|e| Error::Io(e.to_string()))
}

fn action(addr: &str, kind: &str, body: &[u8], token: Option<u64>) -> Result<Vec<u8>> {
    let rt = rt()?;
    rt.block_on(async {
        let mut client = connect(addr, token).await?;
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
            .ok_or_else(|| Error::Io("empty action result".into()))?;
        Ok(msg.to_vec())
    })
}

fn fetch_shuffle(owner: &str, key: &str, token: Option<u64>) -> Result<Vec<Batch>> {
    let rt = rt()?;
    rt.block_on(async {
        let mut client = connect(owner, token).await?;
        let stream = client
            .do_get(Ticket {
                ticket: key.as_bytes().to_vec().into(),
            })
            .await
            .map_err(flight_error)?;
        let rbs: Vec<RecordBatch> = stream.try_collect().await.map_err(flight_error)?;
        rbs.into_iter().map(Batch::from_arrow).collect()
    })
}

/// A worker's fence refusal stays typed; everything else is transport.
fn flight_error(error: arrow_flight::error::FlightError) -> Error {
    match &error {
        arrow_flight::error::FlightError::Tonic(status)
            if status.code() == tonic::Code::FailedPrecondition
                && status.message().starts_with("fenced: ") =>
        {
            Error::Fenced(status.message().trim_start_matches("fenced: ").to_string())
        }
        _ => Error::Io(error.to_string()),
    }
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
