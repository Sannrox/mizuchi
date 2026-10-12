//! Versioned query history and metrics API for external UIs.
//!
//! Off until the embedder sets [`QueryApiConfig`]. See ADR 0012.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

use super::CancelToken;
use super::metrics::OperatorMetrics;
use super::query_ui::{self, UiResponse};
use super::report::{InputScan, OutputWrite, QueryReport};
use crate::error::{Error, Result};

/// Contract id listed in `/healthz` and `$id` of the committed schema.
pub const QUERY_API_CONTRACT: &str = "mizuchi.query-api/v1";

/// Published JSON Schema (`crates/mizuchi/schemas/query-api-v1.json`).
pub const QUERY_API_SCHEMA: &str = include_str!("../../schemas/query-api-v1.json");

/// Embedder-supplied listen, auth, persistence, and retention.
#[derive(Clone)]
pub struct QueryApiConfig {
    /// Address `QueryApiServer` binds. Loopback does not need
    /// [`super::SessionConfig::allow_network`]; other addresses do.
    pub bind: SocketAddr,
    /// When set, data endpoints require `Authorization: Bearer <token>`.
    pub auth_token: Option<String>,
    /// JSONL file for completed queries. `None` keeps history in memory.
    pub history_path: Option<PathBuf>,
    /// How many completed queries to keep.
    pub retention: QueryRetention,
    /// Unpacked console directory served at `/ui/`. `None` leaves those
    /// routes absent (ADR 0014).
    pub ui_dir: Option<PathBuf>,
}

impl std::fmt::Debug for QueryApiConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryApiConfig")
            .field("bind", &self.bind)
            .field("auth_token", &self.auth_token.as_ref().map(|_| "_"))
            .field("history_path", &self.history_path)
            .field("retention", &self.retention)
            .field("ui_dir", &self.ui_dir)
            .finish()
    }
}

/// Caps on persisted and in-memory completed-query history.
#[derive(Clone, Debug)]
pub struct QueryRetention {
    /// Maximum completed records retained (oldest dropped first).
    pub max_entries: usize,
    /// Drop records whose `started_at_us` is older than now minus this.
    pub max_age_us: Option<u64>,
}

impl Default for QueryRetention {
    fn default() -> Self {
        Self {
            max_entries: 1024,
            max_age_us: None,
        }
    }
}

/// Completed or failed query as stored in history.
#[derive(Clone, Debug)]
pub struct QueryRecord {
    /// Stable id (`q-<n>`).
    pub id: String,
    /// SQL text or plan text for logical-plan executes.
    pub sql: String,
    /// Terminal or running status.
    pub status: QueryStatus,
    /// Unix microseconds when the query was registered.
    pub started_at_us: u64,
    /// Unix microseconds when it left the running set.
    pub ended_at_us: Option<u64>,
    /// Wall time spent in the engine.
    pub duration_ns: u64,
    /// Output rows.
    pub rows: u64,
    /// Memory-pool high water for an instrumented run.
    pub peak_bytes: u64,
    /// Spill file bytes written during an instrumented run.
    pub spilled_bytes: u64,
    /// Failure message when status is failed or cancelled.
    pub error: Option<String>,
    /// Logical and physical plan text.
    pub plan: Option<String>,
    /// Read/write report JSON.
    pub report: Option<Value>,
    /// Per-operator metrics JSON.
    pub metrics: Option<Value>,
}

/// Lifecycle of one recorded query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryStatus {
    /// Currently executing.
    Running,
    /// Finished and produced batches (or EXPLAIN text).
    Completed,
    /// Engine error other than cancel.
    Failed,
    /// Cooperative cancel.
    Cancelled,
}

impl QueryStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

/// One in-flight query as listed by the running endpoint.
#[derive(Clone, Debug)]
pub struct RunningQuery {
    /// Id assigned at start.
    pub id: String,
    /// SQL or plan text.
    pub sql: String,
    /// Unix microseconds when registered.
    pub started_at_us: u64,
    /// Time since registration.
    pub elapsed_ns: u64,
    /// Coarse stage label (`execute`).
    pub stage: String,
}

/// Thread-safe query history, running set, and cancel tokens.
pub struct QueryLog {
    inner: Mutex<Inner>,
    persist: Option<PathBuf>,
}

impl std::fmt::Debug for QueryLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryLog").finish_non_exhaustive()
    }
}

struct Inner {
    seq: u64,
    retention: QueryRetention,
    history: Vec<QueryRecord>,
    running: BTreeMap<String, RunningInner>,
}

struct RunningInner {
    sql: String,
    started_at_us: u64,
    started: Instant,
    cancel: CancelToken,
}

/// Guard that records a query into [`QueryLog`] when it finishes.
pub(super) struct QuerySpan {
    log: Arc<QueryLog>,
    id: String,
    sql: String,
    started_at_us: u64,
    start: Instant,
    query_cancel: CancelToken,
    finished: bool,
}

#[derive(Default)]
pub(super) struct RunStats {
    pub peak_bytes: u64,
    pub spilled_bytes: u64,
    pub metrics: Option<OperatorMetrics>,
}

impl QueryLog {
    /// In-memory history. Lost on process exit.
    pub fn memory(retention: QueryRetention) -> Self {
        Self {
            inner: Mutex::new(Inner {
                seq: 0,
                retention,
                history: Vec::new(),
                running: BTreeMap::new(),
            }),
            persist: None,
        }
    }

    /// Load completed queries from `path` (created if missing).
    pub fn open(path: impl AsRef<Path>, retention: QueryRetention) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| Error::Io(e.to_string()))?;
        }
        let mut history = Vec::new();
        let mut seq = 0;
        if path.exists() {
            let file = fs::File::open(&path).map_err(|e| Error::Io(e.to_string()))?;
            for line in BufReader::new(file).lines() {
                let line = line.map_err(|e| Error::Io(e.to_string()))?;
                if line.trim().is_empty() {
                    continue;
                }
                let rec = record_from_json(&line)?;
                if let Some(n) = rec.id.strip_prefix("q-")
                    && let Ok(v) = n.parse::<u64>()
                {
                    seq = seq.max(v);
                }
                history.push(rec);
            }
        }
        let log = Self {
            inner: Mutex::new(Inner {
                seq,
                retention,
                history,
                running: BTreeMap::new(),
            }),
            persist: Some(path),
        };
        {
            let mut inner = lock(&log.inner)?;
            prune(&mut inner);
            persist_all(log.persist.as_deref(), &inner.history)?;
        }
        Ok(log)
    }

    /// Completed records, newest last, after filter/search/pagination.
    pub fn list(
        &self,
        status: Option<QueryStatus>,
        search: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<QueryRecord>, usize)> {
        let inner = lock(&self.inner)?;
        let search = search.map(|s| s.to_ascii_lowercase());
        let filtered: Vec<QueryRecord> = inner
            .history
            .iter()
            .rev()
            .filter(|r| status.is_none_or(|s| r.status == s))
            .filter(|r| {
                search
                    .as_ref()
                    .is_none_or(|q| r.sql.to_ascii_lowercase().contains(q))
            })
            .cloned()
            .collect();
        let total = filtered.len();
        let end = offset.saturating_add(limit).min(total);
        let page = if offset >= total {
            Vec::new()
        } else {
            filtered[offset..end].to_vec()
        };
        Ok((page, total))
    }

    /// One completed or running record.
    pub fn get(&self, id: &str) -> Result<Option<QueryRecord>> {
        let inner = lock(&self.inner)?;
        if let Some(r) = inner.history.iter().find(|r| r.id == id) {
            return Ok(Some(r.clone()));
        }
        if let Some(run) = inner.running.get(id) {
            return Ok(Some(running_record(id, run)));
        }
        Ok(None)
    }

    /// Queries that have started and not yet finished.
    pub fn running(&self) -> Result<Vec<RunningQuery>> {
        let inner = lock(&self.inner)?;
        Ok(inner
            .running
            .iter()
            .map(|(id, run)| RunningQuery {
                id: id.clone(),
                sql: run.sql.clone(),
                started_at_us: run.started_at_us,
                elapsed_ns: run.started.elapsed().as_nanos() as u64,
                stage: "execute".into(),
            })
            .collect())
    }

    /// Cancel a running query. `Ok(false)` when it is not running.
    pub fn cancel(&self, id: &str) -> Result<bool> {
        let inner = lock(&self.inner)?;
        match inner.running.get(id) {
            Some(run) => {
                run.cancel.cancel();
                Ok(true)
            }
            None => Ok(false),
        }
    }

    pub(super) fn begin_on(log: &Arc<QueryLog>, sql: &str) -> QuerySpan {
        let query_cancel = CancelToken::new();
        let started_at_us = now_us();
        let start = Instant::now();
        let id = {
            let mut inner = lock(&log.inner).expect("query log lock");
            inner.seq += 1;
            let id = format!("q-{}", inner.seq);
            inner.running.insert(
                id.clone(),
                RunningInner {
                    sql: sql.to_string(),
                    started_at_us,
                    started: start,
                    cancel: query_cancel.clone(),
                },
            );
            id
        };
        QuerySpan {
            log: Arc::clone(log),
            id,
            sql: sql.to_string(),
            started_at_us,
            start,
            query_cancel,
            finished: false,
        }
    }
}

impl QuerySpan {
    pub(super) fn token(&self) -> CancelToken {
        self.query_cancel.clone()
    }

    pub(super) fn finish_ok(
        mut self,
        rows: u64,
        report: &QueryReport,
        stats: &RunStats,
    ) -> Result<()> {
        let metrics = stats
            .metrics
            .as_ref()
            .and_then(|m| serde_json::from_str(&m.to_json()).ok());
        let rec = QueryRecord {
            id: self.id.clone(),
            sql: self.sql.clone(),
            status: QueryStatus::Completed,
            started_at_us: self.started_at_us,
            ended_at_us: Some(now_us()),
            duration_ns: self.start.elapsed().as_nanos() as u64,
            rows,
            peak_bytes: stats.peak_bytes,
            spilled_bytes: stats.spilled_bytes,
            error: None,
            plan: Some(report.plan.clone()),
            report: Some(report_to_json(report)),
            metrics,
        };
        self.finished = true;
        self.log.complete(rec)
    }

    pub(super) fn finish_err(mut self, err: &Error) {
        let status = if matches!(err, Error::Cancelled) {
            QueryStatus::Cancelled
        } else {
            QueryStatus::Failed
        };
        let rec = QueryRecord {
            id: self.id.clone(),
            sql: self.sql.clone(),
            status,
            started_at_us: self.started_at_us,
            ended_at_us: Some(now_us()),
            duration_ns: self.start.elapsed().as_nanos() as u64,
            rows: 0,
            peak_bytes: 0,
            spilled_bytes: 0,
            error: Some(err.to_string()),
            plan: None,
            report: None,
            metrics: None,
        };
        let _ = self.log.complete(rec);
        self.finished = true;
    }
}

impl Drop for QuerySpan {
    fn drop(&mut self) {
        if !self.finished {
            let rec = QueryRecord {
                id: self.id.clone(),
                sql: self.sql.clone(),
                status: QueryStatus::Failed,
                started_at_us: self.started_at_us,
                ended_at_us: Some(now_us()),
                duration_ns: self.start.elapsed().as_nanos() as u64,
                rows: 0,
                peak_bytes: 0,
                spilled_bytes: 0,
                error: Some("aborted".into()),
                plan: None,
                report: None,
                metrics: None,
            };
            let _ = self.log.complete(rec);
        }
    }
}

impl QueryLog {
    fn complete(&self, rec: QueryRecord) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        inner.running.remove(&rec.id);
        inner.history.push(rec);
        prune(&mut inner);
        persist_all(self.persist.as_deref(), &inner.history)
    }
}

/// HTTP front over a [`QueryLog`]. Dropping the server unbinds.
pub struct QueryApiServer {
    addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    inflight: Arc<AtomicUsize>,
    join: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for QueryApiServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryApiServer")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl QueryApiServer {
    /// Bind `cfg.bind` and serve `/healthz`, `/v1/*`, and `/ui/` from `log`.
    ///
    /// Non-loopback addresses require `allow_network` (ADR 0008). Prefer
    /// [`super::Session::serve_query_api`], which passes the session grant.
    pub fn bind(log: Arc<QueryLog>, cfg: &QueryApiConfig) -> Result<Self> {
        Self::bind_with_network(log, cfg, false)
    }

    pub(crate) fn bind_with_network(
        log: Arc<QueryLog>,
        cfg: &QueryApiConfig,
        allow_network: bool,
    ) -> Result<Self> {
        if cfg.auth_token.as_ref().is_some_and(|t| t.is_empty()) {
            return Err(Error::Plan("query API auth_token must be non-empty".into()));
        }
        if !cfg.bind.ip().is_loopback() && !allow_network {
            return Err(Error::Io(
                "query API non-loopback bind requires allow_network".into(),
            ));
        }
        let listener = TcpListener::bind(cfg.bind).map_err(|e| Error::Io(e.to_string()))?;
        listener
            .set_nonblocking(true)
            .map_err(|e| Error::Io(e.to_string()))?;
        let addr = listener
            .local_addr()
            .map_err(|e| Error::Io(e.to_string()))?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let inflight = Arc::new(AtomicUsize::new(0));
        let auth = cfg.auth_token.clone();
        let ui_dir = cfg.ui_dir.clone();
        let stop = shutdown.clone();
        let live = inflight.clone();
        let join = thread::spawn(move || serve_loop(listener, log, auth, ui_dir, stop, live));
        Ok(Self {
            addr,
            shutdown,
            inflight,
            join: Some(join),
        })
    }

    /// Bound address, including the ephemeral port when bind was `:0`.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for QueryApiServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_secs(2);
        while self.inflight.load(Ordering::SeqCst) > 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn serve_loop(
    listener: TcpListener,
    log: Arc<QueryLog>,
    auth: Option<String>,
    ui_dir: Option<PathBuf>,
    shutdown: Arc<AtomicBool>,
    inflight: Arc<AtomicUsize>,
) {
    while !shutdown.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((mut stream, _)) => {
                if shutdown.load(Ordering::SeqCst) {
                    break;
                }
                if inflight.load(Ordering::SeqCst) >= 16 {
                    let _ = write_json(&mut stream, 503, &json!({"error": "busy"}));
                    continue;
                }
                inflight.fetch_add(1, Ordering::SeqCst);
                let log = Arc::clone(&log);
                let auth = auth.clone();
                let ui_dir = ui_dir.clone();
                let inflight = Arc::clone(&inflight);
                thread::spawn(move || {
                    let _ = handle_stream(&log, auth.as_deref(), ui_dir.as_deref(), &mut stream);
                    inflight.fetch_sub(1, Ordering::SeqCst);
                });
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::Interrupted => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(_) => {
                if shutdown.load(Ordering::SeqCst) {
                    break;
                }
                thread::sleep(Duration::from_millis(20));
            }
        }
    }
}

fn handle_stream(
    log: &QueryLog,
    auth: Option<&str>,
    ui_dir: Option<&Path>,
    stream: &mut TcpStream,
) -> Result<()> {
    let per_io = Duration::from_millis(200);
    stream
        .set_read_timeout(Some(per_io))
        .map_err(|e| Error::Io(e.to_string()))?;
    stream
        .set_write_timeout(Some(per_io))
        .map_err(|e| Error::Io(e.to_string()))?;
    let deadline = Instant::now() + Duration::from_secs(2);
    let (head, _body) = read_http_head(stream, deadline)?;
    let start = head.lines().next().unwrap_or("");
    let mut parts = start.split_whitespace();
    let method = parts.next().unwrap_or("");
    let raw_path = parts.next().unwrap_or("");
    let (path, _) = split_path(raw_path);
    if method == "GET"
        && let Some(root) = ui_dir
        && query_ui::is_ui_route(&path)
    {
        if path == "/" || path == "/ui" {
            return write_redirect(stream, "ui/");
        }
        let rel = path.strip_prefix("/ui/").unwrap_or("");
        return write_ui(stream, query_ui::serve(root, rel));
    }
    let (status, payload) = if let Some(token) = auth
        && !is_public(raw_path, ui_dir.is_some())
        && !bearer_ok(&head, token)
    {
        (401, json!({"error": "unauthorized"}))
    } else {
        dispatch(log, method, raw_path)
    };
    write_json(stream, status, &payload)
}

fn is_public(path: &str, ui: bool) -> bool {
    let p = path.split('?').next().unwrap_or(path);
    p == "/healthz" || p == "/v1/schema" || (ui && query_ui::is_ui_route(p))
}

fn bearer_ok(head: &str, token: &str) -> bool {
    let Some(value) = header_value(head, "authorization") else {
        return false;
    };
    let value = value.trim();
    let prefix = "Bearer ";
    value.len() >= prefix.len()
        && value.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
        && &value[prefix.len()..] == token
}

fn dispatch(log: &QueryLog, method: &str, raw_path: &str) -> (u16, Value) {
    let (path, query) = split_path(raw_path);
    match (method, path.as_str()) {
        ("GET", "/healthz") => (
            200,
            json!({
                "ok": true,
                "contract": QUERY_API_CONTRACT,
                "version": crate::VERSION,
            }),
        ),
        ("GET", "/v1/schema") => match serde_json::from_str::<Value>(QUERY_API_SCHEMA) {
            Ok(v) => (200, v),
            Err(e) => (500, json!({"error": e.to_string()})),
        },
        ("GET", "/v1/queries/running") => match log.running() {
            Ok(list) => (
                200,
                json!({
                    "queries": list.iter().map(running_json).collect::<Vec<_>>()
                }),
            ),
            Err(e) => (500, json!({"error": e.to_string()})),
        },
        ("GET", "/v1/queries") => list_queries(log, &query),
        ("GET", path) if let Some(id) = path.strip_prefix("/v1/queries/") => {
            if id.contains('/') {
                return (404, json!({"error": "not found"}));
            }
            match log.get(id) {
                Ok(Some(rec)) => (200, record_json(&rec, true)),
                Ok(None) => (404, json!({"error": "not found"})),
                Err(e) => (500, json!({"error": e.to_string()})),
            }
        }
        ("POST", path) if let Some(rest) = path.strip_prefix("/v1/queries/") => {
            let Some(id) = rest.strip_suffix("/cancel") else {
                return (404, json!({"error": "not found"}));
            };
            match log.cancel(id) {
                Ok(true) => (200, json!({"id": id, "cancelled": true})),
                Ok(false) => {
                    if log.get(id).ok().flatten().is_some() {
                        (409, json!({"error": "not running"}))
                    } else {
                        (404, json!({"error": "not found"}))
                    }
                }
                Err(e) => (500, json!({"error": e.to_string()})),
            }
        }
        _ => (404, json!({"error": "not found"})),
    }
}

fn list_queries(log: &QueryLog, query: &BTreeMap<String, String>) -> (u16, Value) {
    let status = match query.get("status").map(|s| s.as_str()) {
        Some(s) => match QueryStatus::parse(s) {
            Some(st) => Some(st),
            None => return (400, json!({"error": "invalid status"})),
        },
        None => None,
    };
    let search = query.get("q").map(|s| s.as_str());
    let offset = query
        .get("offset")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let limit = query
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(50)
        .min(500);
    match log.list(status, search, offset, limit) {
        Ok((page, total)) => (
            200,
            json!({
                "queries": page.iter().map(|r| record_json(r, false)).collect::<Vec<_>>(),
                "total": total,
                "offset": offset,
                "limit": limit,
            }),
        ),
        Err(e) => (500, json!({"error": e.to_string()})),
    }
}

fn record_json(rec: &QueryRecord, detail: bool) -> Value {
    let mut v = json!({
        "id": rec.id,
        "sql": rec.sql,
        "status": rec.status.as_str(),
        "started_at_us": rec.started_at_us,
        "ended_at_us": rec.ended_at_us,
        "duration_ns": rec.duration_ns,
        "rows": rec.rows,
        "peak_bytes": rec.peak_bytes,
        "spilled_bytes": rec.spilled_bytes,
        "error": rec.error,
    });
    if detail {
        v["plan"] = rec.plan.clone().map(Value::String).unwrap_or(Value::Null);
        v["report"] = rec.report.clone().unwrap_or(Value::Null);
        v["metrics"] = rec.metrics.clone().unwrap_or(Value::Null);
    }
    v
}

fn running_json(q: &RunningQuery) -> Value {
    json!({
        "id": q.id,
        "sql": q.sql,
        "status": "running",
        "started_at_us": q.started_at_us,
        "elapsed_ns": q.elapsed_ns,
        "stage": q.stage,
    })
}

fn running_record(id: &str, run: &RunningInner) -> QueryRecord {
    QueryRecord {
        id: id.to_string(),
        sql: run.sql.clone(),
        status: QueryStatus::Running,
        started_at_us: run.started_at_us,
        ended_at_us: None,
        duration_ns: run.started.elapsed().as_nanos() as u64,
        rows: 0,
        peak_bytes: 0,
        spilled_bytes: 0,
        error: None,
        plan: None,
        report: None,
        metrics: None,
    }
}

fn report_to_json(r: &QueryReport) -> Value {
    json!({
        "engine_version": r.engine_version,
        "plan": r.plan,
        "inputs": r.inputs.iter().map(input_json).collect::<Vec<_>>(),
        "outputs": r.outputs.iter().map(output_json).collect::<Vec<_>>(),
    })
}

fn input_json(i: &InputScan) -> Value {
    json!({
        "dataset": i.dataset,
        "path": i.path,
        "partitions": i.partitions,
        "columns": i.columns,
        "mtime_ms": i.mtime_ms,
        "etag": i.etag,
    })
}

fn output_json(o: &OutputWrite) -> Value {
    json!({
        "path": o.path,
        "partitions": o.partitions,
        "mode": o.mode,
        "rows": o.rows,
        "bytes": o.bytes,
        "commit_id": o.commit_id,
    })
}

fn record_from_json(line: &str) -> Result<QueryRecord> {
    let v: Value = serde_json::from_str(line).map_err(|e| Error::Io(e.to_string()))?;
    let status = v
        .get("status")
        .and_then(Value::as_str)
        .and_then(QueryStatus::parse)
        .ok_or_else(|| Error::Io("history line missing status".into()))?;
    Ok(QueryRecord {
        id: v
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        sql: v
            .get("sql")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        status,
        started_at_us: v.get("started_at_us").and_then(Value::as_u64).unwrap_or(0),
        ended_at_us: v.get("ended_at_us").and_then(Value::as_u64),
        duration_ns: v.get("duration_ns").and_then(Value::as_u64).unwrap_or(0),
        rows: v.get("rows").and_then(Value::as_u64).unwrap_or(0),
        peak_bytes: v.get("peak_bytes").and_then(Value::as_u64).unwrap_or(0),
        spilled_bytes: v.get("spilled_bytes").and_then(Value::as_u64).unwrap_or(0),
        error: v.get("error").and_then(Value::as_str).map(str::to_string),
        plan: v.get("plan").and_then(Value::as_str).map(str::to_string),
        report: v.get("report").cloned().filter(|x| !x.is_null()),
        metrics: v.get("metrics").cloned().filter(|x| !x.is_null()),
    })
}

fn persist_all(path: Option<&Path>, history: &[QueryRecord]) -> Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "history.jsonl".into());
    let tmp = path.with_file_name(format!("{name}.{}.{}.tmp", std::process::id(), now_us()));
    let mut file = persist_create_tmp(&tmp, path).map_err(|e| Error::Io(e.to_string()))?;
    let write = (|| {
        for rec in history {
            let line = serde_json::to_string(&record_json(rec, true))
                .map_err(|e| Error::Io(e.to_string()))?;
            writeln!(file, "{line}").map_err(|e| Error::Io(e.to_string()))?;
        }
        file.flush().map_err(|e| Error::Io(e.to_string()))
    })();
    if let Err(e) = write {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    fs::rename(&tmp, path).map_err(|e| {
        let _ = fs::remove_file(&tmp);
        Error::Io(e.to_string())
    })?;
    Ok(())
}

fn persist_create_tmp(tmp: &Path, dest: &Path) -> std::io::Result<std::fs::File> {
    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let mode = fs::metadata(dest)
            .map(|m| m.permissions().mode() & 0o777)
            .unwrap_or(0o600);
        opts.mode(mode);
        let file = opts.open(tmp)?;
        let mut perms = file.metadata()?.permissions();
        perms.set_mode(mode);
        fs::set_permissions(tmp, perms)?;
        Ok(file)
    }
    #[cfg(not(unix))]
    {
        let _ = dest;
        opts.open(tmp)
    }
}

fn prune(inner: &mut Inner) {
    let now = now_us();
    if let Some(max_age) = inner.retention.max_age_us {
        inner
            .history
            .retain(|r| now.saturating_sub(r.started_at_us) <= max_age);
    }
    let max = inner.retention.max_entries.max(1);
    if inner.history.len() > max {
        let drop = inner.history.len() - max;
        inner.history.drain(0..drop);
    }
}

fn lock<T>(m: &Mutex<T>) -> Result<std::sync::MutexGuard<'_, T>> {
    m.lock()
        .map_err(|_| Error::Eval("query log lock poisoned".into()))
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

fn split_path(raw: &str) -> (String, BTreeMap<String, String>) {
    match raw.split_once('?') {
        Some((path, qs)) => {
            let mut map = BTreeMap::new();
            for pair in qs.split('&') {
                if pair.is_empty() {
                    continue;
                }
                let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
                map.insert(url_decode(k), url_decode(v));
            }
            (path.to_string(), map)
        }
        None => (raw.to_string(), BTreeMap::new()),
    }
}

fn url_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < b.len() => {
                let hi = b[i + 1];
                let lo = b[i + 2];
                match (from_hex(hi), from_hex(lo)) {
                    (Some(h), Some(l)) => {
                        out.push((h << 4) | l);
                        i += 3;
                    }
                    _ => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

fn from_hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn read_http_head(stream: &mut TcpStream, deadline: Instant) -> Result<(String, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        if Instant::now() >= deadline {
            return Err(Error::Io("HTTP request timed out".into()));
        }
        let n = match stream.read(&mut tmp) {
            Ok(n) => n,
            Err(e)
                if e.kind() == ErrorKind::TimedOut
                    || e.kind() == ErrorKind::WouldBlock
                    || e.kind() == ErrorKind::Interrupted =>
            {
                continue;
            }
            Err(e) => return Err(Error::Io(e.to_string())),
        };
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..pos]).into_owned();
            let body = buf[pos + 4..].to_vec();
            return Ok((head, body));
        }
        if buf.len() > 64 * 1024 {
            return Err(Error::Io("HTTP headers too large".into()));
        }
    }
    Err(Error::Io("invalid HTTP request".into()))
}

fn header_value<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    for line in head.lines().skip(1) {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        if k.trim().eq_ignore_ascii_case(name) {
            return Some(v.trim());
        }
    }
    None
}

fn write_json(stream: &mut TcpStream, status: u16, payload: &Value) -> Result<()> {
    let body = serde_json::to_vec(payload).map_err(|e| Error::Io(e.to_string()))?;
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        409 => "Conflict",
        _ => "Error",
    };
    let extra = if status == 401 {
        "WWW-Authenticate: Bearer\r\n"
    } else {
        ""
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{extra}\r\n",
        body.len()
    );
    stream
        .write_all(head.as_bytes())
        .map_err(|e| Error::Io(e.to_string()))?;
    stream
        .write_all(&body)
        .map_err(|e| Error::Io(e.to_string()))?;
    Ok(())
}

fn write_redirect(stream: &mut TcpStream, location: &str) -> Result<()> {
    let head = format!(
        "HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(head.as_bytes())
        .map_err(|e| Error::Io(e.to_string()))
}

fn write_ui(stream: &mut TcpStream, resp: UiResponse) -> Result<()> {
    let reason = match resp.status {
        200 => "OK",
        404 => "Not Found",
        _ => "Error",
    };
    let extra = match resp.location {
        Some(loc) => format!("Location: {loc}\r\n"),
        None => String::new(),
    };
    let head = format!(
        "HTTP/1.1 {} {reason}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: {}\r\nContent-Security-Policy: {}\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: no-referrer\r\nConnection: close\r\n{extra}\r\n",
        resp.status,
        resp.content_type,
        resp.body.len(),
        resp.cache_control,
        query_ui::CSP,
    );
    stream
        .write_all(head.as_bytes())
        .map_err(|e| Error::Io(e.to_string()))?;
    stream
        .write_all(&resp.body)
        .map_err(|e| Error::Io(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::{Session, SessionConfig};
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc as StdArc;

    fn sample_session() -> Session {
        let mut s = Session::new();
        let schema = StdArc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![StdArc::new(Int64Array::from(vec![Some(1), Some(2)]))],
        )
        .unwrap();
        s.register(
            "t",
            schema,
            vec![crate::batch::Batch::from_arrow(batch).unwrap()],
        )
        .unwrap();
        s
    }

    fn http_json(addr: SocketAddr, method: &str, path: &str, token: Option<&str>) -> (u16, Value) {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut req =
            format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n");
        if let Some(t) = token {
            req.push_str(&format!("Authorization: Bearer {t}\r\n"));
        }
        req.push_str("\r\n");
        stream.write_all(req.as_bytes()).unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).unwrap();
        let text = String::from_utf8_lossy(&buf);
        let status = text
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let body = text.split("\r\n\r\n").nth(1).unwrap_or("{}");
        let val = serde_json::from_str(body).unwrap_or(json!({}));
        (status, val)
    }

    #[test]
    fn url_decode_handles_utf8_and_malformed_percent() {
        assert_eq!(url_decode("a%2Bb"), "a+b");
        assert_eq!(url_decode("caf%C3%A9"), "café");
        let _ = url_decode("%€");
        let _ = url_decode("%");
        let _ = url_decode("%2");
    }

    #[test]
    fn bearer_matches_scheme_case_insensitively_and_token_exactly() {
        assert!(bearer_ok(
            "GET /\r\nAuthorization: Bearer secret\r\n",
            "secret"
        ));
        assert!(bearer_ok(
            "GET /\r\nAuthorization: bearer secret\r\n",
            "secret"
        ));
        assert!(!bearer_ok(
            "GET /\r\nAuthorization: Bearer SECRET\r\n",
            "secret"
        ));
        assert!(!bearer_ok(
            "GET /\r\nAuthorization: Bearer secret\r\n",
            "SECRET"
        ));
    }

    #[test]
    fn schema_is_committed_and_names_contract() {
        let schema: Value = serde_json::from_str(QUERY_API_SCHEMA).unwrap();
        assert_eq!(
            schema["$id"].as_str().unwrap(),
            "https://github.com/Sannrox/mizuchi/schemas/query-api-v1.json"
        );
        assert_eq!(
            schema["$defs"]["Health"]["properties"]["contract"]["const"]
                .as_str()
                .unwrap(),
            QUERY_API_CONTRACT
        );
        for key in ["ok", "contract", "version"] {
            assert!(
                schema["$defs"]["Health"]["required"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|v| v.as_str() == Some(key)),
                "{key}"
            );
        }
    }

    #[test]
    fn default_session_has_no_query_log() {
        let s = Session::new();
        assert!(s.query_log().is_none());
        let err = s.serve_query_api().unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
        let s = Session::from_config(SessionConfig::default()).unwrap();
        assert!(s.query_log().is_none());
    }

    #[test]
    fn history_records_sql_and_metrics() {
        let log = StdArc::new(QueryLog::memory(QueryRetention::default()));
        let s = sample_session().with_query_log(log.clone());
        let out = s.execute_sql("SELECT a FROM t").unwrap();
        assert_eq!(out[0].num_rows(), 2);
        let (page, total) = log.list(None, None, 0, 10).unwrap();
        assert_eq!(total, 1);
        assert_eq!(page[0].sql, "SELECT a FROM t");
        assert_eq!(page[0].status, QueryStatus::Completed);
        assert_eq!(page[0].rows, 2);
        assert!(page[0].plan.as_ref().is_some_and(|p| p.contains("Scan")));
        assert!(page[0].metrics.is_some());
        assert!(page[0].report.is_some());
    }

    #[cfg(unix)]
    #[test]
    fn history_file_is_owner_readable_only() {
        use std::os::unix::fs::PermissionsExt;
        let root =
            std::env::temp_dir().join(format!("mizuchi-qpriv-{}-{}", std::process::id(), now_us()));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("history.jsonl");
        let log = StdArc::new(QueryLog::open(&path, QueryRetention::default()).unwrap());
        let s = sample_session().with_query_log(log);
        s.execute_sql("SELECT a FROM t").unwrap();
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "{mode:o}");
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn history_survives_reopen() {
        let root =
            std::env::temp_dir().join(format!("mizuchi-qhist-{}-{}", std::process::id(), now_us()));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("history.jsonl");
        let log = StdArc::new(QueryLog::open(&path, QueryRetention::default()).unwrap());
        let s = sample_session().with_query_log(log.clone());
        s.execute_sql("SELECT a FROM t WHERE a > 0").unwrap();
        let id = log.list(None, None, 0, 1).unwrap().0[0].id.clone();
        drop(s);
        drop(log);
        let again = QueryLog::open(&path, QueryRetention::default()).unwrap();
        let rec = again.get(&id).unwrap().expect("restored");
        assert_eq!(rec.sql, "SELECT a FROM t WHERE a > 0");
        assert_eq!(rec.status, QueryStatus::Completed);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn filter_search_and_pagination() {
        let log = StdArc::new(QueryLog::memory(QueryRetention {
            max_entries: 10,
            max_age_us: None,
        }));
        let s = sample_session().with_query_log(log.clone());
        s.execute_sql("SELECT a FROM t").unwrap();
        s.execute_sql("SELECT a + 1 AS b FROM t").unwrap();
        let err = s.execute_sql("SELECT missing FROM t").unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
        let (failed, n) = log.list(Some(QueryStatus::Failed), None, 0, 10).unwrap();
        assert_eq!(n, 1);
        assert!(failed[0].sql.contains("missing"));
        let (found, n) = log.list(None, Some("a + 1"), 0, 10).unwrap();
        assert_eq!(n, 1);
        assert!(found[0].sql.contains("a + 1"));
        let (page, total) = log.list(None, None, 1, 1).unwrap();
        assert_eq!(total, 3);
        assert_eq!(page.len(), 1);
    }

    #[test]
    fn http_history_metrics_running_cancel_and_auth() {
        let log = StdArc::new(QueryLog::memory(QueryRetention::default()));
        let cfg = QueryApiConfig {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            auth_token: Some("secret".into()),
            history_path: None,
            retention: QueryRetention::default(),
            ui_dir: None,
        };
        let server = QueryApiServer::bind(log.clone(), &cfg).unwrap();
        let addr = server.addr();

        let (st, health) = http_json(addr, "GET", "/healthz", None);
        assert_eq!(st, 200);
        assert_eq!(health["ok"], true);
        assert_eq!(health["contract"], QUERY_API_CONTRACT);
        assert_eq!(health["version"], crate::VERSION);

        let (st, _) = http_json(addr, "GET", "/v1/queries", None);
        assert_eq!(st, 401);

        let s = sample_session().with_query_log(log.clone());
        s.execute_sql("SELECT a FROM t").unwrap();
        let (st, list) = http_json(addr, "GET", "/v1/queries", Some("secret"));
        assert_eq!(st, 200);
        assert_eq!(list["total"], 1);
        let id = list["queries"][0]["id"].as_str().unwrap().to_string();
        let (st, detail) = http_json(addr, "GET", &format!("/v1/queries/{id}"), Some("secret"));
        assert_eq!(st, 200);
        assert!(detail["metrics"].is_object());
        assert!(detail["report"]["plan"].as_str().unwrap().contains("Scan"));
        assert_eq!(detail["rows"], 2);

        let (st, schema) = http_json(addr, "GET", "/v1/schema", None);
        assert_eq!(st, 200);
        assert_eq!(
            schema["$defs"]["Health"]["properties"]["contract"]["const"],
            QUERY_API_CONTRACT
        );

        let (st, running) = http_json(addr, "GET", "/v1/queries/running", Some("secret"));
        assert_eq!(st, 200);
        assert_eq!(running["queries"].as_array().unwrap().len(), 0);

        let (st, _) = http_json(
            addr,
            "POST",
            &format!("/v1/queries/{id}/cancel"),
            Some("secret"),
        );
        assert_eq!(st, 409);

        let token = CancelToken::new();
        let mut slow = Session::new();
        let n = 50_000i64;
        let schema = StdArc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![StdArc::new(Int64Array::from(
                (0..n).map(Some).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        slow.register(
            "t",
            schema,
            vec![crate::batch::Batch::from_arrow(batch).unwrap()],
        )
        .unwrap();
        let slow = slow.with_query_log(log.clone()).with_cancel(token);
        let handle = thread::spawn(move || {
            slow.execute_sql("SELECT a, COUNT(*) AS c, SUM(a) AS s FROM t GROUP BY a")
        });
        let id = {
            let mut got = None;
            for _ in 0..200 {
                let list = log.running().unwrap();
                if let Some(q) = list.first() {
                    got = Some(q.id.clone());
                    break;
                }
                thread::sleep(Duration::from_millis(1));
            }
            got.expect("running query")
        };
        let (st, body) = http_json(
            addr,
            "POST",
            &format!("/v1/queries/{id}/cancel"),
            Some("secret"),
        );
        assert_eq!(st, 200);
        assert_eq!(body["cancelled"], true);
        let result = handle.join().expect("join");
        match result {
            Err(Error::Cancelled) | Ok(_) => {}
            Err(other) => panic!("{other:?}"),
        }
        drop(server);
    }

    #[test]
    fn non_loopback_bind_requires_network_grant() {
        let cfg = SessionConfig {
            query_api: Some(QueryApiConfig {
                bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 9),
                auth_token: None,
                history_path: None,
                retention: QueryRetention::default(),
                ui_dir: None,
            }),
            allow_network: false,
            ..SessionConfig::default()
        };
        let err = Session::from_config(cfg).unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err:?}");
    }

    #[test]
    fn wildcard_bind_drop_does_not_hang() {
        let log = StdArc::new(QueryLog::memory(QueryRetention::default()));
        let cfg = QueryApiConfig {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            auth_token: None,
            history_path: None,
            retention: QueryRetention::default(),
            ui_dir: None,
        };
        let server = QueryApiServer::bind_with_network(log, &cfg, true).unwrap();
        let loopback = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), server.addr().port());
        let (st, health) = http_json(loopback, "GET", "/healthz", None);
        assert_eq!(st, 200);
        assert_eq!(health["ok"], true);
        drop(server);
    }

    #[test]
    fn empty_auth_token_is_rejected() {
        let log = StdArc::new(QueryLog::memory(QueryRetention::default()));
        let cfg = QueryApiConfig {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            auth_token: Some(String::new()),
            history_path: None,
            retention: QueryRetention::default(),
            ui_dir: None,
        };
        let err = QueryApiServer::bind(log, &cfg).unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
    }

    #[test]
    fn public_bind_rejects_non_loopback_without_grant() {
        let log = StdArc::new(QueryLog::memory(QueryRetention::default()));
        let cfg = QueryApiConfig {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 9),
            auth_token: None,
            history_path: None,
            retention: QueryRetention::default(),
            ui_dir: None,
        };
        let err = QueryApiServer::bind(log, &cfg).unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err:?}");
    }

    #[test]
    fn query_api_config_debug_redacts_token() {
        let cfg = QueryApiConfig {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            auth_token: Some("super-secret".into()),
            history_path: None,
            retention: QueryRetention::default(),
            ui_dir: None,
        };
        let text = format!("{cfg:?}");
        assert!(!text.contains("super-secret"), "{text}");
        assert!(text.contains("auth_token: Some(\"_\")") || text.contains("auth_token: Some(_)"));
    }

    fn http_raw(
        addr: SocketAddr,
        method: &str,
        path: &str,
        token: Option<&str>,
    ) -> (u16, String, String) {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut req =
            format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n");
        if let Some(t) = token {
            req.push_str(&format!("Authorization: Bearer {t}\r\n"));
        }
        req.push_str("\r\n");
        stream.write_all(req.as_bytes()).unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).unwrap();
        let text = String::from_utf8_lossy(&buf);
        let status = text
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let (headers, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
        (status, headers.to_string(), body.to_string())
    }

    #[test]
    fn http_serves_console_dir_without_token_and_keeps_api_auth() {
        let root = std::env::temp_dir().join(format!("mizuchi-ui-http-{}", std::process::id()));
        std::fs::create_dir_all(root.join("assets")).unwrap();
        std::fs::write(
            root.join("index.html"),
            "<!doctype html><html><head><title>mizuchi</title><script type=\"module\" src=\"./assets/app.js\"></script></head></html>",
        )
        .unwrap();
        std::fs::write(root.join("assets/app.js"), "export {}").unwrap();
        let log = StdArc::new(QueryLog::memory(QueryRetention::default()));
        let cfg = QueryApiConfig {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            auth_token: Some("secret".into()),
            history_path: None,
            retention: QueryRetention::default(),
            ui_dir: Some(root.clone()),
        };
        let server = QueryApiServer::bind(log, &cfg).unwrap();
        let addr = server.addr();

        let (st, headers, _) = http_raw(addr, "GET", "/", None);
        assert_eq!(st, 302);
        assert!(
            headers.to_ascii_lowercase().contains("location: ui/"),
            "{headers}"
        );

        let (st, headers, body) = http_raw(addr, "GET", "/ui/", None);
        assert_eq!(st, 200);
        assert!(body.contains("mizuchi"));
        assert!(
            headers
                .to_ascii_lowercase()
                .contains("content-security-policy:"),
            "{headers}"
        );

        let (st, _, body) = http_raw(addr, "GET", "/ui/running", None);
        assert_eq!(st, 200);
        assert!(body.contains("mizuchi"));

        let (st, _, body) = http_raw(addr, "GET", "/ui/queries/q-1", None);
        assert_eq!(st, 200);
        assert!(body.contains("<base href=\"../\">"), "{body}");

        let (st, _, body) = http_raw(addr, "GET", "/ui/assets/app.js", None);
        assert_eq!(st, 200);
        assert_eq!(body, "export {}");

        let (st, _, _) = http_raw(addr, "GET", "/ui/../Cargo.toml", None);
        assert_eq!(st, 404);
        let (st, _, _) = http_raw(addr, "GET", "/ui/%2e%2e/Cargo.toml", None);
        assert_eq!(st, 404);

        let (st, _) = http_json(addr, "GET", "/v1/queries", None);
        assert_eq!(st, 401);
        let (st, _) = http_json(addr, "GET", "/v1/queries", Some("secret"));
        assert_eq!(st, 200);

        drop(server);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn staged_history_records_metrics() {
        let log = StdArc::new(QueryLog::memory(QueryRetention::default()));
        let s = sample_session().with_query_log(log.clone());
        s.execute_staged("SELECT a FROM t").unwrap();
        let (page, total) = log.list(None, None, 0, 10).unwrap();
        assert_eq!(total, 1);
        assert_eq!(page[0].sql, "SELECT a FROM t");
        assert_eq!(page[0].status, QueryStatus::Completed);
        assert!(page[0].metrics.is_some(), "{:?}", page[0].metrics);
        assert!(page[0].plan.as_ref().is_some_and(|p| p.contains("Scan")));
    }
}
