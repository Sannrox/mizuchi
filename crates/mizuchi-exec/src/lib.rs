//! Pull-based vectorized operators over Arrow record batches.
//!
//! See `docs/decisions/0004-pull-vectorized-execution.md`.

mod agg;
mod cache;
mod cluster;
mod config;
mod engine;
mod incremental;
mod join;
mod key;
pub mod memory;
mod merge;
mod metrics;
mod op;
mod query_api;
mod query_ui;
mod recursive;
mod report;
mod runtime_filter;
mod schedule;
mod setop;
mod sort;
mod stage;
mod stream;
mod window;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, BooleanArray, RecordBatch, RecordBatchOptions, RecordBatchReader, StringArray,
    new_null_array,
};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::boolean::{and, is_not_null};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use mizuchi_core::batch::{Batch, BatchPolicy};
use mizuchi_core::error::{Error, Result};
use mizuchi_core::expr::Expr;
use mizuchi_core::udaf::CombineAgg;
use mizuchi_plan::{
    AlterTable, Catalog, CatalogProvider, Ddl, LogicalPlan, NamedExpr, TableIdent, apply_alter,
    bind_ddl, normalize_name,
};

pub use cluster::{Cluster, ClusterConfig, WorkerResources};
pub use config::SessionConfig;
pub use engine::Engine;
pub use incremental::MicroBatchAgg;
pub use merge::MergeStats;
pub use metrics::OperatorMetrics;
pub use mizuchi_core::CancelToken;
pub use op::{CpuLowering, Lowering, Operator};
pub use query_api::{
    QUERY_API_CONTRACT, QUERY_API_SCHEMA, QueryApiConfig, QueryApiServer, QueryLog, QueryRecord,
    QueryRetention, QueryStatus, RunningQuery,
};
pub use report::{InputScan, OutputWrite, QueryReport, QueryResult};
pub use stream::{EventWindow, StreamOutput, StreamQuery, StreamQueryConfig, StreamTrigger};

thread_local! {
    static ACTIVE_CANCEL: RefCell<Option<CancelToken>> = const { RefCell::new(None) };
}

/// In-memory tables plus a batch-size policy.
#[derive(Debug)]
pub struct Session {
    tables: BTreeMap<String, MemTable>,
    catalog: Catalog,
    policy: BatchPolicy,
    /// Parallel morsel workers. Default 1 keeps the pull iterator path.
    /// Scan, combinable aggregate, inner join probe, and wrapping
    /// sort/limit run on this many threads.
    workers: usize,
    /// Caller-provided directory for Arrow IPC shuffle files.
    shuffle_dir: Option<PathBuf>,
    memory_limit: Option<u64>,
    spill_dir: Option<PathBuf>,
    cancel: CancelToken,
    /// Shared owner budget when this session was opened from [`Engine`].
    budget: Option<std::sync::Arc<memory::Budget>>,
    /// A shuffle partition is skewed when it exceeds this many bytes and
    /// also exceeds five times the median partition size.
    skew_threshold: u64,
    /// Target size when splitting a skewed partition.
    advisory_partition_size: u64,
    /// Results of executed plans, keyed by plan text and table generations.
    cache: Mutex<cache::QueryCache>,
    /// Selects a physical implementation per operator. CPU by default.
    lowering: op::LoweringArc,
    allowed_read_paths: Vec<PathBuf>,
    allowed_write_paths: Vec<PathBuf>,
    /// When true, [`Session::register_postgres`] may open TCP.
    allow_network: bool,
    /// When true, stage tasks may run a speculative copy; first commit wins.
    speculate: bool,
    /// After an inner hash-join build, push integer join keys into the
    /// probe-side scan. Off is still correct; scans may ignore the filter.
    runtime_join_filters: bool,
    /// Combinable user-defined aggregates, keyed by lower-case SQL name.
    udafs: BTreeMap<String, Arc<dyn CombineAgg>>,
    /// Query-start clock in microseconds since the Unix epoch. `None` uses
    /// the process clock when binding `CURRENT_DATE` / `NOW()`.
    now_micros: Option<i64>,
    query_log: Option<Arc<query_api::QueryLog>>,
    query_api: Option<query_api::QueryApiConfig>,
}

impl Default for Session {
    fn default() -> Self {
        Self {
            tables: BTreeMap::new(),
            catalog: Catalog::new(),
            policy: BatchPolicy::default(),
            workers: 1,
            shuffle_dir: None,
            memory_limit: None,
            spill_dir: None,
            cancel: CancelToken::new(),
            budget: None,
            skew_threshold: 256 * 1024 * 1024,
            advisory_partition_size: 64 * 1024 * 1024,
            cache: Mutex::new(cache::QueryCache::default()),
            lowering: op::LoweringArc::default(),
            allowed_read_paths: Vec::new(),
            allowed_write_paths: Vec::new(),
            allow_network: false,
            speculate: false,
            runtime_join_filters: true,
            udafs: BTreeMap::new(),
            now_micros: None,
            query_log: None,
            query_api: None,
        }
    }
}

impl Clone for Session {
    fn clone(&self) -> Self {
        Self {
            tables: self.tables.clone(),
            catalog: self.catalog.clone(),
            policy: self.policy,
            workers: self.workers,
            shuffle_dir: self.shuffle_dir.clone(),
            memory_limit: self.memory_limit,
            spill_dir: self.spill_dir.clone(),
            cancel: self.cancel.clone(),
            budget: self.budget.clone(),
            skew_threshold: self.skew_threshold,
            advisory_partition_size: self.advisory_partition_size,
            // A clone is a new session copy (forked worker counts, tests).
            // It does not inherit cached results.
            cache: Mutex::new(cache::QueryCache::default()),
            lowering: self.lowering.clone(),
            allowed_read_paths: self.allowed_read_paths.clone(),
            allowed_write_paths: self.allowed_write_paths.clone(),
            allow_network: self.allow_network,
            speculate: self.speculate,
            runtime_join_filters: self.runtime_join_filters,
            udafs: self.udafs.clone(),
            now_micros: self.now_micros,
            query_log: self.query_log.clone(),
            query_api: self.query_api.clone(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct TableSource {
    path: Option<PathBuf>,
    partitions: Vec<String>,
    mtime_ms: Option<u64>,
    etag: Option<String>,
}

#[derive(Clone, Debug)]
struct MemTable {
    schema: SchemaRef,
    batches: Vec<Batch>,
    stats: mizuchi_plan::TableStats,
    /// Bumped on every [`Session::register`] of this name.
    generation: u64,
    source: TableSource,
}

impl Session {
    /// Empty session with the default morsel size.
    pub fn new() -> Self {
        Self::default()
    }

    /// Open a session from runner-supplied resource and path limits.
    pub fn from_config(cfg: SessionConfig) -> Result<Self> {
        let workers = cfg.workers.unwrap_or(1);
        let mut session = Self::new().with_workers(workers)?;
        if let Some(bytes) = cfg.memory_limit {
            session = session.with_memory_limit(bytes);
        }
        if let Some(dir) = cfg.spill_dir {
            session = session.with_spill_dir(dir);
        }
        if let Some(dir) = cfg.shuffle_dir {
            session = session.with_shuffle_dir(dir);
        }
        session.allowed_read_paths = config::resolve_roots(cfg.allowed_read_paths)?;
        session.allowed_write_paths = config::resolve_roots(cfg.allowed_write_paths)?;
        session.allow_network = cfg.allow_network;
        session.now_micros = cfg.now_micros;
        if let Some(api) = cfg.query_api {
            if api.auth_token.as_ref().is_some_and(|t| t.is_empty()) {
                return Err(Error::Plan("query API auth_token must be non-empty".into()));
            }
            if !api.bind.ip().is_loopback() && !session.allow_network {
                return Err(Error::Io(
                    "query API non-loopback bind requires allow_network".into(),
                ));
            }
            let log = if let Some(path) = &api.history_path {
                config::assert_allowed(path, &session.allowed_write_paths)?;
                query_api::QueryLog::open(path, api.retention.clone())?
            } else {
                query_api::QueryLog::memory(api.retention.clone())
            };
            session.query_log = Some(Arc::new(log));
            session.query_api = Some(api);
        }
        Ok(session)
    }

    pub(crate) fn with_owner_share(
        workers: usize,
        memory: u64,
        budget: std::sync::Arc<memory::Budget>,
    ) -> Self {
        Self {
            workers,
            memory_limit: Some(memory),
            budget: Some(budget),
            ..Self::default()
        }
    }

    /// Number of worker threads for morsel scheduling and staged execution.
    /// `n` must be at least 1.
    pub fn with_workers(mut self, n: usize) -> Result<Self> {
        if n == 0 {
            return Err(Error::Plan("workers must be greater than zero".into()));
        }
        self.workers = n;
        Ok(self)
    }

    /// Live worker thread count for this session.
    pub fn workers(&self) -> usize {
        self.workers
    }

    /// Run a second copy of each stage task; the first commit wins.
    pub fn with_speculate(mut self, on: bool) -> Self {
        self.speculate = on;
        self
    }

    /// Push inner-join build keys into the probe-side scan. Default is on.
    pub fn with_runtime_join_filters(mut self, on: bool) -> Self {
        self.runtime_join_filters = on;
        self
    }

    /// Cooperative cancellation token checked between morsels.
    pub fn with_cancel(mut self, token: CancelToken) -> Self {
        self.cancel = token;
        self
    }

    /// Token for the execute currently in `with_query_span`, else the session token.
    pub(crate) fn exec_token(&self) -> CancelToken {
        ACTIVE_CANCEL
            .with(|c| c.borrow().clone())
            .unwrap_or_else(|| self.cancel.clone())
    }

    pub(crate) fn push_cancel(token: CancelToken) -> Option<CancelToken> {
        ACTIVE_CANCEL.with(|c| c.replace(Some(token)))
    }

    pub(crate) fn restore_cancel(prev: Option<CancelToken>) {
        ACTIVE_CANCEL.with(|c| {
            *c.borrow_mut() = prev;
        });
    }

    /// Record executes into `log`. Off unless the embedder attaches a log
    /// or sets [`SessionConfig::query_api`].
    pub fn with_query_log(mut self, log: Arc<query_api::QueryLog>) -> Self {
        self.query_log = Some(log);
        self
    }

    /// History and running-query log, when recording is enabled.
    pub fn query_log(&self) -> Option<Arc<query_api::QueryLog>> {
        self.query_log.clone()
    }

    /// Bind the configured query API. Fails when [`SessionConfig::query_api`]
    /// was not set.
    pub fn serve_query_api(&self) -> Result<query_api::QueryApiServer> {
        let cfg = self
            .query_api
            .as_ref()
            .ok_or_else(|| Error::Plan("query API is not configured".into()))?;
        let log = self
            .query_log
            .clone()
            .ok_or_else(|| Error::Plan("query API is not configured".into()))?;
        query_api::QueryApiServer::bind_with_network(log, cfg, self.allow_network)
    }

    /// Directory for shuffle files written as Arrow IPC. The engine never
    /// opens network paths; the caller supplies the directory.
    pub fn with_shuffle_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.shuffle_dir = Some(dir.into());
        self
    }

    /// Per-query working-set cap in bytes. Operators fail closed when the cap
    /// cannot be met and no spill directory is set.
    pub fn with_memory_limit(mut self, bytes: u64) -> Self {
        self.memory_limit = Some(bytes);
        self
    }

    /// Cap this session at [`memory::system_budget`] and spill under the
    /// process temp directory. Join, aggregate, and sort then degrade by
    /// spilling hash partitions or runs instead of failing.
    pub fn with_system_memory_budget(self) -> Self {
        let dir = memory::private_spill_root().unwrap_or_else(|_| {
            std::env::temp_dir().join(format!("mizuchi-spill-{}", std::process::id()))
        });
        self.with_memory_limit(memory::system_budget())
            .with_spill_dir(dir)
    }

    /// Directory for operator spill files (Arrow IPC). Removed per query.
    pub fn with_spill_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.spill_dir = Some(dir.into());
        self
    }

    /// Register a combinable aggregate callable from SQL by [`CombineAgg::name`].
    pub fn register_udaf(&mut self, agg: Arc<dyn CombineAgg>) -> Result<()> {
        let name = agg.name().to_ascii_lowercase();
        if name.is_empty() {
            return Err(Error::Plan("aggregate name is empty".into()));
        }
        if matches!(name.as_str(), "count" | "sum" | "avg" | "min" | "max") {
            return Err(Error::Plan(format!("aggregate name {name} is reserved")));
        }
        self.udafs.insert(name, agg);
        if let Ok(mut cache) = self.cache.lock() {
            cache.clear();
        }
        Ok(())
    }

    /// Byte floor for treating a shuffle partition as skewed. Also must exceed
    /// five times the median partition size.
    pub fn with_skew_threshold(mut self, bytes: u64) -> Self {
        self.skew_threshold = bytes;
        self
    }

    /// Target size in bytes when splitting a skewed shuffle partition.
    pub fn with_advisory_partition_size(mut self, bytes: u64) -> Self {
        self.advisory_partition_size = bytes.max(1);
        self
    }

    /// Replace the physical-operator lowering. CPU operators remain the default.
    pub fn with_lowering(mut self, lowering: std::sync::Arc<dyn Lowering>) -> Self {
        self.lowering = op::LoweringArc(lowering);
        self
    }

    /// Pin `CURRENT_DATE` and `NOW()` to `micros` since the Unix epoch.
    pub fn with_now_micros(mut self, micros: i64) -> Self {
        self.now_micros = Some(micros);
        self
    }

    fn query_now_micros(&self) -> i64 {
        self.now_micros
            .unwrap_or_else(mizuchi_plan::unix_micros_now)
    }

    /// Register an in-memory table. All batches must share `schema`.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        schema: SchemaRef,
        batches: Vec<Batch>,
    ) -> Result<()> {
        let schema = mizuchi_core::schema::from_arrow_schema(schema)?;
        for batch in &batches {
            if batch.schema().as_ref() != schema.as_ref() {
                return Err(Error::InvalidBatch(
                    "registered batch schema does not match the table".into(),
                ));
            }
        }
        let mut stats = mizuchi_plan::table_stats_from_batches(schema.as_ref(), &batches);
        let name = normalize_name(&name.into());
        if stats.partitions.is_empty()
            && let Some(existing) = self.catalog.stats(&name)
        {
            stats.partition_columns = existing.partition_columns.clone();
            stats.partitions = existing.partitions.clone();
        }
        let generation = self
            .tables
            .get(&name)
            .map(|t| t.generation.saturating_add(1))
            .unwrap_or(1);
        self.invalidate_cache(&name);
        self.catalog.register(name.clone(), schema.clone())?;
        self.catalog.set_stats(name.clone(), stats.clone())?;
        self.tables.insert(
            name,
            MemTable {
                schema,
                batches,
                stats,
                generation,
                source: TableSource::default(),
            },
        );
        Ok(())
    }

    /// Replace the session catalog (memory or file-backed). Catalog tables
    /// with no session data become empty scans so `SELECT` binds and runs.
    pub fn with_catalog(mut self, catalog: Catalog) -> Self {
        self.catalog = catalog;
        self.sync_empty_catalog_tables();
        self
    }

    fn sync_empty_catalog_tables(&mut self) {
        let namespaces = self.catalog.list_namespaces();
        for ns in namespaces {
            let raw = if ns.catalog == mizuchi_plan::DEFAULT_CATALOG {
                ns.namespace.clone()
            } else {
                format!("{}.{}", ns.catalog, ns.namespace)
            };
            let Ok(names) = self.catalog.list_tables(Some(&raw)) else {
                continue;
            };
            for table in names {
                let Ok(ident) = TableIdent::from_parts(&ns.catalog, &ns.namespace, &table) else {
                    continue;
                };
                let name = ident.sql_name();
                if self.tables.contains_key(&name) {
                    continue;
                }
                let Ok(schema) = self.catalog.table(&name).cloned() else {
                    continue;
                };
                let stats = self.catalog.stats(&name).cloned().unwrap_or_default();
                self.tables.insert(
                    name,
                    MemTable {
                        schema,
                        batches: Vec::new(),
                        stats,
                        generation: 1,
                        source: TableSource::default(),
                    },
                );
            }
        }
    }

    /// Live catalog used for name resolution and DDL.
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Record Hive partition directories used by scan reports.
    pub fn set_partitions(
        &mut self,
        name: impl Into<String>,
        columns: Vec<String>,
        partitions: Vec<String>,
    ) -> Result<()> {
        let name = normalize_name(&name.into());
        self.catalog
            .set_partitions(&name, columns.clone(), partitions.clone())?;
        if let Some(table) = self.tables.get_mut(&name) {
            table.stats.partition_columns = columns;
            table.stats.partitions = partitions;
        }
        Ok(())
    }

    /// `CREATE` / `DROP` / `ALTER TABLE`.
    pub fn execute_ddl(&mut self, sql: &str) -> Result<()> {
        self.cancel.check()?;
        match bind_ddl(sql)? {
            Ddl::CreateTable {
                ident,
                schema,
                if_not_exists,
            } => {
                if self.catalog.table(&ident.sql_name()).is_ok() {
                    if if_not_exists {
                        return Ok(());
                    }
                    return Err(Error::Plan(format!(
                        "table already exists: {}",
                        ident.sql_name()
                    )));
                }
                self.catalog.create_table(mizuchi_plan::TableMeta {
                    ident: ident.clone(),
                    schema: schema.clone(),
                    stats: mizuchi_plan::TableStats::default(),
                })?;
                self.register(ident.sql_name(), schema, Vec::new())
            }
            Ddl::DropTable { ident, if_exists } => {
                let name = ident.sql_name();
                self.catalog.drop_table(name.clone(), if_exists)?;
                self.tables.remove(&normalize_name(&name));
                self.invalidate_cache(&name);
                Ok(())
            }
            Ddl::AlterTable { ident, action } => {
                let name = ident.sql_name();
                let current = self.catalog.table(&name)?.clone();
                let next = apply_alter(current.as_ref(), &action)?;
                self.catalog.alter_table(&ident, next.clone())?;
                if let Some(table) = self.tables.get_mut(&normalize_name(&name)) {
                    let kept_cols = table.stats.partition_columns.clone();
                    let kept_parts = table.stats.partitions.clone();
                    table.batches = reshape_batches(&table.batches, next.clone(), &action)?;
                    table.schema = next.clone();
                    table.stats =
                        mizuchi_plan::table_stats_from_batches(next.as_ref(), &table.batches);
                    table.stats.partition_columns = kept_cols;
                    table.stats.partitions = kept_parts;
                    table.generation = table.generation.saturating_add(1);
                }
                self.invalidate_cache(&name);
                Ok(())
            }
            Ddl::ShowTables { .. } => Err(Error::Plan("SHOW TABLES is a query".into())),
        }
    }

    /// `SHOW TABLES` as a two-column batch (`namespace`, `table_name`).
    pub fn show_tables(&self, namespace: Option<&str>) -> Result<Vec<Batch>> {
        show_tables_batches(&self.catalog, namespace)
    }

    /// Register a Parquet file or dataset directory as an in-memory table
    /// after the read allow-list check.
    pub fn register_parquet(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        opts: mizuchi_io::ParquetReadOptions<'_>,
    ) -> Result<()> {
        let path = path.as_ref();
        config::assert_allowed(path, &self.allowed_read_paths)?;
        let (batches, source) = read_parquet_tree(path, opts)?;
        let schema = match batches.first() {
            Some(batch) => batch.schema(),
            None => {
                return Err(Error::Io(format!("{} produced no batches", path.display())));
            }
        };
        let name = name.into();
        self.register(name.clone(), schema, batches)?;
        if let Some(table) = self.tables.get_mut(&normalize_name(&name)) {
            table.source = source;
        }
        Ok(())
    }

    /// Register a CSV file as an in-memory table after the read allow-list
    /// check.
    pub fn register_csv(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        opts: mizuchi_io::CsvReadOptions<'_>,
    ) -> Result<()> {
        let path = path.as_ref();
        config::assert_allowed(path, &self.allowed_read_paths)?;
        let batches = mizuchi_io::read_csv(path, opts)?;
        let schema = match batches.first() {
            Some(batch) => batch.schema(),
            None => {
                return Err(Error::Io(format!("{} produced no batches", path.display())));
            }
        };
        self.register(name, schema, batches)
    }

    /// Register a newline-delimited JSON file after the read allow-list check.
    pub fn register_json(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        opts: mizuchi_io::JsonReadOptions<'_>,
    ) -> Result<()> {
        let path = path.as_ref();
        config::assert_allowed(path, &self.allowed_read_paths)?;
        let batches = mizuchi_io::read_json(path, opts)?;
        let schema = match batches.first() {
            Some(batch) => batch.schema(),
            None => {
                return Err(Error::Io(format!("{} produced no batches", path.display())));
            }
        };
        self.register(name, schema, batches)
    }

    /// Register an Avro Object Container File after the read allow-list check.
    pub fn register_avro(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        opts: mizuchi_io::AvroReadOptions<'_>,
    ) -> Result<()> {
        let path = path.as_ref();
        config::assert_allowed(path, &self.allowed_read_paths)?;
        let batches = mizuchi_io::read_avro(path, opts)?;
        let schema = match batches.first() {
            Some(batch) => batch.schema(),
            None => {
                return Err(Error::Io(format!("{} produced no batches", path.display())));
            }
        };
        self.register(name, schema, batches)
    }

    /// Register an ORC file after the read allow-list check.
    pub fn register_orc(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        opts: mizuchi_io::OrcReadOptions<'_>,
    ) -> Result<()> {
        let path = path.as_ref();
        config::assert_allowed(path, &self.allowed_read_paths)?;
        let batches = mizuchi_io::read_orc(path, opts)?;
        let schema = match batches.first() {
            Some(batch) => batch.schema(),
            None => {
                return Err(Error::Io(format!("{} produced no batches", path.display())));
            }
        };
        self.register(name, schema, batches)
    }

    /// Register a PostgreSQL table or query after the network grant check.
    pub fn register_postgres(
        &mut self,
        name: impl Into<String>,
        source: &mizuchi_postgres::PostgresSource,
        opts: mizuchi_postgres::PostgresReadOptions<'_>,
    ) -> Result<()> {
        if !self.allow_network {
            return Err(Error::Io("network is not granted".into()));
        }
        let batches = mizuchi_postgres::read_postgres(source, opts)?;
        let schema = match batches.first() {
            Some(batch) => batch.schema(),
            None => {
                return Err(Error::Io("PostgreSQL source produced no batches".into()));
            }
        };
        self.register(name, schema, batches)
    }

    /// Register batches from an Arrow C Data Interface stream.
    pub fn register_arrow_stream(
        &mut self,
        name: impl Into<String>,
        reader: arrow::ffi_stream::ArrowArrayStreamReader,
    ) -> Result<()> {
        let schema = mizuchi_core::schema::from_arrow_schema(reader.schema())?;
        let mut batches = Vec::new();
        for item in reader {
            let batch = item.map_err(|e| Error::Io(e.to_string()))?;
            batches.push(Batch::from_arrow(batch)?);
        }
        self.register(name, schema, batches)
    }

    /// Open a continuous query. Recovers from `cfg.checkpoint` when metadata
    /// from a previous run is present. Checkpoint and sink paths must pass
    /// the write allow-list (and the read allow-list, for recovery).
    pub fn stream_query(&self, schema: SchemaRef, cfg: StreamQueryConfig) -> Result<StreamQuery> {
        config::assert_allowed(&cfg.checkpoint, &self.allowed_write_paths)?;
        config::assert_allowed(&cfg.checkpoint, &self.allowed_read_paths)?;
        config::assert_allowed(&cfg.sink, &self.allowed_write_paths)?;
        stream::StreamQuery::open_or_new(schema, cfg, self.cancel.clone())
    }

    /// Write batches as Parquet after the write allow-list check.
    pub fn write_parquet(
        &self,
        dest: impl AsRef<Path>,
        batches: &[Batch],
        opts: mizuchi_io::ParquetWriteOptions<'_>,
    ) -> Result<()> {
        let _ = self.write_parquet_reported(dest, batches, opts)?;
        Ok(())
    }

    /// Write newline-delimited JSON after the write allow-list check.
    pub fn write_json(&self, dest: impl AsRef<Path>, batches: &[Batch]) -> Result<()> {
        let dest = dest.as_ref();
        config::assert_allowed(dest, &self.allowed_write_paths)?;
        mizuchi_io::write_json(dest, batches)
    }

    /// Write an Avro Object Container File after the write allow-list check.
    pub fn write_avro(&self, dest: impl AsRef<Path>, batches: &[Batch]) -> Result<()> {
        let dest = dest.as_ref();
        config::assert_allowed(dest, &self.allowed_write_paths)?;
        mizuchi_io::write_avro(dest, batches)
    }

    /// Write an uncompressed ORC file after the write allow-list check.
    pub fn write_orc(&self, dest: impl AsRef<Path>, batches: &[Batch]) -> Result<()> {
        let dest = dest.as_ref();
        config::assert_allowed(dest, &self.allowed_write_paths)?;
        mizuchi_io::write_orc(dest, batches)
    }

    /// [`Self::write_parquet`] plus the output side of a [`QueryReport`].
    pub fn write_parquet_reported(
        &self,
        dest: impl AsRef<Path>,
        batches: &[Batch],
        opts: mizuchi_io::ParquetWriteOptions<'_>,
    ) -> Result<QueryReport> {
        let dest = dest.as_ref();
        config::assert_allowed(dest, &self.allowed_write_paths)?;
        let stats = mizuchi_io::write_parquet_stats(dest, batches, opts, &self.cancel)?;
        Ok(QueryReport::from_write(OutputWrite {
            path: stats.path,
            partitions: stats.partitions,
            mode: stats.mode,
            rows: stats.rows,
            bytes: stats.bytes,
            commit_id: stats.commit_id,
        }))
    }

    /// Batches and schema of a registered table.
    pub fn table_data(&self, name: &str) -> Result<(SchemaRef, Vec<Batch>)> {
        let table = self
            .tables
            .get(&normalize_name(name))
            .ok_or_else(|| Error::Plan(format!("table not found: {name}")))?;
        Ok((table.schema.clone(), table.batches.clone()))
    }

    /// Apply a `MERGE` statement to a registered target table.
    pub fn merge_sql(&mut self, sql: &str) -> Result<MergeStats> {
        self.cancel.check()?;
        let mut catalog = Catalog::new();
        for (name, table) in &self.tables {
            catalog.register(name, table.schema.clone())?;
            catalog.set_stats(name, table.stats.clone())?;
        }
        let plan = mizuchi_plan::bind_merge_at(sql, &catalog, self.query_now_micros())?;
        let (batches, stats) = merge::run(self, &plan)?;
        self.register(stats.table.clone(), plan.target_schema, batches)?;
        Ok(stats)
    }

    /// Write a registered table as a Delta table after the write allow-list check.
    pub fn write_delta(
        &self,
        dest: impl AsRef<Path>,
        name: &str,
        mode: mizuchi_io::WriteMode,
    ) -> Result<mizuchi_io::LakeSnapshot> {
        let dest = dest.as_ref();
        config::assert_allowed(dest, &self.allowed_write_paths)?;
        let (_, batches) = self.table_data(name)?;
        mizuchi_io::write_delta(dest, &batches, mode)
    }

    /// Load a Delta snapshot as an in-memory table after the read allow-list check.
    pub fn register_delta(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        travel: mizuchi_io::TimeTravel,
    ) -> Result<()> {
        let path = path.as_ref();
        config::assert_allowed(path, &self.allowed_read_paths)?;
        let batches = mizuchi_io::read_delta(path, travel)?;
        let schema = match batches.first() {
            Some(batch) => batch.schema(),
            None => return Err(Error::Io(format!("{} produced no batches", path.display()))),
        };
        self.register(name, schema, batches)
    }

    /// Merge into a Delta table: read the current snapshot, apply `sql`, commit.
    pub fn merge_delta(
        &mut self,
        dest: impl AsRef<Path>,
        sql: &str,
    ) -> Result<mizuchi_io::LakeSnapshot> {
        let dest = dest.as_ref();
        config::assert_allowed(dest, &self.allowed_write_paths)?;
        config::assert_allowed(dest, &self.allowed_read_paths)?;
        let target = mizuchi_plan::merge_target_table(sql)?;
        let head = mizuchi_io::delta_head(dest)?;
        let batches = mizuchi_io::read_delta(dest, mizuchi_io::TimeTravel::Latest)?;
        let schema = match batches.first() {
            Some(b) => b.schema(),
            None => {
                return Err(Error::Io(format!("{} produced no batches", dest.display())));
            }
        };
        self.merge_and_commit(sql, target, schema, batches, |session, name| {
            let (_, out) = session.table_data(name)?;
            mizuchi_io::write_delta_at(
                dest,
                &out,
                mizuchi_io::WriteMode::Overwrite,
                Some(head.version),
            )
        })
    }

    /// Write a registered table as an Iceberg table after the write allow-list check.
    pub fn write_iceberg(
        &self,
        dest: impl AsRef<Path>,
        name: &str,
        mode: mizuchi_io::WriteMode,
    ) -> Result<mizuchi_io::LakeSnapshot> {
        let dest = dest.as_ref();
        config::assert_allowed(dest, &self.allowed_write_paths)?;
        let (_, batches) = self.table_data(name)?;
        mizuchi_io::write_iceberg(dest, &batches, mode)
    }

    /// Load an Iceberg snapshot as an in-memory table after the read allow-list check.
    pub fn register_iceberg(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        travel: mizuchi_io::TimeTravel,
    ) -> Result<()> {
        let path = path.as_ref();
        config::assert_allowed(path, &self.allowed_read_paths)?;
        let batches = mizuchi_io::read_iceberg(path, travel)?;
        let schema = match batches.first() {
            Some(batch) => batch.schema(),
            None => return Err(Error::Io(format!("{} produced no batches", path.display()))),
        };
        self.register(name, schema, batches)
    }

    /// Merge into an Iceberg table: read the current snapshot, apply `sql`, commit.
    pub fn merge_iceberg(
        &mut self,
        dest: impl AsRef<Path>,
        sql: &str,
    ) -> Result<mizuchi_io::LakeSnapshot> {
        let dest = dest.as_ref();
        config::assert_allowed(dest, &self.allowed_write_paths)?;
        config::assert_allowed(dest, &self.allowed_read_paths)?;
        let target = mizuchi_plan::merge_target_table(sql)?;
        let head = mizuchi_io::iceberg_head(dest)?;
        let batches = mizuchi_io::read_iceberg(dest, mizuchi_io::TimeTravel::Latest)?;
        let schema = match batches.first() {
            Some(b) => b.schema(),
            None => {
                return Err(Error::Io(format!("{} produced no batches", dest.display())));
            }
        };
        self.merge_and_commit(sql, target, schema, batches, |session, name| {
            let (_, out) = session.table_data(name)?;
            mizuchi_io::write_iceberg_at(
                dest,
                &out,
                mizuchi_io::WriteMode::Overwrite,
                Some(head.version),
            )
        })
    }

    /// Write a registered table as Delta objects under `prefix`.
    pub fn write_delta_store(
        &self,
        store: &dyn object_store::ObjectStore,
        prefix: &str,
        name: &str,
        mode: mizuchi_io::WriteMode,
    ) -> Result<mizuchi_io::LakeSnapshot> {
        let (_, batches) = self.table_data(name)?;
        mizuchi_io::write_delta_store(store, prefix, &batches, mode)
    }

    /// Load a Delta snapshot from an object store prefix as an in-memory table.
    pub fn register_delta_store(
        &mut self,
        name: impl Into<String>,
        store: &dyn object_store::ObjectStore,
        prefix: &str,
        travel: mizuchi_io::TimeTravel,
    ) -> Result<()> {
        let batches = mizuchi_io::read_delta_store(store, prefix, travel)?;
        let schema = match batches.first() {
            Some(batch) => batch.schema(),
            None => return Err(Error::Io(format!("{prefix} produced no batches"))),
        };
        self.register(name, schema, batches)
    }

    /// Merge into a Delta table on an object store prefix.
    pub fn merge_delta_store(
        &mut self,
        store: &dyn object_store::ObjectStore,
        prefix: &str,
        sql: &str,
    ) -> Result<mizuchi_io::LakeSnapshot> {
        let target = mizuchi_plan::merge_target_table(sql)?;
        let head = mizuchi_io::delta_head_store(store, prefix)?;
        let batches = mizuchi_io::read_delta_store(store, prefix, mizuchi_io::TimeTravel::Latest)?;
        let schema = match batches.first() {
            Some(b) => b.schema(),
            None => return Err(Error::Io(format!("{prefix} produced no batches"))),
        };
        self.merge_and_commit(sql, target, schema, batches, |session, name| {
            let (_, out) = session.table_data(name)?;
            mizuchi_io::write_delta_store_at(
                store,
                prefix,
                &out,
                mizuchi_io::WriteMode::Overwrite,
                Some(head.version),
            )
        })
    }

    /// Write a registered table as Iceberg objects under `prefix`.
    pub fn write_iceberg_store(
        &self,
        store: &dyn object_store::ObjectStore,
        prefix: &str,
        name: &str,
        mode: mizuchi_io::WriteMode,
    ) -> Result<mizuchi_io::LakeSnapshot> {
        let (_, batches) = self.table_data(name)?;
        mizuchi_io::write_iceberg_store(store, prefix, &batches, mode)
    }

    /// Load an Iceberg snapshot from an object store prefix as an in-memory table.
    pub fn register_iceberg_store(
        &mut self,
        name: impl Into<String>,
        store: &dyn object_store::ObjectStore,
        prefix: &str,
        travel: mizuchi_io::TimeTravel,
    ) -> Result<()> {
        let batches = mizuchi_io::read_iceberg_store(store, prefix, travel)?;
        let schema = match batches.first() {
            Some(batch) => batch.schema(),
            None => return Err(Error::Io(format!("{prefix} produced no batches"))),
        };
        self.register(name, schema, batches)
    }

    /// Merge into an Iceberg table on an object store prefix.
    pub fn merge_iceberg_store(
        &mut self,
        store: &dyn object_store::ObjectStore,
        prefix: &str,
        sql: &str,
    ) -> Result<mizuchi_io::LakeSnapshot> {
        let target = mizuchi_plan::merge_target_table(sql)?;
        let head = mizuchi_io::iceberg_head_store(store, prefix)?;
        let batches =
            mizuchi_io::read_iceberg_store(store, prefix, mizuchi_io::TimeTravel::Latest)?;
        let schema = match batches.first() {
            Some(b) => b.schema(),
            None => return Err(Error::Io(format!("{prefix} produced no batches"))),
        };
        self.merge_and_commit(sql, target, schema, batches, |session, name| {
            let (_, out) = session.table_data(name)?;
            mizuchi_io::write_iceberg_store_at(
                store,
                prefix,
                &out,
                mizuchi_io::WriteMode::Overwrite,
                Some(head.version),
            )
        })
    }

    fn merge_and_commit(
        &mut self,
        sql: &str,
        target: String,
        schema: SchemaRef,
        batches: Vec<Batch>,
        commit: impl FnOnce(&Session, &str) -> Result<mizuchi_io::LakeSnapshot>,
    ) -> Result<mizuchi_io::LakeSnapshot> {
        let key = normalize_name(&target);
        let previous = self.tables.get(&key).cloned();
        self.register(target, schema, batches)?;
        let result = (|| {
            let stats = self.merge_sql(sql)?;
            commit(self, &stats.table)
        })();
        match result {
            Ok(snap) => Ok(snap),
            Err(e) => {
                match previous {
                    Some(table) => {
                        self.tables.insert(key, table);
                    }
                    None => {
                        self.tables.remove(&key);
                    }
                }
                Err(e)
            }
        }
    }

    /// Bind and optimize `sql` against registered tables.
    pub fn logical_plan(&self, sql: &str) -> Result<LogicalPlan> {
        self.bind_optimized(sql)
    }

    /// Run an already-bound logical plan. Re-optimizes, then pulls batches.
    pub fn execute(&self, plan: &LogicalPlan) -> Result<Vec<Batch>> {
        Ok(self.execute_reported(plan)?.batches)
    }

    /// [`Self::execute`] plus a [`QueryReport`].
    pub fn execute_reported(&self, plan: &LogicalPlan) -> Result<QueryResult> {
        let plan_sql = report::plan_text(plan);
        self.with_query_span(&plan_sql, |instrument| {
            self.exec_token().check()?;
            let plan = mizuchi_plan::optimize(plan.clone())?;
            let (batches, stats) = self.execute_run(&plan, instrument)?;
            Ok((
                QueryResult {
                    report: self.report_for(&plan),
                    batches,
                    peak_bytes: stats.peak_bytes,
                    spilled_bytes: stats.spilled_bytes,
                },
                stats,
            ))
        })
    }

    /// Times a later [`Self::execute_sql`] or [`Self::execute_staged`] reused a
    /// stored result because the plan and table generations still matched.
    pub fn cache_hits(&self) -> u64 {
        self.cache.lock().map(|c| c.hits).unwrap_or(0)
    }

    /// Plan and run `sql` against registered tables.
    pub fn execute_sql(&self, sql: &str) -> Result<Vec<Batch>> {
        Ok(self.execute_sql_reported(sql)?.batches)
    }

    /// [`Self::execute_sql`] plus a [`QueryReport`].
    pub fn execute_sql_reported(&self, sql: &str) -> Result<QueryResult> {
        self.with_query_span(sql, |instrument| self.execute_sql_inner(sql, instrument))
    }

    fn with_query_span(
        &self,
        sql: &str,
        run: impl FnOnce(bool) -> Result<(QueryResult, query_api::RunStats)>,
    ) -> Result<QueryResult> {
        let span = self
            .query_log
            .as_ref()
            .map(|log| query_api::QueryLog::begin_on(log, sql));
        let instrument = span.is_some();
        let prev = span
            .as_ref()
            .map(|s| Self::push_cancel(s.token().merge(&self.cancel)));
        let result = run(instrument);
        if prev.is_some() {
            Self::restore_cancel(prev.flatten());
        }
        match (result, span) {
            (Ok((qr, stats)), Some(span)) => {
                let rows: u64 = qr.batches.iter().map(|b| b.num_rows() as u64).sum();
                span.finish_ok(rows, &qr.report, &stats)?;
                Ok(qr)
            }
            (Err(e), Some(span)) => {
                span.finish_err(&e);
                Err(e)
            }
            (Ok((qr, _)), None) => Ok(qr),
            (Err(e), None) => Err(e),
        }
    }

    fn execute_sql_inner(
        &self,
        sql: &str,
        instrument: bool,
    ) -> Result<(QueryResult, query_api::RunStats)> {
        self.exec_token().check()?;
        if let Ok(Ddl::ShowTables { namespace }) = bind_ddl(sql) {
            let batches = self.show_tables(namespace.as_deref())?;
            return Ok((
                QueryResult {
                    batches,
                    report: QueryReport::new("SHOW TABLES".into(), Vec::new()),
                    peak_bytes: 0,
                    spilled_bytes: 0,
                },
                query_api::RunStats::default(),
            ));
        }
        if let Some((analyze, inner)) = explain_sql(sql)? {
            let text = if analyze {
                self.explain_analyze(&inner)?
            } else {
                self.explain(&inner)?
            };
            let batches = plan_text_batch(&text)?;
            return Ok((
                QueryResult {
                    batches,
                    report: QueryReport::new(text, Vec::new()),
                    peak_bytes: 0,
                    spilled_bytes: 0,
                },
                query_api::RunStats::default(),
            ));
        }
        let plan = self.bind_optimized(sql)?;
        let (batches, stats) = self.execute_run(&plan, instrument)?;
        Ok((
            QueryResult {
                report: self.report_for(&plan),
                batches,
                peak_bytes: stats.peak_bytes,
                spilled_bytes: stats.spilled_bytes,
            },
            stats,
        ))
    }

    fn report_for(&self, plan: &LogicalPlan) -> QueryReport {
        let mut inputs = Vec::new();
        collect_scan_reports(plan, self, &mut inputs);
        QueryReport::new(report::plan_text(plan), inputs)
    }

    fn execute_run(
        &self,
        plan: &LogicalPlan,
        instrument: bool,
    ) -> Result<(Vec<Batch>, query_api::RunStats)> {
        if instrument {
            let pool = self.memory_pool()?;
            let (op, tree) = build_physical(plan, self, &pool, true, None)?;
            let batches = collect(op, &self.exec_token())?;
            return Ok((
                batches,
                query_api::RunStats {
                    peak_bytes: pool.peak(),
                    spilled_bytes: pool.spilled(),
                    metrics: Some(tree.snapshot()),
                },
            ));
        }
        self.execute_optimized_stats(plan)
    }

    fn execute_optimized_stats(
        &self,
        plan: &LogicalPlan,
    ) -> Result<(Vec<Batch>, query_api::RunStats)> {
        let pool = self.memory_pool()?;
        let batches = self.execute_cached(plan, || {
            if self.workers > 1
                && let Some(out) = schedule::try_parallel(plan, self, &pool)?
            {
                return Ok(out);
            }
            collect(physical(plan, self, &pool)?, &self.exec_token())
        })?;
        Ok((
            batches,
            query_api::RunStats {
                peak_bytes: pool.peak(),
                spilled_bytes: pool.spilled(),
                metrics: None,
            },
        ))
    }

    /// Logical and physical plan text. Does not run the query.
    pub fn explain(&self, sql: &str) -> Result<String> {
        let plan = self.bind_optimized(sql)?;
        Ok(format!(
            "== Logical Plan ==\n{}== Physical Plan ==\n{}",
            plan.explain(),
            metrics::physical_shape(&plan).format(false)
        ))
    }

    /// Per-operator metrics as a tree. Runs the pull plan once.
    pub fn query_metrics(&self, sql: &str) -> Result<OperatorMetrics> {
        let plan = self.bind_optimized(sql)?;
        let pool = self.memory_pool()?;
        let (op, tree) = build_physical(&plan, self, &pool, true, None)?;
        let _ = collect(op, &self.exec_token())?;
        Ok(tree.snapshot())
    }

    /// Split the optimized plan into stages at hash-partition exchanges.
    pub fn explain_stages(&self, sql: &str) -> Result<String> {
        stage::explain_stages(&self.bind_optimized(sql)?, self.workers.max(1))
    }

    /// Run `sql` as shuffle-map and result stages on `workers` threads.
    pub fn execute_staged(&self, sql: &str) -> Result<Vec<Batch>> {
        self.with_query_span(sql, |instrument| {
            let plan = self.bind_optimized(sql)?;
            let (batches, stats) = stage::execute_staged_traced(&plan, self, instrument)?;
            Ok((
                QueryResult {
                    report: self.report_for(&plan),
                    batches,
                    peak_bytes: stats.peak_bytes,
                    spilled_bytes: stats.spilled_bytes,
                },
                stats,
            ))
        })
        .map(|r| r.batches)
    }

    /// Run the query and print logical/physical plans with per-operator
    /// rows, batches, elapsed time, and peak pool bytes. Appends adaptive
    /// shuffle notes from the staged path.
    pub fn explain_analyze(&self, sql: &str) -> Result<String> {
        let plan = self.bind_optimized(sql)?;
        let pool = self.memory_pool()?;
        let (op, tree) = build_physical(&plan, self, &pool, true, None)?;
        let _batches = collect(op, &self.exec_token())?;
        let mut text = format!(
            "== Logical Plan ==\n{}== Physical Plan ==\n{}",
            plan.explain(),
            tree.format(true)
        );
        text.push_str(&stage::explain_analyze(&plan, self)?);
        text.push_str(&format!(
            "query peak_bytes={} spilled_bytes={}\n",
            pool.peak(),
            pool.spilled()
        ));
        Ok(text)
    }

    pub(crate) fn memory_pool(&self) -> Result<memory::MemoryPool> {
        match &self.budget {
            Some(budget) => memory::MemoryPool::with_budget(budget.clone(), self.spill_dir.clone()),
            None => memory::MemoryPool::new(self.memory_limit, self.spill_dir.clone()),
        }
    }

    pub(crate) fn table_morsels(&self, table: &str) -> Result<Vec<Batch>> {
        let table = self
            .tables
            .get(&normalize_name(table))
            .ok_or_else(|| Error::Plan(format!("table not found at execution: {table}")))?;
        let mut morsels = Vec::new();
        for batch in &table.batches {
            morsels.extend(batch.split_by_policy(&self.policy));
        }
        Ok(morsels)
    }

    fn bind_optimized(&self, sql: &str) -> Result<LogicalPlan> {
        let mut catalog = self.catalog.planning_copy();
        for (name, table) in &self.tables {
            if catalog.table(name).is_err() {
                catalog.register(name, table.schema.clone())?;
            }
            let mut stats = table.stats.clone();
            if stats.partitions.is_empty()
                && let Ok(parts) = catalog.partitions(name)
            {
                stats.partitions = parts;
            }
            catalog.set_stats(name, stats)?;
        }
        let plan = mizuchi_plan::plan_sql_at(sql, &catalog, &self.udafs, self.query_now_micros())?;
        mizuchi_plan::optimize(plan)
    }

    fn execute_cached(
        &self,
        plan: &LogicalPlan,
        run: impl FnOnce() -> Result<Vec<Batch>>,
    ) -> Result<Vec<Batch>> {
        let key = cache::key(plan, &self.table_generations());
        if let Some(hit) = self
            .cache
            .lock()
            .map_err(|_| Error::Eval("query cache lock poisoned".into()))?
            .lookup(&key)
        {
            return Ok(hit);
        }
        let out = run()?;
        self.cache
            .lock()
            .map_err(|_| Error::Eval("query cache lock poisoned".into()))?
            .store(key, &out);
        Ok(out)
    }

    fn table_generations(&self) -> BTreeMap<String, u64> {
        self.tables
            .iter()
            .map(|(name, table)| (name.clone(), table.generation))
            .collect()
    }

    fn invalidate_cache(&self, table: &str) {
        if let Ok(mut cache) = self.cache.lock() {
            cache.invalidate_table(table);
        }
    }
}

fn read_parquet_tree(
    path: &Path,
    opts: mizuchi_io::ParquetReadOptions<'_>,
) -> Result<(Vec<Batch>, TableSource)> {
    let mut files = Vec::new();
    if path.is_dir() {
        collect_parquet_files(path, &mut files)?;
        files.sort();
    } else {
        files.push(path.to_path_buf());
    }
    let mut out = Vec::new();
    for file in &files {
        out.extend(mizuchi_io::read_parquet(
            file,
            mizuchi_io::ParquetReadOptions {
                projection: opts.projection,
                prune_row_group: opts.prune_row_group,
                filters: opts.filters,
                policy: opts.policy,
            },
        )?);
    }
    Ok((out, table_source_from_files(path, &files)))
}

fn table_source_from_files(root: &Path, files: &[PathBuf]) -> TableSource {
    let mut partitions: Vec<String> = files
        .iter()
        .filter_map(|file| {
            let rel = file.strip_prefix(root).ok()?;
            let parent = rel.parent()?;
            if parent.as_os_str().is_empty() {
                None
            } else {
                Some(parent.to_string_lossy().replace('\\', "/"))
            }
        })
        .collect();
    partitions.sort();
    partitions.dedup();
    let mtime_ms = files
        .iter()
        .filter_map(|f| std::fs::metadata(f).ok()?.modified().ok())
        .filter_map(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64)
        .max();
    TableSource {
        path: Some(root.to_path_buf()),
        partitions,
        mtime_ms,
        etag: None,
    }
}

fn reshape_batches(batches: &[Batch], next: SchemaRef, action: &AlterTable) -> Result<Vec<Batch>> {
    match action {
        AlterTable::AddColumn { data_type, .. } => batches
            .iter()
            .map(|batch| {
                let mut cols = batch.as_arrow().columns().to_vec();
                cols.push(arrow::array::new_null_array(data_type, batch.num_rows()));
                Batch::try_new(next.clone(), cols)
            })
            .collect(),
        AlterTable::RenameColumn { .. } => batches
            .iter()
            .map(|batch| Batch::try_new(next.clone(), batch.as_arrow().columns().to_vec()))
            .collect(),
    }
}

fn show_tables_batches(catalog: &Catalog, namespace: Option<&str>) -> Result<Vec<Batch>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("namespace", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
    ]));
    let ns_label = namespace.unwrap_or(mizuchi_plan::DEFAULT_NAMESPACE);
    let names = catalog.list_tables(namespace)?;
    let ns_col = StringArray::from(vec![ns_label.to_string(); names.len()]);
    let name_col = StringArray::from(names);
    Ok(vec![Batch::try_new(
        schema,
        vec![Arc::new(ns_col), Arc::new(name_col)],
    )?])
}

fn collect_scan_reports(plan: &LogicalPlan, session: &Session, out: &mut Vec<InputScan>) {
    match plan {
        LogicalPlan::Scan {
            table,
            schema,
            projection,
            filters,
            ..
        } => {
            let mut columns = match projection {
                Some(names) => names.clone(),
                None => schema.fields().iter().map(|f| f.name().clone()).collect(),
            };
            columns.sort();
            let source = session
                .tables
                .get(&normalize_name(table))
                .map(|t| t.source.clone())
                .unwrap_or_default();
            let mut partitions = source.partitions;
            if partitions.is_empty()
                && let Ok(parts) = session.catalog.partitions(table)
            {
                partitions = parts;
            }
            if !filters.is_empty() {
                partitions = mizuchi_io::prune_partitions(&partitions, filters);
            }
            partitions.sort();
            out.push(InputScan {
                dataset: table.clone(),
                path: source.path.map(|p| p.display().to_string()),
                partitions,
                columns,
                mtime_ms: source.mtime_ms,
                etag: source.etag,
            });
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Window { input, .. } => collect_scan_reports(input, session, out),
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::Intersect { left, right, .. }
        | LogicalPlan::Except { left, right, .. } => {
            collect_scan_reports(left, session, out);
            collect_scan_reports(right, session, out);
        }
        LogicalPlan::Union { inputs, .. } => {
            for input in inputs {
                collect_scan_reports(input, session, out);
            }
        }
        LogicalPlan::Recursive { anchors, step, .. } => {
            for anchor in anchors {
                collect_scan_reports(anchor, session, out);
            }
            collect_scan_reports(step, session, out);
        }
        LogicalPlan::Empty { .. } | LogicalPlan::WorkScan { .. } => {}
        LogicalPlan::Scalar { input, .. } => collect_scan_reports(input, session, out),
    }
}

fn collect_parquet_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    let ents = std::fs::read_dir(dir).map_err(|e| Error::Io(format!("{}: {e}", dir.display())))?;
    for ent in ents {
        let ent = ent.map_err(|e| Error::Io(format!("{}: {e}", dir.display())))?;
        let name = ent.file_name();
        let name_s = name.to_string_lossy();
        if name_s.starts_with('.') {
            continue;
        }
        let p = ent.path();
        if p.is_dir() {
            collect_parquet_files(&p, out)?;
        } else if name_s.ends_with(".parquet") {
            out.push(p);
        }
    }
    Ok(())
}

/// Pull iterator of morsels. CPU default selected by [`CpuLowering`].
pub(crate) enum Exec {
    Empty {
        schema: SchemaRef,
        done: bool,
    },
    Scan {
        batches: Vec<Batch>,
        index: usize,
    },
    Filter {
        input: Box<dyn Operator>,
        predicate: Expr,
    },
    Project {
        input: Box<dyn Operator>,
        exprs: Vec<NamedExpr>,
        schema: SchemaRef,
    },
    Limit {
        input: Box<dyn Operator>,
        skip: u64,
        fetch: Option<u64>,
    },
}

impl CpuLowering {
    pub(crate) fn empty(schema: SchemaRef) -> Result<Box<dyn Operator>> {
        Ok(Box::new(Exec::Empty {
            schema,
            done: false,
        }))
    }

    pub(crate) fn scan(batches: Vec<Batch>) -> Result<Box<dyn Operator>> {
        Ok(Box::new(Exec::Scan { batches, index: 0 }))
    }

    pub(crate) fn filter(input: Box<dyn Operator>, predicate: Expr) -> Result<Box<dyn Operator>> {
        Ok(Box::new(Exec::Filter { input, predicate }))
    }

    pub(crate) fn project(
        input: Box<dyn Operator>,
        exprs: Vec<NamedExpr>,
        schema: SchemaRef,
    ) -> Result<Box<dyn Operator>> {
        Ok(Box::new(Exec::Project {
            input,
            exprs,
            schema,
        }))
    }

    pub(crate) fn limit(
        input: Box<dyn Operator>,
        skip: u64,
        fetch: Option<u64>,
    ) -> Result<Box<dyn Operator>> {
        Ok(Box::new(Exec::Limit { input, skip, fetch }))
    }
}

impl Lowering for CpuLowering {
    fn empty(&self, schema: SchemaRef) -> Result<Box<dyn Operator>> {
        Self::empty(schema)
    }

    fn scan(&self, batches: Vec<Batch>) -> Result<Box<dyn Operator>> {
        Self::scan(batches)
    }

    fn filter(&self, input: Box<dyn Operator>, predicate: Expr) -> Result<Box<dyn Operator>> {
        Self::filter(input, predicate)
    }

    fn project(
        &self,
        input: Box<dyn Operator>,
        exprs: Vec<NamedExpr>,
        schema: SchemaRef,
    ) -> Result<Box<dyn Operator>> {
        Self::project(input, exprs, schema)
    }

    fn limit(
        &self,
        input: Box<dyn Operator>,
        skip: u64,
        fetch: Option<u64>,
    ) -> Result<Box<dyn Operator>> {
        Self::limit(input, skip, fetch)
    }
}

impl Operator for Exec {
    fn next_batch(&mut self, cancel: &CancelToken) -> Result<Option<Batch>> {
        cancel.check()?;
        match self {
            Self::Empty { schema, done } => {
                if *done {
                    return Ok(None);
                }
                *done = true;
                Ok(Some(one_row_empty(schema.clone())?))
            }
            Self::Scan { batches, index } => {
                if *index >= batches.len() {
                    return Ok(None);
                }
                let batch = batches[*index].clone();
                *index += 1;
                Ok(Some(batch))
            }
            Self::Filter { input, predicate } => loop {
                let Some(batch) = input.next_batch(cancel)? else {
                    return Ok(None);
                };
                if let Some(out) = filter_batch(&batch, predicate)? {
                    return Ok(Some(out));
                }
            },
            Self::Project {
                input,
                exprs,
                schema,
            } => {
                let Some(batch) = input.next_batch(cancel)? else {
                    return Ok(None);
                };
                Ok(Some(project_batch(&batch, exprs, schema.clone())?))
            }
            Self::Limit { input, skip, fetch } => limit_next(input.as_mut(), skip, fetch, cancel),
        }
    }
}

fn limit_next(
    input: &mut dyn Operator,
    skip: &mut u64,
    fetch: &mut Option<u64>,
    cancel: &CancelToken,
) -> Result<Option<Batch>> {
    if matches!(fetch, Some(0)) {
        return Ok(None);
    }
    loop {
        let Some(batch) = input.next_batch(cancel)? else {
            return Ok(None);
        };
        let n = batch.num_rows() as u64;
        if *skip >= n {
            *skip -= n;
            continue;
        }
        let start = *skip as usize;
        *skip = 0;
        let available = batch.num_rows() - start;
        let take_n = match *fetch {
            None => available,
            Some(f) => available.min(usize::try_from(f).unwrap_or(usize::MAX)),
        };
        if take_n == 0 {
            return Ok(None);
        }
        if let Some(f) = fetch {
            *f -= take_n as u64;
        }
        return Ok(Some(batch.slice(start, take_n)?));
    }
}

pub(crate) fn physical(
    plan: &LogicalPlan,
    session: &Session,
    pool: &memory::MemoryPool,
) -> Result<Box<dyn Operator>> {
    Ok(build_physical(plan, session, pool, false, None)?.0)
}

pub(crate) fn physical_with_work(
    plan: &LogicalPlan,
    session: &Session,
    pool: &memory::MemoryPool,
    analyze: bool,
    work: Option<(&str, &[Batch])>,
) -> Result<Box<dyn Operator>> {
    Ok(build_physical(plan, session, pool, analyze, work)?.0)
}

pub(crate) fn build_physical(
    plan: &LogicalPlan,
    session: &Session,
    pool: &memory::MemoryPool,
    analyze: bool,
    work: Option<(&str, &[Batch])>,
) -> Result<(Box<dyn Operator>, metrics::PhysNode)> {
    build_physical_filtered(plan, session, pool, analyze, work, &[])
}

fn build_physical_filtered(
    plan: &LogicalPlan,
    session: &Session,
    pool: &memory::MemoryPool,
    analyze: bool,
    work: Option<(&str, &[Batch])>,
    runtime: &[Expr],
) -> Result<(Box<dyn Operator>, metrics::PhysNode)> {
    let lowering = session.lowering.0.as_ref();
    match plan {
        LogicalPlan::Empty { schema } => Ok(metrics::finish(
            "Empty",
            lowering.empty(schema.clone())?,
            Vec::new(),
            analyze,
            pool,
        )),
        LogicalPlan::WorkScan { name, .. } => {
            let batches = match work {
                Some((wname, batches)) if wname == name.as_str() => batches,
                _ => {
                    return Err(Error::Plan(format!("working table {name} is not in scope")));
                }
            };
            let mut morsels = Vec::new();
            for batch in batches {
                morsels.extend(batch.split_by_policy(&session.policy));
            }
            Ok(metrics::finish(
                "WorkScan",
                lowering.scan(morsels)?,
                Vec::new(),
                analyze,
                pool,
            ))
        }
        LogicalPlan::Scan {
            table,
            projection,
            filters,
            schema,
            ..
        } => {
            let table = session
                .tables
                .get(&normalize_name(table))
                .ok_or_else(|| Error::Plan(format!("table not found at execution: {table}")))?;
            let source: Vec<&Batch> = table.batches.iter().collect();
            let mut morsels = Vec::new();
            for batch in source {
                let mut batch = batch.clone();
                let project_first = projection
                    .as_ref()
                    .is_some_and(|names| filters_on_projected(filters, names));
                if project_first && let Some(names) = projection {
                    batch = project_named(batch, names)?;
                }
                for pred in filters {
                    match filter_batch(&batch, pred)? {
                        Some(kept) => batch = kept,
                        None => {
                            batch = Batch::empty(batch.schema())?;
                            break;
                        }
                    }
                }
                if !project_first && let Some(names) = projection {
                    batch = project_named(batch, names)?;
                }
                if !runtime.is_empty() {
                    match runtime_filter::apply_preds(batch, runtime)? {
                        Some(kept) => batch = kept,
                        None => continue,
                    }
                }
                morsels.extend(batch.split_by_policy(&session.policy));
            }
            let (op, mut node) =
                metrics::finish("Scan", lowering.scan(morsels)?, Vec::new(), analyze, pool);
            if let Some(rf) = runtime_detail(runtime, schema.as_ref()) {
                node.detail = rf;
            }
            Ok((op, node))
        }
        LogicalPlan::Filter { input, predicate } => {
            // Do not push runtime preds through a Filter: a join key like
            // `100 / x` may only be safe after this predicate.
            let (child, child_node) =
                build_physical_filtered(input, session, pool, analyze, work, &[])?;
            let mut op = lowering.filter(child, predicate.clone())?;
            for pred in runtime {
                op = lowering.filter(op, pred.clone())?;
            }
            Ok(metrics::finish(
                "Filter",
                op,
                vec![child_node],
                analyze,
                pool,
            ))
        }
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            let rewritten: Option<Vec<Expr>> = runtime
                .iter()
                .map(|e| runtime_filter::rewrite_through_project(e, exprs))
                .collect();
            let (child_runtime, leftover) = match rewritten {
                Some(preds) => (preds, Vec::new()),
                None => (Vec::new(), runtime.to_vec()),
            };
            let (child, child_node) =
                build_physical_filtered(input, session, pool, analyze, work, &child_runtime)?;
            let projected = lowering.project(child, exprs.clone(), schema.clone())?;
            if leftover.is_empty() {
                Ok(metrics::finish(
                    "Project",
                    projected,
                    vec![child_node],
                    analyze,
                    pool,
                ))
            } else {
                let mut op = projected;
                for pred in leftover {
                    op = lowering.filter(op, pred)?;
                }
                Ok(metrics::finish(
                    "Project",
                    op,
                    vec![child_node],
                    analyze,
                    pool,
                ))
            }
        }
        LogicalPlan::Aggregate {
            input,
            group,
            aggs,
            schema,
        } => {
            let (child, child_node) = build_physical(input, session, pool, analyze, work)?;
            let t0 = Instant::now();
            let copied0 = pool.copied();
            let batches = agg::run(
                child,
                group,
                aggs,
                schema.clone(),
                &session.policy,
                pool,
                &session.exec_token(),
            )?;
            let ns = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let (op, node) = metrics::finish(
                "HashAggregate",
                lowering.scan(batches)?,
                vec![child_node],
                analyze,
                pool,
            );
            if analyze {
                metrics::add_blocking(
                    &node,
                    ns,
                    pool.peak(),
                    pool.copied().saturating_sub(copied0),
                );
            }
            Ok((op, node))
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            on,
            schema,
        } => {
            // Keep parent runtime preds above this join. A fallible parent
            // key like `100 / fact.k` may only be safe after this join
            // removes rows that would fail.
            let left_ncols = left.schema().fields().len();
            let (right_op, right_node) =
                build_physical_filtered(right, session, pool, analyze, work, &[])?;
            let mut build_ns = 0u64;
            let mut build_copied = 0u64;
            let (left_op, left_node, right_op) = if *join_type == mizuchi_plan::JoinType::Inner
                && session.runtime_join_filters
                && !pool.has_limit()
            {
                let t_build = Instant::now();
                let copied_build = pool.copied();
                let right_batches = collect(right_op, &session.exec_token())?;
                let mut left_runtime = Vec::new();
                if let Some(rf) = runtime_filter::from_build(
                    on.as_ref(),
                    left_ncols,
                    left.schema().as_ref(),
                    &right_batches,
                ) {
                    left_runtime.extend(rf.preds);
                }
                build_ns = u64::try_from(t_build.elapsed().as_nanos()).unwrap_or(u64::MAX);
                build_copied = pool.copied().saturating_sub(copied_build);
                // Probe-side construction stays outside this join's timer so
                // nested blocking children keep disjoint exclusive time.
                let (left_op, left_node) =
                    build_physical_filtered(left, session, pool, analyze, work, &left_runtime)?;
                (left_op, left_node, lowering.scan(right_batches)?)
            } else {
                let (left_op, left_node) =
                    build_physical_filtered(left, session, pool, analyze, work, &[])?;
                (left_op, left_node, right_op)
            };
            let t0 = Instant::now();
            let copied0 = pool.copied();
            let batches = join::run(
                left_op,
                right_op,
                join::JoinSpec {
                    join_type: *join_type,
                    on: on.as_ref(),
                    left_schema: left.schema().clone(),
                    right_schema: right.schema().clone(),
                    schema: schema.clone(),
                },
                &session.policy,
                pool,
                &session.exec_token(),
                session.workers,
            )?;
            let mut joined = lowering.scan(batches)?;
            for pred in runtime {
                joined = lowering.filter(joined, pred.clone())?;
            }
            let ns = u64::try_from(t0.elapsed().as_nanos())
                .unwrap_or(u64::MAX)
                .saturating_add(build_ns);
            let (op, node) = metrics::finish(
                "HashJoin",
                joined,
                vec![left_node, right_node],
                analyze,
                pool,
            );
            if analyze {
                metrics::add_blocking(
                    &node,
                    ns,
                    pool.peak(),
                    pool.copied()
                        .saturating_sub(copied0)
                        .saturating_add(build_copied),
                );
            }
            Ok((op, node))
        }
        LogicalPlan::Sort { input, order } => {
            let (child, child_node) = build_physical(input, session, pool, analyze, work)?;
            let t0 = Instant::now();
            let copied0 = pool.copied();
            let batches = sort::ordered(
                child,
                input.schema().clone(),
                order,
                0,
                None,
                &session.policy,
                pool,
                &session.exec_token(),
            )?;
            let ns = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let (op, node) = metrics::finish(
                "Sort",
                lowering.scan(batches)?,
                vec![child_node],
                analyze,
                pool,
            );
            if analyze {
                metrics::add_blocking(
                    &node,
                    ns,
                    pool.peak(),
                    pool.copied().saturating_sub(copied0),
                );
            }
            Ok((op, node))
        }
        LogicalPlan::Limit { input, skip, fetch } => match input.as_ref() {
            LogicalPlan::Sort {
                input: sorted,
                order,
            } => {
                let (child, child_node) = build_physical(sorted, session, pool, analyze, work)?;
                let t0 = Instant::now();
                let copied0 = pool.copied();
                let batches = sort::ordered(
                    child,
                    sorted.schema().clone(),
                    order,
                    *skip,
                    *fetch,
                    &session.policy,
                    pool,
                    &session.exec_token(),
                )?;
                let ns = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
                let (op, node) = metrics::finish(
                    "Sort",
                    lowering.scan(batches)?,
                    vec![child_node],
                    analyze,
                    pool,
                );
                if analyze {
                    metrics::add_blocking(
                        &node,
                        ns,
                        pool.peak(),
                        pool.copied().saturating_sub(copied0),
                    );
                }
                Ok((op, node))
            }
            _ => {
                let (child, child_node) = build_physical(input, session, pool, analyze, work)?;
                Ok(metrics::finish(
                    "Limit",
                    lowering.limit(child, *skip, *fetch)?,
                    vec![child_node],
                    analyze,
                    pool,
                ))
            }
        },
        LogicalPlan::Union {
            inputs,
            schema,
            all,
        } => {
            let mut ops = Vec::with_capacity(inputs.len());
            let mut children = Vec::with_capacity(inputs.len());
            for input in inputs {
                let (op, node) = build_physical(input, session, pool, analyze, work)?;
                ops.push(op);
                children.push(node);
            }
            let t0 = Instant::now();
            let copied0 = pool.copied();
            let batches = setop::union(
                ops,
                schema.clone(),
                *all,
                &session.policy,
                pool,
                &session.exec_token(),
            )?;
            let ns = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let (op, node) =
                metrics::finish("Union", lowering.scan(batches)?, children, analyze, pool);
            if analyze {
                metrics::add_blocking(
                    &node,
                    ns,
                    pool.peak(),
                    pool.copied().saturating_sub(copied0),
                );
            }
            Ok((op, node))
        }
        LogicalPlan::Intersect {
            left,
            right,
            schema,
            all,
        } => {
            let (left_op, left_node) = build_physical(left, session, pool, analyze, work)?;
            let (right_op, right_node) = build_physical(right, session, pool, analyze, work)?;
            let t0 = Instant::now();
            let copied0 = pool.copied();
            let batches = setop::intersect(
                left_op,
                right_op,
                schema.clone(),
                *all,
                &session.policy,
                pool,
                &session.exec_token(),
            )?;
            let ns = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let (op, node) = metrics::finish(
                "Intersect",
                lowering.scan(batches)?,
                vec![left_node, right_node],
                analyze,
                pool,
            );
            if analyze {
                metrics::add_blocking(
                    &node,
                    ns,
                    pool.peak(),
                    pool.copied().saturating_sub(copied0),
                );
            }
            Ok((op, node))
        }
        LogicalPlan::Except {
            left,
            right,
            schema,
            all,
        } => {
            let (left_op, left_node) = build_physical(left, session, pool, analyze, work)?;
            let (right_op, right_node) = build_physical(right, session, pool, analyze, work)?;
            let t0 = Instant::now();
            let copied0 = pool.copied();
            let batches = setop::except(
                left_op,
                right_op,
                schema.clone(),
                *all,
                &session.policy,
                pool,
                &session.exec_token(),
            )?;
            let ns = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let (op, node) = metrics::finish(
                "Except",
                lowering.scan(batches)?,
                vec![left_node, right_node],
                analyze,
                pool,
            );
            if analyze {
                metrics::add_blocking(
                    &node,
                    ns,
                    pool.peak(),
                    pool.copied().saturating_sub(copied0),
                );
            }
            Ok((op, node))
        }
        LogicalPlan::Recursive {
            name,
            anchors,
            step,
            schema,
        } => {
            let t0 = Instant::now();
            let copied0 = pool.copied();
            let batches = recursive::run(name, anchors, step, schema, session, pool, analyze)?;
            let ns = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let mut children: Vec<metrics::PhysNode> =
                anchors.iter().map(metrics::physical_shape).collect();
            children.push(metrics::physical_shape(step));
            let (op, node) = metrics::finish(
                "Recursive",
                lowering.scan(batches)?,
                children,
                analyze,
                pool,
            );
            if analyze {
                metrics::add_blocking(
                    &node,
                    ns,
                    pool.peak(),
                    pool.copied().saturating_sub(copied0),
                );
            }
            Ok((op, node))
        }
        LogicalPlan::Window {
            input,
            partition,
            order,
            frame,
            fns,
            schema,
        } => {
            let (child, child_node) = build_physical(input, session, pool, analyze, work)?;
            let t0 = Instant::now();
            let copied0 = pool.copied();
            let batches = window::run(
                child,
                input.schema().clone(),
                partition,
                order,
                *frame,
                fns,
                schema.clone(),
                &session.policy,
                pool,
                &session.exec_token(),
            )?;
            let ns = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let (op, node) = metrics::finish(
                "Window",
                lowering.scan(batches)?,
                vec![child_node],
                analyze,
                pool,
            );
            if analyze {
                metrics::add_blocking(
                    &node,
                    ns,
                    pool.peak(),
                    pool.copied().saturating_sub(copied0),
                );
            }
            Ok((op, node))
        }
        LogicalPlan::Scalar { input, schema } => {
            let (child, child_node) = build_physical(input, session, pool, analyze, work)?;
            let t0 = Instant::now();
            let copied0 = pool.copied();
            let batches = collect(child, &session.exec_token())?;
            let out = scalar_one_row(batches, schema.clone())?;
            let ns = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let (op, node) = metrics::finish(
                "Scalar",
                lowering.scan(vec![out])?,
                vec![child_node],
                analyze,
                pool,
            );
            if analyze {
                metrics::add_blocking(
                    &node,
                    ns,
                    pool.peak(),
                    pool.copied().saturating_sub(copied0),
                );
            }
            Ok((op, node))
        }
    }
}

pub(crate) fn scalar_one_row(batches: Vec<Batch>, schema: SchemaRef) -> Result<Batch> {
    let n: usize = batches.iter().map(Batch::num_rows).sum();
    if n > 1 {
        return Err(Error::Eval(
            "scalar subquery returned more than one row".into(),
        ));
    }
    if n == 0 {
        let cols: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|f| new_null_array(f.data_type(), 1))
            .collect();
        return Batch::try_new(schema, cols);
    }
    batches
        .into_iter()
        .find(|b| b.num_rows() > 0)
        .ok_or_else(|| Error::Eval("scalar subquery row missing".into()))
}

fn explain_sql(sql: &str) -> Result<Option<(bool, String)>> {
    if !sql
        .trim_start()
        .get(..7)
        .is_some_and(|s| s.eq_ignore_ascii_case("EXPLAIN"))
    {
        return Ok(None);
    }
    let stmt = mizuchi_plan::parse_statements(sql)?;
    match stmt {
        sqlparser::ast::Statement::Explain {
            analyze,
            statement,
            describe_alias,
            ..
        } => {
            if !matches!(describe_alias, sqlparser::ast::DescribeAlias::Explain) {
                return Err(Error::NotImplemented("DESCRIBE"));
            }
            Ok(Some((analyze, statement.to_string())))
        }
        _ => Ok(None),
    }
}

fn plan_text_batch(text: &str) -> Result<Vec<Batch>> {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("plan", arrow::datatypes::DataType::Utf8, false),
    ]));
    let batch = Batch::try_new(
        schema,
        vec![Arc::new(arrow::array::StringArray::from(vec![Some(text)]))],
    )?;
    Ok(vec![batch])
}

pub(crate) fn collect(mut exec: Box<dyn Operator>, cancel: &CancelToken) -> Result<Vec<Batch>> {
    let mut out = Vec::new();
    while let Some(batch) = exec.next_batch(cancel)? {
        if batch.num_rows() > 0 {
            out.push(batch);
        }
    }
    Ok(out)
}

fn runtime_detail(runtime: &[Expr], schema: &Schema) -> Option<String> {
    if runtime.is_empty() {
        return None;
    }
    let summary = runtime_filter::RuntimeFilter {
        preds: runtime.to_vec(),
        summary: String::new(),
    };
    Some(runtime_filter::format_detail(&summary, schema))
}

pub(crate) fn filter_batch(batch: &Batch, predicate: &Expr) -> Result<Option<Batch>> {
    let mask = predicate.eval(batch)?;
    let bools = mask
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| Error::Eval("filter predicate is not boolean".into()))?;
    let not_null = is_not_null(bools).map_err(|e| Error::Eval(e.to_string()))?;
    let keep = and(bools, &not_null).map_err(|e| Error::Eval(e.to_string()))?;
    let filtered =
        filter_record_batch(batch.as_arrow(), &keep).map_err(|e| Error::Eval(e.to_string()))?;
    if filtered.num_rows() == 0 {
        return Ok(None);
    }
    Ok(Some(Batch::from_arrow(filtered)?))
}

pub(crate) fn project_named(batch: Batch, names: &[String]) -> Result<Batch> {
    let mut indices = Vec::with_capacity(names.len());
    for name in names {
        let idx = batch
            .schema()
            .index_of(name)
            .map_err(|e| Error::Plan(format!("scan projection: {e}")))?;
        indices.push(idx);
    }
    batch.project(&indices)
}

pub(crate) fn filters_on_projected(filters: &[Expr], names: &[String]) -> bool {
    fn walk(expr: &Expr, names: &[String]) -> bool {
        match expr {
            Expr::Nth(_) => false,
            Expr::Column(name) => names.iter().any(|n| n == name),
            Expr::Literal(_) => true,
            Expr::Binary(_, l, r) => walk(l, names) && walk(r, names),
            Expr::Unary(_, c) | Expr::Cast { expr: c, .. } | Expr::Substring { expr: c, .. } => {
                walk(c, names)
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand.as_deref().is_none_or(|e| walk(e, names))
                    && when_then
                        .iter()
                        .all(|(w, t)| walk(w, names) && walk(t, names))
                    && else_expr.as_deref().is_none_or(|e| walk(e, names))
            }
            Expr::Coalesce(args) | Expr::Concat(args) | Expr::Array(args) => {
                args.iter().all(|e| walk(e, names))
            }
        }
    }
    filters.iter().all(|f| walk(f, names))
}

pub(crate) fn project_batch(
    batch: &Batch,
    exprs: &[NamedExpr],
    schema: SchemaRef,
) -> Result<Batch> {
    let mut columns = Vec::with_capacity(exprs.len());
    for named in exprs {
        columns.push(named.expr.eval(batch)?);
    }
    Batch::try_new(schema, columns)
}

fn one_row_empty(schema: SchemaRef) -> Result<Batch> {
    let inner = RecordBatch::try_new_with_options(
        schema,
        Vec::new(),
        &RecordBatchOptions::default().with_row_count(Some(1)),
    )
    .map_err(|err| Error::InvalidBatch(err.to_string()))?;
    Batch::from_arrow(inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, ArrayRef, Date32Array, DictionaryArray, Float64Array, Int32Array, Int64Array,
        LargeStringArray, StringArray, UInt32Array,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use mizuchi_core::udaf::ProductI64;
    use std::collections::{BTreeMap, HashSet};
    use std::sync::Arc;

    fn table() -> (SchemaRef, Vec<Batch>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), None])),
                Arc::new(StringArray::from(vec![
                    Some("x"),
                    Some("y"),
                    Some("z"),
                    Some("n"),
                ])),
            ],
        )
        .unwrap();
        (schema, vec![batch])
    }

    fn session() -> Session {
        let (schema, batches) = table();
        let mut s = Session::new();
        s.register("t", schema, batches).unwrap();
        s
    }

    fn col_i64(batches: &[Batch], name: &str) -> Vec<Option<i64>> {
        let mut out = Vec::new();
        for batch in batches {
            let col = batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..col.len() {
                if col.is_null(i) {
                    out.push(None);
                } else {
                    out.push(Some(col.value(i)));
                }
            }
        }
        out
    }

    fn col_f64(batches: &[Batch], name: &str) -> Vec<Option<f64>> {
        let mut out = Vec::new();
        for batch in batches {
            let col = batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for i in 0..col.len() {
                if col.is_null(i) {
                    out.push(None);
                } else {
                    out.push(Some(col.value(i)));
                }
            }
        }
        out
    }

    fn col_utf8(batches: &[Batch], name: &str) -> Vec<Option<String>> {
        let mut out = Vec::new();
        for batch in batches {
            let col = batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..col.len() {
                if col.is_null(i) {
                    out.push(None);
                } else {
                    out.push(Some(col.value(i).to_owned()));
                }
            }
        }
        out
    }

    #[test]
    fn scan_projects_and_filters_end_to_end() {
        let out = session()
            .execute_sql("SELECT a FROM t WHERE a > 1")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(2), Some(3)]);
    }

    #[test]
    fn explain_prints_logical_and_physical() {
        let text = session()
            .explain("SELECT a FROM t WHERE a > 1 ORDER BY a LIMIT 2")
            .unwrap();
        assert!(text.contains("== Logical Plan =="), "{text}");
        assert!(text.contains("== Physical Plan =="), "{text}");
        assert!(text.contains("Scan"), "{text}");
        assert!(text.contains("Sort"), "{text}");
        assert!(
            !text.contains("elapsed_ns="),
            "EXPLAIN must not run metrics: {text}"
        );
    }

    #[test]
    fn analyze_populates_per_operator_metrics() {
        let s = join_session();
        let sql = "SELECT t.a, COUNT(*) FROM t JOIN u ON t.a = u.id GROUP BY t.a";
        let text = s.explain_analyze(sql).unwrap();
        assert!(text.contains("HashJoin"), "{text}");
        assert!(text.contains("HashAggregate"), "{text}");
        assert!(text.contains("rows="), "{text}");
        assert!(text.contains("batches="), "{text}");
        assert!(text.contains("elapsed_ns="), "{text}");
        assert!(text.contains("exclusive_ns="), "{text}");
        assert!(text.contains("peak_bytes="), "{text}");
        assert!(text.contains("copied_bytes="), "{text}");

        let metrics = s.query_metrics(sql).unwrap();
        assert_eq!(metrics.name, "HashAggregate");
        assert!(metrics.rows > 0, "{metrics:?}");
        assert!(metrics.batches > 0, "{metrics:?}");
        assert!(metrics.elapsed_ns > 0, "{metrics:?}");
        assert_eq!(metrics.children.len(), 1);
        assert_eq!(metrics.children[0].name, "HashJoin");
        assert!(metrics.blocking, "HashAggregate is a blocking build");
        assert!(metrics.children[0].blocking, "HashJoin is a blocking build");
        let nested: u64 = metrics
            .children
            .iter()
            .filter(|c| !c.blocking)
            .map(|c| c.elapsed_ns)
            .sum();
        assert_eq!(
            metrics.exclusive_ns(),
            metrics.elapsed_ns.saturating_sub(nested)
        );
        let ranked = metrics.ranked_exclusive();
        assert!(!ranked.is_empty());
        assert!(ranked.iter().any(|n| n.name == "HashJoin"), "{ranked:?}");
        let json = metrics.to_json();
        assert!(json.contains("\"name\":\"HashAggregate\""), "{json}");
        assert!(json.contains("\"name\":\"HashJoin\""), "{json}");
        assert!(json.contains("\"rows\":"), "{json}");
        assert!(json.contains("\"exclusive_ns\":"), "{json}");
        assert!(json.contains("\"copied_bytes\":"), "{json}");
    }

    #[test]
    fn join_skips_concat_of_probe_morsels() {
        let build_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let build = Batch::try_new(
            build_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
        )
        .unwrap();
        let probe_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let mut probe = Vec::new();
        for i in 0..8 {
            probe.push(
                Batch::try_new(
                    probe_schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![i + 1])),
                        Arc::new(Int64Array::from(vec![i * 10])),
                    ],
                )
                .unwrap(),
            );
        }
        let mut s = Session::new();
        s.register("dim", build_schema, vec![build]).unwrap();
        s.register("fact", probe_schema, probe).unwrap();
        let sql = "SELECT dim.id, fact.v FROM dim JOIN fact ON dim.id = fact.k";
        let out = s.execute_sql(sql).unwrap();
        let n: usize = out.iter().map(Batch::num_rows).sum();
        assert_eq!(n, 3);
        let metrics = s.query_metrics(sql).unwrap();
        let text = s.explain_analyze(sql).unwrap();
        assert!(text.contains("copied_bytes="), "{text}");
        let mut nodes = Vec::new();
        metrics.walk(&mut nodes);
        let join = nodes
            .iter()
            .find(|n| n.name == "HashJoin")
            .expect("HashJoin");
        assert_eq!(
            join.copied_bytes, 0,
            "probe morsels must not be concatenated: {join:?}"
        );
    }

    #[test]
    fn left_join_keeps_unmatched_rows_once_across_probe_morsels() {
        let left_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let left = Batch::try_new(
            left_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 99]))],
        )
        .unwrap();
        let right_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let right = vec![
            Batch::try_new(
                right_schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1_i64]))],
            )
            .unwrap(),
            Batch::try_new(
                right_schema.clone(),
                vec![Arc::new(Int64Array::from(vec![2_i64]))],
            )
            .unwrap(),
        ];
        let mut s = Session::new();
        s.register("dim", left_schema, vec![left]).unwrap();
        s.register("fact", right_schema, right).unwrap();
        let out = s
            .execute_sql("SELECT dim.id, fact.k FROM dim LEFT JOIN fact ON dim.id = fact.k")
            .unwrap();
        let mut pairs: Vec<(Option<i64>, Option<i64>)> = Vec::new();
        for batch in &out {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let ks = batch
                .column_by_name("k")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                pairs.push((
                    if ids.is_null(i) {
                        None
                    } else {
                        Some(ids.value(i))
                    },
                    if ks.is_null(i) {
                        None
                    } else {
                        Some(ks.value(i))
                    },
                ));
            }
        }
        pairs.sort();
        assert_eq!(
            pairs,
            vec![(Some(1), Some(1)), (Some(2), Some(2)), (Some(99), None),]
        );
    }

    #[test]
    fn execute_sql_explain_returns_plan_text() {
        let out = session()
            .execute_sql("EXPLAIN SELECT a FROM t LIMIT 1")
            .unwrap();
        let plan = col_utf8(&out, "plan");
        assert_eq!(plan.len(), 1);
        let text = plan[0].as_ref().unwrap();
        assert!(text.contains("== Logical Plan =="), "{text}");
        assert!(text.contains("Limit"), "{text}");
        let analyzed = session()
            .execute_sql("EXPLAIN ANALYZE SELECT a FROM t LIMIT 1")
            .unwrap();
        let text = col_utf8(&analyzed, "plan")[0].as_ref().unwrap().clone();
        assert!(text.contains("elapsed_ns="), "{text}");
        assert!(text.contains("Limit"), "{text}");
    }

    #[test]
    fn execute_sql_without_explain_still_returns_rows() {
        let out = session()
            .execute_sql("SELECT a FROM t WHERE a > 1")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(2), Some(3)]);
    }

    #[test]
    fn coalesce_filter_after_reordering_projection() {
        let out = session()
            .execute_sql("SELECT a FROM (SELECT b, a FROM t) s WHERE COALESCE(a, 0) > 1")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(2), Some(3)]);
    }

    #[test]
    fn filter_drops_null_predicates() {
        let out = session()
            .execute_sql("SELECT a FROM t WHERE a = 1")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1)]);
    }

    #[test]
    fn projection_renames() {
        let out = session()
            .execute_sql("SELECT a + 1 AS x FROM t WHERE a IS NOT NULL")
            .unwrap();
        assert_eq!(col_i64(&out, "x"), vec![Some(2), Some(3), Some(4)]);
    }

    #[test]
    fn select_literal_from_empty_relation() {
        let out = Session::new().execute_sql("SELECT 1 AS x").unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 1);
        assert_eq!(col_i64(&out, "x"), vec![Some(1)]);
    }

    fn join_session() -> Session {
        let t_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let t = Batch::try_new(
            t_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(2),
                Some(3),
                None,
            ]))],
        )
        .unwrap();
        let u_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("x", DataType::Int64, true),
        ]));
        let u = Batch::try_new(
            u_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(2), Some(2), Some(4), None])),
                Arc::new(Int64Array::from(vec![
                    Some(10),
                    Some(30),
                    Some(40),
                    Some(99),
                ])),
            ],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", t_schema, vec![t]).unwrap();
        s.register("u", u_schema, vec![u]).unwrap();
        s
    }

    fn sorted_pairs(batches: &[Batch], left: &str, right: &str) -> Vec<(Option<i64>, Option<i64>)> {
        let mut rows: Vec<(Option<i64>, Option<i64>)> = col_i64(batches, left)
            .into_iter()
            .zip(col_i64(batches, right))
            .collect();
        rows.sort_by_key(|(l, r)| (l.unwrap_or(i64::MIN), r.unwrap_or(i64::MIN)));
        rows
    }

    /// Independent nested-loop reference with SQL null-equality (null never matches).
    fn reference_join(kind: &str) -> Vec<(Option<i64>, Option<i64>)> {
        let left = [Some(1i64), Some(2), Some(2), Some(3), None];
        let right = [Some(2i64), Some(2), Some(4), None];
        let mut out = Vec::new();
        let mut right_hit = [false; 4];
        for l in left {
            let mut matched = false;
            for (ri, r) in right.iter().enumerate() {
                let eq = matches!((l, r), (Some(a), Some(b)) if a == *b);
                if eq {
                    matched = true;
                    right_hit[ri] = true;
                    if kind != "semi" && kind != "anti" {
                        out.push((l, *r));
                    }
                }
            }
            match kind {
                "semi" if matched => out.push((l, l)),
                "anti" if !matched => out.push((l, l)),
                "left" | "full" if !matched => out.push((l, None)),
                _ => {}
            }
        }
        if kind == "right" || kind == "full" {
            for (ri, r) in right.iter().enumerate() {
                if !right_hit[ri] {
                    out.push((None, *r));
                }
            }
        }
        out.sort_by_key(|(l, r)| (l.unwrap_or(i64::MIN), r.unwrap_or(i64::MIN)));
        out
    }

    #[test]
    fn inner_join_matches_when_either_side_is_smaller() {
        let small: Vec<Option<i64>> = vec![Some(0), Some(1), Some(2)];
        let large: Vec<Option<i64>> = (0..100).map(|i| Some(i * 2)).collect();
        fn join(
            left_vals: &[Option<i64>],
            right_vals: &[Option<i64>],
        ) -> Vec<(Option<i64>, Option<i64>)> {
            let l_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
            let r_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
            let left = Batch::try_new(
                l_schema.clone(),
                vec![Arc::new(Int64Array::from(left_vals.to_vec()))],
            )
            .unwrap();
            let right = Batch::try_new(
                r_schema.clone(),
                vec![Arc::new(Int64Array::from(right_vals.to_vec()))],
            )
            .unwrap();
            let mut s = Session::new();
            s.register("t", l_schema, vec![left]).unwrap();
            s.register("u", r_schema, vec![right]).unwrap();
            let out = s
                .execute_sql("SELECT t.a, u.id FROM t JOIN u ON t.a = u.id")
                .unwrap();
            sorted_pairs(&out, "a", "id")
        }
        let a = join(&small, &large);
        let b = join(&large, &small);
        assert_eq!(a, b);
        assert_eq!(a, vec![(Some(0), Some(0)), (Some(2), Some(2))]);
    }

    fn fact_dim_session() -> Session {
        let dim_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let dim = Batch::try_new(
            dim_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![
                Some(10),
                Some(20),
                Some(30),
            ]))],
        )
        .unwrap();
        let fact_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let fact = Batch::try_new(
            fact_schema.clone(),
            vec![
                Arc::new(Int64Array::from((1..=100).map(Some).collect::<Vec<_>>())),
                Arc::new(Int64Array::from((1..=100).map(Some).collect::<Vec<_>>())),
            ],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("dim", dim_schema, vec![dim]).unwrap();
        s.register("fact", fact_schema, vec![fact]).unwrap();
        s
    }

    fn scan_rows(text: &str, needle: &str) -> Option<u64> {
        for line in text.lines() {
            if line.contains("Scan") && line.contains(needle) && line.contains("rows=") {
                let after = line.split("rows=").nth(1)?;
                let n = after.split_whitespace().next()?;
                return n.parse().ok();
            }
        }
        None
    }

    #[test]
    fn runtime_filter_int32_join_stays_correct() {
        let dim_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let dim = Batch::try_new(
            dim_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(10), Some(20)]))],
        )
        .unwrap();
        let fact_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]));
        let fact = Batch::try_new(
            fact_schema.clone(),
            vec![Arc::new(Int32Array::from(
                (1..=40).map(Some).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("dim", dim_schema, vec![dim]).unwrap();
        s.register("fact", fact_schema, vec![fact]).unwrap();
        let sql = "SELECT fact.k, dim.id FROM fact JOIN dim ON fact.k = dim.id";
        let off = s.clone().with_runtime_join_filters(false);
        let on_n: usize = s
            .execute_sql(sql)
            .unwrap()
            .iter()
            .map(Batch::num_rows)
            .sum();
        let off_n: usize = off
            .execute_sql(sql)
            .unwrap()
            .iter()
            .map(Batch::num_rows)
            .sum();
        assert_eq!(on_n, 2);
        assert_eq!(on_n, off_n);
        let text = s.explain_analyze(sql).unwrap();
        assert!(text.contains("runtime_filter="), "{text}");
    }

    #[test]
    fn runtime_filter_join_matches_with_filters_on_and_off() {
        let sql = "SELECT fact.k, dim.id FROM fact JOIN dim ON fact.k = dim.id";
        let on = fact_dim_session();
        let off = fact_dim_session().with_runtime_join_filters(false);
        let a = sorted_pairs(&on.execute_sql(sql).unwrap(), "k", "id");
        let b = sorted_pairs(&off.execute_sql(sql).unwrap(), "k", "id");
        assert_eq!(
            a,
            vec![
                (Some(10), Some(10)),
                (Some(20), Some(20)),
                (Some(30), Some(30))
            ]
        );
        assert_eq!(a, b);
    }

    #[test]
    fn runtime_filter_explain_analyze_reduces_probe_scan_rows() {
        let sql = "SELECT fact.k, dim.id FROM fact JOIN dim ON fact.k = dim.id";
        let on = fact_dim_session();
        let off = fact_dim_session().with_runtime_join_filters(false);
        let on_text = on.explain_analyze(sql).unwrap();
        let off_text = off.explain_analyze(sql).unwrap();
        assert!(
            on_text.contains("runtime_filter="),
            "expected runtime filter on scan:\n{on_text}"
        );
        assert!(
            !off_text.contains("runtime_filter="),
            "filters off must not label a runtime filter:\n{off_text}"
        );
        let on_rows = scan_rows(&on_text, "runtime_filter=").expect(&on_text);
        let off_rows = off_text
            .lines()
            .filter(|l| l.contains("Scan") && l.contains("rows="))
            .filter_map(|l| {
                l.split("rows=")
                    .nth(1)?
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
            })
            .max()
            .expect(&off_text);
        assert!(
            on_rows < off_rows,
            "probe scan rows on={on_rows} off={off_rows}\non:\n{on_text}\noff:\n{off_text}"
        );
        assert_eq!(on_rows, 3, "{on_text}");
    }

    #[test]
    fn runtime_filter_parallel_path_matches_pull_path() {
        let sql = "SELECT fact.k, dim.id FROM fact JOIN dim ON fact.k = dim.id";
        let pull = fact_dim_session();
        let parallel = fact_dim_session().with_workers(2).unwrap();
        let a = sorted_pairs(&pull.execute_sql(sql).unwrap(), "k", "id");
        let b = sorted_pairs(&parallel.execute_sql(sql).unwrap(), "k", "id");
        assert_eq!(a, b);
        let text = parallel.explain_analyze(sql).unwrap();
        assert!(text.contains("runtime_filter="), "{text}");
    }

    #[test]
    fn runtime_filter_does_not_eval_join_key_before_guarding_filter() {
        let dim_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let dim = Batch::try_new(
            dim_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(10), Some(5)]))],
        )
        .unwrap();
        let fact_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let fact = Batch::try_new(
            fact_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![
                Some(0),
                Some(10),
                Some(20),
            ]))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("dim", dim_schema, vec![dim]).unwrap();
        s.register("fact", fact_schema, vec![fact]).unwrap();
        let sql = "SELECT t.k, dim.id FROM (SELECT k, k + 1 AS y FROM fact) t \
                   JOIN dim ON (100 / t.k) = dim.id WHERE t.y <> 1";
        let out = s
            .execute_sql(sql)
            .unwrap_or_else(|e| panic!("guarded division must not fail: {e}"));
        let n: usize = out.iter().map(Batch::num_rows).sum();
        assert_eq!(n, 2);
    }

    #[test]
    fn runtime_filter_does_not_eval_join_key_before_guarding_join() {
        let dim_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let dim = Batch::try_new(
            dim_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(10)]))],
        )
        .unwrap();
        let guard_schema = Arc::new(Schema::new(vec![Field::new("g", DataType::Int64, true)]));
        let guard = Batch::try_new(
            guard_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(10)]))],
        )
        .unwrap();
        let fact_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let fact = Batch::try_new(
            fact_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(0), Some(10)]))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("dim", dim_schema, vec![dim]).unwrap();
        s.register("guard", guard_schema, vec![guard]).unwrap();
        s.register("fact", fact_schema, vec![fact]).unwrap();
        let sql = "SELECT fact.k, dim.id FROM fact \
                   JOIN guard ON fact.k = guard.g \
                   JOIN dim ON (100 / fact.k) = dim.id";
        let out = s
            .execute_sql(sql)
            .unwrap_or_else(|e| panic!("guarded nested join must not fail: {e}"));
        let n: usize = out.iter().map(Batch::num_rows).sum();
        assert_eq!(n, 1);
        let parallel = s.with_workers(2).unwrap();
        let out = parallel
            .execute_sql(sql)
            .unwrap_or_else(|e| panic!("parallel guarded nested join must not fail: {e}"));
        let n: usize = out.iter().map(Batch::num_rows).sum();
        assert_eq!(n, 1);
    }

    #[test]
    fn runtime_filter_left_join_does_not_drop_probe_rows() {
        let sql = "SELECT fact.k FROM fact LEFT JOIN dim ON fact.k = dim.id";
        let s = fact_dim_session();
        let out = s.execute_sql(sql).unwrap();
        let n: usize = out.iter().map(Batch::num_rows).sum();
        assert_eq!(n, 100);
        let text = s.explain_analyze(sql).unwrap();
        assert!(
            !text.contains("runtime_filter="),
            "left join must not filter the probe:\n{text}"
        );
    }

    #[test]
    fn runtime_filter_q3_and_store_sales_scans() {
        // Tiny q3 / store_sales shape: filtered dim on the build side, large fact
        // as probe. Same pattern as tpch_q3 (orders/lineitem) and tpcds_store_sales.
        let dim_schema = Arc::new(Schema::new(vec![
            Field::new("d_date_sk", DataType::Int64, true),
            Field::new("d_year", DataType::Int64, true),
        ]));
        let dim = Batch::try_new(
            dim_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4)])),
                Arc::new(Int64Array::from(vec![
                    Some(1993),
                    Some(1994),
                    Some(1994),
                    Some(1995),
                ])),
            ],
        )
        .unwrap();
        let fact_schema = Arc::new(Schema::new(vec![
            Field::new("ss_sold_date_sk", DataType::Int64, true),
            Field::new("ss_item_sk", DataType::Int64, true),
        ]));
        let fact = Batch::try_new(
            fact_schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    (0..40).map(|i| Some(1 + i % 4)).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    (0..40).map(|i| Some(1 + i % 8)).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("date_dim", dim_schema, vec![dim]).unwrap();
        s.register("store_sales", fact_schema, vec![fact]).unwrap();
        let sql = "SELECT COUNT(*) AS n FROM store_sales JOIN date_dim \
                   ON ss_sold_date_sk = d_date_sk WHERE d_year = 1994";
        let off = s.clone().with_runtime_join_filters(false);
        let on_n = col_i64(&s.execute_sql(sql).unwrap(), "n");
        let off_n = col_i64(&off.execute_sql(sql).unwrap(), "n");
        assert_eq!(on_n, off_n);
        let text = s.explain_analyze(sql).unwrap();
        assert!(
            text.contains("runtime_filter="),
            "store_sales probe must show the filter:\n{text}"
        );
        let rows = scan_rows(&text, "runtime_filter=").expect(&text);
        assert!(rows < 40, "store_sales scan rows={rows}\n{text}");
    }

    #[test]
    fn runtime_filter_parquet_direct_skips_row_groups() {
        use mizuchi_io::{ColumnRange, RowGroupStats, row_group_cannot_match};
        let dim_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let dim = Batch::try_new(
            dim_schema,
            vec![Arc::new(Int64Array::from(vec![Some(1), Some(2)]))],
        )
        .unwrap();
        let rf = runtime_filter::from_build(
            Some(&Expr::Binary(
                mizuchi_core::expr::BinaryOp::Eq,
                Box::new(Expr::nth(0)),
                Box::new(Expr::nth(1)),
            )),
            1,
            dim.schema().as_ref(),
            std::slice::from_ref(&dim),
        )
        .expect("filter");
        let schema = Schema::new(vec![Field::new("k", DataType::Int64, true)]);
        let pred = runtime_filter::nth_to_column(&rf.preds[0], &schema).unwrap();
        let hit = RowGroupStats {
            index: 0,
            num_rows: 2,
            column_names: vec!["k".into()],
            i64_ranges: vec![ColumnRange {
                name: "k".into(),
                min_i64: Some(1),
                max_i64: Some(2),
            }],
            f64_ranges: vec![],
        };
        let miss = RowGroupStats {
            index: 1,
            num_rows: 2,
            column_names: vec!["k".into()],
            i64_ranges: vec![ColumnRange {
                name: "k".into(),
                min_i64: Some(100),
                max_i64: Some(101),
            }],
            f64_ranges: vec![],
        };
        assert!(!row_group_cannot_match(&hit, &pred), "{pred:?}");
        assert!(row_group_cannot_match(&miss, &pred), "{pred:?}");
    }

    #[test]
    fn inner_join_one_hundred_thousand_int64_keys() {
        const N: i64 = 100_000;
        let l_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let r_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let left = Batch::try_new(
            l_schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(0..N))],
        )
        .unwrap();
        let right = Batch::try_new(
            r_schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(
                (N / 2)..(N / 2 + 1_000),
            ))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", l_schema, vec![left]).unwrap();
        s.register("u", r_schema, vec![right]).unwrap();
        let sql = "SELECT COUNT(*) AS c FROM t JOIN u ON t.a = u.id";
        let out = s.execute_sql(sql).unwrap();
        assert_eq!(col_i64(&out, "c"), vec![Some(1_000)]);
        let parallel = s.with_workers(4).unwrap().execute_sql(sql).unwrap();
        assert_eq!(col_i64(&parallel, "c"), vec![Some(1_000)]);
    }

    #[test]
    fn inner_join_matches_across_probe_morsels_and_duplicate_keys() {
        let l_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let r_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let left = Batch::try_new(
            l_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 1, 2]))],
        )
        .unwrap();
        let right_a = Batch::try_new(
            r_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        )
        .unwrap();
        let right_b = Batch::try_new(
            r_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![2i64, 9]))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", l_schema, vec![left]).unwrap();
        s.register("u", r_schema, vec![right_a, right_b]).unwrap();
        let sql = "SELECT COUNT(*) AS c FROM t JOIN u ON t.a = u.id";
        let out = s.execute_sql(sql).unwrap();
        assert_eq!(col_i64(&out, "c"), vec![Some(3)]);
        let parallel = s.with_workers(2).unwrap().execute_sql(sql).unwrap();
        assert_eq!(col_i64(&parallel, "c"), vec![Some(3)]);
    }

    #[test]
    fn inner_join_sparse_integer_keys_still_match() {
        let l_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let r_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let left = Batch::try_new(
            l_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![0i64, 1_000_000_000]))],
        )
        .unwrap();
        let right = Batch::try_new(
            r_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_000_000_000i64, 2]))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", l_schema, vec![left]).unwrap();
        s.register("u", r_schema, vec![right]).unwrap();
        let out = s
            .execute_sql("SELECT COUNT(*) AS c FROM t JOIN u ON t.a = u.id")
            .unwrap();
        assert_eq!(col_i64(&out, "c"), vec![Some(1)]);
    }

    #[test]
    fn inner_join_utf8_keys_hash_without_extract() {
        let l_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let r_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let left = Batch::try_new(
            l_schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                Some("red"),
                Some("blue"),
                None,
                Some("red"),
            ]))],
        )
        .unwrap();
        let right = Batch::try_new(
            r_schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                Some("blue"),
                Some("green"),
                Some("red"),
            ]))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", l_schema, vec![left]).unwrap();
        s.register("u", r_schema, vec![right]).unwrap();
        let out = s
            .execute_sql("SELECT t.a, u.id FROM t JOIN u ON t.a = u.id")
            .unwrap();
        let mut got: Vec<(String, String)> = col_utf8(&out, "a")
            .into_iter()
            .zip(col_utf8(&out, "id"))
            .map(|(a, b)| (a.unwrap(), b.unwrap()))
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("blue".into(), "blue".into()),
                ("red".into(), "red".into()),
                ("red".into(), "red".into()),
            ]
        );
    }

    #[test]
    fn join_type_matrix_matches_reference() {
        let s = join_session();
        let cases = [
            ("inner", "SELECT t.a, u.id FROM t JOIN u ON t.a = u.id"),
            ("left", "SELECT t.a, u.id FROM t LEFT JOIN u ON t.a = u.id"),
            (
                "right",
                "SELECT t.a, u.id FROM t RIGHT JOIN u ON t.a = u.id",
            ),
            (
                "full",
                "SELECT t.a, u.id FROM t FULL OUTER JOIN u ON t.a = u.id",
            ),
        ];
        for (kind, sql) in cases {
            let out = s.execute_sql(sql).unwrap();
            assert_eq!(
                sorted_pairs(&out, "a", "id"),
                reference_join(kind),
                "{kind}"
            );
        }
    }

    #[test]
    fn semi_and_anti_keep_left_rows() {
        let s = join_session();
        let semi = s
            .execute_sql("SELECT t.a FROM t LEFT SEMI JOIN u ON t.a = u.id")
            .unwrap();
        let mut got = col_i64(&semi, "a");
        got.sort_by_key(|v| v.unwrap_or(i64::MIN));
        let mut want: Vec<Option<i64>> =
            reference_join("semi").into_iter().map(|(l, _)| l).collect();
        want.sort_by_key(|v| v.unwrap_or(i64::MIN));
        assert_eq!(got, want);

        let anti = s
            .execute_sql("SELECT t.a FROM t LEFT ANTI JOIN u ON t.a = u.id")
            .unwrap();
        let mut got = col_i64(&anti, "a");
        got.sort_by_key(|v| v.unwrap_or(i64::MIN));
        let mut want: Vec<Option<i64>> =
            reference_join("anti").into_iter().map(|(l, _)| l).collect();
        want.sort_by_key(|v| v.unwrap_or(i64::MIN));
        assert_eq!(got, want);
    }

    #[test]
    fn cross_join_is_cartesian() {
        let s = join_session();
        let out = s
            .execute_sql("SELECT t.a, u.id FROM t CROSS JOIN u")
            .unwrap();
        assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 5 * 4);
    }

    #[test]
    fn join_residual_predicate() {
        let s = join_session();
        let out = s
            .execute_sql("SELECT t.a, u.id FROM t JOIN u ON t.a = u.id AND u.x > 10")
            .unwrap();
        assert_eq!(
            sorted_pairs(&out, "a", "id"),
            vec![(Some(2), Some(2)), (Some(2), Some(2))]
        );
    }

    #[test]
    fn three_table_join_matches_after_reorder() {
        let fact_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("amt", DataType::Int64, true),
        ]));
        let id_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let fact = Batch::try_new(
            fact_schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    (0..100).map(|i| Some(i % 10)).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from((0..100).map(Some).collect::<Vec<_>>())),
            ],
        )
        .unwrap();
        let d0 = Batch::try_new(
            id_schema.clone(),
            vec![Arc::new(Int64Array::from(
                (0..10).map(Some).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        let d1 = Batch::try_new(
            id_schema.clone(),
            vec![Arc::new(Int64Array::from(
                (0..100).map(Some).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("fact", fact_schema, vec![fact]).unwrap();
        s.register("d0", id_schema.clone(), vec![d0]).unwrap();
        s.register("d1", id_schema.clone(), vec![d1]).unwrap();
        let out = s
            .execute_sql(
                "SELECT fact.amt FROM fact JOIN d1 ON fact.id = d1.id JOIN d0 ON d1.id = d0.id",
            )
            .unwrap();
        let amts = col_i64(&out, "amt");
        assert_eq!(amts.len(), 100);
        assert_eq!(
            amts.iter().map(|v| v.unwrap_or(0)).sum::<i64>(),
            (0..100).sum::<i64>()
        );
        let filtered = s
            .execute_sql(
                "SELECT fact.amt FROM fact JOIN d1 ON fact.id = d1.id AND d1.id > 5 JOIN d0 ON d1.id = d0.id",
            )
            .unwrap();
        let famts = col_i64(&filtered, "amt");
        assert_eq!(famts.len(), 40);
        assert!(famts.iter().all(|v| v.unwrap() % 10 > 5));

        let extra = Batch::try_new(
            id_schema.clone(),
            vec![Arc::new(Int64Array::from(
                (0..10).map(Some).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        s.register("d2", id_schema, vec![extra]).unwrap();
        let four = s
            .execute_sql(
                "SELECT fact.amt FROM fact JOIN d1 ON fact.id = d1.id JOIN d0 ON d1.id = d0.id JOIN d2 ON d0.id = d2.id",
            )
            .unwrap();
        assert_eq!(col_i64(&four, "amt").len(), 100);
    }

    #[test]
    fn same_leaf_on_equality_rejects_nulls_after_reorder() {
        let fact_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("flag", DataType::Int64, true),
        ]));
        let id_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let fact = Batch::try_new(
            fact_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(0), Some(1), Some(2)])),
                Arc::new(Int64Array::from(vec![Some(1), None, Some(1)])),
            ],
        )
        .unwrap();
        let dim = Batch::try_new(
            id_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(0), Some(1), Some(2)]))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("fact", fact_schema, vec![fact]).unwrap();
        s.register("d0", id_schema.clone(), vec![dim.clone()])
            .unwrap();
        s.register("d1", id_schema, vec![dim]).unwrap();
        let out = s
            .execute_sql(
                "SELECT fact.id FROM fact JOIN d1 ON fact.id = d1.id AND fact.flag = fact.flag JOIN d0 ON d1.id = d0.id ORDER BY id",
            )
            .unwrap();
        assert_eq!(col_i64(&out, "id"), vec![Some(0), Some(2)]);
    }

    #[test]
    fn join_then_agg_folds_into_unique_build() {
        let dim_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("cat", DataType::Int64, true),
            Field::new("extra", DataType::Int64, true),
        ]));
        let fact_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let dim = Batch::try_new(
            dim_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(Int64Array::from(vec![Some(10), Some(10), Some(20)])),
                Arc::new(Int64Array::from(vec![Some(100), Some(200), Some(300)])),
            ],
        )
        .unwrap();
        let fact = Batch::try_new(
            fact_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(1),
                    Some(2),
                    Some(3),
                    Some(3),
                    Some(9),
                ])),
                Arc::new(Int64Array::from(vec![
                    Some(5),
                    Some(7),
                    Some(3),
                    Some(11),
                    Some(1),
                    Some(99),
                ])),
            ],
        )
        .unwrap();
        let mut s = Session::new().with_workers(2).unwrap();
        s.register("dim", dim_schema, vec![dim]).unwrap();
        s.register("fact", fact_schema.clone(), vec![fact]).unwrap();

        let by_cat = s
            .execute_sql(
                "SELECT d.cat, SUM(f.v) AS s FROM fact f JOIN dim d ON f.id = d.id GROUP BY d.cat ORDER BY cat",
            )
            .unwrap();
        assert_eq!(col_i64(&by_cat, "cat"), vec![Some(10), Some(20)]);
        assert_eq!(col_i64(&by_cat, "s"), vec![Some(15), Some(12)]);

        let by_key = s
            .execute_sql(
                "SELECT f.id, d.extra, SUM(f.v) AS s FROM fact f JOIN dim d ON f.id = d.id GROUP BY f.id, d.extra ORDER BY id",
            )
            .unwrap();
        assert_eq!(col_i64(&by_key, "id"), vec![Some(1), Some(2), Some(3)]);
        assert_eq!(
            col_i64(&by_key, "extra"),
            vec![Some(100), Some(200), Some(300)]
        );
        assert_eq!(col_i64(&by_key, "s"), vec![Some(12), Some(3), Some(12)]);

        let n = mizuchi_core::batch::DEFAULT_MAX_ROWS + 16;
        let mut ids = Vec::with_capacity(n);
        let mut vals = Vec::with_capacity(n);
        let mut expect = [0i64; 4];
        for i in 0..n {
            let id = (i % 3) as i64 + 1;
            let v = (i % 7) as i64 + 1;
            ids.push(Some(id));
            vals.push(Some(v));
            expect[id as usize] += v;
        }
        let wide = Batch::try_new(
            fact_schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(vals)),
            ],
        )
        .unwrap();
        s.register("fact_wide", fact_schema, vec![wide]).unwrap();
        let wide_out = s
            .execute_sql(
                "SELECT f.id, SUM(f.v) AS s FROM fact_wide f JOIN dim d ON f.id = d.id GROUP BY f.id ORDER BY id",
            )
            .unwrap();
        assert_eq!(col_i64(&wide_out, "id"), vec![Some(1), Some(2), Some(3)]);
        assert_eq!(
            col_i64(&wide_out, "s"),
            vec![Some(expect[1]), Some(expect[2]), Some(expect[3])]
        );

        let root = spill_root("join-fold");
        let limited = s
            .clone()
            .with_memory_limit(64 * 1024)
            .with_spill_dir(root.clone());
        let limited_out = limited
            .execute_sql(
                "SELECT d.cat, SUM(f.v) AS s FROM fact f JOIN dim d ON f.id = d.id GROUP BY d.cat ORDER BY cat",
            )
            .unwrap();
        assert_eq!(col_i64(&limited_out, "cat"), vec![Some(10), Some(20)]);
        assert_eq!(col_i64(&limited_out, "s"), vec![Some(15), Some(12)]);
        let left = leftover(&root);
        let _ = std::fs::remove_dir_all(&root);
        assert!(left.is_empty(), "{left:?}");
    }

    #[test]
    fn join_coalesce_filter_pushes_to_right() {
        let s = join_session();
        let out = s
            .execute_sql("SELECT t.a, u.x FROM t JOIN u ON t.a = u.id WHERE COALESCE(u.x, 0) > 20")
            .unwrap();
        assert_eq!(
            sorted_pairs(&out, "a", "x"),
            vec![(Some(2), Some(30)), (Some(2), Some(30))]
        );
    }

    #[test]
    fn ungrouped_aggs_skip_nulls_except_count_star() {
        let out = session()
            .execute_sql(
                "SELECT COUNT(*) AS cs, COUNT(a) AS cv, SUM(a) AS s, MIN(a) AS mn, MAX(a) AS mx, AVG(a) AS av, COUNT(DISTINCT a) AS cd FROM t",
            )
            .unwrap();
        assert_eq!(col_i64(&out, "cs"), vec![Some(4)]);
        assert_eq!(col_i64(&out, "cv"), vec![Some(3)]);
        assert_eq!(col_i64(&out, "s"), vec![Some(6)]);
        assert_eq!(col_i64(&out, "mn"), vec![Some(1)]);
        assert_eq!(col_i64(&out, "mx"), vec![Some(3)]);
        let av = col_f64(&out, "av");
        assert_eq!(av.len(), 1);
        assert!((av[0].unwrap() - 2.0).abs() < 1e-12);
        assert_eq!(col_i64(&out, "cd"), vec![Some(3)]);
    }

    #[test]
    fn grouped_agg_puts_null_keys_in_one_group() {
        let out = session()
            .execute_sql("SELECT a, COUNT(*) AS c FROM t GROUP BY a")
            .unwrap();
        let mut rows: Vec<(Option<i64>, Option<i64>)> = col_i64(&out, "a")
            .into_iter()
            .zip(col_i64(&out, "c"))
            .collect();
        rows.sort_by_key(|(k, _)| k.unwrap_or(i64::MIN));
        assert_eq!(
            rows,
            vec![
                (None, Some(1)),
                (Some(1), Some(1)),
                (Some(2), Some(1)),
                (Some(3), Some(1)),
            ]
        );
    }

    #[test]
    fn grouped_dictionary_utf8_keys_count() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "flag",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )]));
        let keys = Int32Array::from(vec![Some(0), Some(1), Some(0), None, Some(1)]);
        let values: ArrayRef = Arc::new(StringArray::from(vec!["A", "N"]));
        let dict = DictionaryArray::try_new(keys, values).unwrap();
        let batch = Batch::try_new(schema.clone(), vec![Arc::new(dict)]).unwrap();
        let mut s = Session::new();
        s.register("d", schema, vec![batch]).unwrap();
        let out = s
            .execute_sql("SELECT flag, COUNT(*) AS c FROM d GROUP BY flag")
            .unwrap();
        let mut rows: Vec<(Option<String>, Option<i64>)> = Vec::new();
        for batch in &out {
            let flags = batch.column_by_name("flag").unwrap();
            let counts = batch
                .column_by_name("c")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                let flag = if flags.is_null(i) {
                    None
                } else if let Some(utf8) = flags.as_any().downcast_ref::<StringArray>() {
                    Some(utf8.value(i).to_owned())
                } else {
                    let dict = flags
                        .as_any()
                        .downcast_ref::<DictionaryArray<Int32Type>>()
                        .unwrap();
                    let values = dict
                        .values()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    Some(values.value(dict.keys().value(i) as usize).to_owned())
                };
                let c = if counts.is_null(i) {
                    None
                } else {
                    Some(counts.value(i))
                };
                rows.push((flag, c));
            }
        }
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            rows,
            vec![
                (None, Some(1)),
                (Some("A".into()), Some(2)),
                (Some("N".into()), Some(2)),
            ]
        );
    }

    #[test]
    fn packed_utf8_group_keys_round_trip_including_nulls() {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, true)]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                None,
                Some(""),
                Some("aa"),
                Some("aa"),
                Some("abcdefghijklmnop"),
                Some("abcdefghijklmnopq"),
                None,
            ])) as ArrayRef],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", schema, vec![batch]).unwrap();
        let out = s
            .execute_sql("SELECT k, COUNT(*) AS c FROM t GROUP BY k")
            .unwrap();
        let mut rows: Vec<(Option<String>, i64)> = Vec::new();
        for batch in &out {
            let keys = batch.column_by_name("k").unwrap();
            let counts = batch
                .column_by_name("c")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                let k = if keys.is_null(i) {
                    None
                } else {
                    Some(
                        keys.as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(i)
                            .to_owned(),
                    )
                };
                rows.push((k, counts.value(i)));
            }
        }
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            rows,
            vec![
                (None, 2),
                (Some(String::new()), 1),
                (Some("aa".into()), 2),
                (Some("abcdefghijklmnop".into()), 1),
                (Some("abcdefghijklmnopq".into()), 1),
            ]
        );
    }

    #[test]
    fn dictionary_filter_keeps_dictionary_and_matches_utf8_literal() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "flag",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )]));
        let keys = Int32Array::from(vec![Some(0), Some(1), Some(0), None, Some(1)]);
        let values: ArrayRef = Arc::new(StringArray::from(vec!["A", "N"]));
        let dict = DictionaryArray::try_new(keys, values).unwrap();
        let batch = Batch::try_new(schema.clone(), vec![Arc::new(dict)]).unwrap();
        let mut s = Session::new();
        s.register("d", schema, vec![batch]).unwrap();
        let out = s
            .execute_sql("SELECT flag FROM d WHERE flag = 'A' OR flag IS NULL")
            .unwrap();
        assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        let mut got: Vec<Option<String>> = Vec::new();
        for batch in &out {
            let col = batch.column_by_name("flag").unwrap();
            assert!(
                matches!(col.data_type(), DataType::Dictionary(_, v) if v.as_ref() == &DataType::Utf8),
                "filter kept dictionary, got {}",
                col.data_type()
            );
            let dict = col
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .unwrap();
            let values = dict
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..dict.len() {
                if dict.is_null(i) {
                    got.push(None);
                } else {
                    got.push(Some(values.value(dict.keys().value(i) as usize).to_owned()));
                }
            }
        }
        got.sort();
        assert_eq!(got, vec![None, Some("A".into()), Some("A".into())]);
    }

    #[test]
    fn kernel_filter_project_and_ungrouped_agg_one_million_rows() {
        const N: i64 = 1_000_000;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(0..N))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", schema, vec![batch]).unwrap();
        let filtered = s
            .execute_sql("SELECT id FROM t WHERE id >= 999990")
            .unwrap();
        assert_eq!(
            col_i64(&filtered, "id"),
            vec![
                Some(999990),
                Some(999991),
                Some(999992),
                Some(999993),
                Some(999994),
                Some(999995),
                Some(999996),
                Some(999997),
                Some(999998),
                Some(999999),
            ]
        );
        let projected = s.execute_sql("SELECT id + 1 AS x FROM t").unwrap();
        let mut n = 0i64;
        let mut first = None;
        let mut last = None;
        for batch in &projected {
            let col = batch
                .column_by_name("x")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            if first.is_none() {
                first = Some(col.value(0));
            }
            last = Some(col.value(col.len() - 1));
            n += col.len() as i64;
        }
        assert_eq!(n, N);
        assert_eq!(first, Some(1));
        assert_eq!(last, Some(N));
        let summed = s
            .execute_sql("SELECT SUM(id) AS s, COUNT(*) AS c FROM t")
            .unwrap();
        assert_eq!(col_i64(&summed, "c"), vec![Some(N)]);
        let sum = col_i64(&summed, "s")[0].unwrap();
        assert_eq!(sum, N * (N - 1) / 2);
    }

    #[test]
    fn window_row_number_rank_and_count() {
        let out = session()
            .execute_sql(
                "SELECT a, ROW_NUMBER() OVER (ORDER BY a) AS rn, \
                 RANK() OVER (ORDER BY a) AS r, \
                 DENSE_RANK() OVER (ORDER BY a) AS d \
                 FROM t WHERE a IS NOT NULL",
            )
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1), Some(2), Some(3)]);
        assert_eq!(col_i64(&out, "rn"), vec![Some(1), Some(2), Some(3)]);
        assert_eq!(col_i64(&out, "r"), vec![Some(1), Some(2), Some(3)]);
        assert_eq!(col_i64(&out, "d"), vec![Some(1), Some(2), Some(3)]);
        let c = session()
            .execute_sql("SELECT COUNT(*) OVER () AS c FROM t WHERE a IS NOT NULL")
            .unwrap();
        assert_eq!(col_i64(&c, "c"), vec![Some(3), Some(3), Some(3)]);
        let running = session()
            .execute_sql("SELECT COUNT(*) OVER (ORDER BY a) AS c FROM t WHERE a IS NOT NULL")
            .unwrap();
        assert_eq!(col_i64(&running, "c"), vec![Some(1), Some(2), Some(3)]);
    }

    #[test]
    fn window_rank_ties_and_running_sum() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(1), Some(2)])),
                Arc::new(Int64Array::from(vec![
                    Some(10),
                    Some(10),
                    Some(20),
                    Some(5),
                ])),
            ],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("w", schema, vec![batch]).unwrap();
        let ranks = s
            .execute_sql(
                "SELECT g, v, \
                 RANK() OVER (PARTITION BY g ORDER BY v) AS r, \
                 DENSE_RANK() OVER (PARTITION BY g ORDER BY v) AS d \
                 FROM w",
            )
            .unwrap();
        assert_eq!(
            col_i64(&ranks, "g"),
            vec![Some(1), Some(1), Some(1), Some(2)]
        );
        assert_eq!(
            col_i64(&ranks, "v"),
            vec![Some(10), Some(10), Some(20), Some(5)]
        );
        assert_eq!(
            col_i64(&ranks, "r"),
            vec![Some(1), Some(1), Some(3), Some(1)]
        );
        assert_eq!(
            col_i64(&ranks, "d"),
            vec![Some(1), Some(1), Some(2), Some(1)]
        );
        let sums = s
            .execute_sql(
                "SELECT g, SUM(v) OVER (PARTITION BY g ORDER BY v \
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM w",
            )
            .unwrap();
        assert_eq!(
            col_i64(&sums, "s"),
            vec![Some(10), Some(20), Some(40), Some(5)]
        );
        let range_sums = s
            .execute_sql("SELECT g, SUM(v) OVER (PARTITION BY g ORDER BY v) AS s FROM w")
            .unwrap();
        assert_eq!(
            col_i64(&range_sums, "s"),
            vec![Some(20), Some(20), Some(40), Some(5)]
        );
    }

    #[test]
    fn window_named_window() {
        let out = session()
            .execute_sql("SELECT ROW_NUMBER() OVER w AS n FROM t WHERE a IS NOT NULL WINDOW w AS (ORDER BY a)")
            .unwrap();
        assert_eq!(col_i64(&out, "n"), vec![Some(1), Some(2), Some(3)]);
    }

    #[test]
    fn window_sum_rejects_non_integer() {
        let err = session()
            .execute_sql("SELECT SUM(CAST(a AS DOUBLE)) OVER () FROM t WHERE a IS NOT NULL");
        assert!(matches!(err, Err(Error::NotImplemented(_))), "{err:?}");
    }

    #[test]
    fn window_fails_closed_without_spill() {
        let (s, cap) = register_numbered(12, 32);
        let err = s
            .clone()
            .with_memory_limit(cap)
            .execute_sql("SELECT ROW_NUMBER() OVER (ORDER BY a) AS n FROM t");
        assert!(matches!(err, Err(Error::Memory(_))), "{err:?}");
        let err = s
            .with_memory_limit(cap)
            .execute_sql("SELECT COUNT(*) OVER () AS c FROM t");
        assert!(matches!(err, Err(Error::Memory(_))), "{err:?}");
    }

    #[test]
    fn cte_inline_and_chained() {
        let out = Session::new()
            .execute_sql("WITH c AS (SELECT 1 AS n) SELECT n FROM c")
            .unwrap();
        assert_eq!(col_i64(&out, "n"), vec![Some(1)]);
        let out = Session::new()
            .execute_sql(
                "WITH c AS (SELECT 1 AS n), d AS (SELECT n + 1 AS n FROM c) SELECT n FROM d",
            )
            .unwrap();
        assert_eq!(col_i64(&out, "n"), vec![Some(2)]);
    }

    #[test]
    fn recursive_cte_working_table() {
        let out = Session::new()
            .execute_sql(
                "WITH RECURSIVE c AS (\
                 SELECT 1 AS n UNION ALL SELECT n + 1 FROM c WHERE n < 3\
                 ) SELECT n FROM c",
            )
            .unwrap();
        assert_eq!(col_i64(&out, "n"), vec![Some(1), Some(2), Some(3)]);
    }

    #[test]
    fn recursive_cte_iteration_limit() {
        let err = Session::new().execute_sql(
            "WITH RECURSIVE c AS (SELECT 1 AS n UNION ALL SELECT n FROM c) SELECT n FROM c",
        );
        assert!(matches!(err, Err(Error::Eval(_))), "{err:?}");
        assert!(
            err.unwrap_err().to_string().contains("iteration limit"),
            "expected iteration limit"
        );
    }

    #[test]
    fn recursive_cte_fails_closed_without_spill() {
        let (s, cap) = register_numbered(12, 32);
        let err = s.with_memory_limit(cap).execute_sql(
            "WITH RECURSIVE c AS (SELECT a FROM t UNION ALL SELECT a FROM c WHERE a < 0) SELECT a FROM c",
        );
        assert!(matches!(err, Err(Error::Memory(_))), "{err:?}");
    }

    #[test]
    fn recursive_cte_seed_can_scan_same_named_table() {
        let out = session()
            .execute_sql(
                "WITH RECURSIVE t AS (\
                 SELECT a FROM t UNION ALL SELECT a FROM t WHERE a < 0\
                 ) SELECT a FROM t",
            )
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1), Some(2), Some(3), None]);
    }

    #[test]
    fn recursive_cte_step_joins_qualified_base_table() {
        let out = session()
            .execute_sql(
                "WITH RECURSIVE t AS (\
                 SELECT a FROM t WHERE a = 1 UNION ALL \
                 SELECT p.a FROM public.t p INNER JOIN t ON p.a = t.a + 1\
                 ) SELECT a FROM t",
            )
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1), Some(2), Some(3)]);
    }

    #[test]
    fn nested_with_shadows_outer_cte() {
        let out = Session::new()
            .execute_sql(
                "WITH c AS (SELECT 1 AS n) SELECT n FROM (WITH c AS (SELECT 2 AS n) SELECT n FROM c) s",
            )
            .unwrap();
        assert_eq!(col_i64(&out, "n"), vec![Some(2)]);
    }

    #[test]
    fn nested_recursive_cte_shadows_outer_name() {
        let out = Session::new()
            .execute_sql(
                "WITH c AS (SELECT 1 AS n) SELECT n FROM (\
                 WITH RECURSIVE c AS (\
                 SELECT 2 AS n UNION ALL SELECT n + 1 FROM c WHERE n < 4\
                 ) SELECT n FROM c) s",
            )
            .unwrap();
        assert_eq!(col_i64(&out, "n"), vec![Some(2), Some(3), Some(4)]);
    }

    #[test]
    fn quoted_cte_name_round_trips() {
        let out = Session::new()
            .execute_sql(r#"WITH "C" AS (SELECT 1 AS n) SELECT n FROM "C""#)
            .unwrap();
        assert_eq!(col_i64(&out, "n"), vec![Some(1)]);
    }

    #[test]
    fn subquery_in_list_and_in_select() {
        let out = session()
            .execute_sql("SELECT a FROM t WHERE a IN (1, 2)")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1), Some(2)]);
        let out = session()
            .execute_sql("SELECT a FROM t WHERE a IN (SELECT a FROM t)")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1), Some(2), Some(3)]);
        let out = session()
            .execute_sql("SELECT a FROM t WHERE a NOT IN (SELECT a FROM t)")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), Vec::<Option<i64>>::new());
        let out = session()
            .execute_sql("SELECT a FROM t WHERE a NOT IN (SELECT a FROM t WHERE a = 1)")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(2), Some(3)]);
        let out = session()
            .execute_sql("SELECT a FROM t WHERE a NOT IN (SELECT a FROM t WHERE a = 99)")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1), Some(2), Some(3), None]);
        let out = session()
            .execute_sql("SELECT a FROM t WHERE 99 NOT IN (SELECT a FROM t)")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), Vec::<Option<i64>>::new());
        let out = session()
            .execute_sql("SELECT a FROM t WHERE NULL NOT IN (SELECT a FROM t WHERE a IS NOT NULL)")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), Vec::<Option<i64>>::new());
        let two = session().with_workers(2).unwrap();
        let out = two
            .execute_sql("SELECT a FROM t WHERE a NOT IN (SELECT a FROM t)")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), Vec::<Option<i64>>::new());
    }

    #[test]
    fn subquery_exists_and_scalar() {
        let out = session()
            .execute_sql("SELECT a FROM t WHERE EXISTS (SELECT 1 FROM t u WHERE u.a = t.a)")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1), Some(2), Some(3)]);
        let out = session()
            .execute_sql("SELECT a FROM t WHERE EXISTS (SELECT 1 FROM t LIMIT 0)")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), Vec::<Option<i64>>::new());
        let err = session().execute_sql(
            "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM t u WHERE u.a = t.a LIMIT 1)",
        );
        assert!(matches!(err, Err(Error::NotImplemented(_))), "{err:?}");
        let err = session().execute_sql("SELECT a FROM t WHERE EXISTS (SELECT nosuch FROM t)");
        assert!(matches!(err, Err(Error::Plan(_))), "{err:?}");
        let err =
            session().execute_sql("SELECT a FROM t WHERE EXISTS (SELECT 1 FROM t HAVING false)");
        assert!(matches!(err, Err(Error::NotImplemented(_))), "{err:?}");
        let out = session()
            .execute_sql(
                "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM t u WHERE u.a = CAST(t.a AS BIGINT))",
            )
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1), Some(2), Some(3)]);
        let out = session()
            .execute_sql("SELECT (SELECT a FROM t WHERE a = 1)")
            .unwrap();
        assert_eq!(out[0].num_rows(), 1);
        let col = out[0].column(0);
        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 1);
        let out = session()
            .execute_sql("SELECT (SELECT a FROM t WHERE a = 1) AS x, a FROM t WHERE a IS NOT NULL")
            .unwrap();
        assert_eq!(col_i64(&out, "x"), vec![Some(1), Some(1), Some(1)]);
        assert_eq!(col_i64(&out, "a"), vec![Some(1), Some(2), Some(3)]);
        let err = session().execute_sql("SELECT (SELECT a FROM t WHERE a IS NOT NULL)");
        assert!(matches!(err, Err(Error::Eval(_))), "{err:?}");
    }

    #[test]
    fn subquery_lateral_projects_outer() {
        let out = session()
            .execute_sql("SELECT a FROM t, LATERAL (SELECT a AS x) s WHERE t.a = s.x")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1), Some(2), Some(3)]);
        let err = session().execute_sql("SELECT a FROM t, LATERAL (SELECT a AS x LIMIT 0) s");
        assert!(matches!(err, Err(Error::NotImplemented(_))), "{err:?}");
    }

    #[test]
    fn setops_union_intersect_except() {
        let s = session();
        let out = s.execute_sql("SELECT 1 AS x UNION SELECT 2").unwrap();
        assert_eq!(col_i64(&out, "x"), vec![Some(1), Some(2)]);
        let out = s.execute_sql("SELECT 1 AS x UNION ALL SELECT 1").unwrap();
        assert_eq!(col_i64(&out, "x"), vec![Some(1), Some(1)]);
        let out = s
            .execute_sql("SELECT 1 AS x UNION DISTINCT SELECT 1")
            .unwrap();
        assert_eq!(col_i64(&out, "x"), vec![Some(1)]);
        let out = s
            .execute_sql("SELECT a FROM t UNION SELECT a FROM t")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1), Some(2), Some(3), None]);
        let out = s
            .execute_sql("SELECT a FROM t INTERSECT SELECT a FROM t WHERE a = 1")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1)]);
        let out = s
            .execute_sql("SELECT a FROM t EXCEPT SELECT a FROM t WHERE a = 1")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(2), Some(3), None]);
        let out = s
            .execute_sql(
                "SELECT a FROM t WHERE a IS NOT NULL INTERSECT ALL SELECT a FROM t WHERE a = 1",
            )
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1)]);
        let out = s
            .execute_sql("SELECT a FROM t WHERE a IN (1, 1) EXCEPT ALL SELECT a FROM t WHERE a = 2")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1)]);
        let err = s.execute_sql("SELECT 1 UNION SELECT 'x'");
        assert!(matches!(err, Err(Error::Plan(_))), "{err:?}");
        let err = s.execute_sql("SELECT 1, 2 UNION SELECT 1");
        assert!(matches!(err, Err(Error::Plan(_))), "{err:?}");
        let out = s.execute_sql("SELECT NULL AS x UNION SELECT 1").unwrap();
        assert_eq!(col_i64(&out, "x"), vec![None, Some(1)]);
        let out = s
            .execute_sql("SELECT CAST(1 AS INT) AS x UNION SELECT 2")
            .unwrap();
        assert_eq!(col_i64(&out, "x"), vec![Some(1), Some(2)]);
        let err = s.execute_sql("SELECT ARRAY[1] AS a UNION SELECT ARRAY[1]");
        assert!(matches!(err, Err(Error::NotImplemented(_))), "{err:?}");
        let out = s
            .execute_sql("SELECT ARRAY[1] AS a UNION ALL SELECT ARRAY[1]")
            .unwrap();
        assert_eq!(out.iter().map(Batch::num_rows).sum::<usize>(), 2);
    }

    #[test]
    fn decimal_cast_and_arithmetic() {
        let out = Session::new()
            .execute_sql("SELECT CAST(1 AS DECIMAL(10, 2)) AS x")
            .unwrap();
        assert_eq!(
            out[0].schema().field(0).data_type(),
            &DataType::Decimal64(10, 2)
        );
        let col = out[0].column(0);
        assert_eq!(
            col.as_any()
                .downcast_ref::<arrow::array::Decimal64Array>()
                .unwrap()
                .value(0),
            100
        );
        let out = Session::new()
            .execute_sql(
                "SELECT CAST('1.50' AS DECIMAL(10, 2)) + CAST('2.25' AS DECIMAL(10, 2)) AS x",
            )
            .unwrap();
        assert_eq!(
            out[0].schema().field(0).data_type(),
            &DataType::Decimal64(11, 2)
        );
        let col = out[0].column(0);
        assert_eq!(
            col.as_any()
                .downcast_ref::<arrow::array::Decimal64Array>()
                .unwrap()
                .value(0),
            375
        );
        let out = Session::new()
            .execute_sql("SELECT CAST(CAST(3 AS DECIMAL(10, 2)) AS DOUBLE PRECISION) AS x")
            .unwrap();
        assert_eq!(col_f64(&out, "x"), vec![Some(3.0)]);
        let err = Session::new().execute_sql("SELECT CAST(1000 AS DECIMAL(3, 2))");
        assert!(matches!(err, Err(Error::Eval(_))), "{err:?}");
        let out = Session::new()
            .execute_sql(
                "SELECT CAST('999.9' AS DECIMAL(4, 1)) * CAST('999.9' AS DECIMAL(4, 1)) AS x",
            )
            .unwrap();
        assert_eq!(
            out[0].schema().field(0).data_type(),
            &DataType::Decimal32(9, 2)
        );
        let col = out[0].column(0);
        assert_eq!(
            col.as_any()
                .downcast_ref::<arrow::array::Decimal32Array>()
                .unwrap()
                .value(0),
            99_980_001
        );
        let err = Session::new().execute_sql("SELECT SUM(CAST(1 AS DECIMAL(10, 2)))");
        assert!(matches!(err, Err(Error::Plan(_))), "{err:?}");
    }

    #[test]
    fn setops_fails_closed_without_spill() {
        let (s, cap) = register_numbered(12, 32);
        let err = s
            .with_memory_limit(cap)
            .execute_sql("SELECT a FROM t UNION ALL SELECT a FROM t");
        assert!(matches!(err, Err(Error::Memory(_))), "{err:?}");
    }

    #[test]
    fn recursive_cte_multiple_seeds() {
        let out = Session::new()
            .execute_sql(
                "WITH RECURSIVE c AS (\
                 SELECT 1 AS n UNION ALL SELECT 10 AS n UNION ALL \
                 SELECT n + 1 FROM c WHERE n < 2\
                 ) SELECT n FROM c",
            )
            .unwrap();
        assert_eq!(col_i64(&out, "n"), vec![Some(1), Some(10), Some(2)]);
    }

    #[test]
    fn count_distinct_ignores_nulls() {
        let out = session()
            .execute_sql("SELECT COUNT(DISTINCT a) AS cd FROM t")
            .unwrap();
        assert_eq!(col_i64(&out, "cd"), vec![Some(3)]);
    }

    #[test]
    fn sql_function_library_and_nullif() {
        let out = session()
            .execute_sql(
                "SELECT abs(-4) AS a, length('é') AS n, trim('  x ') AS t, ceil(1.1) AS c, \
                 floor(1.9) AS f, round(1.5) AS r, nullif(1, 1) AS z, nullif(1, 2) AS k, \
                 array_length(ARRAY[1, 2, 3], 1) AS al",
            )
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(4)]);
        assert_eq!(col_i64(&out, "n"), vec![Some(1)]);
        assert_eq!(col_utf8(&out, "t"), vec![Some("x".into())]);
        assert!((col_f64(&out, "c")[0].unwrap() - 2.0).abs() < 1e-12);
        assert!((col_f64(&out, "f")[0].unwrap() - 1.0).abs() < 1e-12);
        assert!((col_f64(&out, "r")[0].unwrap() - 2.0).abs() < 1e-12);
        assert_eq!(col_i64(&out, "z"), vec![None]);
        assert_eq!(col_i64(&out, "k"), vec![Some(1)]);
        assert_eq!(col_i64(&out, "al"), vec![Some(3)]);
    }

    #[test]
    fn datetime_sql_casts_and_clock() {
        const NOW: i64 = 1_577_836_800_000_000; // 2020-01-01 00:00:00 UTC
        let s = Session::new().with_now_micros(NOW);
        let out = s
            .execute_sql("SELECT CAST('2020-01-01' AS DATE) AS d")
            .unwrap();
        let d = out[0].column(0);
        assert_eq!(d.data_type(), &DataType::Date32);
        assert_eq!(
            d.as_any().downcast_ref::<Date32Array>().unwrap().value(0),
            18_262
        );
        let out = s
            .execute_sql("SELECT CAST('2020-01-01 00:00:00' AS TIMESTAMP) AS ts")
            .unwrap();
        let ts = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), NOW);
        let out = s.execute_sql("SELECT INTERVAL '1' DAY AS i").unwrap();
        assert_eq!(
            out[0].schema().field(0).data_type(),
            &DataType::Interval(arrow::datatypes::IntervalUnit::DayTime)
        );
        let out = s
            .execute_sql("SELECT EXTRACT(YEAR FROM DATE '2020-01-01') AS y")
            .unwrap();
        assert_eq!(col_i64(&out, "y"), vec![Some(2020)]);
        let out = s
            .execute_sql("SELECT DATE_TRUNC('day', TIMESTAMP '2020-01-02 15:04:05') AS t")
            .unwrap();
        let t = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(t.value(0), 1_577_923_200_000_000);
        let out = s
            .execute_sql("SELECT CURRENT_DATE AS d, NOW() AS t")
            .unwrap();
        assert_eq!(
            out[0]
                .column(0)
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(0),
            18_262
        );
        assert_eq!(
            out[0]
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .unwrap()
                .value(0),
            NOW
        );
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(1), Some(2)]))],
        )
        .unwrap();
        let mut ordered = Session::new().with_now_micros(NOW);
        ordered
            .register("t", schema.clone(), vec![batch.clone()])
            .unwrap();
        let out = ordered
            .execute_sql(
                "SELECT n FROM t ORDER BY CASE WHEN CURRENT_DATE = DATE '2020-01-01' THEN n ELSE -n END",
            )
            .unwrap();
        assert_eq!(col_i64(&out, "n"), vec![Some(1), Some(2)]);
        let mut merged = Session::new().with_now_micros(NOW);
        merged
            .register("t", schema.clone(), vec![batch.clone()])
            .unwrap();
        merged
            .register(
                "s",
                schema,
                vec![
                    Batch::try_new(
                        batch.schema().clone(),
                        vec![Arc::new(Int64Array::from(vec![Some(1)]))],
                    )
                    .unwrap(),
                ],
            )
            .unwrap();
        merged
            .merge_sql(
                "MERGE INTO t USING s ON t.n = s.n \
                 WHEN MATCHED AND CURRENT_DATE = DATE '2020-01-01' THEN DELETE",
            )
            .unwrap();
        let out = merged.execute_sql("SELECT n FROM t").unwrap();
        assert_eq!(col_i64(&out, "n"), vec![Some(2)]);
    }

    #[test]
    fn extract_from_date32_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, true)]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(Date32Array::from(vec![Some(0), Some(32)]))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", schema, vec![batch]).unwrap();
        let out = s
            .execute_sql(
                "SELECT extract(year FROM d) AS y, extract(month FROM d) AS m, \
                 extract(day FROM d) AS day, extract(epoch FROM d) AS e FROM t",
            )
            .unwrap();
        assert_eq!(col_i64(&out, "y"), vec![Some(1970), Some(1970)]);
        assert_eq!(col_i64(&out, "m"), vec![Some(1), Some(2)]);
        assert_eq!(col_i64(&out, "day"), vec![Some(1), Some(2)]);
        let e = col_f64(&out, "e");
        assert_eq!(e[0], Some(0.0));
        assert_eq!(e[1], Some(32.0 * 86_400.0));
    }

    #[test]
    fn udaf_product_parallel_and_spill() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let mut batches = Vec::new();
        for i in 0..8 {
            let g = i % 2;
            batches.push(
                Batch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![Some(g), Some(g)])),
                        Arc::new(Int64Array::from(vec![Some(2), Some(3)])),
                    ],
                )
                .unwrap(),
            );
        }
        let cap = tight_limit(&batches);
        let mut s = Session::new();
        s.register("t", schema, batches).unwrap();
        s.register_udaf(Arc::new(ProductI64)).unwrap();
        let sql = "SELECT g, product(v) AS p FROM t GROUP BY g";
        let mut want: Vec<(Option<i64>, Option<i64>)> = col_i64(&s.execute_sql(sql).unwrap(), "g")
            .into_iter()
            .zip(col_i64(&s.execute_sql(sql).unwrap(), "p"))
            .collect();
        want.sort_by_key(|(g, _)| *g);
        assert_eq!(want, vec![(Some(0), Some(1296)), (Some(1), Some(1296))]);

        let parallel = s.clone().with_workers(2).unwrap().execute_sql(sql).unwrap();
        let mut got: Vec<(Option<i64>, Option<i64>)> = col_i64(&parallel, "g")
            .into_iter()
            .zip(col_i64(&parallel, "p"))
            .collect();
        got.sort_by_key(|(g, _)| *g);
        assert_eq!(got, want);

        let root = spill_root("udaf");
        let spilled = s
            .clone()
            .with_memory_limit(cap)
            .with_spill_dir(root.clone())
            .execute_sql(sql)
            .unwrap();
        let mut got: Vec<(Option<i64>, Option<i64>)> = col_i64(&spilled, "g")
            .into_iter()
            .zip(col_i64(&spilled, "p"))
            .collect();
        got.sort_by_key(|(g, _)| *g);
        assert_eq!(got, want);
        let left = leftover(&root);
        let _ = std::fs::remove_dir_all(&root);
        assert!(left.is_empty(), "{left:?}");
    }

    #[derive(Debug)]
    struct SumI64AsF64;

    impl CombineAgg for SumI64AsF64 {
        fn name(&self) -> &str {
            "sumf"
        }

        fn result_type(&self) -> DataType {
            DataType::Float64
        }

        fn identity(&self) -> ArrayRef {
            Arc::new(Float64Array::from(vec![Some(0.0)]))
        }

        fn update(&self, state: &dyn Array, value: &dyn Array) -> Result<ArrayRef> {
            let s = state
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| Error::Eval("expected float64 state".into()))?;
            if value.is_null(0) {
                return Ok(Arc::new(Float64Array::from(vec![Some(s.value(0))])));
            }
            let v = value.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                Error::Eval(format!("expected int64 value, got {}", value.data_type()))
            })?;
            Ok(Arc::new(Float64Array::from(vec![Some(
                s.value(0) + v.value(0) as f64,
            )])))
        }

        fn merge(&self, left: &dyn Array, right: &dyn Array) -> Result<ArrayRef> {
            let l = left
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| Error::Eval("expected float64 buffer".into()))?;
            let r = right
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| Error::Eval("expected float64 buffer".into()))?;
            Ok(Arc::new(Float64Array::from(vec![Some(
                l.value(0) + r.value(0),
            )])))
        }
    }

    #[derive(Debug)]
    struct ConstI64(i64);

    impl CombineAgg for ConstI64 {
        fn name(&self) -> &str {
            "product"
        }

        fn result_type(&self) -> DataType {
            DataType::Int64
        }

        fn identity(&self) -> ArrayRef {
            Arc::new(Int64Array::from(vec![Some(self.0)]))
        }

        fn update(&self, _state: &dyn Array, _value: &dyn Array) -> Result<ArrayRef> {
            Ok(Arc::new(Int64Array::from(vec![Some(self.0)])))
        }

        fn merge(&self, _left: &dyn Array, _right: &dyn Array) -> Result<ArrayRef> {
            Ok(Arc::new(Int64Array::from(vec![Some(self.0)])))
        }
    }

    #[test]
    fn udaf_update_keeps_argument_type() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(2), Some(3)]))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", schema, vec![batch]).unwrap();
        s.register_udaf(Arc::new(SumI64AsF64)).unwrap();
        let out = s.execute_sql("SELECT sumf(v) AS s FROM t").unwrap();
        assert_eq!(col_f64(&out, "s"), vec![Some(5.0)]);
    }

    #[test]
    fn register_udaf_invalidates_query_cache() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(2), Some(3)]))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", schema, vec![batch]).unwrap();
        s.register_udaf(Arc::new(ProductI64)).unwrap();
        let sql = "SELECT product(v) AS p FROM t";
        assert_eq!(col_i64(&s.execute_sql(sql).unwrap(), "p"), vec![Some(6)]);
        let _ = s.execute_sql(sql).unwrap();
        assert_eq!(s.cache_hits(), 1);
        s.register_udaf(Arc::new(ConstI64(1))).unwrap();
        assert_eq!(col_i64(&s.execute_sql(sql).unwrap(), "p"), vec![Some(1)]);
        assert_eq!(s.cache_hits(), 1);
    }

    #[test]
    fn array_length_rejects_utf8() {
        let err = Session::new()
            .execute_sql("SELECT array_length('é', 1) AS n")
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("array_length requires list"), "{msg}");
    }

    #[test]
    fn empty_table_ungrouped_count_zero_sum_null() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let mut s = Session::new();
        s.register("t", schema, vec![]).unwrap();
        let out = s
            .execute_sql(
                "SELECT COUNT(*) AS c, SUM(a) AS s, AVG(a) AS av, COUNT(DISTINCT a) AS cd FROM t",
            )
            .unwrap();
        assert_eq!(col_i64(&out, "c"), vec![Some(0)]);
        assert_eq!(col_i64(&out, "s"), vec![None]);
        assert_eq!(col_f64(&out, "av"), vec![None]);
        assert_eq!(col_i64(&out, "cd"), vec![Some(0)]);
    }

    #[test]
    fn empty_table_grouped_emits_no_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let mut s = Session::new();
        s.register("t", schema, vec![]).unwrap();
        let out = s
            .execute_sql("SELECT a, COUNT(*) AS c FROM t GROUP BY a")
            .unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn count_star_without_from_is_one() {
        let out = Session::new().execute_sql("SELECT COUNT(*) AS c").unwrap();
        assert_eq!(col_i64(&out, "c"), vec![Some(1)]);
    }

    #[test]
    fn having_filters_groups() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2, 3, 3, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 1, 2, 3])),
            ],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", schema, vec![batch]).unwrap();
        let out = s
            .execute_sql("SELECT g, COUNT(*) FROM t GROUP BY g HAVING COUNT(*) > 1")
            .unwrap();
        let mut rows: Vec<(Option<i64>, Option<i64>)> = col_i64(&out, "g")
            .into_iter()
            .zip(col_i64(&out, "count"))
            .collect();
        rows.sort_by_key(|(k, _)| k.unwrap_or(i64::MIN));
        assert_eq!(rows, vec![(Some(1), Some(2)), (Some(3), Some(3))]);
    }

    #[test]
    fn min_max_strings() {
        let out = session()
            .execute_sql("SELECT MIN(b) AS mn, MAX(b) AS mx FROM t")
            .unwrap();
        assert_eq!(col_utf8(&out, "mn"), vec![Some("n".into())]);
        assert_eq!(col_utf8(&out, "mx"), vec![Some("z".into())]);
    }

    #[test]
    fn sum_uint32_keeps_unsigned_type() {
        let schema = Arc::new(Schema::new(vec![Field::new("u", DataType::UInt32, true)]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(UInt32Array::from(vec![Some(1), None, Some(2)]))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", schema, vec![batch]).unwrap();
        let out = s.execute_sql("SELECT SUM(u) AS s FROM t").unwrap();
        let col = out[0]
            .column_by_name("s")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(col.value(0), 3);
    }

    #[test]
    fn float_signed_zero_is_one_group() {
        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(vec![0.0, -0.0, 1.0]))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", schema, vec![batch]).unwrap();
        let grouped = s
            .execute_sql("SELECT f, COUNT(*) AS c FROM t GROUP BY f")
            .unwrap();
        assert_eq!(grouped.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        let distinct = s
            .execute_sql("SELECT COUNT(DISTINCT f) AS cd FROM t")
            .unwrap();
        assert_eq!(col_i64(&distinct, "cd"), vec![Some(2)]);
    }

    #[test]
    fn large_utf8_group_keys_round_trip() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::LargeUtf8,
            true,
        )]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(LargeStringArray::from(vec![
                Some("a"),
                Some("a"),
                Some("b"),
            ]))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", schema, vec![batch]).unwrap();
        let out = s
            .execute_sql("SELECT s, COUNT(*) AS c FROM t GROUP BY s")
            .unwrap();
        let mut rows: Vec<(Option<String>, Option<i64>)> = Vec::new();
        for batch in &out {
            let col = batch
                .column_by_name("s")
                .unwrap()
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap();
            let counts = batch
                .column_by_name("c")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..col.len() {
                let key = if col.is_null(i) {
                    None
                } else {
                    Some(col.value(i).to_owned())
                };
                rows.push((
                    key,
                    if counts.is_null(i) {
                        None
                    } else {
                        Some(counts.value(i))
                    },
                ));
            }
        }
        rows.sort();
        assert_eq!(
            rows,
            vec![(Some("a".into()), Some(2)), (Some("b".into()), Some(1))]
        );
    }

    #[test]
    fn distinct_sum_fails_closed() {
        assert!(matches!(
            session().execute_sql("SELECT SUM(DISTINCT a) FROM t"),
            Err(Error::NotImplemented("DISTINCT aggregate"))
        ));
    }

    #[derive(Default)]
    struct RefAgg {
        count_star: i64,
        count_val: i64,
        sum: i64,
        sum_seen: bool,
        min: Option<i64>,
        max: Option<i64>,
        avg_sum: f64,
        avg_n: i64,
        distinct: HashSet<i64>,
    }

    impl RefAgg {
        fn add(&mut self, v: Option<i64>) {
            self.count_star += 1;
            if let Some(v) = v {
                self.count_val += 1;
                self.sum += v;
                self.sum_seen = true;
                self.min = Some(self.min.map_or(v, |m| m.min(v)));
                self.max = Some(self.max.map_or(v, |m| m.max(v)));
                self.avg_sum += v as f64;
                self.avg_n += 1;
                self.distinct.insert(v);
            }
        }
    }

    fn sweep_session(n_groups: i64, rows_per_group: i64) -> (Session, BTreeMap<i64, RefAgg>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let mut gs = Vec::new();
        let mut vs = Vec::new();
        let mut expected: BTreeMap<i64, RefAgg> = BTreeMap::new();
        let mut i = 0i64;
        for _ in 0..rows_per_group {
            for g in 0..n_groups {
                let v = if i % 7 == 0 { None } else { Some(i % 11) };
                gs.push(Some(g));
                vs.push(v);
                expected.entry(g).or_default().add(v);
                i += 1;
            }
        }
        let batch = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(gs)),
                Arc::new(Int64Array::from(vs)),
            ],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", schema, vec![batch]).unwrap();
        (s, expected)
    }

    struct GotAgg {
        count_star: i64,
        count_val: i64,
        sum: Option<i64>,
        min: Option<i64>,
        max: Option<i64>,
        avg: Option<f64>,
        distinct: i64,
    }

    fn grouped_map(batches: &[Batch]) -> BTreeMap<i64, GotAgg> {
        let g = col_i64(batches, "g");
        let cs = col_i64(batches, "cs");
        let cv = col_i64(batches, "cv");
        let s = col_i64(batches, "s");
        let mn = col_i64(batches, "mn");
        let mx = col_i64(batches, "mx");
        let av = col_f64(batches, "av");
        let cd = col_i64(batches, "cd");
        let mut map = BTreeMap::new();
        for i in 0..g.len() {
            map.insert(
                g[i].unwrap(),
                GotAgg {
                    count_star: cs[i].unwrap(),
                    count_val: cv[i].unwrap(),
                    sum: s[i],
                    min: mn[i],
                    max: mx[i],
                    avg: av[i],
                    distinct: cd[i].unwrap(),
                },
            );
        }
        map
    }

    fn assert_matches_reference(n_groups: i64, rows_per_group: i64) {
        let (s, expected) = sweep_session(n_groups, rows_per_group);
        let out = s
            .execute_sql(
                "SELECT g, COUNT(*) AS cs, COUNT(v) AS cv, SUM(v) AS s, MIN(v) AS mn, MAX(v) AS mx, AVG(v) AS av, COUNT(DISTINCT v) AS cd FROM t GROUP BY g",
            )
            .unwrap();
        let got = grouped_map(&out);
        assert_eq!(got.len(), expected.len(), "group cardinality {n_groups}");
        for (g, exp) in expected {
            let row = got.get(&g).unwrap();
            assert_eq!(row.count_star, exp.count_star, "count(*) group {g}");
            assert_eq!(row.count_val, exp.count_val, "count(v) group {g}");
            assert_eq!(
                row.sum,
                if exp.sum_seen { Some(exp.sum) } else { None },
                "sum group {g}"
            );
            assert_eq!(row.min, exp.min, "min group {g}");
            assert_eq!(row.max, exp.max, "max group {g}");
            match (row.avg, exp.avg_n) {
                (None, 0) => {}
                (Some(got_av), n) if n > 0 => {
                    let want = exp.avg_sum / n as f64;
                    assert!(
                        (got_av - want).abs() < 1e-9,
                        "avg group {g}: {got_av} vs {want}"
                    );
                }
                other => panic!("avg group {g}: {other:?}"),
            }
            assert_eq!(
                row.distinct,
                exp.distinct.len() as i64,
                "count distinct group {g}"
            );
        }
    }

    #[test]
    fn group_cardinality_sweep_matches_reference() {
        for n_groups in [1, 16, 257, 4097] {
            assert_matches_reference(n_groups, 3);
        }
    }

    #[test]
    fn limit_keeps_input_order() {
        let out = session().execute_sql("SELECT a FROM t LIMIT 2").unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1), Some(2)]);
    }

    #[test]
    fn limit_offset_skips_then_takes() {
        let out = session()
            .execute_sql("SELECT a FROM t LIMIT 2 OFFSET 1")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(2), Some(3)]);
    }

    #[test]
    fn offset_past_end_is_empty() {
        let out = session().execute_sql("SELECT a FROM t OFFSET 10").unwrap();
        assert!(out.iter().all(|b| b.num_rows() == 0) || out.is_empty());
    }

    #[test]
    fn limit_zero_is_empty() {
        let out = session().execute_sql("SELECT a FROM t LIMIT 0").unwrap();
        assert!(out.iter().all(|b| b.num_rows() == 0) || out.is_empty());
    }

    #[test]
    fn order_by_asc_nulls_last() {
        let out = session().execute_sql("SELECT a FROM t ORDER BY a").unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1), Some(2), Some(3), None]);
    }

    #[test]
    fn order_by_desc_nulls_first() {
        let out = session()
            .execute_sql("SELECT a FROM t ORDER BY a DESC")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![None, Some(3), Some(2), Some(1)]);
    }

    #[test]
    fn order_by_nulls_first_and_last() {
        let first = session()
            .execute_sql("SELECT a FROM t ORDER BY a NULLS FIRST")
            .unwrap();
        assert_eq!(col_i64(&first, "a"), vec![None, Some(1), Some(2), Some(3)]);
        let last = session()
            .execute_sql("SELECT a FROM t ORDER BY a DESC NULLS LAST")
            .unwrap();
        assert_eq!(col_i64(&last, "a"), vec![Some(3), Some(2), Some(1), None]);
    }

    #[test]
    fn order_by_two_keys() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
        ]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("b"), Some("a"), Some("a")])),
                Arc::new(Int64Array::from(vec![Some(0), Some(2), Some(1)])),
            ],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("k", schema, vec![batch]).unwrap();
        let out = s.execute_sql("SELECT g, n FROM k ORDER BY g, n").unwrap();
        assert_eq!(
            col_utf8(&out, "g"),
            vec![Some("a".into()), Some("a".into()), Some("b".into())]
        );
        assert_eq!(col_i64(&out, "n"), vec![Some(1), Some(2), Some(0)]);
    }

    #[test]
    fn order_by_limit_is_topk() {
        let out = session()
            .execute_sql("SELECT a FROM t ORDER BY a LIMIT 2")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1), Some(2)]);
        let out = session()
            .execute_sql("SELECT a FROM t ORDER BY a DESC LIMIT 2 OFFSET 1")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(3), Some(2)]);
    }

    #[test]
    fn topk_on_large_input_matches_reference() {
        let n = 5000i64;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let values: Vec<Option<i64>> = (0..n).rev().map(Some).collect();
        let batch =
            Batch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap();
        let mut s = Session::new();
        s.register("big", schema, vec![batch]).unwrap();
        let out = s
            .execute_sql("SELECT a FROM big ORDER BY a LIMIT 7 OFFSET 3")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), (3..10).map(Some).collect::<Vec<_>>());
        let desc = s
            .execute_sql("SELECT a FROM big ORDER BY a DESC LIMIT 5")
            .unwrap();
        assert_eq!(
            col_i64(&desc, "a"),
            (n - 5..n).rev().map(Some).collect::<Vec<_>>()
        );
    }

    fn spill_root(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "mizuchi-mem-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn leftover(dir: &std::path::Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().into_owned()))
            .collect()
    }

    fn numbered_batches(n_batches: usize, rows: usize, start: i64) -> (SchemaRef, Vec<Batch>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("g", DataType::Int64, true),
        ]));
        let mut batches = Vec::new();
        let mut k = start;
        for _ in 0..n_batches {
            let mut a = Vec::with_capacity(rows);
            let mut g = Vec::with_capacity(rows);
            for _ in 0..rows {
                a.push(Some(k));
                g.push(Some(k.rem_euclid(7)));
                k += 1;
            }
            batches.push(
                Batch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int64Array::from(a)), Arc::new(Int64Array::from(g))],
                )
                .unwrap(),
            );
        }
        (schema, batches)
    }

    fn tight_limit(batches: &[Batch]) -> u64 {
        let sz = batches[0].as_arrow().get_array_memory_size() as u64;
        sz + sz / 2
    }

    fn register_numbered(n_batches: usize, rows: usize) -> (Session, u64) {
        let (schema, batches) = numbered_batches(n_batches, rows, 0);
        let cap = tight_limit(&batches);
        let mut s = Session::new();
        s.register("t", schema, batches).unwrap();
        (s, cap)
    }

    #[test]
    fn sort_spills_under_tiny_limit_and_matches() {
        let (s, cap) = register_numbered(12, 32);
        let sql = "SELECT a FROM t ORDER BY a";
        let want = col_i64(&s.execute_sql(sql).unwrap(), "a");
        let root = spill_root("sort");
        let got = s
            .clone()
            .with_memory_limit(cap)
            .with_spill_dir(root.clone())
            .execute_sql(sql)
            .unwrap();
        assert_eq!(col_i64(&got, "a"), want);
        let left = leftover(&root);
        let _ = std::fs::remove_dir_all(&root);
        assert!(left.is_empty(), "{left:?}");
    }

    #[test]
    fn join_and_agg_spill_under_tiny_limit_and_match() {
        let (t_schema, t_batches) = numbered_batches(12, 32, 0);
        let (u_schema, u_batches) = numbered_batches(8, 16, 0);
        // Finish() of a few aggregate columns is larger than one scan morsel
        // after Arrow buffer rounding; keep the cap below the registered set.
        let cap = tight_limit(&t_batches)
            .max(tight_limit(&u_batches))
            .saturating_mul(8);
        let mut s = Session::new();
        s.register("t", t_schema, t_batches).unwrap();
        s.register("u", u_schema, u_batches).unwrap();
        let root = spill_root("join-agg");
        let limited = s
            .clone()
            .with_memory_limit(cap)
            .with_spill_dir(root.clone());

        let join_sql = "SELECT t.a, u.a AS id FROM t JOIN u ON t.a = u.a";
        let want_join = sorted_pairs(&s.execute_sql(join_sql).unwrap(), "a", "id");
        let got_join = sorted_pairs(&limited.execute_sql(join_sql).unwrap(), "a", "id");
        assert_eq!(got_join, want_join);

        let fused = "SELECT COUNT(*) AS c FROM t JOIN u ON t.a = u.a";
        let want_fused = col_i64(&s.execute_sql(fused).unwrap(), "c");
        let got_fused = col_i64(&limited.execute_sql(fused).unwrap(), "c");
        assert_eq!(got_fused, want_fused);

        let semi_sql = "SELECT t.a FROM t LEFT SEMI JOIN u ON t.a = u.a";
        let mut want_semi = col_i64(&s.execute_sql(semi_sql).unwrap(), "a");
        let mut got_semi = col_i64(&limited.execute_sql(semi_sql).unwrap(), "a");
        want_semi.sort();
        got_semi.sort();
        assert_eq!(got_semi, want_semi);

        let agg_sql =
            "SELECT g, COUNT(*) AS c, SUM(a) AS s, MIN(a) AS mn, MAX(a) AS mx FROM t GROUP BY g";
        let want_agg = s.execute_sql(agg_sql).unwrap();
        let got_agg = limited.execute_sql(agg_sql).unwrap();
        let mut want_rows: Vec<_> = col_i64(&want_agg, "g")
            .into_iter()
            .zip(col_i64(&want_agg, "c"))
            .zip(col_i64(&want_agg, "s"))
            .collect();
        let mut got_rows: Vec<_> = col_i64(&got_agg, "g")
            .into_iter()
            .zip(col_i64(&got_agg, "c"))
            .zip(col_i64(&got_agg, "s"))
            .collect();
        want_rows.sort();
        got_rows.sort();
        assert_eq!(got_rows, want_rows);
        assert_eq!(
            col_i64(&got_agg, "mn").len(),
            col_i64(&want_agg, "mn").len()
        );

        let left = leftover(&root);
        let _ = std::fs::remove_dir_all(&root);
        assert!(left.is_empty(), "{left:?}");
    }

    #[test]
    fn query_completes_under_budget_below_working_set() {
        let (t_schema, t_batches) = numbered_batches(24, 64, 0);
        let (u_schema, u_batches) = numbered_batches(4, 16, 0);
        let working: u64 = t_batches
            .iter()
            .chain(u_batches.iter())
            .map(|b| b.as_arrow().get_array_memory_size() as u64)
            .sum();
        let largest = t_batches
            .iter()
            .chain(u_batches.iter())
            .map(|b| b.as_arrow().get_array_memory_size() as u64)
            .max()
            .unwrap();
        // Join output is wider than a scan morsel; keep the cap below the
        // registered working set and above one pipeline batch.
        let cap = (working / 3).max(largest * 8);
        assert!(
            cap < working,
            "cap {cap} must sit below working set {working}"
        );
        let mut s = Session::new();
        s.register("t", t_schema, t_batches).unwrap();
        s.register("u", u_schema, u_batches).unwrap();
        let sql = "SELECT t.g, COUNT(*) AS n FROM t JOIN u ON t.a = u.a GROUP BY t.g ORDER BY g";
        let want = col_i64(&s.execute_sql(sql).unwrap(), "n");
        let root = spill_root("budget");
        let limited = s
            .clone()
            .with_memory_limit(cap)
            .with_spill_dir(root.clone());
        let got = limited
            .execute_sql_reported(sql)
            .unwrap_or_else(|e| panic!("budget below working set must spill: {e}"));
        assert_eq!(col_i64(&got.batches, "n"), want);
        assert!(
            got.spilled_bytes > 0,
            "peak={} spilled={}",
            got.peak_bytes,
            got.spilled_bytes
        );
        assert!(
            got.peak_bytes <= cap,
            "peak={} cap={} spilled={}",
            got.peak_bytes,
            cap,
            got.spilled_bytes
        );
        let text = limited.explain_analyze(sql).unwrap();
        assert!(text.contains("query peak_bytes="), "{text}");
        assert!(text.contains("spilled_bytes="), "{text}");
        let left = leftover(&root);
        let _ = std::fs::remove_dir_all(&root);
        assert!(left.is_empty(), "{left:?}");
        let budgeted = s.with_system_memory_budget();
        assert!(budgeted.execute_sql(sql).is_ok());
        assert!(memory::system_budget() >= 64 * 1024 * 1024);
    }

    #[test]
    fn memory_limit_without_spill_dir_fails_closed() {
        let (s, cap) = register_numbered(12, 32);
        let err = s
            .clone()
            .with_memory_limit(cap)
            .execute_sql("SELECT a FROM t ORDER BY a");
        assert!(matches!(err, Err(Error::Memory(_))), "{err:?}");
        let err = s
            .clone()
            .with_memory_limit(cap)
            .execute_sql("SELECT g, COUNT(*) AS c FROM t GROUP BY g");
        assert!(matches!(err, Err(Error::Memory(_))), "{err:?}");
    }

    #[test]
    fn avg_and_count_distinct_fail_closed_when_they_would_spill() {
        let (s, cap) = register_numbered(12, 32);
        let root = spill_root("avg");
        let limited = s
            .clone()
            .with_memory_limit(cap)
            .with_spill_dir(root.clone());
        let avg = limited.execute_sql("SELECT AVG(a) AS av FROM t");
        assert!(matches!(avg, Err(Error::Memory(_))), "{avg:?}");
        let cd = limited.execute_sql("SELECT COUNT(DISTINCT a) AS cd FROM t");
        assert!(matches!(cd, Err(Error::Memory(_))), "{cd:?}");
        let left = leftover(&root);
        let _ = std::fs::remove_dir_all(&root);
        assert!(left.is_empty(), "{left:?}");
    }

    #[test]
    fn outer_join_fails_closed_when_it_would_spill() {
        let (t_schema, t_batches) = numbered_batches(12, 32, 0);
        let (u_schema, u_batches) = numbered_batches(8, 16, 0);
        let cap = tight_limit(&t_batches).max(tight_limit(&u_batches));
        let mut s = Session::new();
        s.register("t", t_schema, t_batches).unwrap();
        s.register("u", u_schema, u_batches).unwrap();
        let root = spill_root("outer");
        let err = s
            .with_memory_limit(cap)
            .with_spill_dir(root.clone())
            .execute_sql("SELECT t.a, u.a AS id FROM t LEFT JOIN u ON t.a = u.a");
        let left = leftover(&root);
        let _ = std::fs::remove_dir_all(&root);
        assert!(matches!(err, Err(Error::Memory(_))), "{err:?}");
        assert!(left.is_empty(), "{left:?}");
    }

    #[test]
    fn two_workers_match_one_on_scan_and_agg() {
        let (schema, batches) = numbered_batches(16, 64, 0);
        let mut s = Session::new();
        s.register("t", schema, batches).unwrap();
        let one = s.clone();
        let many = s.with_workers(4).unwrap();
        let scan_sql = "SELECT a FROM t WHERE a > 10";
        assert_eq!(
            col_i64(&one.execute_sql(scan_sql).unwrap(), "a"),
            col_i64(&many.execute_sql(scan_sql).unwrap(), "a")
        );
        let agg_sql = "SELECT g, COUNT(*) AS c, SUM(a) AS s FROM t GROUP BY g";
        let want = one.execute_sql(agg_sql).unwrap();
        let got = many.execute_sql(agg_sql).unwrap();
        let mut want_rows: Vec<_> = col_i64(&want, "g")
            .into_iter()
            .zip(col_i64(&want, "c"))
            .zip(col_i64(&want, "s"))
            .collect();
        let mut got_rows: Vec<_> = col_i64(&got, "g")
            .into_iter()
            .zip(col_i64(&got, "c"))
            .zip(col_i64(&got, "s"))
            .collect();
        want_rows.sort();
        got_rows.sort();
        assert_eq!(got_rows, want_rows);
        let avg_sql = "SELECT g, AVG(a) AS v FROM t GROUP BY g ORDER BY g";
        let want_avg = col_f64(&one.execute_sql(avg_sql).unwrap(), "v");
        let got_avg = col_f64(&many.execute_sql(avg_sql).unwrap(), "v");
        assert_eq!(want_avg.len(), got_avg.len());
        for (w, g) in want_avg.iter().zip(got_avg.iter()) {
            match (w, g) {
                (Some(a), Some(b)) => assert!((a - b).abs() < 1e-9, "{a} vs {b}"),
                (None, None) => {}
                other => panic!("{other:?}"),
            }
        }
        let mut join_s = Session::new();
        let (t_schema, t_batches) = numbered_batches(16, 64, 0);
        let (u_schema, u_batches) = numbered_batches(8, 64, 0);
        join_s.register("t", t_schema, t_batches).unwrap();
        join_s.register("u", u_schema, u_batches).unwrap();
        let join_sql = "SELECT COUNT(*) AS c FROM t JOIN u ON t.a = u.a";
        let one_j = join_s.clone().execute_sql(join_sql).unwrap();
        let many_j = join_s
            .with_workers(4)
            .unwrap()
            .execute_sql(join_sql)
            .unwrap();
        assert_eq!(col_i64(&one_j, "c"), col_i64(&many_j, "c"));
    }

    #[test]
    fn cancel_before_execute_fails_closed() {
        let token = CancelToken::new();
        token.cancel();
        let (s, _) = register_numbered(8, 32);
        let err = s
            .with_cancel(token)
            .execute_sql("SELECT a FROM t WHERE a >= 0");
        assert!(matches!(err, Err(Error::Cancelled)), "{err:?}");
    }

    #[test]
    fn cancel_stops_a_running_query() {
        let token = CancelToken::new();
        let (schema, batches) = numbered_batches(64, 256, 0);
        let mut s = Session::new();
        s.register("t", schema, batches).unwrap();
        let s = s.with_cancel(token.clone()).with_workers(2).unwrap();
        let started = std::time::Instant::now();
        let handle = std::thread::spawn(move || {
            s.execute_sql("SELECT g, COUNT(*) AS c, SUM(a) AS s FROM t GROUP BY g")
        });
        std::thread::sleep(std::time::Duration::from_millis(2));
        token.cancel();
        let result = handle.join().expect("worker");
        let elapsed = started.elapsed();
        match result {
            Err(Error::Cancelled) => {}
            Ok(_) => {
                let err = Session::new()
                    .with_cancel(token)
                    .execute_sql("SELECT 1 AS x");
                assert!(matches!(err, Err(Error::Cancelled)), "{err:?}");
            }
            Err(other) => panic!("{other:?}"),
        }
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "cancel took {elapsed:?}"
        );
    }

    struct DropAll;

    impl Operator for DropAll {
        fn next_batch(&mut self, cancel: &CancelToken) -> Result<Option<Batch>> {
            cancel.check()?;
            Ok(None)
        }
    }

    struct DropScanLowering;

    impl Lowering for DropScanLowering {
        fn scan(&self, _batches: Vec<Batch>) -> Result<Box<dyn Operator>> {
            Ok(Box::new(DropAll))
        }
    }

    #[test]
    fn custom_scan_lowering_is_selected() {
        let default_rows: usize = session()
            .execute_sql("SELECT a FROM t")
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(default_rows, 4);

        let session = session().with_lowering(Arc::new(DropScanLowering));
        let out = session.execute_sql("SELECT a FROM t").unwrap();
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 0, "custom scan lowering must run instead of CPU scan");
    }

    #[test]
    fn execute_ddl_and_show_tables() {
        let mut s = Session::new();
        s.execute_ddl("CREATE TABLE items (id BIGINT, note TEXT)")
            .unwrap();
        s.execute_ddl("ALTER TABLE items ADD COLUMN year BIGINT")
            .unwrap();
        assert_eq!(s.catalog().table("items").unwrap().fields().len(), 3);
        let shown = s.execute_sql("SHOW TABLES").unwrap();
        assert_eq!(col_utf8(&shown, "table_name"), vec![Some("items".into())]);
        assert_eq!(col_utf8(&shown, "namespace"), vec![Some("public".into())]);
        s.execute_ddl("DROP TABLE items").unwrap();
        assert!(s.catalog().table("items").is_err());
        s.execute_ddl("DROP TABLE IF EXISTS items").unwrap();
    }

    #[test]
    fn three_part_names_query_and_show() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(1), Some(2)]))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("lake.sales.orders", schema, vec![batch])
            .unwrap();
        let out = s.execute_sql("SELECT id FROM lake.sales.orders").unwrap();
        assert_eq!(col_i64(&out, "id"), vec![Some(1), Some(2)]);
        let shown = s.execute_sql("SHOW TABLES FROM lake.sales").unwrap();
        assert_eq!(col_utf8(&shown, "table_name"), vec![Some("orders".into())]);
        assert_eq!(
            col_utf8(&shown, "namespace"),
            vec![Some("lake.sales".into())]
        );
    }

    #[test]
    fn alter_table_rewrites_batches_and_invalidates_cache() {
        let mut s = session();
        let before = s.execute_sql("SELECT a FROM t").unwrap();
        assert_eq!(col_i64(&before, "a"), vec![Some(1), Some(2), Some(3), None]);
        s.execute_ddl("ALTER TABLE t ADD COLUMN note TEXT").unwrap();
        let notes = s.execute_sql("SELECT note FROM t").unwrap();
        assert_eq!(col_utf8(&notes, "note"), vec![None, None, None, None]);
        s.execute_ddl("ALTER TABLE t RENAME COLUMN a TO ident")
            .unwrap();
        let renamed = s.execute_sql("SELECT ident FROM t").unwrap();
        assert_eq!(
            col_i64(&renamed, "ident"),
            vec![Some(1), Some(2), Some(3), None]
        );
    }

    #[test]
    fn file_catalog_select_does_not_rewrite_sidecar() {
        let dir =
            std::env::temp_dir().join(format!("mizuchi-catalog-select-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(1)]))],
        )
        .unwrap();
        let mut cat = Catalog::open_file(&dir).unwrap();
        cat.register("t", schema.clone()).unwrap();
        let mut s = Session::new().with_catalog(cat);
        s.register("t", schema, vec![batch]).unwrap();
        let json = dir.join("mizuchi").join("public").join("t.json");
        let before = std::fs::read(&json).unwrap();
        let out = s.execute_sql("SELECT id FROM t").unwrap();
        assert_eq!(col_i64(&out, "id"), vec![Some(1)]);
        let after = std::fs::read(&json).unwrap();
        assert_eq!(before, after);
        let empty = Session::new().with_catalog(Catalog::open_file(&dir).unwrap());
        let rows: usize = empty
            .execute_sql("SELECT id FROM t")
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn three_part_qualified_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(9)]))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("lake.sales.orders", schema, vec![batch])
            .unwrap();
        let out = s
            .execute_sql("SELECT orders.id FROM lake.sales.orders")
            .unwrap();
        assert_eq!(col_i64(&out, "id"), vec![Some(9)]);
    }

    #[test]
    fn catalog_partitions_drive_report_pruning() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("year", DataType::Int64, true),
        ]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
                Arc::new(Int64Array::from(vec![Some(2020), Some(2021)])),
            ],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("sales", schema, vec![batch]).unwrap();
        s.set_partitions(
            "sales",
            vec!["year".into()],
            vec!["year=2020".into(), "year=2021".into()],
        )
        .unwrap();
        let result = s
            .execute_sql_reported("SELECT id FROM sales WHERE year = 2020")
            .unwrap();
        assert_eq!(
            result.report.inputs[0].partitions,
            vec!["year=2020".to_string()]
        );
        assert_eq!(col_i64(&result.batches, "id"), vec![Some(1)]);
    }
}
