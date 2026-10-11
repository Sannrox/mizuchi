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
mod memory;
mod merge;
mod metrics;
mod op;
mod recursive;
mod report;
mod schedule;
mod sort;
mod stage;
mod stream;
mod window;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use arrow::array::{
    Array, BooleanArray, RecordBatch, RecordBatchOptions, RecordBatchReader, StringArray,
};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::boolean::{and, is_not_null};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::batch::{Batch, BatchPolicy};
use crate::error::{Error, Result};
use crate::expr::Expr;
use crate::plan::{
    AlterTable, Catalog, CatalogProvider, Ddl, LogicalPlan, NamedExpr, TableIdent, apply_alter,
    bind_ddl, normalize_name,
};
use crate::udaf::CombineAgg;

pub use cluster::{Cluster, ClusterConfig, WorkerResources};
pub use config::SessionConfig;
pub use engine::Engine;
pub use incremental::MicroBatchAgg;
pub use merge::MergeStats;
pub use metrics::OperatorMetrics;
pub use op::{CpuLowering, Lowering, Operator};
pub use report::{InputScan, OutputWrite, QueryReport, QueryResult};
pub use stream::{EventWindow, StreamOutput, StreamQuery, StreamQueryConfig, StreamTrigger};

/// Cooperative cancellation flag. Operators check it between morsels.
#[derive(Clone, Debug, Default)]
pub struct CancelToken {
    flag: Arc<AtomicBool>,
}

impl CancelToken {
    /// A token that is not cancelled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Request that the running query stop at the next morsel boundary.
    pub fn cancel(&self) {
        self.flag.store(true, Ordering::SeqCst);
    }

    /// Whether [`Self::cancel`] has been called.
    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    pub(crate) fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(Error::Cancelled)
        } else {
            Ok(())
        }
    }
}

/// In-memory tables plus a batch-size policy.
#[derive(Debug)]
pub struct Session {
    tables: BTreeMap<String, MemTable>,
    catalog: Catalog,
    policy: BatchPolicy,
    /// Parallel morsel workers. Default 1 keeps the pull iterator path.
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
    /// Combinable user-defined aggregates, keyed by lower-case SQL name.
    udafs: BTreeMap<String, Arc<dyn CombineAgg>>,
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
            udafs: BTreeMap::new(),
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
            udafs: self.udafs.clone(),
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
    stats: crate::plan::TableStats,
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

    /// Cooperative cancellation token checked between morsels.
    pub fn with_cancel(mut self, token: CancelToken) -> Self {
        self.cancel = token;
        self
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

    /// Register an in-memory table. All batches must share `schema`.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        schema: SchemaRef,
        batches: Vec<Batch>,
    ) -> Result<()> {
        let schema = crate::schema::from_arrow_schema(schema)?;
        for batch in &batches {
            if batch.schema().as_ref() != schema.as_ref() {
                return Err(Error::InvalidBatch(
                    "registered batch schema does not match the table".into(),
                ));
            }
        }
        let mut stats = crate::plan::table_stats_from_batches(schema.as_ref(), &batches);
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
            let raw = if ns.catalog == crate::plan::DEFAULT_CATALOG {
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
                self.catalog.create_table(crate::plan::TableMeta {
                    ident: ident.clone(),
                    schema: schema.clone(),
                    stats: crate::plan::TableStats::default(),
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
                        crate::plan::table_stats_from_batches(next.as_ref(), &table.batches);
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
        opts: crate::read::ParquetReadOptions<'_>,
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
        opts: crate::read::CsvReadOptions<'_>,
    ) -> Result<()> {
        let path = path.as_ref();
        config::assert_allowed(path, &self.allowed_read_paths)?;
        let batches = crate::read::read_csv(path, opts)?;
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
        opts: crate::read::JsonReadOptions<'_>,
    ) -> Result<()> {
        let path = path.as_ref();
        config::assert_allowed(path, &self.allowed_read_paths)?;
        let batches = crate::read::read_json(path, opts)?;
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
        opts: crate::read::AvroReadOptions<'_>,
    ) -> Result<()> {
        let path = path.as_ref();
        config::assert_allowed(path, &self.allowed_read_paths)?;
        let batches = crate::read::read_avro(path, opts)?;
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
        opts: crate::read::OrcReadOptions<'_>,
    ) -> Result<()> {
        let path = path.as_ref();
        config::assert_allowed(path, &self.allowed_read_paths)?;
        let batches = crate::read::read_orc(path, opts)?;
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
        source: &crate::postgres::PostgresSource,
        opts: crate::postgres::PostgresReadOptions<'_>,
    ) -> Result<()> {
        if !self.allow_network {
            return Err(Error::Io("network is not granted".into()));
        }
        let batches = crate::postgres::read_postgres(source, opts)?;
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
        let schema = crate::schema::from_arrow_schema(reader.schema())?;
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
        opts: crate::write::ParquetWriteOptions<'_>,
    ) -> Result<()> {
        let _ = self.write_parquet_reported(dest, batches, opts)?;
        Ok(())
    }

    /// Write newline-delimited JSON after the write allow-list check.
    pub fn write_json(&self, dest: impl AsRef<Path>, batches: &[Batch]) -> Result<()> {
        let dest = dest.as_ref();
        config::assert_allowed(dest, &self.allowed_write_paths)?;
        crate::write::write_json(dest, batches)
    }

    /// Write an Avro Object Container File after the write allow-list check.
    pub fn write_avro(&self, dest: impl AsRef<Path>, batches: &[Batch]) -> Result<()> {
        let dest = dest.as_ref();
        config::assert_allowed(dest, &self.allowed_write_paths)?;
        crate::write::write_avro(dest, batches)
    }

    /// Write an uncompressed ORC file after the write allow-list check.
    pub fn write_orc(&self, dest: impl AsRef<Path>, batches: &[Batch]) -> Result<()> {
        let dest = dest.as_ref();
        config::assert_allowed(dest, &self.allowed_write_paths)?;
        crate::write::write_orc(dest, batches)
    }

    /// [`Self::write_parquet`] plus the output side of a [`QueryReport`].
    pub fn write_parquet_reported(
        &self,
        dest: impl AsRef<Path>,
        batches: &[Batch],
        opts: crate::write::ParquetWriteOptions<'_>,
    ) -> Result<QueryReport> {
        let dest = dest.as_ref();
        config::assert_allowed(dest, &self.allowed_write_paths)?;
        let stats = crate::write::write_parquet_stats(dest, batches, opts, &self.cancel)?;
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
        let plan = crate::plan::bind_merge(sql, &catalog)?;
        let (batches, stats) = merge::run(self, &plan)?;
        self.register(stats.table.clone(), plan.target_schema, batches)?;
        Ok(stats)
    }

    /// Write a registered table as a Delta table after the write allow-list check.
    pub fn write_delta(
        &self,
        dest: impl AsRef<Path>,
        name: &str,
        mode: crate::write::WriteMode,
    ) -> Result<crate::lake::LakeSnapshot> {
        let dest = dest.as_ref();
        config::assert_allowed(dest, &self.allowed_write_paths)?;
        let (_, batches) = self.table_data(name)?;
        crate::lake::write_delta(dest, &batches, mode)
    }

    /// Load a Delta snapshot as an in-memory table after the read allow-list check.
    pub fn register_delta(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        travel: crate::lake::TimeTravel,
    ) -> Result<()> {
        let path = path.as_ref();
        config::assert_allowed(path, &self.allowed_read_paths)?;
        let batches = crate::lake::read_delta(path, travel)?;
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
    ) -> Result<crate::lake::LakeSnapshot> {
        let dest = dest.as_ref();
        config::assert_allowed(dest, &self.allowed_write_paths)?;
        config::assert_allowed(dest, &self.allowed_read_paths)?;
        let target = crate::plan::merge_target_table(sql)?;
        let head = crate::lake::delta_head(dest)?;
        let batches = crate::lake::read_delta(dest, crate::lake::TimeTravel::Latest)?;
        let schema = match batches.first() {
            Some(b) => b.schema(),
            None => {
                return Err(Error::Io(format!("{} produced no batches", dest.display())));
            }
        };
        self.merge_and_commit(sql, target, schema, batches, |session, name| {
            let (_, out) = session.table_data(name)?;
            crate::lake::write_delta_at(
                dest,
                &out,
                crate::write::WriteMode::Overwrite,
                Some(head.version),
            )
        })
    }

    /// Write a registered table as an Iceberg table after the write allow-list check.
    pub fn write_iceberg(
        &self,
        dest: impl AsRef<Path>,
        name: &str,
        mode: crate::write::WriteMode,
    ) -> Result<crate::lake::LakeSnapshot> {
        let dest = dest.as_ref();
        config::assert_allowed(dest, &self.allowed_write_paths)?;
        let (_, batches) = self.table_data(name)?;
        crate::lake::write_iceberg(dest, &batches, mode)
    }

    /// Load an Iceberg snapshot as an in-memory table after the read allow-list check.
    pub fn register_iceberg(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        travel: crate::lake::TimeTravel,
    ) -> Result<()> {
        let path = path.as_ref();
        config::assert_allowed(path, &self.allowed_read_paths)?;
        let batches = crate::lake::read_iceberg(path, travel)?;
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
    ) -> Result<crate::lake::LakeSnapshot> {
        let dest = dest.as_ref();
        config::assert_allowed(dest, &self.allowed_write_paths)?;
        config::assert_allowed(dest, &self.allowed_read_paths)?;
        let target = crate::plan::merge_target_table(sql)?;
        let head = crate::lake::iceberg_head(dest)?;
        let batches = crate::lake::read_iceberg(dest, crate::lake::TimeTravel::Latest)?;
        let schema = match batches.first() {
            Some(b) => b.schema(),
            None => {
                return Err(Error::Io(format!("{} produced no batches", dest.display())));
            }
        };
        self.merge_and_commit(sql, target, schema, batches, |session, name| {
            let (_, out) = session.table_data(name)?;
            crate::lake::write_iceberg_at(
                dest,
                &out,
                crate::write::WriteMode::Overwrite,
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
        commit: impl FnOnce(&Session, &str) -> Result<crate::lake::LakeSnapshot>,
    ) -> Result<crate::lake::LakeSnapshot> {
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
        self.cancel.check()?;
        let plan = crate::opt::optimize(plan.clone())?;
        let batches = self.execute_optimized(&plan)?;
        Ok(QueryResult {
            report: self.report_for(&plan),
            batches,
        })
    }

    /// Times a later [`Self::execute_sql`] or [`Self::execute_staged`] reused a
    /// stored result because the plan and table generations still matched.
    pub fn cache_hits(&self) -> u64 {
        self.cache.lock().map(|c| c.hits).unwrap_or(0)
    }

    /// Plan and run `sql` against registered tables.
    pub fn execute_sql(&self, sql: &str) -> Result<Vec<Batch>> {
        self.cancel.check()?;
        if let Ok(Ddl::ShowTables { namespace }) = bind_ddl(sql) {
            return self.show_tables(namespace.as_deref());
        }
        if let Some((analyze, inner)) = explain_sql(sql)? {
            let text = if analyze {
                self.explain_analyze(&inner)?
            } else {
                self.explain(&inner)?
            };
            return plan_text_batch(&text);
        }
        let plan = self.bind_optimized(sql)?;
        self.execute_optimized(&plan)
    }

    /// [`Self::execute_sql`] plus a [`QueryReport`].
    pub fn execute_sql_reported(&self, sql: &str) -> Result<QueryResult> {
        self.cancel.check()?;
        if let Ok(Ddl::ShowTables { namespace }) = bind_ddl(sql) {
            let batches = self.show_tables(namespace.as_deref())?;
            return Ok(QueryResult {
                batches,
                report: QueryReport::new("SHOW TABLES".into(), Vec::new()),
            });
        }
        if let Some((analyze, inner)) = explain_sql(sql)? {
            let text = if analyze {
                self.explain_analyze(&inner)?
            } else {
                self.explain(&inner)?
            };
            let batches = plan_text_batch(&text)?;
            return Ok(QueryResult {
                batches,
                report: QueryReport::new(text, Vec::new()),
            });
        }
        let plan = self.bind_optimized(sql)?;
        let batches = self.execute_optimized(&plan)?;
        Ok(QueryResult {
            report: self.report_for(&plan),
            batches,
        })
    }

    fn report_for(&self, plan: &LogicalPlan) -> QueryReport {
        let mut inputs = Vec::new();
        collect_scan_reports(plan, self, &mut inputs);
        QueryReport::new(report::plan_text(plan), inputs)
    }

    fn execute_optimized(&self, plan: &LogicalPlan) -> Result<Vec<Batch>> {
        self.execute_cached(plan, || {
            let pool = self.memory_pool()?;
            if self.workers > 1
                && let Some(out) = schedule::try_parallel(plan, self, &pool)?
            {
                return Ok(out);
            }
            collect(physical(plan, self, &pool)?, &self.cancel)
        })
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
        let _ = collect(op, &self.cancel)?;
        Ok(tree.snapshot())
    }

    /// Split the optimized plan into stages at hash-partition exchanges.
    pub fn explain_stages(&self, sql: &str) -> Result<String> {
        stage::explain_stages(&self.bind_optimized(sql)?, self.workers.max(1))
    }

    /// Run `sql` as shuffle-map and result stages on `workers` threads.
    pub fn execute_staged(&self, sql: &str) -> Result<Vec<Batch>> {
        stage::execute_staged(&self.bind_optimized(sql)?, self)
    }

    /// Run the query and print logical/physical plans with per-operator
    /// rows, batches, elapsed time, and peak pool bytes. Appends adaptive
    /// shuffle notes from the staged path.
    pub fn explain_analyze(&self, sql: &str) -> Result<String> {
        let plan = self.bind_optimized(sql)?;
        let pool = self.memory_pool()?;
        let (op, tree) = build_physical(&plan, self, &pool, true, None)?;
        let _batches = collect(op, &self.cancel)?;
        let mut text = format!(
            "== Logical Plan ==\n{}== Physical Plan ==\n{}",
            plan.explain(),
            tree.format(true)
        );
        text.push_str(&stage::explain_analyze(&plan, self)?);
        Ok(text)
    }

    pub(super) fn memory_pool(&self) -> Result<memory::MemoryPool> {
        match &self.budget {
            Some(budget) => memory::MemoryPool::with_budget(budget.clone(), self.spill_dir.clone()),
            None => memory::MemoryPool::new(self.memory_limit, self.spill_dir.clone()),
        }
    }

    pub(super) fn table_morsels(&self, table: &str) -> Result<Vec<Batch>> {
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
        let plan = crate::plan::plan_sql_with_udafs(sql, &catalog, &self.udafs)?;
        crate::opt::optimize(plan)
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
    opts: crate::read::ParquetReadOptions<'_>,
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
        out.extend(crate::read::read_parquet(
            file,
            crate::read::ParquetReadOptions {
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
    let ns_label = namespace.unwrap_or(crate::plan::DEFAULT_NAMESPACE);
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
                partitions = crate::store::prune_partitions(&partitions, filters);
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
        LogicalPlan::Join { left, right, .. } => {
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
pub(super) enum Exec {
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

pub(super) fn physical(
    plan: &LogicalPlan,
    session: &Session,
    pool: &memory::MemoryPool,
) -> Result<Box<dyn Operator>> {
    Ok(build_physical(plan, session, pool, false, None)?.0)
}

pub(super) fn physical_with_work(
    plan: &LogicalPlan,
    session: &Session,
    pool: &memory::MemoryPool,
    analyze: bool,
    work: Option<(&str, &[Batch])>,
) -> Result<Box<dyn Operator>> {
    Ok(build_physical(plan, session, pool, analyze, work)?.0)
}

fn build_physical(
    plan: &LogicalPlan,
    session: &Session,
    pool: &memory::MemoryPool,
    analyze: bool,
    work: Option<(&str, &[Batch])>,
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
                for pred in filters {
                    match filter_batch(&batch, pred)? {
                        Some(kept) => batch = kept,
                        None => {
                            batch = Batch::empty(batch.schema())?;
                            break;
                        }
                    }
                }
                if let Some(names) = projection {
                    let mut indices = Vec::with_capacity(names.len());
                    for name in names {
                        let idx = batch
                            .schema()
                            .index_of(name)
                            .map_err(|e| Error::Plan(format!("scan projection: {e}")))?;
                        indices.push(idx);
                    }
                    batch = batch.project(&indices)?;
                }
                morsels.extend(batch.split_by_policy(&session.policy));
            }
            Ok(metrics::finish(
                "Scan",
                lowering.scan(morsels)?,
                Vec::new(),
                analyze,
                pool,
            ))
        }
        LogicalPlan::Filter { input, predicate } => {
            let (child, child_node) = build_physical(input, session, pool, analyze, work)?;
            Ok(metrics::finish(
                "Filter",
                lowering.filter(child, predicate.clone())?,
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
            let (child, child_node) = build_physical(input, session, pool, analyze, work)?;
            Ok(metrics::finish(
                "Project",
                lowering.project(child, exprs.clone(), schema.clone())?,
                vec![child_node],
                analyze,
                pool,
            ))
        }
        LogicalPlan::Aggregate {
            input,
            group,
            aggs,
            schema,
        } => {
            let (child, child_node) = build_physical(input, session, pool, analyze, work)?;
            let t0 = Instant::now();
            let batches = agg::run(
                child,
                group,
                aggs,
                schema.clone(),
                &session.policy,
                pool,
                &session.cancel,
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
                metrics::add_blocking(&node, ns, pool.peak());
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
            let (left_op, left_node) = build_physical(left, session, pool, analyze, work)?;
            let (right_op, right_node) = build_physical(right, session, pool, analyze, work)?;
            let t0 = Instant::now();
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
                &session.cancel,
            )?;
            let ns = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let (op, node) = metrics::finish(
                "HashJoin",
                lowering.scan(batches)?,
                vec![left_node, right_node],
                analyze,
                pool,
            );
            if analyze {
                metrics::add_blocking(&node, ns, pool.peak());
            }
            Ok((op, node))
        }
        LogicalPlan::Sort { input, order } => {
            let (child, child_node) = build_physical(input, session, pool, analyze, work)?;
            let t0 = Instant::now();
            let batches = sort::ordered(
                child,
                input.schema().clone(),
                order,
                0,
                None,
                &session.policy,
                pool,
                &session.cancel,
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
                metrics::add_blocking(&node, ns, pool.peak());
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
                let batches = sort::ordered(
                    child,
                    sorted.schema().clone(),
                    order,
                    *skip,
                    *fetch,
                    &session.policy,
                    pool,
                    &session.cancel,
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
                    metrics::add_blocking(&node, ns, pool.peak());
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
        LogicalPlan::Union { .. } => Err(Error::NotImplemented("physical union")),
        LogicalPlan::Recursive {
            name,
            anchors,
            step,
            schema,
        } => {
            let t0 = Instant::now();
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
                metrics::add_blocking(&node, ns, pool.peak());
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
                &session.cancel,
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
                metrics::add_blocking(&node, ns, pool.peak());
            }
            Ok((op, node))
        }
    }
}

fn explain_sql(sql: &str) -> Result<Option<(bool, String)>> {
    if !sql
        .trim_start()
        .get(..7)
        .is_some_and(|s| s.eq_ignore_ascii_case("EXPLAIN"))
    {
        return Ok(None);
    }
    let stmt = crate::plan::parse_statements(sql)?;
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

pub(super) fn collect(mut exec: Box<dyn Operator>, cancel: &CancelToken) -> Result<Vec<Batch>> {
    let mut out = Vec::new();
    while let Some(batch) = exec.next_batch(cancel)? {
        if batch.num_rows() > 0 {
            out.push(batch);
        }
    }
    Ok(out)
}

pub(super) fn filter_batch(batch: &Batch, predicate: &Expr) -> Result<Option<Batch>> {
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

pub(super) fn project_batch(
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
    use crate::udaf::ProductI64;
    use arrow::array::{
        Array, ArrayRef, Date32Array, Float64Array, Int64Array, LargeStringArray, StringArray,
        UInt32Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
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
        assert!(text.contains("peak_bytes="), "{text}");

        let metrics = s.query_metrics(sql).unwrap();
        assert_eq!(metrics.name, "HashAggregate");
        assert!(metrics.rows > 0, "{metrics:?}");
        assert!(metrics.batches > 0, "{metrics:?}");
        assert!(metrics.elapsed_ns > 0, "{metrics:?}");
        assert_eq!(metrics.children.len(), 1);
        assert_eq!(metrics.children[0].name, "HashJoin");
        let json = metrics.to_json();
        assert!(json.contains("\"name\":\"HashAggregate\""), "{json}");
        assert!(json.contains("\"name\":\"HashJoin\""), "{json}");
        assert!(json.contains("\"rows\":"), "{json}");
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
        let cap = tight_limit(&t_batches).max(tight_limit(&u_batches));
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
