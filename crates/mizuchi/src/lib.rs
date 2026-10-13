//! mizuchi: an Arrow-native query and transform engine.
//!
//! Columnar memory is standard Apache Arrow record batches. See [`batch`] and
//! [`schema`]. Layers, outside in:
//! SQL / DataFrame -> logical plan -> optimizer -> physical plan -> execution.
//!
//! Embedding runners pass [`SessionConfig`] (memory, threads, spill, allowed
//! paths, network grant), register Arrow batches, files, or an Arrow C stream,
//! and pull results as batches. [`Cluster`] sizes in-process workers with CPU,
//! memory, and spill grants. [`StreamQuery`] runs continuous micro-batches
//! with event-time watermarks, windows, and a checkpointed Parquet sink.
//! The engine opens TCP only when
//! [`SessionConfig::allow_network`] is set; denied paths, denied network, and
//! memory caps surface as [`Error::Io`] and [`Error::Memory`].
//! Query history and the versioned metrics HTTP API (`/v1`,
//! [`QUERY_API_CONTRACT`]) stay off until
//! [`SessionConfig::query_api`] is set (ADR 0012). An optional console
//! directory is served at `/ui/` (ADR 0014).
//! [`VERSION`] and [`Session::explain`] are the strings a runner records.
//! See `crates/mizuchi/examples/embed.rs`.

pub use mizuchi_core::batch;
pub use mizuchi_core::expr;
pub use mizuchi_core::schema;
pub use mizuchi_core::udaf;
pub use mizuchi_exec as exec;
pub use mizuchi_io::lake;
pub use mizuchi_io::read;
pub use mizuchi_io::store;
pub use mizuchi_io::write;
pub use mizuchi_plan::opt;
pub use mizuchi_plan::plan;
pub use mizuchi_plan::sql;
pub use mizuchi_postgres as postgres;

pub use mizuchi_core::{
    Batch, BatchPolicy, BinaryOp, BorrowedBatch, CancelToken, CombineAgg, DEFAULT_MAX_ROWS, Error,
    Expr, ProductI64, Result, UnaryOp, from_arrow_schema, is_supported_data_type, to_arrow_schema,
};
pub use mizuchi_exec::{
    Cluster, ClusterConfig, Engine, EventWindow, InputScan, MergeStats, MicroBatchAgg,
    OperatorMetrics, OutputWrite, QUERY_API_CONTRACT, QUERY_API_SCHEMA, QueryApiConfig,
    QueryApiServer, QueryLog, QueryRecord, QueryReport, QueryResult, QueryRetention, QueryStatus,
    RunningQuery, Session, SessionConfig, StreamOutput, StreamQuery, StreamQueryConfig,
    StreamTrigger, WorkerResources,
};
#[cfg(feature = "flight")]
pub use mizuchi_flight::{RemoteCluster, serve_worker, spawn_worker};
pub use mizuchi_io::{
    AvroReadOptions, ColumnRange, CsvReadOptions, FloatRange, JsonReadOptions, LakeSnapshot,
    ObjectStore, OrcReadOptions, ParquetReadOptions, ParquetScanStats, ParquetWriteOptions,
    RowGroupStats, SchemaEvolution, TimeTravel, WriteMode, WriteStats, delta_head,
    delta_head_store, evolve_delta, evolve_delta_store, evolve_iceberg, evolve_iceberg_store,
    iceberg_head, iceberg_head_store, prune_partitions, read_avro, read_csv, read_delta,
    read_delta_store, read_iceberg, read_iceberg_store, read_json, read_orc, read_parquet,
    read_parquet_reported, read_parquet_store, read_parquet_store_reported, row_group_cannot_match,
    write_avro, write_delta, write_delta_at, write_delta_store, write_delta_store_at,
    write_iceberg, write_iceberg_at, write_iceberg_store, write_iceberg_store_at, write_json,
    write_orc, write_parquet, write_parquet_cancel, write_parquet_stats, write_parquet_store,
};
pub use mizuchi_plan::{
    AlterTable, Catalog, CatalogProvider, ColumnStats, DEFAULT_CATALOG, DEFAULT_NAMESPACE, Ddl,
    IcebergRestCatalog, JoinType, LogicalPlan, MergeActionPlan, MergeClausePlan, MergeMatch,
    MergePlan, NamespaceIdent, TableIdent, TableMeta, TableStats, apply_alter, bind_ddl,
    bind_merge, estimate_rows, merge_target_table, optimize, parse_sql, plan_sql,
    plan_sql_with_udafs, table_stats_from_batches,
};
pub use mizuchi_postgres::{PostgresReadOptions, PostgresSource, postgres_scan_sql, read_postgres};

pub use arrow::array::{ArrayRef, RecordBatch};
pub use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
pub use arrow::ffi_stream::ArrowArrayStreamReader;

/// Crate version, for receipts and diagnostics in embedding runners.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_sql_fails_closed_on_syntax_errors() {
        assert!(matches!(
            plan_sql("SELECT FROM", &Catalog::new()),
            Err(Error::Parse(_))
        ));
    }

    #[test]
    fn version_is_set() {
        assert!(!VERSION.is_empty());
    }
}
