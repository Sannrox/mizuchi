//! mizuchi: an Arrow-native query and transform engine.
//!
//! Columnar memory is standard Apache Arrow record batches. See [`batch`] and
//! [`schema`]. Planned layers, outside in:
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
//! [`VERSION`] and [`Session::explain`] are the strings a runner records.
//! See `crates/mizuchi/examples/embed.rs`.

pub mod batch;
mod error;
pub mod exec;
pub mod expr;
pub mod lake;
pub mod opt;
mod orc;
pub mod plan;
mod postgres;
pub mod read;
pub mod schema;
pub mod sql;
pub mod store;
pub mod udaf;
pub mod write;

pub use batch::{Batch, BatchPolicy, BorrowedBatch, DEFAULT_MAX_ROWS};
pub use error::{Error, Result};
pub use exec::{
    CancelToken, Cluster, ClusterConfig, Engine, EventWindow, InputScan, MergeStats, MicroBatchAgg,
    OperatorMetrics, OutputWrite, QueryReport, QueryResult, Session, SessionConfig, StreamOutput,
    StreamQuery, StreamQueryConfig, StreamTrigger, WorkerResources,
};
#[cfg(feature = "flight")]
pub use exec::{RemoteCluster, serve_worker, spawn_worker};
pub use expr::{BinaryOp, Expr, UnaryOp};
pub use lake::{
    LakeSnapshot, SchemaEvolution, TimeTravel, delta_head, delta_head_store, evolve_delta,
    evolve_delta_store, evolve_iceberg, evolve_iceberg_store, iceberg_head, iceberg_head_store,
    read_delta, read_delta_store, read_iceberg, read_iceberg_store, write_delta, write_delta_at,
    write_delta_store, write_delta_store_at, write_iceberg, write_iceberg_at, write_iceberg_store,
    write_iceberg_store_at,
};
pub use object_store::ObjectStore;
pub use opt::{estimate_rows, optimize};
pub use plan::{
    AlterTable, Catalog, CatalogProvider, ColumnStats, DEFAULT_CATALOG, DEFAULT_NAMESPACE, Ddl,
    IcebergRestCatalog, JoinType, LogicalPlan, MergeActionPlan, MergeClausePlan, MergeMatch,
    MergePlan, NamespaceIdent, TableIdent, TableMeta, TableStats, apply_alter, bind_ddl,
    bind_merge, merge_target_table, plan_sql, plan_sql_with_udafs, table_stats_from_batches,
};
pub use postgres::{PostgresReadOptions, PostgresSource, postgres_scan_sql, read_postgres};
pub use read::{
    AvroReadOptions, ColumnRange, CsvReadOptions, FloatRange, JsonReadOptions, OrcReadOptions,
    ParquetReadOptions, ParquetScanStats, RowGroupStats, read_avro, read_csv, read_json, read_orc,
    read_parquet, read_parquet_reported, row_group_cannot_match,
};
pub use schema::{from_arrow_schema, is_supported_data_type, to_arrow_schema};
pub use sql::parse_sql;
pub use store::{
    prune_partitions, read_parquet_store, read_parquet_store_reported, write_parquet_store,
};
pub use udaf::{CombineAgg, ProductI64};
pub use write::{
    ParquetWriteOptions, WriteMode, WriteStats, write_avro, write_json, write_orc, write_parquet,
    write_parquet_cancel, write_parquet_stats,
};

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
