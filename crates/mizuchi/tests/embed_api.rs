//! Public embedder contract: config, plan/execute, files, C stream, limits.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{Array, Int64Array, RecordBatchIterator};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use mizuchi::{
    ArrowArrayStreamReader, Batch, CancelToken, Error, Field, ParquetReadOptions,
    ParquetWriteOptions, RecordBatch, Schema, SchemaRef, Session, SessionConfig, VERSION,
};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "a",
        mizuchi::DataType::Int64,
        true,
    )]))
}

fn numbered(n: i64) -> (SchemaRef, Vec<Batch>) {
    let schema = schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(
            (0..n).map(Some).collect::<Vec<_>>(),
        ))],
    )
    .unwrap();
    (schema, vec![Batch::from_arrow(batch).unwrap()])
}

fn tmp(label: &str) -> std::path::PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(1);
    std::env::temp_dir().join(format!(
        "mizuchi-embed-{}-{}-{}",
        label,
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ))
}

#[test]
fn version_and_plan_string_are_stable() {
    let (schema, batches) = numbered(4);
    let mut s = Session::new();
    s.register("t", schema, batches).unwrap();
    assert!(!VERSION.is_empty());
    let text = s.explain("SELECT a FROM t").unwrap();
    assert!(text.contains("Logical Plan"), "{text}");
    assert!(text.contains("Physical Plan"), "{text}");
}

#[test]
fn execute_logical_plan_matches_sql() {
    let (schema, batches) = numbered(8);
    let mut s = Session::new();
    s.register("t", schema, batches).unwrap();
    let sql = "SELECT a FROM t WHERE a >= 4";
    let from_sql = s.execute_sql(sql).unwrap();
    let plan = s.logical_plan(sql).unwrap();
    let from_plan = s.execute(&plan).unwrap();
    let col = |b: &[Batch]| {
        b.iter()
            .flat_map(|batch| {
                batch
                    .as_arrow()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .map(|v| v.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(col(&from_sql), col(&from_plan));
}

#[test]
fn cancel_via_config_session_fails_closed() {
    let token = CancelToken::new();
    token.cancel();
    let (schema, batches) = numbered(4);
    let mut s = Session::from_config(SessionConfig {
        memory_limit: Some(8 * 1024 * 1024),
        workers: Some(1),
        ..SessionConfig::default()
    })
    .unwrap()
    .with_cancel(token);
    s.register("t", schema, batches).unwrap();
    let err = s.execute_sql("SELECT a FROM t");
    assert!(matches!(err, Err(Error::Cancelled)), "{err:?}");
}

#[test]
fn memory_limit_without_spill_fails_closed() {
    let (schema, batches) = numbered(64);
    let mut s = Session::from_config(SessionConfig {
        memory_limit: Some(64),
        workers: Some(1),
        ..SessionConfig::default()
    })
    .unwrap();
    s.register("t", schema, batches).unwrap();
    let err = s.execute_sql("SELECT a FROM t ORDER BY a");
    assert!(matches!(err, Err(Error::Memory(_))), "{err:?}");
}

#[test]
fn path_allow_list_rejects_reads_and_writes_outside_roots() {
    let allowed = tmp("ok");
    let denied = tmp("no");
    std::fs::create_dir_all(&allowed).unwrap();
    std::fs::create_dir_all(&denied).unwrap();
    let (schema, batches) = numbered(4);
    let mut open = Session::new();
    open.register("t", schema.clone(), batches.clone()).unwrap();
    open.write_parquet(
        denied.join("data"),
        &batches,
        ParquetWriteOptions::default(),
    )
    .unwrap();

    let mut s = Session::from_config(SessionConfig {
        allowed_read_paths: vec![allowed.clone()],
        allowed_write_paths: vec![allowed.clone()],
        ..SessionConfig::default()
    })
    .unwrap();
    let read_err = s.register_parquet("x", denied.join("data"), ParquetReadOptions::default());
    assert!(matches!(read_err, Err(Error::Io(_))), "{read_err:?}");
    let write_err = s.write_parquet(denied.join("out"), &batches, ParquetWriteOptions::default());
    assert!(matches!(write_err, Err(Error::Io(_))), "{write_err:?}");

    s.register("t", schema, batches.clone()).unwrap();
    s.write_parquet(
        allowed.join("out"),
        &batches,
        ParquetWriteOptions::default(),
    )
    .unwrap();
    s.register_parquet("u", allowed.join("out"), ParquetReadOptions::default())
        .unwrap();
    let rows = s.execute_sql("SELECT COUNT(*) AS c FROM u").unwrap();
    assert!(!rows.is_empty());
    let _ = std::fs::remove_dir_all(&allowed);
    let _ = std::fs::remove_dir_all(&denied);
}

#[test]
fn register_arrow_stream_round_trips() {
    let (schema, batches) = numbered(5);
    let rbs: Vec<RecordBatch> = batches.iter().map(|b| b.as_arrow().clone()).collect();
    let iter = RecordBatchIterator::new(rbs.into_iter().map(Ok), schema.clone());
    let stream = FFI_ArrowArrayStream::new(Box::new(iter));
    let reader = ArrowArrayStreamReader::try_new(stream).unwrap();
    let mut s = Session::new();
    s.register_arrow_stream("t", reader).unwrap();
    let out = s.execute_sql("SELECT COUNT(*) AS c FROM t").unwrap();
    let arr = out[0]
        .as_arrow()
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(arr.value(0), 5);
}
