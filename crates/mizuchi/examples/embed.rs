//! Minimal embedding runner: config, register Arrow, plan, execute, write.

use std::sync::Arc;

use arrow::array::Int64Array;
use mizuchi::{
    Batch, CancelToken, ParquetReadOptions, ParquetWriteOptions, RecordBatch, Session,
    SessionConfig, VERSION,
};

fn main() -> mizuchi::Result<()> {
    let root = std::env::temp_dir().join(format!("mizuchi-embed-{}", std::process::id()));
    std::fs::create_dir_all(&root).map_err(|e| mizuchi::Error::Io(e.to_string()))?;
    let cancel = CancelToken::new();
    let cfg = SessionConfig {
        memory_limit: Some(32 * 1024 * 1024),
        workers: Some(1),
        spill_dir: Some(root.join("spill")),
        allowed_read_paths: vec![root.clone()],
        allowed_write_paths: vec![root.clone()],
        ..SessionConfig::default()
    };
    let mut session = Session::from_config(cfg)?.with_cancel(cancel);
    let schema = Arc::new(mizuchi::Schema::new(vec![mizuchi::Field::new(
        "a",
        mizuchi::DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![Some(1), Some(2)]))],
    )
    .map_err(|e| mizuchi::Error::InvalidBatch(e.to_string()))?;
    session.register("t", schema, vec![Batch::from_arrow(batch)?])?;

    eprintln!("engine {VERSION}");
    eprintln!("{}", session.explain("SELECT a FROM t")?);
    let plan = session.logical_plan("SELECT a FROM t")?;
    let rows = session.execute(&plan)?;
    session.write_parquet(root.join("out"), &rows, ParquetWriteOptions::default())?;
    session.register_parquet("u", root.join("out"), ParquetReadOptions::default())?;
    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}
