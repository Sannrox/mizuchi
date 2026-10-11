//! Linux filesystem limits applied by the test process, not by the engine.
//!
//! Enable with `--features sandbox`. Skip when the kernel ABI is missing.
//! Library sources do not depend on this file.

#![cfg(all(feature = "sandbox", target_os = "linux"))]

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::Int64Array;
use landlock::{
    ABI, Access, AccessFs, Ruleset, RulesetAttr, RulesetCreatedAttr, RulesetStatus,
    path_beneath_rules,
};
use mizuchi::{
    Batch, DataType, Error, Field, ParquetReadOptions, ParquetWriteOptions, RecordBatch, Schema,
    SchemaRef, Session, SessionConfig,
};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
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
        "mizuchi-sandbox-os-{}-{}-{}",
        label,
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ))
}

/// Restrict this process to `root` (read-write) plus common read-only system
/// paths the runtime already needs. Returns whether the kernel enforced it.
fn restrict_to(root: &Path) -> bool {
    let abi = ABI::V1;
    let status = (|| {
        Ruleset::default()
            .handle_access(AccessFs::from_all(abi))?
            .create()?
            .add_rules(path_beneath_rules(
                &["/usr", "/etc", "/dev", "/proc", "/lib", "/lib64", "/sys"],
                AccessFs::from_read(abi),
            ))?
            .add_rules(path_beneath_rules(&[root], AccessFs::from_all(abi)))?
            .restrict_self()
    })();
    match status {
        Ok(s) => !matches!(s.ruleset, RulesetStatus::NotEnforced),
        Err(e) => {
            eprintln!("skip: filesystem-limit ABI unavailable: {e}");
            false
        }
    }
}

#[test]
fn os_fs_limits_denied_path_is_io() {
    let root = tmp("ok");
    let denied = tmp("no");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&denied).unwrap();
    let (schema, batches) = numbered(4);
    let mut open = Session::new();
    open.register("t", schema.clone(), batches.clone()).unwrap();
    open.write_parquet(root.join("data"), &batches, ParquetWriteOptions::default())
        .unwrap();
    open.write_parquet(
        denied.join("data"),
        &batches,
        ParquetWriteOptions::default(),
    )
    .unwrap();

    if !restrict_to(&root) {
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&denied);
        return;
    }

    // Empty allow-list: the OS fence is the grant. The engine stays generic.
    let mut s = Session::from_config(SessionConfig {
        spill_dir: Some(root.join("spill")),
        ..SessionConfig::default()
    })
    .unwrap();
    s.register_parquet("ok", root.join("data"), ParquetReadOptions::default())
        .unwrap();
    let rows = s.execute_sql("SELECT COUNT(*) AS c FROM ok").unwrap();
    assert!(!rows.is_empty());
    let err = s.register_parquet("no", denied.join("data"), ParquetReadOptions::default());
    assert!(matches!(err, Err(Error::Io(_))), "{err:?}");
}
