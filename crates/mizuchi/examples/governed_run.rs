//! One governed run end to end, as shown in `docs/governed-run.md`.
//!
//! A session with a memory cap, narrow filesystem grants and a policy
//! context runs one query over partitioned Parquet, writes the result, and
//! checks the report an embedding runner would record. The asserts are the
//! page's claims; `tests/governed_run.rs` runs this file in the default tests.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use mizuchi::{
    Batch, ColumnLabels, DataType, Field, ParquetReadOptions, ParquetWriteOptions, PolicyContext,
    RecordBatch, Result, Schema, Session, SessionConfig, VERSION, WriteMode,
};

const CAP: u64 = 64 * 1024 * 1024;

pub fn main() -> Result<()> {
    let root = scratch_root();
    let out = run(&root);
    let _ = std::fs::remove_dir_all(&root);
    out
}

fn run(root: &Path) -> Result<()> {
    let input = root.join("in/orders");
    let output = root.join("out/totals");
    write_orders(&input)?;

    // 1. Session: memory cap, spill directory, and read/write grants.
    let cfg = SessionConfig {
        memory_limit: Some(CAP),
        workers: Some(1),
        spill_dir: Some(root.join("spill")),
        allowed_read_paths: vec![root.join("in")],
        allowed_write_paths: vec![root.join("out"), root.join("spill")],
        ..SessionConfig::default()
    };
    let mut session = Session::from_config(cfg)?;
    session.register_parquet("orders", &input, ParquetReadOptions::default())?;

    // 2. Policy context: who asks, what they may see, and how data is marked.
    let policy = PolicyContext::new("acme", "analyst@acme", "nightly-runner")
        .allow_table("orders")
        .row_filter("orders", "region = 'eu'")
        .mask_column("orders", "ssn")
        .mark_table("orders", "tenant:acme")
        .mark_column("orders", "customer", "pii")
        .mark_table("totals", "tenant:acme")
        .mark_column("totals", "customer", "pii")
        .mark_column("totals", "total", "pii");
    let session = session.with_policy(policy).with_governed(true);

    // 3. One query. The row filter and partition pruning apply before the scan.
    let sql = "SELECT customer, SUM(amount) AS total FROM orders \
               WHERE year = 2026 GROUP BY customer ORDER BY customer";
    let result = session.execute_sql_reported(sql)?;
    let report = &result.report;

    assert_eq!(customers(&result.batches), ["ana", "bo"]);
    assert_eq!(report.engine_version, VERSION);
    assert_eq!(report.principal.as_deref(), Some("analyst@acme"));
    assert_eq!(report.actor.as_deref(), Some("nightly-runner"));
    assert!(report.plan.contains("== Logical Plan =="));
    assert!(report.plan.contains("== Physical Plan =="));

    // Reads after prune: only the 2026 partition, never the masked column.
    let [scan] = report.inputs.as_slice() else {
        panic!("one input scan, got {:?}", report.inputs);
    };
    assert_eq!(scan.dataset, "orders");
    assert_eq!(scan.partitions, ["year=2026"]);
    assert!(!scan.columns.iter().any(|c| c == "ssn"));

    // Markings: labels that reached each output column. `total` carries
    // `pii` too, because the grouping key decides which rows it sums.
    assert_eq!(
        report.column_markings,
        [
            labels("customer", &["pii", "tenant:acme"]),
            labels("total", &["pii", "tenant:acme"]),
        ]
    );

    // Engine peak: the session pool high water, at or below the cap.
    assert!(result.peak_bytes > 0 && result.peak_bytes <= CAP);

    // 4. Write. A governed write names its destination table, whose declared
    // markings must cover every label on the written columns.
    let write = session.write_parquet_reported_to(
        &output,
        "totals",
        &result.batches,
        &report.column_markings,
        ParquetWriteOptions::default(),
    )?;
    let [written] = write.outputs.as_slice() else {
        panic!("one output, got {:?}", write.outputs);
    };
    assert_eq!(written.rows, 2);
    assert!(written.commit_id.starts_with("overwrite:2:"));
    assert_eq!(write.column_markings, report.column_markings);

    // 5. What the policy refuses.
    assert!(session.execute_sql("SELECT ssn FROM orders").is_err());
    assert!(
        session
            .write_parquet_reported_to(
                root.join("elsewhere"),
                "totals",
                &result.batches,
                &report.column_markings,
                ParquetWriteOptions::default(),
            )
            .is_err()
    );
    let foreign = [labels("customer", &["secret"]), labels("total", &[])];
    assert!(
        session
            .write_parquet_reported_to(
                &output,
                "totals",
                &result.batches,
                &foreign,
                ParquetWriteOptions::default(),
            )
            .is_err()
    );
    Ok(())
}

/// Orders partitioned by year, written by an ungoverned setup session.
fn write_orders(dest: &Path) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("customer", DataType::Utf8, true),
        Field::new("ssn", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![2025, 2026, 2026, 2026, 2026])),
            Arc::new(StringArray::from(vec!["eu", "eu", "eu", "us", "eu"])),
            Arc::new(StringArray::from(vec!["ana", "ana", "bo", "cy", "bo"])),
            Arc::new(StringArray::from(vec!["1", "2", "3", "4", "5"])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
        ],
    )
    .map_err(|e| mizuchi::Error::InvalidBatch(e.to_string()))?;
    Session::new().write_parquet(
        dest,
        &[Batch::from_arrow(batch)?],
        ParquetWriteOptions {
            partition_by: &[String::from("year")],
            mode: WriteMode::Overwrite,
            encryption: None,
        },
    )
}

fn customers(batches: &[Batch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            col.iter().flatten().map(str::to_owned).collect::<Vec<_>>()
        })
        .collect()
}

fn labels(name: &str, labels: &[&str]) -> ColumnLabels {
    ColumnLabels {
        name: name.into(),
        labels: labels.iter().map(|l| l.to_string()).collect(),
    }
}

fn scratch_root() -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    std::env::temp_dir().join(format!("mizuchi-governed-{}-{nanos}", std::process::id()))
}
