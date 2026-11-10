//! Query reports: pruned scans, partitioned writes, stable across runs.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{Int64Array, StringArray};
use mizuchi::{
    Batch, BinaryOp, Expr, Field, ParquetReadOptions, ParquetWriteOptions, RecordBatch, Schema,
    Session, VERSION, WriteMode, read_parquet_store_reported, write_parquet_store,
};
use object_store::memory::InMemory;

fn tmp(label: &str) -> std::path::PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(1);
    std::env::temp_dir().join(format!(
        "mizuchi-report-{}-{}-{}",
        label,
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ))
}

fn sample() -> Batch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("year", mizuchi::DataType::Int64, true),
        Field::new("id", mizuchi::DataType::Int64, true),
        Field::new("name", mizuchi::DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(2020), Some(2020), Some(2021)])),
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
        ],
    )
    .unwrap();
    Batch::from_arrow(batch).unwrap()
}

#[test]
fn partitioned_write_report_is_stable() {
    let dest = tmp("write");
    let batch = sample();
    let opts = ParquetWriteOptions {
        partition_by: &[String::from("year")],
        mode: WriteMode::Overwrite,
        encryption: None,
    };
    let s = Session::new();
    let first = s
        .write_parquet_reported(&dest, std::slice::from_ref(&batch), opts)
        .unwrap();
    let _ = std::fs::remove_dir_all(&dest);
    let second = s.write_parquet_reported(&dest, &[batch], opts).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.engine_version, VERSION);
    assert_eq!(first.outputs.len(), 1);
    let out = &first.outputs[0];
    assert_eq!(out.mode, "overwrite");
    assert_eq!(out.rows, 3);
    assert!(out.bytes > 0, "parquet bytes");
    assert_eq!(out.partitions, vec!["year=2020", "year=2021"]);
    assert_eq!(
        out.commit_id,
        format!("overwrite:3:{}:year=2020/year=2021", out.bytes)
    );
    let _ = std::fs::remove_dir_all(&dest);
}

#[test]
fn pruned_scan_report_lists_kept_partitions() {
    let dest = tmp("scan");
    let batch = sample();
    let mut s = Session::new();
    s.write_parquet(
        &dest,
        &[batch],
        ParquetWriteOptions {
            partition_by: &[String::from("year")],
            mode: WriteMode::Overwrite,
            encryption: None,
        },
    )
    .unwrap();
    s.register_parquet("t", &dest, ParquetReadOptions::default())
        .unwrap();
    let first = s
        .execute_sql_reported("SELECT id FROM t WHERE year = 2020")
        .unwrap();
    let second = s
        .execute_sql_reported("SELECT id FROM t WHERE year = 2020")
        .unwrap();
    assert_eq!(first.report, second.report);
    assert_eq!(first.report.engine_version, VERSION);
    assert!(first.report.plan.contains("Logical Plan"));
    assert!(first.report.plan.contains("Physical Plan"));
    assert_eq!(first.report.inputs.len(), 1);
    let scan = &first.report.inputs[0];
    assert_eq!(scan.dataset, "t");
    assert_eq!(scan.partitions, vec!["year=2020"]);
    assert!(scan.columns.contains(&"id".to_string()));
    assert!(scan.path.is_some());
    assert!(scan.mtime_ms.is_some());
    let _ = std::fs::remove_dir_all(&dest);
}

#[test]
fn store_pruned_scan_report_matches_fetched_partitions() {
    let store = InMemory::new();
    let batch = sample();
    write_parquet_store(
        &store,
        "ds",
        &[batch],
        ParquetWriteOptions {
            partition_by: &[String::from("year")],
            mode: WriteMode::Overwrite,
            encryption: None,
        },
    )
    .unwrap();
    let pred = Expr::Binary(
        BinaryOp::Eq,
        Box::new(Expr::column("year")),
        Box::new(Expr::lit_i64(2020)),
    );
    let opts = ParquetReadOptions {
        filters: Some(std::slice::from_ref(&pred)),
        ..ParquetReadOptions::default()
    };
    let (_batches, scan) = read_parquet_store_reported(&store, "ds", opts).unwrap();
    assert_eq!(scan.partitions, vec!["year=2020"]);
    assert_eq!(scan.dataset, "ds");
}

#[test]
fn registered_parquet_changed_on_disk_fails_until_registered_again() {
    let dir = tmp("changed");
    let file = dir.join("t.parquet");
    std::fs::create_dir_all(&dir).unwrap();
    let mut s = Session::new();
    let write = |s: &Session, batches: &[Batch]| {
        s.write_parquet(
            &file,
            batches,
            ParquetWriteOptions {
                partition_by: &[],
                mode: WriteMode::Overwrite,
                encryption: None,
            },
        )
        .unwrap();
    };
    write(&s, &[sample()]);
    s.register_parquet("t", &file, ParquetReadOptions::default())
        .unwrap();
    let count = |s: &Session, tag: i64| -> mizuchi::Result<i64> {
        let out = s.execute_sql(&format!("SELECT COUNT(*) AS n FROM t WHERE id <> {tag}"))?;
        Ok(out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0))
    };
    assert_eq!(count(&s, -1).unwrap(), 3);

    write(&s, &[sample(), sample()]);
    for tag in [-1, -2] {
        // -1 repeats the first query, so its cached result must not answer.
        let err = count(&s, tag).unwrap_err().to_string();
        assert!(err.contains("changed since it was registered"), "{err}");
    }
    let err = s
        .execute_ddl("ALTER TABLE t ADD COLUMN z BIGINT")
        .unwrap_err();
    assert!(
        err.to_string().contains("changed since it was registered"),
        "{err}"
    );
    assert_eq!(
        s.catalog().table("t").unwrap().fields().len(),
        3,
        "catalog kept"
    );

    s.register_parquet("t", &file, ParquetReadOptions::default())
        .unwrap();
    assert_eq!(count(&s, -3).unwrap(), 6);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn decimal_filter_on_registered_parquet_is_not_pruned_by_raw_stats() {
    use arrow::array::Decimal128Array;
    let dir = tmp("decimal");
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("t.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "amount",
        mizuchi::DataType::Decimal128(9, 2),
        false,
    )]));
    // Stored unscaled as 100 and 250: raw footer stats exceed the literal 2.
    let amounts = Decimal128Array::from(vec![100, 250])
        .with_precision_and_scale(9, 2)
        .unwrap();
    let batch =
        Batch::from_arrow(RecordBatch::try_new(schema, vec![Arc::new(amounts)]).unwrap()).unwrap();
    let mut s = Session::new();
    s.write_parquet(&file, &[batch], ParquetWriteOptions::default())
        .unwrap();
    s.register_parquet("t", &file, ParquetReadOptions::default())
        .unwrap();
    let out = s
        .execute_sql("SELECT COUNT(*) AS n FROM t WHERE amount < 2")
        .unwrap();
    let n = out[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 1);
    let _ = std::fs::remove_dir_all(&dir);
}
