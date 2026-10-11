//! Delta and Iceberg table round-trips, time travel, MERGE, and conflicts.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use mizuchi::{
    Batch, SchemaEvolution, Session, TimeTravel, WriteMode, evolve_delta, evolve_iceberg,
    read_delta, read_iceberg, write_delta, write_iceberg,
};

static SEQ: AtomicU64 = AtomicU64::new(1);

fn tmp(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "mizuchi-lake-it-{}-{}-{}",
        tag,
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&p);
    p
}

fn sample() -> Batch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    Batch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
        ],
    )
    .unwrap()
}

fn col_i64(batches: &[Batch], name: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let col = b.column_by_name(name).unwrap();
        if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
            for i in 0..a.len() {
                if !a.is_null(i) {
                    out.push(a.value(i));
                }
            }
        } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
            for i in 0..a.len() {
                if !a.is_null(i) {
                    out.push(a.value(i) as i64);
                }
            }
        }
    }
    out.sort();
    out
}

#[test]
fn delta_round_trip_time_travel_and_conflict() {
    let dest = tmp("delta");
    write_delta(&dest, &[sample()], WriteMode::Overwrite).unwrap();
    assert_eq!(
        col_i64(&read_delta(&dest, TimeTravel::Latest).unwrap(), "id"),
        vec![1, 2]
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let more = Batch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(3)])),
            Arc::new(StringArray::from(vec![Some("c")])),
        ],
    )
    .unwrap();
    write_delta(&dest, &[more], WriteMode::Append).unwrap();
    assert_eq!(
        col_i64(&read_delta(&dest, TimeTravel::Latest).unwrap(), "id"),
        vec![1, 2, 3]
    );
    assert_eq!(
        col_i64(&read_delta(&dest, TimeTravel::Version(0)).unwrap(), "id"),
        vec![1, 2]
    );
    let dest2 = tmp("delta-conflict");
    let head = write_delta(&dest2, &[sample()], WriteMode::Overwrite).unwrap();
    mizuchi::write_delta_at(&dest2, &[sample()], WriteMode::Append, Some(head.version)).unwrap();
    let err = mizuchi::write_delta_at(&dest2, &[sample()], WriteMode::Append, Some(head.version))
        .unwrap_err();
    assert!(err.to_string().contains("conflict"), "{err}");
    let _ = fs::remove_dir_all(&dest2);
    let _ = fs::remove_dir_all(&dest);
}

#[test]
fn iceberg_round_trip_time_travel_and_conflict() {
    let dest = tmp("iceberg");
    let first = write_iceberg(&dest, &[sample()], WriteMode::Overwrite).unwrap();
    assert_eq!(first.version, 1);
    assert_eq!(
        col_i64(&read_iceberg(&dest, TimeTravel::Latest).unwrap(), "id"),
        vec![1, 2]
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let more = Batch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(9)])),
            Arc::new(StringArray::from(vec![Some("z")])),
        ],
    )
    .unwrap();
    write_iceberg(&dest, &[more], WriteMode::Append).unwrap();
    assert_eq!(
        col_i64(&read_iceberg(&dest, TimeTravel::Latest).unwrap(), "id"),
        vec![1, 2, 9]
    );
    assert_eq!(
        col_i64(&read_iceberg(&dest, TimeTravel::Version(1)).unwrap(), "id"),
        vec![1, 2]
    );
    let dest2 = tmp("iceberg-conflict");
    let head = write_iceberg(&dest2, &[sample()], WriteMode::Overwrite).unwrap();
    mizuchi::write_iceberg_at(&dest2, &[sample()], WriteMode::Append, Some(head.version)).unwrap();
    let err = mizuchi::write_iceberg_at(&dest2, &[sample()], WriteMode::Append, Some(head.version))
        .unwrap_err();
    assert!(err.to_string().contains("conflict"), "{err}");
    let _ = fs::remove_dir_all(&dest2);
    let _ = fs::remove_dir_all(&dest);
}

#[test]
fn schema_evolution_add_rename_widen() {
    let dest = tmp("evolve");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
    let batch = Batch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![Some(1), Some(2)]))],
    )
    .unwrap();
    write_delta(&dest, std::slice::from_ref(&batch), WriteMode::Overwrite).unwrap();
    evolve_delta(
        &dest,
        SchemaEvolution::AddColumn {
            name: "label".into(),
            data_type: DataType::Utf8,
        },
    )
    .unwrap();
    evolve_delta(
        &dest,
        SchemaEvolution::RenameColumn {
            from: "id".into(),
            to: "key".into(),
        },
    )
    .unwrap();
    evolve_delta(
        &dest,
        SchemaEvolution::WidenColumn {
            name: "key".into(),
            data_type: DataType::Int64,
        },
    )
    .unwrap();
    let got = read_delta(&dest, TimeTravel::Latest).unwrap();
    let schema = got[0].schema();
    assert!(schema.field_with_name("key").is_ok(), "{schema:?}");
    assert!(schema.field_with_name("label").is_ok(), "{schema:?}");
    assert_eq!(
        schema.field_with_name("key").unwrap().data_type(),
        &DataType::Int64
    );
    assert_eq!(col_i64(&got, "key"), vec![1, 2]);
    assert!(got[0].column_by_name("label").unwrap().is_null(0));

    let ice = tmp("evolve-ice");
    write_iceberg(&ice, &[batch], WriteMode::Overwrite).unwrap();
    evolve_iceberg(
        &ice,
        SchemaEvolution::AddColumn {
            name: "label".into(),
            data_type: DataType::Utf8,
        },
    )
    .unwrap();
    evolve_iceberg(
        &ice,
        SchemaEvolution::RenameColumn {
            from: "id".into(),
            to: "key".into(),
        },
    )
    .unwrap();
    evolve_iceberg(
        &ice,
        SchemaEvolution::WidenColumn {
            name: "key".into(),
            data_type: DataType::Int64,
        },
    )
    .unwrap();
    let got = read_iceberg(&ice, TimeTravel::Latest).unwrap();
    assert_eq!(col_i64(&got, "key"), vec![1, 2]);
    let _ = fs::remove_dir_all(&dest);
    let _ = fs::remove_dir_all(&ice);
}

#[test]
fn merge_sql_and_lake_commit() {
    let mut session = Session::new();
    let t_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let t = Batch::try_new(
        t_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
            Arc::new(Int64Array::from(vec![Some(10), Some(20)])),
        ],
    )
    .unwrap();
    let s = Batch::try_new(
        t_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(2), Some(3)])),
            Arc::new(Int64Array::from(vec![Some(99), Some(30)])),
        ],
    )
    .unwrap();
    session.register("t", t_schema.clone(), vec![t]).unwrap();
    session.register("s", t_schema, vec![s]).unwrap();
    let stats = session
        .merge_sql(
            "MERGE INTO t USING s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET v = s.v \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
        )
        .unwrap();
    assert_eq!(stats.updated, 1);
    assert_eq!(stats.inserted, 1);
    let out = session
        .execute_sql("SELECT id, v FROM t ORDER BY id")
        .unwrap();
    assert_eq!(col_i64(&out, "id"), vec![1, 2, 3]);
    assert_eq!(col_i64(&out, "v"), vec![10, 30, 99]);

    let delta = tmp("merge-delta");
    session
        .write_delta(&delta, "t", WriteMode::Overwrite)
        .unwrap();
    let src = Batch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![Some(1)])),
            Arc::new(Int64Array::from(vec![Some(7)])),
        ],
    )
    .unwrap();
    session.register("src", src.schema(), vec![src]).unwrap();
    session
        .merge_delta(
            &delta,
            "MERGE INTO t USING src ON t.id = src.id WHEN MATCHED THEN UPDATE SET v = src.v",
        )
        .unwrap();
    let got = read_delta(&delta, TimeTravel::Latest).unwrap();
    // id 1 updated to 7; 2 and 3 unchanged
    let mut pairs = Vec::new();
    for b in &got {
        let id = b
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let v = b
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..id.len() {
            pairs.push((id.value(i), v.value(i)));
        }
    }
    pairs.sort();
    assert_eq!(pairs, vec![(1, 7), (2, 99), (3, 30)]);

    let ice = tmp("merge-ice");
    session
        .write_iceberg(&ice, "t", WriteMode::Overwrite)
        .unwrap();
    session
        .merge_iceberg(
            &ice,
            "MERGE INTO t USING src ON t.id = src.id WHEN MATCHED THEN DELETE",
        )
        .unwrap();
    let got = read_iceberg(&ice, TimeTravel::Latest).unwrap();
    assert_eq!(col_i64(&got, "id"), vec![2, 3]);
    let _ = fs::remove_dir_all(&delta);
    let _ = fs::remove_dir_all(&ice);
}

#[test]
fn merge_one_source_row_may_update_many_targets() {
    let mut session = Session::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let t = Batch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(1)])),
            Arc::new(Int64Array::from(vec![Some(10), Some(20)])),
        ],
    )
    .unwrap();
    let s = Batch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1)])),
            Arc::new(Int64Array::from(vec![Some(99)])),
        ],
    )
    .unwrap();
    session.register("t", schema.clone(), vec![t]).unwrap();
    session.register("s", schema, vec![s]).unwrap();
    let stats = session
        .merge_sql("MERGE INTO t USING s ON t.k = s.k WHEN MATCHED THEN UPDATE SET v = s.v")
        .unwrap();
    assert_eq!(stats.updated, 2);
    let out = session
        .execute_sql("SELECT k, v FROM t ORDER BY v")
        .unwrap();
    assert_eq!(col_i64(&out, "v"), vec![99, 99]);
}

#[test]
fn merge_rejects_two_source_rows_for_one_target() {
    let mut session = Session::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let t = Batch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1)])),
            Arc::new(Int64Array::from(vec![Some(10)])),
        ],
    )
    .unwrap();
    let s = Batch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(1)])),
            Arc::new(Int64Array::from(vec![Some(7), Some(8)])),
        ],
    )
    .unwrap();
    session.register("t", schema.clone(), vec![t]).unwrap();
    session.register("s", schema, vec![s]).unwrap();
    let err = session
        .merge_sql("MERGE INTO t USING s ON t.k = s.k WHEN MATCHED THEN UPDATE SET v = s.v")
        .unwrap_err();
    assert!(err.to_string().contains("more than one source"), "{err}");
}

#[test]
fn merge_update_star_uses_source_and_fails_closed() {
    let mut session = Session::new();
    let t_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let t = Batch::try_new(
        t_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1)])),
            Arc::new(Int64Array::from(vec![Some(10)])),
        ],
    )
    .unwrap();
    let s = Batch::try_new(
        t_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1)])),
            Arc::new(Int64Array::from(vec![Some(99)])),
        ],
    )
    .unwrap();
    session.register("t", t_schema.clone(), vec![t]).unwrap();
    session.register("s", t_schema, vec![s]).unwrap();
    session
        .merge_sql(
            "MERGE INTO t USING (SELECT id, v FROM s) src ON t.id = src.id \
             WHEN MATCHED THEN UPDATE SET *",
        )
        .unwrap();
    let out = session.execute_sql("SELECT id, v FROM t").unwrap();
    assert_eq!(col_i64(&out, "v"), vec![99]);

    let extra = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
        Field::new("extra", DataType::Int64, true),
    ]));
    let wide = Batch::try_new(
        extra.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1)])),
            Arc::new(Int64Array::from(vec![Some(1)])),
            Arc::new(Int64Array::from(vec![Some(0)])),
        ],
    )
    .unwrap();
    session.register("wide", extra, vec![wide]).unwrap();
    let err = session
        .merge_sql("MERGE INTO wide USING s ON wide.id = s.id WHEN MATCHED THEN UPDATE SET *")
        .unwrap_err();
    assert!(err.to_string().contains("column not found"), "{err}");
}

#[test]
fn merge_delta_conflict_restores_session_table() {
    use std::os::unix::fs::PermissionsExt;
    let mut session = Session::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let t = Batch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1)])),
            Arc::new(Int64Array::from(vec![Some(10)])),
        ],
    )
    .unwrap();
    let s = Batch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1)])),
            Arc::new(Int64Array::from(vec![Some(99)])),
        ],
    )
    .unwrap();
    session.register("t", schema.clone(), vec![t]).unwrap();
    session.register("s", schema, vec![s]).unwrap();
    let dest = tmp("merge-restore");
    session
        .write_delta(&dest, "t", WriteMode::Overwrite)
        .unwrap();
    let log = dest.join("_delta_log");
    let original = fs::metadata(&log).unwrap().permissions();
    let mut locked = original.clone();
    locked.set_mode(0o555);
    fs::set_permissions(&log, locked).unwrap();
    let err = session
        .merge_delta(
            &dest,
            "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v",
        )
        .unwrap_err();
    fs::set_permissions(&log, original).unwrap();
    assert!(
        err.to_string().contains("conflict") || err.to_string().contains("io"),
        "{err}"
    );
    let out = session.execute_sql("SELECT id, v FROM t").unwrap();
    assert_eq!(
        col_i64(&out, "v"),
        vec![10],
        "uncommitted merge must not stick"
    );
    let _ = fs::remove_dir_all(&dest);
}

#[test]
fn merge_fails_closed_when_memory_limit_is_hit() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let t = Batch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
            Arc::new(Int64Array::from(vec![Some(10), Some(20)])),
        ],
    )
    .unwrap();
    let s = Batch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(2)])),
            Arc::new(Int64Array::from(vec![Some(99)])),
        ],
    )
    .unwrap();
    let mut session = Session::new().with_memory_limit(1);
    session.register("t", schema.clone(), vec![t]).unwrap();
    session.register("s", schema, vec![s]).unwrap();
    let err = session
        .merge_sql("MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v")
        .unwrap_err();
    assert!(matches!(err, mizuchi::Error::Memory(_)), "{err:?}");
}
