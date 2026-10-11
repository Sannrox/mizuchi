//! Nested-type read tests for JSON, Avro, and ORC.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, ListBuilder, StringArray, StringBuilder, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use mizuchi::{
    AvroReadOptions, Batch, JsonReadOptions, OrcReadOptions, Session, SessionConfig, read_avro,
    read_json, read_orc, write_avro, write_json, write_orc,
};

fn nested_batch() -> Batch {
    let name = StringArray::from(vec![Some("a"), Some("b")]);
    let nested = StructArray::from(vec![(
        Arc::new(Field::new("name", DataType::Utf8, true)),
        Arc::new(name) as arrow::array::ArrayRef,
    )]);
    let mut tags = ListBuilder::new(StringBuilder::new());
    tags.values().append_value("x");
    tags.values().append_value("y");
    tags.append(true);
    tags.values().append_value("z");
    tags.append(true);
    let tags = tags.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new(
            "nested",
            DataType::Struct(Fields::from(vec![Field::new("name", DataType::Utf8, true)])),
            true,
        ),
        Field::new("tags", tags.data_type().clone(), true),
    ]));
    Batch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
            Arc::new(nested),
            Arc::new(tags),
        ],
    )
    .unwrap()
}

fn tmp(suffix: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "mizuchi-fmt-{}-{}-{suffix}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn ids(batches: &[Batch]) -> Vec<i64> {
    let mut out = Vec::new();
    for batch in batches {
        let col = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..col.len() {
            if col.is_valid(i) {
                out.push(col.value(i));
            }
        }
    }
    out
}

#[test]
fn json_ndjson_nested_round_trip_and_projection() {
    let batch = nested_batch();
    let path = tmp("nested.json");
    write_json(&path, &[batch]).unwrap();
    let out = read_json(&path, JsonReadOptions::default()).unwrap();
    assert_eq!(ids(&out), vec![1, 2]);
    assert!(out[0].column_by_name("nested").is_some());
    assert!(out[0].column_by_name("tags").is_some());
    let names = ["id".to_string(), "tags".to_string()];
    let projected = read_json(
        &path,
        JsonReadOptions {
            projection: Some(&names),
            ..JsonReadOptions::default()
        },
    )
    .unwrap();
    assert_eq!(projected[0].num_columns(), 2);
    assert!(projected[0].column_by_name("nested").is_none());
    let _ = std::fs::remove_file(path);
}

#[test]
fn avro_ocf_nested_round_trip_and_projection() {
    let batch = nested_batch();
    let path = tmp("nested.avro");
    write_avro(&path, &[batch]).unwrap();
    let out = read_avro(&path, AvroReadOptions::default()).unwrap();
    assert_eq!(ids(&out), vec![1, 2]);
    assert!(out[0].column_by_name("nested").is_some());
    let names = ["id".to_string()];
    let projected = read_avro(
        &path,
        AvroReadOptions {
            projection: Some(&names),
            ..AvroReadOptions::default()
        },
    )
    .unwrap();
    assert_eq!(projected[0].num_columns(), 1);
    assert_eq!(ids(&projected), vec![1, 2]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn orc_nested_round_trip_and_projection() {
    let batch = nested_batch();
    let path = tmp("nested.orc");
    write_orc(&path, &[batch]).unwrap();
    let out = read_orc(&path, OrcReadOptions::default()).unwrap();
    assert_eq!(ids(&out), vec![1, 2]);
    assert!(out[0].column_by_name("nested").is_some());
    assert!(out[0].column_by_name("tags").is_some());
    let names = ["id".to_string(), "tags".to_string()];
    let projected = read_orc(
        &path,
        OrcReadOptions {
            projection: Some(&names),
            ..OrcReadOptions::default()
        },
    )
    .unwrap();
    assert_eq!(projected[0].num_columns(), 2);
    assert!(projected[0].column_by_name("nested").is_none());
    let _ = std::fs::remove_file(path);
}

#[test]
fn postgres_register_denied_without_network_grant() {
    let mut session = Session::from_config(SessionConfig::default()).unwrap();
    let err = session
        .register_postgres(
            "t",
            &mizuchi::PostgresSource {
                host: "unused".into(),
                port: 5432,
                database: "unused".into(),
                user: "unused".into(),
                password: None,
                ssl: false,
            },
            mizuchi::PostgresReadOptions {
                table: Some("items"),
                ..mizuchi::PostgresReadOptions::default()
            },
        )
        .unwrap_err();
    assert!(err.to_string().contains("network is not granted"));
}
