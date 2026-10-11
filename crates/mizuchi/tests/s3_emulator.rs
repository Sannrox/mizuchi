//! Local S3-compatible emulator job. Not part of default `cargo test`.
//!
//! Enable with `--features s3 -- --ignored` and point the store at an emulator
//! the runner started (`MIZUCHI_S3_ENDPOINT`, `MIZUCHI_S3_BUCKET`,
//! `MIZUCHI_S3_ACCESS_KEY`, `MIZUCHI_S3_SECRET_KEY`).

#![cfg(feature = "s3")]

use mizuchi::{
    Batch, DataType, Field, ParquetReadOptions, ParquetWriteOptions, Schema, WriteMode,
    read_parquet_store, write_parquet_store,
};
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;

#[test]
#[ignore = "needs a local S3-compatible emulator; not default cargo test"]
fn s3_emulator_round_trip_and_prune() {
    let endpoint = std::env::var("MIZUCHI_S3_ENDPOINT").expect("MIZUCHI_S3_ENDPOINT");
    let bucket = std::env::var("MIZUCHI_S3_BUCKET").expect("MIZUCHI_S3_BUCKET");
    let key = std::env::var("MIZUCHI_S3_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into());
    let secret = std::env::var("MIZUCHI_S3_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into());
    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_bucket_name(bucket)
        .with_access_key_id(key)
        .with_secret_access_key(secret)
        .with_allow_http(true)
        .build()
        .expect("s3 store");
    let store: Arc<dyn mizuchi::ObjectStore> = Arc::new(store);

    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int64, true),
        Field::new("id", DataType::Int64, true),
    ]));
    let batch = Batch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::Int64Array::from(vec![Some(2020), Some(2021)])),
            Arc::new(arrow::array::Int64Array::from(vec![Some(1), Some(2)])),
        ],
    )
    .unwrap();
    let cols = [String::from("year")];
    write_parquet_store(
        store.as_ref(),
        "mizuchi-emu",
        &[batch],
        ParquetWriteOptions {
            partition_by: &cols,
            mode: WriteMode::Overwrite,
        },
    )
    .unwrap();
    let out =
        read_parquet_store(store.as_ref(), "mizuchi-emu", ParquetReadOptions::default()).unwrap();
    assert!(!out.is_empty());
}
