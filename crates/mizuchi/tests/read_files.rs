use std::path::PathBuf;

use arrow::array::Int64Array;
use mizuchi::{CsvReadOptions, read_csv};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data")
        .join(name)
}

#[test]
fn csv_fixture_with_header_and_infer() {
    let path = fixture("simple.csv");
    let out = read_csv(
        &path,
        CsvReadOptions {
            has_header: true,
            infer_schema: true,
            ..CsvReadOptions::default()
        },
    )
    .unwrap();
    let col = out[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(col.value(0), 1);
    assert_eq!(col.value(1), 2);
}
