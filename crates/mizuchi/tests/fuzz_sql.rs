//! Grammar-fuzz regression on the stable toolchain.
//!
//! LibFuzzer targets live under `fuzz/` and run in CI for a bounded time.
//! Crashes found there are added here as SQL cases.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use mizuchi::{Batch, Session};

const CASES: &[&str] = &[
    "SELECT 1 AS x ORDER BY x",
    "SELECT NULL AS x ORDER BY x",
    "SELECT 'x' AS x ORDER BY x",
    "SELECT a FROM t WHERE a > 1 ORDER BY a",
    "SELECT a FROM t WHERE a IS NULL ORDER BY a",
    "SELECT a FROM t WHERE b = 'x' ORDER BY a",
    "SELECT a FROM t WHERE a > 0 AND a IS NOT NULL ORDER BY a",
    "SELECT b, COUNT(*) AS c FROM t GROUP BY b ORDER BY b",
    "SELECT a, x FROM t JOIN u ON a = id ORDER BY a, x",
    "SELECT a FROM t ORDER BY a LIMIT 2",
    "SELECT a FROM t WHERE a IS NOT NULL ORDER BY a",
];

#[test]
fn grammar_cases_pull_match_two_workers() {
    for sql in CASES {
        let pull = session();
        let staged = session().with_workers(2).unwrap();
        match (pull.execute_sql(sql), staged.execute_sql(sql)) {
            (Ok(a), Ok(b)) => assert_eq!(rows(&a), rows(&b), "{sql}"),
            (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("workers=2 failed {sql}: {e}"),
            (Err(e), Ok(_)) => panic!("pull failed {sql}: {e}"),
        }
    }
}

#[test]
fn parse_and_plan_reject_garbage_without_panic() {
    let mut catalog = mizuchi::Catalog::new();
    catalog.register("t", t_schema()).unwrap();
    for s in ["", "SELECT", "\0", "SELECT * FROM t; DROP", "SELECT (("] {
        let _ = mizuchi::parse_sql(s);
        let _ = mizuchi::plan_sql(s, &catalog);
    }
}

fn session() -> Session {
    let mut s = Session::new();
    s.register("t", t_schema(), vec![t_batch()]).unwrap();
    s.register("u", u_schema(), vec![u_batch()]).unwrap();
    s
}

fn t_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8, true),
    ]))
}

fn u_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("x", DataType::Int64, true),
    ]))
}

fn t_batch() -> Batch {
    Batch::try_new(
        t_schema(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), None])),
            Arc::new(StringArray::from(vec![
                Some("x"),
                Some("y"),
                Some("z"),
                Some("n"),
            ])),
        ],
    )
    .unwrap()
}

fn u_batch() -> Batch {
    Batch::try_new(
        u_schema(),
        vec![
            Arc::new(Int64Array::from(vec![Some(2), Some(3)])),
            Arc::new(Int64Array::from(vec![Some(10), Some(20)])),
        ],
    )
    .unwrap()
}

fn rows(batches: &[Batch]) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    for batch in batches {
        for r in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());
            for c in 0..batch.num_columns() {
                let col = batch.column(c);
                if col.is_null(r) {
                    row.push("NULL".into());
                } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                    row.push(a.value(r).to_string());
                } else if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
                    row.push(a.value(r).to_string());
                } else {
                    row.push(format!("{}[{r}]", col.data_type()));
                }
            }
            out.push(row);
        }
    }
    out
}
