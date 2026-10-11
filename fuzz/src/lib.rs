//! Shared catalog, session, and SQL grammar for fuzz targets.

use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use mizuchi::{Batch, Catalog, Session};

pub fn catalog() -> Catalog {
    let mut c = Catalog::new();
    c.register("t", t_schema()).unwrap();
    c.register("u", u_schema()).unwrap();
    c
}

pub fn session() -> Session {
    let mut s = Session::new();
    s.register("t", t_schema(), vec![t_batch()]).unwrap();
    s.register("u", u_schema(), vec![u_batch()]).unwrap();
    s
}

pub fn t_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8, true),
    ]))
}

pub fn u_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("x", DataType::Int64, true),
    ]))
}

pub fn t_batch() -> Batch {
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

pub fn u_batch() -> Batch {
    Batch::try_new(
        u_schema(),
        vec![
            Arc::new(Int64Array::from(vec![Some(2), Some(3)])),
            Arc::new(Int64Array::from(vec![Some(10), Some(20)])),
        ],
    )
    .unwrap()
}

/// Deterministic row dump for equality of two successful executions.
pub fn rows(batches: &[Batch]) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    for batch in batches {
        for r in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());
            for c in 0..batch.num_columns() {
                row.push(cell(batch.column(c), r));
            }
            out.push(row);
        }
    }
    out
}

fn cell(col: &mizuchi::ArrayRef, r: usize) -> String {
    if col.is_null(r) {
        return "NULL".into();
    }
    match col.data_type() {
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(r)
            .to_string(),
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(r)
            .to_string(),
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(r)
            .to_string(),
        other => format!("{other}[{r}]"),
    }
}

struct Bits<'a> {
    data: &'a [u8],
    i: usize,
}

impl<'a> Bits<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, i: 0 }
    }

    fn byte(&mut self) -> u8 {
        if self.data.is_empty() {
            return 0;
        }
        let v = self.data[self.i % self.data.len()];
        self.i = self.i.wrapping_add(1);
        v
    }

    fn pick(&mut self, n: u8) -> u8 {
        if n == 0 { 0 } else { self.byte() % n }
    }
}

/// Generate a supported-SQL query from `data`. Always includes ORDER BY so
/// pull and multi-worker results can be compared positionally.
pub fn grammar_sql(data: &[u8]) -> String {
    let mut b = Bits::new(data);
    match b.pick(6) {
        0 => format!("SELECT {} AS x ORDER BY x", lit(&mut b)),
        1 => format!("SELECT a FROM t WHERE {} ORDER BY a", pred(&mut b)),
        2 => "SELECT b, COUNT(*) AS c FROM t GROUP BY b ORDER BY b".into(),
        3 => "SELECT a, x FROM t JOIN u ON a = id ORDER BY a, x".into(),
        4 => format!("SELECT a FROM t ORDER BY a LIMIT {}", u32::from(b.pick(8))),
        _ => "SELECT a FROM t WHERE a IS NOT NULL ORDER BY a".into(),
    }
}

fn lit(b: &mut Bits<'_>) -> String {
    match b.pick(3) {
        0 => format!("{}", i64::from(b.byte())),
        1 => "NULL".into(),
        _ => "'x'".into(),
    }
}

fn pred(b: &mut Bits<'_>) -> String {
    match b.pick(4) {
        0 => format!("a > {}", i64::from(b.byte())),
        1 => "a IS NULL".into(),
        2 => "b = 'x'".into(),
        _ => format!("a > {} AND a IS NOT NULL", i64::from(b.pick(5))),
    }
}
