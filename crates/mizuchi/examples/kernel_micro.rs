//! Micro-benchmarks for columnar filter, projection, and aggregate update.
//!
//! Times Arrow-kernel paths over 1_000_000 Int64 rows. Not part of default
//! `cargo test`. Run:
//!
//! ```sh
//! cargo run --release -p mizuchi --example kernel_micro
//! ```

use std::sync::Arc;
use std::time::Instant;

use arrow::array::Int64Array;
use mizuchi::{Batch, Session};

const N: i64 = 1_000_000;

fn main() -> mizuchi::Result<()> {
    let schema = Arc::new(mizuchi::Schema::new(vec![
        mizuchi::Field::new("id", mizuchi::DataType::Int64, false),
        mizuchi::Field::new("g", mizuchi::DataType::Int64, false),
    ]));
    let ids: Vec<i64> = (0..N).collect();
    let groups: Vec<i64> = (0..N).map(|i| i % 8).collect();
    let batch = Batch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(groups)),
        ],
    )?;
    let mut session = Session::new();
    session.register("t", schema, vec![batch])?;

    let cases = [
        ("filter", "SELECT id FROM t WHERE id >= 999000"),
        ("project", "SELECT id + 1 AS x FROM t"),
        ("agg_ungrouped", "SELECT SUM(id) AS s, COUNT(*) AS c FROM t"),
        (
            "agg_grouped",
            "SELECT g, COUNT(*) AS c, SUM(id) AS s FROM t GROUP BY g",
        ),
    ];
    for (name, sql) in cases {
        let _warmup = session.execute_sql(sql)?;
        let start = Instant::now();
        let out = session.execute_sql(sql)?;
        let ms = start.elapsed().as_secs_f64() * 1_000.0;
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        println!("{name}\t{ms:.3} ms\t{rows} rows");
    }
    Ok(())
}
