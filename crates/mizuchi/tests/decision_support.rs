//! Tiny TPC-H / TPC-DS style suite.
//!
//! A deterministic generator loads in-memory Arrow tables. The harness runs a
//! fixed query set and prints JSON with per-query wall time, row count and
//! `input_plus_result_bytes` (input table bytes plus result bytes, a size
//! figure rather than measured peak memory). Dates are days since 1992-01-01
//! (Int64) so filters stay in the engine dialect.
//! Default `cargo test` uses scale 1 (tens of rows). Larger scales are an
//! ignored test so offline CI stays free of bulky datasets.

mod ds_common;

use std::time::Instant;

use arrow::util::display::{ArrayFormatter, FormatOptions};
use ds_common::{QUERIES, batch_bytes, load};

struct QueryRow {
    id: &'static str,
    elapsed_ms: f64,
    input_plus_result_bytes: u64,
    rows: usize,
}

struct SuiteReport {
    scale: u32,
    input_bytes: u64,
    queries: Vec<QueryRow>,
}

impl SuiteReport {
    fn to_json(&self) -> String {
        let mut s = String::from("{\n");
        s.push_str("  \"suite\": \"decision-support\",\n");
        s.push_str(&format!("  \"scale\": {},\n", self.scale));
        s.push_str(&format!("  \"input_bytes\": {},\n", self.input_bytes));
        s.push_str("  \"queries\": [\n");
        for (i, q) in self.queries.iter().enumerate() {
            s.push_str("    {\n");
            s.push_str(&format!("      \"id\": \"{}\",\n", q.id));
            s.push_str(&format!("      \"elapsed_ms\": {:.3},\n", q.elapsed_ms));
            s.push_str(&format!(
                "      \"input_plus_result_bytes\": {},\n",
                q.input_plus_result_bytes
            ));
            s.push_str(&format!("      \"rows\": {}\n", q.rows));
            s.push_str("    }");
            if i + 1 < self.queries.len() {
                s.push(',');
            }
            s.push('\n');
        }
        s.push_str("  ]\n}\n");
        s
    }
}

fn run_suite(scale: u32) -> SuiteReport {
    let (session, input_bytes) = load(scale);
    let mut queries = Vec::new();
    for spec in QUERIES {
        let start = Instant::now();
        let out = session
            .execute_sql(spec.sql)
            .unwrap_or_else(|e| panic!("{}: {e}\n{}", spec.id, spec.sql));
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        let result_bytes: u64 = out.iter().map(batch_bytes).sum();
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        queries.push(QueryRow {
            id: spec.id,
            elapsed_ms,
            input_plus_result_bytes: input_bytes.saturating_add(result_bytes),
            rows,
        });
    }
    SuiteReport {
        scale,
        input_bytes,
        queries,
    }
}

fn result_text(session: &mizuchi::Session, sql: &str) -> String {
    let out = session
        .execute_sql(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let opts = FormatOptions::default();
    let mut lines = Vec::new();
    for batch in &out {
        let rec = batch.as_arrow();
        let fmts: Vec<ArrayFormatter<'_>> = rec
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &opts).expect("fmt"))
            .collect();
        for i in 0..rec.num_rows() {
            let mut line = String::new();
            for (j, fmt) in fmts.iter().enumerate() {
                if j > 0 {
                    line.push('\t');
                }
                line.push_str(&fmt.value(i).to_string());
            }
            lines.push(line);
        }
    }
    lines.join("\n")
}

#[test]
fn tiny_suite_matches_expected_on_one_and_two_workers() {
    let (one, _) = load(1);
    let two = one.clone().with_workers(2).expect("workers");
    for spec in QUERIES {
        let a = result_text(&one, spec.sql);
        let b = result_text(&two, spec.sql);
        assert_eq!(a, b, "{}", spec.id);
        let want = EXPECTED
            .iter()
            .find(|(id, _)| *id == spec.id)
            .unwrap_or_else(|| panic!("no expected result for {}", spec.id))
            .1;
        assert_eq!(rounded(&a), want, "{}", spec.id);
    }
}

/// Scale-1 results, floats rounded to three places so a change in
/// summation order does not fail the test but a wrong value does.
const EXPECTED: &[(&str, &str)] = &[
    (
        "tpch_q1",
        "A\tF\t316.000\t9760.000\t9172.000\t9628.168\t39.500\t1220.000\t0.060\t8\n\
         A\tO\t300.000\t10000.000\t9497.600\t9874.592\t37.500\t1250.000\t0.050\t8\n\
         N\tF\t308.000\t10080.000\t9472.800\t9943.464\t38.500\t1260.000\t0.060\t8\n\
         N\tO\t304.000\t9840.000\t9340.800\t9716.352\t38.000\t1230.000\t0.050\t8\n\
         R\tF\t80.000\t9920.000\t9324.800\t9797.808\t10.000\t1240.000\t0.060\t8\n\
         R\tO\t80.000\t9680.000\t9099.200\t9465.424\t10.000\t1210.000\t0.060\t8",
    ),
    ("tpch_q3", "15\t1135\t2\t3614.000\n7\t1143\t0\t3388.400"),
    ("tpch_q6", "283.200"),
    (
        "tpcds_store_sales",
        "Books\t147.000\nHome\t21.000\nElectronics\t5.000",
    ),
];

/// Rewrites each fractional field of `text` with three decimal places.
fn rounded(text: &str) -> String {
    text.lines()
        .map(|line| {
            line.split('\t')
                .map(|field| match field.parse::<f64>() {
                    Ok(v) if field.contains('.') => format!("{v:.3}"),
                    _ => field.to_string(),
                })
                .collect::<Vec<_>>()
                .join("\t")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn tiny_scale_suite_reports_json() {
    let report = run_suite(1);
    assert_eq!(report.queries.len(), QUERIES.len());
    assert!(report.input_bytes > 0);
    for (q, spec) in report.queries.iter().zip(QUERIES) {
        assert_eq!(q.id, spec.id);
        assert!(q.elapsed_ms >= 0.0, "{}", q.id);
        assert!(q.input_plus_result_bytes >= report.input_bytes, "{}", q.id);
    }
    let json = report.to_json();
    println!("{json}");
    assert!(json.contains("\"suite\": \"decision-support\""));
    assert!(json.contains("\"elapsed_ms\""));
    assert!(json.contains("\"input_plus_result_bytes\""));
    assert!(json.contains("\"tpch_q1\""));
    assert!(json.contains("\"tpch_q6\""));
    assert!(json.contains("\"tpcds_store_sales\""));
    assert!(json.starts_with('{') && json.trim_end().ends_with('}'));
}

/// Scale 8 is still in-memory and offline; keep it out of the default run.
#[test]
#[ignore]
fn scale8_suite_reports_json() {
    let report = run_suite(8);
    let json = report.to_json();
    println!("{json}");
    assert_eq!(report.queries.len(), QUERIES.len());
}
