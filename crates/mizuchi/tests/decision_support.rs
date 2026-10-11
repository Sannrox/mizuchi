//! Tiny TPC-H / TPC-DS style suite.
//!
//! A deterministic generator loads in-memory Arrow tables. The harness runs a
//! fixed query set and writes JSON with per-query wall time and memory. Dates
//! are days since 1992-01-01 (Int64) so filters stay in the engine dialect.
//! Default `cargo test` uses scale 1 (tens of rows). Larger scales are an
//! ignored test so offline CI stays free of bulky datasets.

mod ds_common;

use std::time::Instant;

use ds_common::{QUERIES, batch_bytes, load};

struct QueryRow {
    id: &'static str,
    elapsed_ms: f64,
    memory_bytes: u64,
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
            s.push_str(&format!("      \"memory_bytes\": {},\n", q.memory_bytes));
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
            memory_bytes: input_bytes.saturating_add(result_bytes),
            rows,
        });
    }
    SuiteReport {
        scale,
        input_bytes,
        queries,
    }
}

#[test]
fn tiny_scale_suite_reports_json() {
    let report = run_suite(1);
    assert_eq!(report.queries.len(), QUERIES.len());
    assert!(report.input_bytes > 0);
    for (q, spec) in report.queries.iter().zip(QUERIES) {
        assert_eq!(q.id, spec.id);
        assert!(q.elapsed_ms >= 0.0, "{}", q.id);
        assert!(q.memory_bytes >= report.input_bytes, "{}", q.id);
    }
    let json = report.to_json();
    println!("{json}");
    assert!(json.contains("\"suite\": \"decision-support\""));
    assert!(json.contains("\"elapsed_ms\""));
    assert!(json.contains("\"memory_bytes\""));
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
