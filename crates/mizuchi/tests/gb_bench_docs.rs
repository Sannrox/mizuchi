//! Default-test contract for the gigabyte-scale bench docs and harness.
//!
//! Does not generate Parquet or time SF1/SF10. Those runs live in
//! `scripts/bench-gb.sh` and the optional `bench-gb` workflow.

use std::fs;
use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    fs::read_to_string(root().join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))
}

#[test]
fn methodology_records_hardware_and_method() {
    let text = read("docs/bench/methodology.md");
    for needle in [
        "single-node workstation",
        "32 GiB",
        "release",
        "median",
        "warmup",
        "Snappy",
        "6_000_000",
        "60_000_000",
        "DuckDB",
        "DataFusion",
        "scripts/bench-gb.sh",
        "scripts/profile-suite.sh",
        "query_metrics",
        "--workers",
    ] {
        assert!(text.contains(needle), "methodology missing {needle}");
    }
    assert!(
        !text.contains("pull_request"),
        "methodology must not imply the default PR CI runs SF1"
    );
}

#[test]
fn results_table_covers_sf1_and_sf10() {
    let text = read("docs/bench/results.md");
    for needle in ["sf1", "sf10", "mizuchi", "DuckDB", "DataFusion"] {
        assert!(text.contains(needle), "results missing {needle}");
    }
    for q in ["tpch_q1", "tpch_q3", "tpch_q6", "tpcds_store_sales"] {
        let hits = text.matches(q).count();
        assert!(
            hits >= 2,
            "{q} should appear for both SF1 and SF10, got {hits}"
        );
    }
    assert!(text.contains("ratio"), "results missing ratio column");
    for needle in [
        "Parquet scan pushdown",
        "row groups kept",
        "clustered q6",
        "late materialization",
    ] {
        assert!(text.contains(needle), "results missing pushdown {needle}");
    }
    for needle in ["worker scaling", "1 worker", "8 ms"] {
        assert!(text.contains(needle), "results missing scaling {needle}");
    }
    // Published times are milliseconds with at least one decimal.
    let has_ms = text.lines().any(|line| {
        line.contains("|")
            && line
                .split('|')
                .any(|cell| cell.trim().parse::<f64>().is_ok() && cell.contains('.'))
    });
    assert!(
        has_ms,
        "results table should include measured millisecond values"
    );
}

#[test]
fn limiters_rank_suite_queries_and_follow_ups() {
    let text = read("docs/bench/limiters.md");
    for needle in [
        "tpch_q1",
        "tpch_q3",
        "tpch_q6",
        "tpcds_store_sales",
        "exclusive",
        "peak_bytes",
        "Scan",
        "Expression",
        "Hash aggregate",
        "Hash join",
        "Copies",
        "Scheduling",
        "#103",
        "#104",
        "#105",
        "#106",
        "#107",
        "query_metrics",
        "scripts/profile-suite.sh",
    ] {
        assert!(text.contains(needle), "limiters missing {needle}");
    }
}

#[test]
fn harness_and_optional_workflow_exist() {
    let root = root();
    for rel in [
        "scripts/bench-gb.sh",
        "scripts/bench_gb_ref.py",
        "scripts/profile-suite.sh",
        "crates/mizuchi/examples/gb_bench.rs",
        ".github/workflows/bench-gb.yml",
        "docs/bench/limiters.md",
    ] {
        assert!(root.join(rel).is_file(), "missing {rel}");
    }
    let yml = read(".github/workflows/bench-gb.yml");
    assert!(yml.contains("workflow_dispatch"));
    assert!(
        !yml.contains("pull_request"),
        "GB bench workflow must not run on pull_request"
    );
    let script = read("scripts/bench-gb.sh");
    assert!(script.contains("gb_bench"));
    assert!(script.contains("bench_gb_ref.py"));
    let profile = read("scripts/profile-suite.sh");
    assert!(profile.contains("gb_bench"));
    assert!(profile.contains("profile"));
    let example = read("crates/mizuchi/examples/gb_bench.rs");
    assert!(example.contains("\"profile\""));
    assert!(example.contains("query_metrics"));
}
