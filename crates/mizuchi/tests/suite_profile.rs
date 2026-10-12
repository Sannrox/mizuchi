//! CPU and allocation profiles for the decision-support suite.
//!
//! Drives shipped [`mizuchi::Session::query_metrics`] on the tiny in-memory
//! generator. Exclusive operator time (`elapsed_ns` minus children) is the
//! physical-plan CPU profile; `peak_bytes` is the pool high-water. Times are
//! not hardcoded: the test checks operator mix and tree invariants. SF1 wall
//! times and ranked limiters live in `docs/bench/limiters.md`.

mod ds_common;

use ds_common::{QUERIES, load};
use mizuchi::OperatorMetrics;

fn walk_names(m: &OperatorMetrics) -> Vec<String> {
    let mut nodes = Vec::new();
    m.walk(&mut nodes);
    nodes.iter().map(|n| n.name.clone()).collect()
}

fn assert_invariants(m: &OperatorMetrics, id: &str) {
    let nested: u64 = m
        .children
        .iter()
        .filter(|c| !c.blocking)
        .map(|c| c.elapsed_ns)
        .sum();
    assert_eq!(
        m.exclusive_ns(),
        m.elapsed_ns.saturating_sub(nested),
        "{id} {}",
        m.name
    );
    assert!(m.elapsed_ns > 0, "{id} {} elapsed_ns=0", m.name);
    for c in &m.children {
        assert_invariants(c, id);
    }
}

fn require_name(names: &[String], want: &str, id: &str) {
    assert!(
        names.iter().any(|n| n == want),
        "{id} missing {want} in {names:?}"
    );
}

#[test]
fn tiny_suite_query_metrics_rank_operators() {
    // Join/aggregate reserve only when a cap is set; the cap is far above tiny.
    let (session, _) = load(1);
    let session = session.with_memory_limit(64u64 * 1024 * 1024);
    for spec in QUERIES {
        let metrics = session
            .query_metrics(spec.sql)
            .unwrap_or_else(|e| panic!("{}: {e}", spec.id));
        assert_invariants(&metrics, spec.id);
        let json = metrics.to_json();
        assert!(
            json.contains("\"exclusive_ns\":"),
            "{} json {json}",
            spec.id
        );
        let ranked_json = metrics.ranked_exclusive_json();
        assert!(
            ranked_json.starts_with('['),
            "{} ranked {ranked_json}",
            spec.id
        );
        let names = walk_names(&metrics);
        let ranked = metrics.ranked_exclusive();
        assert!(!ranked.is_empty(), "{}", spec.id);
        println!(
            "{} peak_bytes={} ranked={}",
            spec.id,
            metrics.peak_bytes,
            ranked
                .iter()
                .map(|n| format!("{}:{}", n.name, n.exclusive_ns()))
                .collect::<Vec<_>>()
                .join(",")
        );
        match spec.id {
            "tpch_q1" => {
                require_name(&names, "HashAggregate", spec.id);
                require_name(&names, "Scan", spec.id);
                assert!(
                    ranked.iter().any(|n| n.name == "HashAggregate"),
                    "{} ranked {:?}",
                    spec.id,
                    ranked.iter().map(|n| &n.name).collect::<Vec<_>>()
                );
            }
            "tpch_q3" => {
                let joins = names.iter().filter(|n| *n == "HashJoin").count();
                assert!(
                    joins >= 2,
                    "{} HashJoin count {joins} in {names:?}",
                    spec.id
                );
                require_name(&names, "HashAggregate", spec.id);
                require_name(&names, "Scan", spec.id);
                let top2: Vec<&str> = ranked.iter().take(2).map(|n| n.name.as_str()).collect();
                assert!(
                    top2.contains(&"HashJoin"),
                    "{} expected HashJoin in top exclusive times {top2:?}",
                    spec.id
                );
                assert!(metrics.peak_bytes > 0, "{} peak_bytes=0", spec.id);
            }
            "tpch_q6" => {
                require_name(&names, "Scan", spec.id);
                require_name(&names, "HashAggregate", spec.id);
            }
            "tpcds_store_sales" => {
                let joins = names.iter().filter(|n| *n == "HashJoin").count();
                assert!(
                    joins >= 2,
                    "{} HashJoin count {joins} in {names:?}",
                    spec.id
                );
                require_name(&names, "HashAggregate", spec.id);
                assert!(
                    ranked.iter().any(|n| n.name == "HashJoin"),
                    "{} missing HashJoin in {:?}",
                    spec.id,
                    ranked.iter().map(|n| &n.name).collect::<Vec<_>>()
                );
                assert!(metrics.peak_bytes > 0, "{} peak_bytes=0", spec.id);
            }
            other => panic!("unexpected suite query {other}"),
        }
    }
}
