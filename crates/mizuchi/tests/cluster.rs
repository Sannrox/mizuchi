//! Cluster coordinator: worker grants, scale, and dispatch-only multi-worker job.

#[allow(dead_code)]
mod ds_common;

use std::sync::Arc;

use arrow::array::Int64Array;
use mizuchi::{Batch, Cluster, ClusterConfig, DataType, Field, RecordBatch, Schema, Session};

fn numbered(n: i64) -> (arrow::datatypes::SchemaRef, Vec<Batch>) {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(
            (0..n).map(Some).collect::<Vec<_>>(),
        ))],
    )
    .unwrap();
    (schema, vec![Batch::from_arrow(batch).unwrap()])
}

fn root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn public_cluster_scales_and_runs() {
    let cluster = Cluster::standalone(ClusterConfig {
        workers: 1,
        min_workers: 1,
        max_workers: 4,
        speculate: true,
        ..ClusterConfig::default()
    })
    .unwrap();
    assert_eq!(cluster.worker_count().unwrap(), 1);
    assert_eq!(cluster.scale_to(2).unwrap(), 2);
    let mut session = cluster.session().unwrap();
    let (schema, batches) = numbered(8);
    session.register("t", schema, batches).unwrap();
    let out = session.execute_sql("SELECT COUNT(*) AS c FROM t").unwrap();
    assert!(!out.is_empty());
    assert_eq!(cluster.release_idle().unwrap(), 1);
    assert_eq!(cluster.worker_count().unwrap(), 1);
}

#[test]
fn two_workers_match_single_session() {
    let (schema, batches) = numbered(12);
    let mut one = Session::new();
    let cluster = Cluster::standalone(ClusterConfig {
        workers: 2,
        threads_per_worker: 1,
        ..ClusterConfig::default()
    })
    .unwrap();
    let mut two = cluster.session().unwrap();
    one.register("t", schema.clone(), batches.clone()).unwrap();
    two.register("t", schema, batches).unwrap();
    let sql = "SELECT a FROM t WHERE a < 10";
    let a = one.execute_sql(sql).unwrap();
    let b = two.execute_sql(sql).unwrap();
    let col = |out: &[Batch]| {
        out.iter()
            .flat_map(|batch| {
                batch
                    .as_arrow()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .map(|v| v.unwrap())
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(col(&a), col(&b));
}

#[test]
fn deploy_manifests_and_optional_workflow() {
    let root = root();
    for rel in [
        "deploy/README.md",
        "deploy/compose.yaml",
        "deploy/kubernetes.yaml",
        ".github/workflows/cluster.yml",
        "crates/mizuchi/tests/cluster.rs",
    ] {
        assert!(root.join(rel).is_file(), "missing {rel}");
    }
    let yml = std::fs::read_to_string(root.join(".github/workflows/cluster.yml")).unwrap();
    assert!(yml.contains("workflow_dispatch"));
    assert!(
        !yml.contains("pull_request"),
        "cluster workflow must not run on pull_request"
    );
    assert!(yml.contains("--test cluster"));
}

#[test]
#[ignore = "multi-worker decision-support; optional cluster workflow"]
fn multi_worker_decision_support() {
    let cluster = Cluster::standalone(ClusterConfig {
        workers: 2,
        threads_per_worker: 1,
        memory_per_worker: 64 * 1024 * 1024,
        ..ClusterConfig::default()
    })
    .unwrap();
    let mut session = cluster.session().unwrap();
    for (name, batch) in ds_common::tables(1) {
        session
            .register(&name, batch.schema(), vec![batch])
            .unwrap();
    }
    for spec in ds_common::QUERIES {
        let out = session
            .execute_sql(spec.sql)
            .unwrap_or_else(|e| panic!("{}: {e}", spec.id));
        let _ = out;
    }
}
