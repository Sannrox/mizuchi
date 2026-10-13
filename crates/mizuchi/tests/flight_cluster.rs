//! Flight workers: grant, shuffle, heartbeat, and a 3-worker kill retry.

#[allow(dead_code)]
mod ds_common;

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use mizuchi::{Batch, RecordBatch, RemoteCluster, spawn_worker};

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

fn col(out: &[Batch]) -> Vec<i64> {
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
        .collect()
}

fn spill() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("mizuchi-flight-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    dir
}

#[test]
fn three_workers_execute_and_shuffle() {
    let dir = spill();
    let (a1, _h1) = spawn_worker(true, Some(dir.clone())).unwrap();
    let (a2, _h2) = spawn_worker(true, Some(dir.clone())).unwrap();
    let (a3, _h3) = spawn_worker(true, Some(dir.clone())).unwrap();
    let cluster = RemoteCluster::connect(
        vec![a1.to_string(), a2.to_string(), a3.to_string()],
        true,
        Some(dir.clone()),
    )
    .unwrap();
    assert_eq!(cluster.heartbeat(), 3);
    let (schema, batches) = numbered(8);
    cluster.register("t", schema, batches).unwrap();
    let out = cluster.execute_sql("SELECT a FROM t WHERE a >= 4").unwrap();
    assert_eq!(col(&out), vec![4, 5, 6, 7]);
    let (schema, batches) = numbered(3);
    let shuffled = cluster.shuffle_round_trip(0, 1, "part0", &batches).unwrap();
    assert_eq!(col(&shuffled), vec![0, 1, 2]);
    assert!(dir.join("part0.arrow").exists());
    let _ = schema;
}

#[test]
fn shuffle_without_spill_uses_flight() {
    let (a1, _h1) = spawn_worker(true, None).unwrap();
    let (a2, _h2) = spawn_worker(true, None).unwrap();
    let cluster = RemoteCluster::connect(vec![a1.to_string(), a2.to_string()], true, None).unwrap();
    let (_schema, batches) = numbered(3);
    let shuffled = cluster.shuffle_round_trip(0, 1, "part1", &batches).unwrap();
    assert_eq!(col(&shuffled), vec![0, 1, 2]);
}

#[test]
fn compose_layout_and_optional_workflow() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    for rel in [
        "deploy/README.md",
        "deploy/compose.yaml",
        "deploy/Dockerfile",
        ".github/workflows/flight.yml",
    ] {
        assert!(root.join(rel).is_file(), "missing {rel}");
    }
    let yml = std::fs::read_to_string(root.join(".github/workflows/flight.yml")).unwrap();
    assert!(yml.contains("workflow_dispatch"));
    assert!(
        !yml.contains("pull_request"),
        "flight workflow must not run on pull_request"
    );
    let compose = std::fs::read_to_string(root.join("deploy/compose.yaml")).unwrap();
    assert!(compose.contains("worker1"));
    assert!(compose.contains("worker2"));
    assert!(compose.contains("worker3"));
}

#[test]
#[ignore = "process workers; optional flight workflow"]
fn three_workers_kill_mid_query_decision_support() {
    let bin = env!("CARGO_BIN_EXE_mizuchi");
    let dir = spill();
    let mut kids = Vec::new();
    let mut addrs = Vec::new();
    for _ in 0..3 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let child = Command::new(bin)
            .arg("worker")
            .arg("--bind")
            .arg(addr.to_string())
            .env("MIZUCHI_ALLOW_NETWORK", "1")
            .env("MIZUCHI_SPILL", &dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        addrs.push(addr.to_string());
        kids.push(child);
    }
    let start = std::time::Instant::now();
    for addr in &addrs {
        let sock: std::net::SocketAddr = addr.parse().unwrap();
        while std::net::TcpStream::connect_timeout(&sock, Duration::from_millis(50)).is_err() {
            if start.elapsed() > Duration::from_secs(10) {
                panic!("worker {addr} did not bind");
            }
            thread::sleep(Duration::from_millis(50));
        }
    }
    let cluster = RemoteCluster::connect(addrs, true, Some(dir)).unwrap();
    while cluster.heartbeat() < 2 {
        if start.elapsed() > Duration::from_secs(10) {
            panic!("need two live workers");
        }
        thread::sleep(Duration::from_millis(50));
    }
    for (name, batch) in ds_common::tables(1) {
        cluster
            .register(&name, batch.schema(), vec![batch])
            .unwrap();
    }
    let mut first = kids.remove(0);
    let killer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(80));
        let _ = first.kill();
        let _ = first.wait();
    });
    let first_sql = ds_common::QUERIES[0].sql;
    let out = cluster
        .execute_sql_slow(first_sql)
        .unwrap_or_else(|e| panic!("{}: {e}", ds_common::QUERIES[0].id));
    assert!(!out.is_empty());
    killer.join().unwrap();
    for spec in ds_common::QUERIES.iter().skip(1) {
        let out = cluster
            .execute_sql(spec.sql)
            .unwrap_or_else(|e| panic!("{}: {e}", spec.id));
        let _ = out;
    }
    for mut kid in kids {
        let _ = kid.kill();
        let _ = kid.wait();
    }
}
