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
    let (a1, _h1) = spawn_worker(true, Some(dir.clone()), None).unwrap();
    let (a2, _h2) = spawn_worker(true, Some(dir.clone()), None).unwrap();
    let (a3, _h3) = spawn_worker(true, Some(dir.clone()), None).unwrap();
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
    let (a1, _h1) = spawn_worker(true, None, None).unwrap();
    let (a2, _h2) = spawn_worker(true, None, None).unwrap();
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

fn lease_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "mizuchi-lease-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

/// #161: the leader dies mid-query, the standby takes the lease with a higher
/// fencing token, and nothing the old leader started lands afterwards.
#[test]
fn a_standby_takes_over_and_the_old_leaders_work_is_refused() {
    use mizuchi::{DirLeaseStore, Error, Leadership, MemoryLeaseStore};

    let lease = lease_dir();
    let (a1, _h1) = spawn_worker(true, None, Some(lease.clone())).unwrap();
    let (a2, _h2) = spawn_worker(true, None, None).unwrap();
    let addrs = vec![a1.to_string(), a2.to_string()];
    let ttl = Duration::from_millis(200);
    let leadership = |holder: &str| {
        Arc::new(Leadership::new(
            Box::new(DirLeaseStore::new(&lease).unwrap()),
            holder,
            ttl,
        ))
    };
    let first = leadership("first");
    let standby = leadership("standby");
    assert_eq!(first.try_lead().unwrap(), Some(1));
    assert_eq!(standby.try_lead().unwrap(), None);

    let leader = RemoteCluster::connect(addrs.clone(), true, None)
        .unwrap()
        .with_leadership(first.clone());
    let (schema, batches) = numbered(10);
    leader
        .register("t", schema.clone(), batches.clone())
        .unwrap();
    // Stage one completes under the first lease.
    assert_eq!(
        col(&leader.execute_sql("SELECT a FROM t WHERE a < 3").unwrap()),
        [0, 1, 2]
    );

    // Stage two is admitted under token 1, then the leader stops renewing
    // (it is killed), and the standby takes over while the stage runs.
    let stage = {
        let leader = leader.clone();
        thread::spawn(move || leader.execute_sql_slow("SELECT SUM(a) AS s FROM t"))
    };
    let token = standby.lead(Duration::from_millis(20)).unwrap();
    assert!(token > 1);
    // The live successor keeps its lease; the killed leader renews nothing.
    let _alive = standby.keep_alive(ttl / 4);
    let successor = RemoteCluster::connect(addrs.clone(), true, None)
        .unwrap()
        .with_leadership(standby.clone());
    successor
        .register("t", schema.clone(), batches.clone())
        .unwrap();

    // The old leader's in-flight stage fails cleanly instead of landing.
    let stale = stage.join().unwrap();
    assert!(matches!(stale, Err(Error::Fenced(_))), "{stale:?}");
    // Its next assignment is refused before it reaches a worker.
    let next = leader.execute_sql("SELECT a FROM t");
    assert!(matches!(next, Err(Error::Fenced(_))), "{next:?}");
    assert!(!first.status().leader);

    // A coordinator that still believes it holds token 1 (a partitioned
    // leader with its own view of the lease) is refused by the workers.
    let partitioned = RemoteCluster::connect(addrs.clone(), true, None)
        .unwrap()
        .with_leadership(Arc::new(Leadership::new(
            Box::new(MemoryLeaseStore::default()),
            "partitioned",
            ttl,
        )));
    let refused = partitioned.execute_sql("SELECT a FROM t");
    assert!(matches!(refused, Err(Error::Fenced(_))), "{refused:?}");

    // A worker restarted after the takeover has no high-water mark, but it
    // mounts the lease directory and refuses token 1 all the same.
    let (restarted, _h3) = spawn_worker(true, None, Some(lease.clone())).unwrap();
    let late = RemoteCluster::connect(vec![restarted.to_string(), a2.to_string()], true, None)
        .unwrap()
        .with_leadership(Arc::new(Leadership::new(
            Box::new(MemoryLeaseStore::default()),
            "late",
            ttl,
        )));
    let refused = late.register("late", schema.clone(), batches.clone());
    assert!(matches!(refused, Err(Error::Fenced(_))), "{refused:?}");

    // The successor runs the query, and reports its leadership.
    assert_eq!(
        col(&successor.execute_sql("SELECT SUM(a) AS s FROM t").unwrap()),
        [45]
    );
    let status = successor.leader_status().unwrap();
    assert!(status.leader);
    assert_eq!(status.token, token);
    assert!(status.lease_age.is_some());
    let _ = std::fs::remove_dir_all(lease);
}

/// #162: the leader dies between stages; the standby resumes the batch query
/// from the committed stage without re-running it, and a committed stage whose
/// output is damaged fails the query instead of yielding a wrong result.
#[test]
fn a_standby_resumes_a_batch_query_from_its_last_committed_stage() {
    use mizuchi::{DirLeaseStore, Leadership, Stage};

    let lease = lease_dir();
    let spill = lease.join("spill");
    std::fs::create_dir_all(&spill).unwrap();
    let (a1, _h1) = spawn_worker(true, Some(spill.clone()), Some(lease.clone())).unwrap();
    let (a2, _h2) = spawn_worker(true, Some(spill.clone()), Some(lease.clone())).unwrap();
    let addrs = vec![a1.to_string(), a2.to_string()];
    let ttl = Duration::from_millis(200);
    let leadership = |holder: &str| {
        Arc::new(Leadership::new(
            Box::new(DirLeaseStore::new(&lease).unwrap()),
            holder,
            ttl,
        ))
    };
    let stages = [
        Stage {
            sql: "SELECT a FROM t WHERE a >= 5".into(),
            output: "big".into(),
        },
        Stage {
            sql: "SELECT SUM(a) AS s FROM big".into(),
            output: "total".into(),
        },
    ];

    let first = leadership("first");
    first.try_lead().unwrap().unwrap();
    let leader = RemoteCluster::connect(addrs.clone(), true, Some(spill.clone()))
        .unwrap()
        .with_leadership(first);
    let (schema, batches) = numbered(10);
    leader.register("t", schema, batches).unwrap();
    // A stage with no rows still hands its columns to the next one.
    let empty = [
        Stage {
            sql: "SELECT a FROM t WHERE a > 100".into(),
            output: "none".into(),
        },
        Stage {
            sql: "SELECT COUNT(a) AS c FROM none".into(),
            output: "count".into(),
        },
    ];
    assert_eq!(col(&leader.run_staged("q0", &empty).unwrap()), [0]);
    // Stage one commits; then the leader dies before stage two.
    assert_eq!(
        col(&leader.run_staged("q1", &stages[..1]).unwrap()),
        [5, 6, 7, 8, 9]
    );
    drop(leader);

    let standby = leadership("standby");
    standby.lead(Duration::from_millis(20)).unwrap();
    let successor = RemoteCluster::connect(addrs.clone(), true, Some(spill.clone()))
        .unwrap()
        .with_leadership(standby.clone());
    // The successor never registers `t`: re-running stage one would fail, so
    // the right total proves the committed stage was reused.
    assert_eq!(col(&successor.run_staged("q1", &stages).unwrap()), [35]);

    // Damage the committed output of stage one.
    let outputs = spill.join("queries").join("q1");
    for entry in std::fs::read_dir(&outputs).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|ext| ext == "arrow")
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("stage-0."))
        {
            std::fs::write(&path, b"partial").unwrap();
        }
    }
    let damaged = successor.run_staged("q1", &stages);
    assert!(
        matches!(&damaged, Err(mizuchi::Error::Io(message)) if message.contains("cannot resume")),
        "{damaged:?}"
    );
    let _ = std::fs::remove_dir_all(lease);
}
