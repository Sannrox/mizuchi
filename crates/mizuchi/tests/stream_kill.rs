//! Kill a streaming child mid-trigger and resume from the checkpoint until
//! the sink matches a batch reference, including late rows behind the watermark.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use mizuchi::{Batch, EventWindow, StreamOutput, StreamQuery, StreamQueryConfig};

const SEEDS: u64 = 24;
const MAX_RESTARTS: u32 = 64;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, true),
        Field::new("k", DataType::Utf8, true),
        Field::new("v", DataType::Int64, true),
    ]))
}

/// One row per trigger. Event 3 is late after the watermark advances.
fn events() -> Vec<(i64, &'static str, i64)> {
    vec![
        (1_000_000, "a", 1),
        (2_000_000, "a", 5),
        (11_000_000, "a", 1),
        (4_000_000, "a", 99),
        (21_000_000, "a", 1),
        (12_000_000, "b", 7),
        (31_000_000, "b", 4),
    ]
}

fn cfg(root: &Path) -> StreamQueryConfig {
    StreamQueryConfig {
        checkpoint: root.join("chk"),
        sink: root.join("sink"),
        event_time: "ts".into(),
        delay: Duration::from_secs(0),
        window: EventWindow::Tumbling {
            size: Duration::from_secs(10),
        },
        group: vec!["k".into()],
        value: Some("v".into()),
        output: StreamOutput::Append,
    }
}

fn row(ts: i64, k: &str, v: i64) -> Batch {
    Batch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![ts])),
            Arc::new(StringArray::from(vec![Some(k)])),
            Arc::new(Int64Array::from(vec![v])),
        ],
    )
    .unwrap()
}

fn committed_next(checkpoint: &Path) -> u64 {
    let dir = checkpoint.join("commits");
    if !dir.exists() {
        return 0;
    }
    let mut ids = Vec::new();
    for ent in fs::read_dir(&dir).unwrap() {
        if let Ok(id) = ent.unwrap().file_name().to_string_lossy().parse::<u64>() {
            ids.push(id);
        }
    }
    ids.iter().max().map(|id| id + 1).unwrap_or(0)
}

fn run_from_checkpoint(root: &Path) {
    let events = events();
    let meta = root.join("chk").join("metadata.json");
    let mut q = if meta.exists() {
        StreamQuery::open(root.join("chk")).expect("recover")
    } else {
        StreamQuery::try_new(schema(), cfg(root)).expect("new query")
    };
    let start = committed_next(&root.join("chk")) as usize;
    for (ts, k, v) in events.into_iter().skip(start) {
        q.ingest(row(ts, k, v)).unwrap();
        q.trigger().unwrap();
    }
}

fn sink_rows(root: &Path) -> Vec<(i64, i64, String, i64, i64)> {
    let q = StreamQuery::open(root.join("chk")).expect("open sink");
    rows_of(&q.committed_output().unwrap())
}

fn rows_of(batches: &[Batch]) -> Vec<(i64, i64, String, i64, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let s = b
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let e = b
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let k = b.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        let c = b.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
        let v = b.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((
                s.value(i),
                e.value(i),
                k.value(i).to_string(),
                c.value(i),
                v.value(i),
            ));
        }
    }
    out.sort();
    out
}

fn reference() -> Vec<(i64, i64, String, i64, i64)> {
    let root = unique("ref");
    run_from_checkpoint(&root);
    let got = sink_rows(&root);
    let _ = fs::remove_dir_all(&root);
    got
}

fn unique(tag: &str) -> PathBuf {
    let n = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let p = std::env::temp_dir().join(format!("mizuchi-stream-kill-{tag}-{n}-{nanos}"));
    let _ = fs::remove_dir_all(&p);
    fs::create_dir_all(&p).unwrap();
    p
}

fn spawn_child(root: &Path) -> std::process::Child {
    Command::new(std::env::current_exe().unwrap())
        .env("MIZUCHI_STREAM_CHILD", "1")
        .env("MIZUCHI_STREAM_ROOT", root)
        .args(["--exact", "child_runs_checkpointed_query"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn child")
}

#[test]
fn child_runs_checkpointed_query() {
    let Ok(root) = std::env::var("MIZUCHI_STREAM_ROOT") else {
        return;
    };
    if std::env::var_os("MIZUCHI_STREAM_CHILD").is_none() {
        return;
    }
    run_from_checkpoint(Path::new(&root));
}

#[test]
fn kill_and_restart_matches_batch_reference() {
    if std::env::var_os("MIZUCHI_STREAM_CHILD").is_some() {
        return;
    }
    let expected = reference();
    assert!(
        expected.iter().any(|r| r.2 == "a" && r.3 == 2 && r.4 == 6),
        "reference must close the first window with both on-time rows: {expected:?}"
    );
    assert!(
        expected.iter().all(|r| r.4 != 99 && r.4 != 7),
        "late rows must not land in the sink: {expected:?}"
    );

    for seed in 0..SEEDS {
        let root = unique(&format!("s{seed}"));
        let mut attempt = 0;
        loop {
            let mut child = spawn_child(&root);
            // First wait is long enough for metadata and the first commit.
            // Later waits mix short kills with occasional long ones so the
            // remaining triggers can finish on a slow runner.
            let wait_ms = if attempt == 0 {
                30 + (seed % 50)
            } else if attempt % 8 == 7 {
                150 + (seed % 100)
            } else {
                2 + ((seed.wrapping_mul(17) + u64::from(attempt) * 13) % 40)
            };
            std::thread::sleep(Duration::from_millis(wait_ms));
            match child.try_wait().unwrap() {
                Some(status) => {
                    assert!(
                        status.success(),
                        "seed {seed} child exited {status} after {attempt} restarts"
                    );
                    break;
                }
                None => {
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }
            attempt += 1;
            assert!(
                attempt < MAX_RESTARTS,
                "seed {seed} did not finish after {MAX_RESTARTS} kills"
            );
        }
        let got = sink_rows(&root);
        assert_eq!(got, expected, "seed {seed} after {attempt} kills");
        let _ = fs::remove_dir_all(&root);
    }
}
