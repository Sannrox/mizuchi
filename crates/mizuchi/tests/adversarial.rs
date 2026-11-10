//! Adversarial sessions against the cap, grant and cancel boundary.
//!
//! Each test runs a hostile workload against a capped session with narrow
//! grants. Every run must end in the typed error for its class, never a
//! panic, never a write outside the granted roots, and the same session must
//! answer a follow-up query afterward. State is in memory or under per-process
//! temp dirs, so the suite runs next to other test processes.
//!
//! | Attack class | Shape | Expected failure |
//! | --- | --- | --- |
//! | accounting dodge, aggregate | one group per row; few groups with huge keys | `Error::Memory` |
//! | accounting dodge, join | build side of wide rows | `Error::Memory` |
//! | oversized literal | a large literal copied onto every buffered row | `Error::Memory` |
//! | repeated exhaustion | the cap is hit many times on one session | `Error::Memory` each time, then a query that fits still runs |
//! | grant escape, table path | `..` traversal and a symlink out of the read root | `Error::Io`, nothing read |
//! | grant escape, spill path | spill dir outside the write roots, directly, by `..`, or by `..` after a symlink | `Error::Io` at session open, nothing written |
//! | cancel during spill | cancel once spill files exist | `Error::Cancelled`, spill files removed |
//! | parse pathology | deep nesting, long chains, huge lists, executed | `Error::Parse` past `MAX_EXPR_DEPTH`; otherwise `Plan` / `Memory` or a result |
//!
//! A new operator adds a row here and a test that drives it into its
//! class's failure through [`capped`] and [`assert_still_usable`].

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arrow::array::{Int64Array, StringArray};
use mizuchi::exec::{CpuLowering, Lowering, Operator};
use mizuchi::{
    Batch, CancelToken, DataType, Error, Field, ParquetReadOptions, ParquetWriteOptions, QueryLog,
    QueryRetention, Schema, SchemaRef, Session, SessionConfig,
};

const ROWS: usize = 1024;

/// Unique per-process temp path; removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(1);
        let path = std::env::temp_dir().join(format!(
            "mizuchi-adv-{label}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// `n_batches` of `ROWS` rows: `a` unique, `g = a % 7`, `s` a string of
/// `width` bytes that is unique per row.
fn table(n_batches: usize, width: usize) -> (SchemaRef, Vec<Batch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("g", DataType::Int64, false),
        Field::new("s", DataType::Utf8, false),
    ]));
    let batches = (0..n_batches)
        .map(|b| {
            let a: Vec<i64> = (0..ROWS).map(|r| (b * ROWS + r) as i64).collect();
            let g: Vec<i64> = a.iter().map(|v| v % 7).collect();
            let s: Vec<String> = a
                .iter()
                .map(|v| {
                    let v = v.to_string();
                    "0".repeat(width.saturating_sub(v.len())) + &v
                })
                .collect();
            Batch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(a)),
                    Arc::new(Int64Array::from(g)),
                    Arc::new(StringArray::from(s)),
                ],
            )
            .unwrap()
        })
        .collect();
    (schema, batches)
}

fn batch_bytes(batches: &[Batch]) -> u64 {
    batches[0].as_arrow().get_array_memory_size() as u64
}

/// One-worker session capped at `cap` bytes with tables `t` and `u`
/// registered from `t`'s data.
fn capped(cfg: SessionConfig, data: &(SchemaRef, Vec<Batch>)) -> Session {
    let mut s = Session::from_config(SessionConfig {
        workers: Some(1),
        ..cfg
    })
    .unwrap();
    s.register("t", data.0.clone(), data.1.clone()).unwrap();
    s.register("u", data.0.clone(), data.1.clone()).unwrap();
    s
}

fn cap(bytes: u64) -> SessionConfig {
    SessionConfig {
        memory_limit: Some(bytes),
        ..SessionConfig::default()
    }
}

/// A small query on the same session still answers after a failure.
fn assert_still_usable(s: &Session) {
    let out = s
        .execute_sql("SELECT COUNT(*) AS c FROM t WHERE a < 10")
        .expect("follow-up query");
    let c = out[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(c, 10);
}

/// Result summary for assertion messages: row count or the error.
fn outcome(res: &mizuchi::Result<Vec<Batch>>) -> String {
    match res {
        Ok(out) => format!(
            "Ok({} rows)",
            out.iter().map(Batch::num_rows).sum::<usize>()
        ),
        Err(e) => format!("Err({e:?})"),
    }
}

fn assert_memory(sql: &str, s: &Session) {
    let res = s.execute_sql(sql);
    assert!(
        matches!(res, Err(Error::Memory(_))),
        "{}: {}",
        &sql[..sql.len().min(80)],
        outcome(&res)
    );
    assert_still_usable(s);
}

fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for e in entries.flatten() {
        let path = e.path();
        if path.is_dir() {
            out.extend(files_under(&path));
        } else {
            out.push(path);
        }
    }
    out
}

#[test]
fn aggregate_with_one_group_per_row_hits_cap() {
    let data = table(64, 8);
    let s = capped(cap(batch_bytes(&data.1) * 2), &data);
    assert_memory("SELECT a, SUM(g) AS x FROM t GROUP BY a", &s);
}

#[test]
fn aggregate_with_huge_keys_hits_cap() {
    // 64 groups, but each key is 16 KiB: per-group overhead stays tiny while
    // key bytes alone are 32x the cap.
    let data = table(1, 16 * 1024);
    let s = capped(cap(32 * 1024), &data);
    assert_memory("SELECT s, COUNT(*) AS c FROM t WHERE a < 64 GROUP BY s", &s);
}

#[test]
fn join_with_wide_build_rows_hits_cap() {
    let data = table(16, 1024);
    let s = capped(cap(batch_bytes(&data.1) * 2), &data);
    assert_memory("SELECT t.a, u.s FROM t JOIN u ON t.a = u.a", &s);
}

#[test]
fn oversized_literal_on_buffered_rows_hits_cap() {
    let data = table(1, 8);
    let s = capped(cap(batch_bytes(&data.1) * 4), &data);
    let pad = "x".repeat(64 * 1024);
    assert_memory(
        &format!("SELECT a, '{pad}' AS pad FROM t ORDER BY pad, a DESC"),
        &s,
    );
}

#[test]
fn repeated_exhaustion_returns_every_reservation() {
    let data = table(4, 8);
    let mut s = capped(cap(batch_bytes(&data.1) * 12), &data);
    let big = table(64, 8);
    s.register("big", big.0, big.1).unwrap();
    let fits = "SELECT a FROM t ORDER BY a DESC";
    s.execute_sql(fits).expect("fits under the cap");
    for _ in 0..32 {
        let res = s.execute_sql("SELECT a, s, COUNT(*) AS c FROM big GROUP BY a, s ORDER BY s");
        assert!(matches!(res, Err(Error::Memory(_))), "{}", outcome(&res));
    }
    s.execute_sql(fits)
        .expect("still fits after repeated exhaustion");
    assert_still_usable(&s);
}

/// A read root with one Parquet file inside and one sibling outside it.
struct Grant {
    _root: TempDir,
    inside: PathBuf,
    outside_file: PathBuf,
}

fn grant() -> Grant {
    let root = TempDir::new("grant");
    let inside = root.0.join("inside");
    let outside = root.0.join("outside");
    std::fs::create_dir_all(&inside).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    let (_, batches) = table(1, 8);
    let w = Session::new();
    for dir in [&inside, &outside] {
        w.write_parquet(
            dir.join("d.parquet"),
            &batches,
            ParquetWriteOptions::default(),
        )
        .unwrap();
    }
    Grant {
        outside_file: outside.join("d.parquet"),
        inside,
        _root: root,
    }
}

fn read_granted(g: &Grant) -> Session {
    let data = table(1, 8);
    capped(
        SessionConfig {
            allowed_read_paths: vec![g.inside.clone()],
            allowed_write_paths: vec![g.inside.clone()],
            ..cap(1 << 30)
        },
        &data,
    )
}

#[test]
fn table_path_traversal_out_of_read_root_is_io() {
    let g = grant();
    let mut s = read_granted(&g);
    let escape = g.inside.join("..").join("outside").join("d.parquet");
    let res = s.register_parquet("x", &escape, ParquetReadOptions::default());
    assert!(matches!(res, Err(Error::Io(_))), "{res:?}");
    assert!(
        s.execute_sql("SELECT * FROM x").is_err(),
        "x was registered"
    );
    s.register_parquet(
        "y",
        g.inside.join("d.parquet"),
        ParquetReadOptions::default(),
    )
    .expect("granted read");
    assert_still_usable(&s);
}

#[cfg(unix)]
#[test]
fn table_path_symlink_out_of_read_root_is_io() {
    let g = grant();
    let link = g.inside.join("link.parquet");
    std::os::unix::fs::symlink(&g.outside_file, &link).unwrap();
    let dir_link = g.inside.join("linkdir");
    std::os::unix::fs::symlink(g.outside_file.parent().unwrap(), &dir_link).unwrap();
    let mut s = read_granted(&g);
    for path in [link, dir_link.join("d.parquet")] {
        let res = s.register_parquet("x", &path, ParquetReadOptions::default());
        assert!(
            matches!(res, Err(Error::Io(_))),
            "{}: {res:?}",
            path.display()
        );
    }
    assert_still_usable(&s);
}

#[test]
fn spill_dir_outside_write_root_is_refused() {
    let root = TempDir::new("spillgrant");
    let granted = root.0.join("granted");
    let outside = root.0.join("outside");
    std::fs::create_dir_all(&granted).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    let mut escapes = vec![outside.clone(), granted.join("..").join("outside")];
    #[cfg(unix)]
    {
        // `link/..` is `outside/` on disk even though it reads as `granted/`.
        std::fs::create_dir_all(outside.join("child")).unwrap();
        std::os::unix::fs::symlink(outside.join("child"), granted.join("link")).unwrap();
        escapes.push(granted.join("link").join("..").join("spill"));
    }
    for spill in escapes {
        let res = Session::from_config(SessionConfig {
            memory_limit: Some(1 << 20),
            spill_dir: Some(spill.clone()),
            allowed_read_paths: vec![granted.clone()],
            allowed_write_paths: vec![granted.clone()],
            ..SessionConfig::default()
        });
        assert!(
            matches!(res, Err(Error::Io(_))),
            "{}: {:?}",
            spill.display(),
            res.err()
        );
    }
    let written: Vec<_> = std::fs::read_dir(&outside)
        .unwrap()
        .flatten()
        .map(|e| e.file_name())
        .filter(|name| name != "child")
        .collect();
    assert!(written.is_empty(), "{written:?}");
}

/// Scan that cancels its own query, through the embedder's query log, once
/// the sort above it has written a spill file.
struct CancelOnSpill {
    inner: Box<dyn Operator>,
    spill: PathBuf,
    log: Arc<QueryLog>,
    armed: Arc<AtomicBool>,
}

impl Operator for CancelOnSpill {
    fn next_batch(&mut self, cancel: &CancelToken) -> mizuchi::Result<Option<Batch>> {
        if self.armed.load(Ordering::SeqCst) && !files_under(&self.spill).is_empty() {
            self.armed.store(false, Ordering::SeqCst);
            for q in self.log.running().unwrap() {
                assert!(self.log.cancel(&q.id).unwrap());
            }
        }
        self.inner.next_batch(cancel)
    }
}

struct CancelOnSpillLowering {
    spill: PathBuf,
    log: Arc<QueryLog>,
    armed: Arc<AtomicBool>,
}

impl Lowering for CancelOnSpillLowering {
    fn scan(&self, batches: Vec<Batch>) -> mizuchi::Result<Box<dyn Operator>> {
        Ok(Box::new(CancelOnSpill {
            inner: Lowering::scan(&CpuLowering, batches)?,
            spill: self.spill.clone(),
            log: self.log.clone(),
            armed: self.armed.clone(),
        }))
    }
}

#[test]
fn cancel_during_spill_removes_spill_files_and_keeps_session() {
    let spill = TempDir::new("cancelspill");
    let data = table(64, 8);
    let log = Arc::new(QueryLog::memory(QueryRetention::default()));
    let armed = Arc::new(AtomicBool::new(true));
    let s = capped(
        SessionConfig {
            spill_dir: Some(spill.0.clone()),
            allowed_write_paths: vec![spill.0.clone()],
            ..cap(batch_bytes(&data.1) * 3 / 2)
        },
        &data,
    )
    .with_query_log(log.clone())
    .with_lowering(Arc::new(CancelOnSpillLowering {
        spill: spill.0.clone(),
        log,
        armed: armed.clone(),
    }));
    let res = s.execute_sql("SELECT a FROM t ORDER BY a DESC");
    assert!(!armed.load(Ordering::SeqCst), "the sort never spilled");
    assert!(matches!(res, Err(Error::Cancelled)), "{}", outcome(&res));
    assert_eq!(files_under(&spill.0), Vec::<PathBuf>::new());
    let out = s.execute_sql("SELECT a FROM t ORDER BY a DESC").unwrap();
    assert_eq!(out.iter().map(Batch::num_rows).sum::<usize>(), 64 * ROWS);
    assert_eq!(files_under(&spill.0), Vec::<PathBuf>::new());
    assert_still_usable(&s);
}

#[test]
fn executed_parse_pathology_fails_typed() {
    let data = table(1, 8);
    let s = capped(cap(batch_bytes(&data.1) * 64), &data);
    let n = 20_000;
    let cases = [
        format!("SELECT {}1{} AS x", "(".repeat(n), ")".repeat(n)),
        format!("SELECT {} AS x FROM t", vec!["a"; n].join(" + ")),
        format!("SELECT a FROM t WHERE {}", vec!["a = 1"; n].join(" OR ")),
        format!("SELECT a FROM t WHERE {}a > 0", "NOT ".repeat(n)),
        format!(
            "SELECT {} FROM t",
            (0..n)
                .map(|i| format!("a AS c{i}"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        format!(
            "SELECT {}a{} AS x FROM t",
            "CASE WHEN a > 0 THEN ".repeat(n / 10),
            " END".repeat(n / 10)
        ),
        format!(
            "SELECT a FROM {}t{}",
            "(SELECT a FROM ".repeat(n / 10),
            ")".repeat(n / 10)
        ),
        format!("SELECT '{}' AS x", "\u{1F600}".repeat(1 << 20)),
        // Deep enough that dropping the tree on a 2 MiB stack would abort,
        // whether it is rejected for depth or by a later syntax error.
        format!("SELECT {} AS x FROM t", vec!["a"; 10 * n].join(" + ")),
        format!("SELECT {} AS x FROM )", vec!["a"; 10 * n].join(" + ")),
    ];
    for sql in &cases {
        let res = s.execute_sql(sql);
        assert!(
            matches!(
                res,
                Ok(_) | Err(Error::Parse(_) | Error::Plan(_) | Error::Memory(_))
            ),
            "{}: {}",
            &sql[..60],
            outcome(&res)
        );
    }
    assert_still_usable(&s);
}

/// Just under the nesting limit, chains the parser builds without its own
/// recursion limit still run; the limit sits below the depth that would
/// overflow a 2 MiB thread stack.
#[test]
fn nesting_just_under_the_limit_executes() {
    let data = table(1, 8);
    let s = capped(cap(batch_bytes(&data.1) * 64), &data);
    let d = mizuchi::sql::MAX_EXPR_DEPTH - 2;
    let in_list = (0..20_000).map(|i| i.to_string()).collect::<Vec<_>>();
    let cases = [
        format!("SELECT {} AS x FROM t", vec!["a"; d].join(" + ")),
        format!("SELECT a FROM t WHERE {}", vec!["a = 1"; d].join(" OR ")),
        format!("SELECT a FROM t WHERE {}", vec!["a >= 0"; d].join(" AND ")),
        format!("SELECT a FROM t WHERE a IN ({})", in_list.join(",")),
    ];
    for sql in &cases {
        let res = s.execute_sql(sql);
        assert!(res.is_ok(), "{}: {}", &sql[..60], outcome(&res));
    }
}
