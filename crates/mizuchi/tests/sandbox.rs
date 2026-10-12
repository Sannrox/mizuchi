//! Generic sandbox contract: the engine honors `SessionConfig` and returns
//! typed errors. OS limits belong to the embedding runner; this crate has no
//! sandbox backend.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::Int64Array;
use mizuchi::{
    Batch, DataType, Error, Field, ParquetReadOptions, ParquetWriteOptions, RecordBatch, Schema,
    SchemaRef, Session, SessionConfig,
};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
}

fn numbered(n: i64) -> (SchemaRef, Vec<Batch>) {
    let schema = schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(
            (0..n).map(Some).collect::<Vec<_>>(),
        ))],
    )
    .unwrap();
    (schema, vec![Batch::from_arrow(batch).unwrap()])
}

fn numbered_batches(n_batches: usize, rows: usize) -> (SchemaRef, Vec<Batch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("g", DataType::Int64, true),
    ]));
    let mut batches = Vec::new();
    let mut k = 0i64;
    for _ in 0..n_batches {
        let mut a = Vec::with_capacity(rows);
        let mut g = Vec::with_capacity(rows);
        for _ in 0..rows {
            a.push(Some(k));
            g.push(Some(k.rem_euclid(7)));
            k += 1;
        }
        batches.push(
            Batch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(a)), Arc::new(Int64Array::from(g))],
            )
            .unwrap(),
        );
    }
    (schema, batches)
}

fn tmp(label: &str) -> std::path::PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(1);
    std::env::temp_dir().join(format!(
        "mizuchi-sandbox-{}-{}-{}",
        label,
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ))
}

fn leftover(dir: &std::path::Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect()
}

fn root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    std::fs::read_to_string(root().join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))
}

fn walk_rs(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            walk_rs(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

#[test]
fn path_allow_list_deny_is_io() {
    let allowed = tmp("ok");
    let denied = tmp("no");
    std::fs::create_dir_all(&allowed).unwrap();
    std::fs::create_dir_all(&denied).unwrap();
    let (schema, batches) = numbered(4);
    let mut open = Session::new();
    open.register("t", schema.clone(), batches.clone()).unwrap();
    open.write_parquet(
        denied.join("data"),
        &batches,
        ParquetWriteOptions::default(),
    )
    .unwrap();

    let mut s = Session::from_config(SessionConfig {
        allowed_read_paths: vec![allowed.clone()],
        allowed_write_paths: vec![allowed.clone()],
        ..SessionConfig::default()
    })
    .unwrap();
    let read_err = s.register_parquet("x", denied.join("data"), ParquetReadOptions::default());
    assert!(matches!(read_err, Err(Error::Io(_))), "{read_err:?}");
    let write_err = s.write_parquet(denied.join("out"), &batches, ParquetWriteOptions::default());
    assert!(matches!(write_err, Err(Error::Io(_))), "{write_err:?}");
    let _ = std::fs::remove_dir_all(&allowed);
    let _ = std::fs::remove_dir_all(&denied);
}

#[cfg(unix)]
#[test]
fn os_denied_read_is_io() {
    use std::os::unix::fs::PermissionsExt;

    let root = tmp("chmod");
    std::fs::create_dir_all(&root).unwrap();
    let (schema, batches) = numbered(4);
    let mut s = Session::new();
    s.register("t", schema, batches.clone()).unwrap();
    let data = root.join("data");
    s.write_parquet(&data, &batches, ParquetWriteOptions::default())
        .unwrap();
    let mut perms = std::fs::metadata(&data).unwrap().permissions();
    let old = perms.mode();
    perms.set_mode(0o000);
    std::fs::set_permissions(&data, perms.clone()).unwrap();
    let err = s.register_parquet("x", &data, ParquetReadOptions::default());
    perms.set_mode(old);
    let _ = std::fs::set_permissions(&data, perms);
    let _ = std::fs::remove_dir_all(&root);
    assert!(matches!(err, Err(Error::Io(_))), "{err:?}");
}

#[test]
fn spill_stays_in_configured_dir() {
    let (schema, batches) = numbered_batches(12, 32);
    let cap = {
        let sz = batches[0].as_arrow().get_array_memory_size() as u64;
        sz + sz / 2
    };
    let spill = tmp("spill");
    let other = tmp("other");
    std::fs::create_dir_all(&spill).unwrap();
    std::fs::create_dir_all(&other).unwrap();
    let mut s = Session::from_config(SessionConfig {
        memory_limit: Some(cap),
        workers: Some(1),
        spill_dir: Some(spill.clone()),
        allowed_read_paths: vec![spill.clone(), other.clone()],
        allowed_write_paths: vec![spill.clone(), other.clone()],
        ..SessionConfig::default()
    })
    .unwrap();
    s.register("t", schema, batches).unwrap();
    let out = s.execute_sql("SELECT a FROM t ORDER BY a").unwrap();
    assert!(!out.is_empty());
    let spill_left = leftover(&spill);
    let other_left = leftover(&other);
    let _ = std::fs::remove_dir_all(&spill);
    let _ = std::fs::remove_dir_all(&other);
    assert!(spill_left.is_empty(), "{spill_left:?}");
    assert!(other_left.is_empty(), "{other_left:?}");
}

#[test]
fn memory_cap_without_spill_is_memory() {
    let (schema, batches) = numbered(64);
    let mut s = Session::from_config(SessionConfig {
        memory_limit: Some(64),
        workers: Some(1),
        ..SessionConfig::default()
    })
    .unwrap();
    s.register("t", schema, batches).unwrap();
    let err = s.execute_sql("SELECT a FROM t ORDER BY a");
    assert!(matches!(err, Err(Error::Memory(_))), "{err:?}");
}

#[test]
fn crate_sources_have_no_sandbox_backend() {
    let crates = root().join("crates");
    let mut files = Vec::new();
    for dir in std::fs::read_dir(&crates).unwrap() {
        let src = dir.unwrap().path().join("src");
        if src.is_dir() {
            walk_rs(&src, &mut files);
        }
    }
    assert!(!files.is_empty());
    for path in files {
        let text = std::fs::read_to_string(&path).unwrap();
        for needle in ["landlock", "seccomp", "cgroup", "prctl", "SYS_landlock"] {
            assert!(
                !text.to_ascii_lowercase().contains(needle),
                "{} names {needle}",
                path.display()
            );
        }
    }
}

#[test]
fn optional_workflow_is_dispatch_only() {
    for rel in [
        "crates/mizuchi/tests/sandbox.rs",
        "crates/mizuchi/tests/sandbox_os.rs",
        ".github/workflows/sandbox.yml",
    ] {
        assert!(root().join(rel).is_file(), "missing {rel}");
    }
    let yml = read(".github/workflows/sandbox.yml");
    assert!(yml.contains("workflow_dispatch"));
    assert!(
        !yml.contains("pull_request"),
        "sandbox workflow must not run on pull_request"
    );
    assert!(yml.contains("--test sandbox"));
    assert!(yml.contains("--features sandbox"));
    let cargo = read("crates/mizuchi/Cargo.toml");
    assert!(cargo.contains("sandbox = []"));
    assert!(cargo.contains("landlock"));
}
