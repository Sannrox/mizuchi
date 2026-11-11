//! A Parquet table larger than the session cap stays out of memory: the
//! process heap grows by at most the cap plus [`MARGIN`] across registration
//! and the queries (scheduled and instrumented), and the engine's own peak
//! stays at or below the cap.
//!
//! This binary installs a counting global allocator, so it holds exactly one
//! test; a second test running alongside would count into the same heap.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use mizuchi::{ParquetReadOptions, QueryLog, QueryRetention, Session, SessionConfig};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

// SAFETY: forwards to `System` and only adds counters.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new) };
        if !p.is_null() {
            let live = if new >= layout.size() {
                LIVE.fetch_add(new - layout.size(), Ordering::Relaxed) + (new - layout.size())
            } else {
                LIVE.fetch_sub(layout.size() - new, Ordering::Relaxed) - (layout.size() - new)
            };
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        p
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

const CAP: u64 = 16 << 20;
/// Allowance above the cap: the morsel each worker decodes but the pool does
/// not hold, Parquet page and footer buffers, and the result batches.
const MARGIN: u64 = 16 << 20;
const ROWS: i64 = 1 << 20;
const COLS: usize = 8;

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(1);
        let path = std::env::temp_dir().join(format!(
            "mizuchi-input-memory-{}-{}",
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

/// `ROWS` x `COLS` Int64 (64 MiB decoded, 4x the cap). Column `k` has eight
/// groups; `c1..` are wide filler a narrow query must not decode.
fn write_table(path: &std::path::Path) {
    let mut fields = vec![Field::new("k", DataType::Int64, false)];
    fields.extend((1..COLS).map(|i| Field::new(format!("c{i}"), DataType::Int64, false)));
    let schema = Arc::new(Schema::new(fields));
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(64 * 1024))
        .build();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    let chunk = 64 * 1024;
    for start in (0..ROWS).step_by(chunk) {
        let ids: Vec<i64> = (start..start + chunk as i64).collect();
        let mut cols: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(
            ids.iter().map(|i| i % 8),
        ))];
        cols.extend((1..COLS).map(|c| {
            Arc::new(Int64Array::from_iter_values(
                ids.iter().map(|i| i * c as i64),
            )) as ArrayRef
        }));
        writer
            .write(&RecordBatch::try_new(schema.clone(), cols).unwrap())
            .unwrap();
    }
    writer.close().unwrap();
}

#[test]
fn parquet_input_larger_than_the_cap_stays_within_cap_plus_margin() {
    let dir = TempDir::new();
    let file = dir.0.join("t.parquet");
    write_table(&file);
    let mut s = Session::from_config(SessionConfig {
        memory_limit: Some(CAP),
        workers: Some(2),
        spill_dir: Some(dir.0.join("spill")),
        allowed_read_paths: vec![dir.0.clone()],
        allowed_write_paths: vec![dir.0.join("spill")],
        ..SessionConfig::default()
    })
    .unwrap();

    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    s.register_parquet("t", &file, ParquetReadOptions::default())
        .unwrap();
    let narrow = s
        .execute_sql_reported("SELECT k, SUM(c1) AS s FROM t GROUP BY k ORDER BY k")
        .unwrap();
    let wide = s
        .execute_sql_reported("SELECT COUNT(*) AS n FROM t WHERE c7 >= 7000000 AND c5 > 0")
        .unwrap();
    // A query log routes execution through the instrumented operator tree.
    let s = s.with_query_log(Arc::new(QueryLog::memory(QueryRetention::default())));
    let logged = s
        .execute_sql_reported("SELECT k, SUM(c2) AS s FROM t GROUP BY k")
        .unwrap();
    let grown = (PEAK.load(Ordering::Relaxed) - base) as u64;

    let rows: usize = narrow.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 8);
    let n = wide.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, ROWS - 1_000_000);
    assert_eq!(
        logged.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        8
    );
    for result in [&narrow, &wide, &logged] {
        assert!(
            result.peak_bytes <= CAP,
            "engine peak {}",
            result.peak_bytes
        );
    }
    assert!(
        grown <= CAP + MARGIN,
        "heap grew {} MiB, cap {} MiB + margin {} MiB",
        grown >> 20,
        CAP >> 20,
        MARGIN >> 20
    );
}
