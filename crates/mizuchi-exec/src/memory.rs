//! Per-query memory pool and Arrow IPC spill files.
//!
//! Operators reserve working-set bytes before they grow. When a reservation
//! cannot fit and a spill directory was given, they write Arrow IPC runs and
//! release the reservation. Without a directory, the engine fails closed.

use std::fs::{self, File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;

use mizuchi_core::batch::Batch;
use mizuchi_core::error::{Error, Result};

/// Bytes held by `batch` according to Arrow's array size accounting.
pub(super) fn batch_bytes(batch: &Batch) -> u64 {
    batch.as_arrow().get_array_memory_size() as u64
}

const BUDGET_MIN: u64 = 64 * 1024 * 1024;
const BUDGET_MAX: u64 = 256 * 1024 * 1024 * 1024;

/// One quarter of machine memory, clamped to 64 MiB .. 256 GiB.
///
/// Embedders pass this to [`super::Session::with_memory_limit`] when they
/// want a default per-query cap. `Session::new` stays unbounded so runners
/// keep policy.
pub fn system_budget() -> u64 {
    match total_memory() {
        Some(total) => (total / 4).clamp(BUDGET_MIN, BUDGET_MAX),
        None => BUDGET_MIN * 4,
    }
}

fn total_memory() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let text = std::fs::read_to_string("/proc/meminfo").ok()?;
        for line in text.lines() {
            let rest = line.strip_prefix("MemTotal:")?;
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb.saturating_mul(1024));
        }
        None
    }
    #[cfg(target_os = "macos")]
    {
        let mut val: u64 = 0;
        let mut len = std::mem::size_of::<u64>();
        // SAFETY: sysctlbyname with hw.memsize writes a u64 into `val`.
        let rc = unsafe {
            sysctlbyname(
                c"hw.memsize".as_ptr(),
                (&raw mut val).cast(),
                &mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        if rc == 0 { Some(val) } else { None }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn sysctlbyname(
        name: *const core::ffi::c_char,
        oldp: *mut core::ffi::c_void,
        oldlenp: *mut usize,
        newp: *mut core::ffi::c_void,
        newlen: usize,
    ) -> i32;
}

/// Shared owner budget. Concurrent sessions of one owner draw from this cap.
#[derive(Debug)]
pub(crate) struct Budget {
    pub limit: u64,
    pub used: Arc<AtomicU64>,
}

/// Query-scoped pool. `used` is the live reservation; `peak` is its high water.
pub(crate) struct MemoryPool {
    limit: Option<u64>,
    used: Arc<AtomicU64>,
    peak: AtomicU64,
    copied: Arc<AtomicU64>,
    spilled: AtomicU64,
    spill_dir: Option<PathBuf>,
    seq: AtomicU64,
}

static JOB_SEQ: AtomicU64 = AtomicU64::new(1);

fn mkdir_private(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new().mode(0o700).create(path)
    }
    #[cfg(not(unix))]
    {
        fs::create_dir(path)
    }
}

fn create_private_file(path: &Path) -> std::io::Result<File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
    }
    #[cfg(not(unix))]
    {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
    }
}

fn exclusive_private_dir(parent: &Path, prefix: &str) -> Result<PathBuf> {
    fs::create_dir_all(parent).map_err(|e| Error::Io(e.to_string()))?;
    let pid = std::process::id();
    loop {
        let dir = parent.join(format!(
            "{}-{}-{}",
            prefix,
            pid,
            JOB_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        match mkdir_private(&dir) {
            Ok(()) => return Ok(dir),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(Error::Io(e.to_string())),
        }
    }
}

fn exclusive_job_dir(root: &Path) -> Result<PathBuf> {
    exclusive_private_dir(root, "mem")
}

/// Unique owner-only directory under the process temp dir.
pub(super) fn private_spill_root() -> Result<PathBuf> {
    exclusive_private_dir(&std::env::temp_dir(), "mizuchi-spill")
}

impl MemoryPool {
    pub(super) fn new(limit: Option<u64>, spill_dir: Option<PathBuf>) -> Result<Self> {
        Self::from_parts(limit, Arc::new(AtomicU64::new(0)), spill_dir)
    }

    /// Draw reservations from an owner budget shared by concurrent sessions.
    pub(super) fn with_budget(budget: Arc<Budget>, spill_dir: Option<PathBuf>) -> Result<Self> {
        Self::from_parts(Some(budget.limit), budget.used.clone(), spill_dir)
    }

    fn from_parts(
        limit: Option<u64>,
        used: Arc<AtomicU64>,
        spill_dir: Option<PathBuf>,
    ) -> Result<Self> {
        let spill_dir = match spill_dir {
            Some(root) => Some(exclusive_job_dir(&root)?),
            None => None,
        };
        Ok(Self {
            limit,
            used,
            peak: AtomicU64::new(0),
            copied: Arc::new(AtomicU64::new(0)),
            spilled: AtomicU64::new(0),
            spill_dir,
            seq: AtomicU64::new(1),
        })
    }

    pub(super) fn can_spill(&self) -> bool {
        self.spill_dir.is_some()
    }

    pub(super) fn has_limit(&self) -> bool {
        self.limit.is_some()
    }

    pub(super) fn limit(&self) -> Option<u64> {
        self.limit
    }

    fn add_used(&self, bytes: u64) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        if let Some(limit) = self.limit {
            let mut current = self.used.load(Ordering::SeqCst);
            loop {
                let next = current.saturating_add(bytes);
                if next > limit {
                    return Err(Error::Memory(format!(
                        "reservation {bytes} would use {next} of limit {limit}"
                    )));
                }
                match self
                    .used
                    .compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst)
                {
                    Ok(_) => break,
                    Err(seen) => current = seen,
                }
            }
        } else {
            self.used.fetch_add(bytes, Ordering::SeqCst);
        }
        let live = self.used.load(Ordering::SeqCst);
        self.peak.fetch_max(live, Ordering::Relaxed);
        Ok(())
    }

    pub(super) fn try_reserve(&self, bytes: u64) -> Result<Reservation<'_>> {
        self.add_used(bytes)?;
        Ok(Reservation { pool: self, bytes })
    }

    pub(super) fn used_handle(&self) -> Arc<AtomicU64> {
        self.used.clone()
    }

    pub(super) fn peak(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    pub(super) fn record_copy(&self, bytes: u64) {
        if bytes > 0 {
            self.copied.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    pub(super) fn copied(&self) -> u64 {
        self.copied.load(Ordering::Relaxed)
    }

    pub(super) fn copied_handle(&self) -> Arc<AtomicU64> {
        self.copied.clone()
    }

    pub(super) fn spilled(&self) -> u64 {
        self.spilled.load(Ordering::Relaxed)
    }

    pub(super) fn write_spill_file(&self, schema: SchemaRef, batches: &[Batch]) -> Result<PathBuf> {
        let path = self.spill_path()?;
        write_spill(&path, schema, batches)?;
        if let Ok(meta) = fs::metadata(&path) {
            self.spilled.fetch_add(meta.len(), Ordering::Relaxed);
        }
        Ok(path)
    }

    pub(super) fn spill_path(&self) -> Result<PathBuf> {
        let dir = self
            .spill_dir
            .as_ref()
            .ok_or_else(|| Error::Memory("spill requested without a spill directory".into()))?;
        let n = self.seq.fetch_add(1, Ordering::Relaxed);
        Ok(dir.join(format!("spill-{n}.arrow")))
    }

    fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.used.fetch_sub(bytes, Ordering::SeqCst);
    }
}

impl Drop for MemoryPool {
    fn drop(&mut self) {
        if let Some(dir) = &self.spill_dir {
            let _ = fs::remove_dir_all(dir);
        }
    }
}

/// Live reservation. Dropping it returns the bytes to the pool.
pub(super) struct Reservation<'a> {
    pool: &'a MemoryPool,
    bytes: u64,
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        self.pool.release(self.bytes);
    }
}

fn mkdir_private_all(path: &Path) -> std::io::Result<()> {
    if path.as_os_str().is_empty() || path == Path::new(".") {
        return Ok(());
    }
    if path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        mkdir_private_all(parent)?;
    }
    match mkdir_private(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e),
    }
}

pub fn write_spill(path: &Path, schema: SchemaRef, batches: &[Batch]) -> Result<()> {
    if let Some(parent) = path.parent() {
        mkdir_private_all(parent).map_err(|e| Error::Io(e.to_string()))?;
    }
    let file = create_private_file(path).map_err(|e| Error::Io(e.to_string()))?;
    let mut writer =
        FileWriter::try_new(file, schema.as_ref()).map_err(|e| Error::Io(e.to_string()))?;
    for batch in batches {
        if batch.num_rows() > 0 {
            writer
                .write(batch.as_arrow())
                .map_err(|e| Error::Io(e.to_string()))?;
        }
    }
    writer.finish().map_err(|e| Error::Io(e.to_string()))?;
    Ok(())
}

pub fn read_spill(path: &Path, schema: SchemaRef) -> Result<Vec<Batch>> {
    let mut out = Vec::new();
    for batch in spill_batches(path)? {
        out.push(batch?);
    }
    if out.is_empty() {
        out.push(Batch::empty(schema)?);
    }
    Ok(out)
}

/// One Arrow IPC record at a time so a multi-batch spill file is not all live.
pub(super) fn spill_batches(path: &Path) -> Result<SpillIter> {
    if !path.exists() {
        return Ok(SpillIter { reader: None });
    }
    let file = File::open(path).map_err(|e| Error::Io(e.to_string()))?;
    let reader = FileReader::try_new(file, None).map_err(|e| Error::Io(e.to_string()))?;
    Ok(SpillIter {
        reader: Some(reader),
    })
}

pub(super) struct SpillIter {
    reader: Option<FileReader<File>>,
}

impl Iterator for SpillIter {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        let reader = self.reader.as_mut()?;
        loop {
            match reader.next() {
                None => return None,
                Some(Err(e)) => return Some(Err(Error::Io(e.to_string()))),
                Some(Ok(batch)) if batch.num_rows() == 0 => continue,
                Some(Ok(batch)) => return Some(Batch::from_arrow(batch)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_fails_closed_without_room() {
        let pool = MemoryPool::new(Some(10), None).unwrap();
        assert!(pool.try_reserve(11).is_err());
        let held = pool.try_reserve(10).unwrap();
        assert!(pool.try_reserve(1).is_err());
        drop(held);
        assert!(pool.try_reserve(10).is_ok());
    }

    #[test]
    fn job_subdir_removed_on_drop() {
        let root = std::env::temp_dir().join(format!(
            "mizuchi-pool-{}-{}",
            std::process::id(),
            JOB_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        {
            let pool = MemoryPool::new(Some(1), Some(root.clone())).unwrap();
            let path = pool.spill_path().unwrap();
            fs::write(&path, b"x").unwrap();
            assert!(path.exists());
        }
        let leftover: Vec<_> = fs::read_dir(&root)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        let _ = fs::remove_dir_all(&root);
        assert!(leftover.is_empty(), "{leftover:?}");
    }

    #[test]
    fn job_dirs_are_exclusive_under_one_root() {
        let root = std::env::temp_dir().join(format!(
            "mizuchi-pool-excl-{}-{}",
            std::process::id(),
            JOB_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        let a = MemoryPool::new(None, Some(root.clone())).unwrap();
        let b = MemoryPool::new(None, Some(root.clone())).unwrap();
        let pa = a.spill_path().unwrap();
        let pb = b.spill_path().unwrap();
        assert_ne!(pa.parent(), pb.parent());
        drop(a);
        drop(b);
        let _ = fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[test]
    fn spill_dir_and_files_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!(
            "mizuchi-pool-mode-{}-{}",
            std::process::id(),
            JOB_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        let pool = MemoryPool::new(None, Some(root.clone())).unwrap();
        let path = pool.spill_path().unwrap();
        let job = path.parent().unwrap();
        let dir_mode = fs::metadata(job).unwrap().permissions().mode() & 0o777;
        assert_eq!(dir_mode, 0o700, "{job:?} mode {dir_mode:o}");
        write_spill(&path, Arc::new(arrow::datatypes::Schema::empty()), &[]).unwrap();
        let file_mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(file_mode, 0o600, "{path:?} mode {file_mode:o}");
        drop(pool);
        let _ = fs::remove_dir_all(&root);
    }
}
