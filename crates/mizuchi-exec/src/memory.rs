//! Per-query memory pool and Arrow IPC spill files.
//!
//! Operators reserve working-set bytes before they grow. When a reservation
//! cannot fit and a spill directory was given, they write Arrow IPC runs and
//! release the reservation. Without a directory, the engine fails closed.

use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, ErrorKind, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;

use mizuchi_core::batch::Batch;
use mizuchi_core::error::{Error, Result};
use mizuchi_io::crypto::{
    SPILL_PLAINTEXT_MAX, SpillHeader, SpillKey, append_spill_frame, is_sealed_spill,
    read_spill_frame, read_spill_header, write_spill_header,
};

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

#[cfg(any(target_os = "linux", test))]
fn parse_memtotal(text: &str) -> Option<u64> {
    for line in text.lines() {
        let Some(rest) = line.strip_prefix("MemTotal:") else {
            continue;
        };
        let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
        return Some(kb.saturating_mul(1024));
    }
    None
}

fn total_memory() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        parse_memtotal(&std::fs::read_to_string("/proc/meminfo").ok()?)
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
    spill_key: Option<Arc<SpillKey>>,
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
            spill_key: None,
        })
    }

    pub(super) fn set_spill_key(&mut self, key: SpillKey) {
        self.spill_key = Some(Arc::new(key));
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
        write_spill_with(&path, schema, batches, self.spill_key.as_deref())?;
        if let Ok(meta) = fs::metadata(&path) {
            self.spilled.fetch_add(meta.len(), Ordering::Relaxed);
        }
        Ok(path)
    }

    pub(super) fn read_spill(&self, path: &Path, schema: SchemaRef) -> Result<Vec<Batch>> {
        read_spill_with(path, schema, self.spill_key.clone())
    }

    pub(super) fn spill_iter(&self, path: &Path) -> Result<SpillIter> {
        spill_batches_with(path, self.spill_key.clone())
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

fn ipc_bytes(schema: &arrow::datatypes::Schema, batch: &Batch) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer = FileWriter::try_new(&mut buf, schema).map_err(|e| Error::Io(e.to_string()))?;
    writer
        .write(batch.as_arrow())
        .map_err(|e| Error::Io(e.to_string()))?;
    writer.finish().map_err(|e| Error::Io(e.to_string()))?;
    Ok(buf)
}

/// Header AAD binds `frame_count`, so count first and drop IPC, then encode
/// one frame at a time. Window size is computed from the original batch so
/// zero-copy slices do not re-count the parent buffer and collapse to one
/// row per frame.
fn count_spill_frames_limited(
    schema: &arrow::datatypes::Schema,
    batch: &Batch,
    max: usize,
) -> Result<u32> {
    fold_spill_windows(batch, max, |part| count_ipc_frames(schema, part, max))
}

fn append_spill_frames_limited<W: std::io::Write>(
    out: &mut W,
    key: &SpillKey,
    schema: &arrow::datatypes::Schema,
    batch: &Batch,
    header: &SpillHeader,
    index: &mut u32,
    max: usize,
) -> Result<()> {
    fold_spill_windows(batch, max, |part| {
        append_ipc_frames(out, key, schema, part, header, index, max)?;
        Ok(0)
    })?;
    Ok(())
}

fn fold_spill_windows(
    batch: &Batch,
    max: usize,
    mut each: impl FnMut(&Batch) -> Result<u32>,
) -> Result<u32> {
    let rows = batch.num_rows();
    if rows == 0 {
        return Ok(0);
    }
    let chunk = spill_row_chunk(batch, max);
    if chunk >= rows {
        return each(batch);
    }
    let mut n = 0u32;
    let mut off = 0usize;
    while off < rows {
        let len = chunk.min(rows - off);
        n = n
            .checked_add(each(&batch.slice(off, len)?)?)
            .ok_or_else(|| Error::Io("too many spill frames".into()))?;
        off += len;
    }
    Ok(n)
}

fn spill_row_chunk(batch: &Batch, max: usize) -> usize {
    let rows = batch.num_rows();
    if rows <= 1 {
        return rows.max(1);
    }
    let bytes = batch.as_arrow().get_array_memory_size().max(1);
    let per_row = bytes.div_ceil(rows).max(1);
    (max / per_row).max(1).min(rows)
}

fn count_ipc_frames(schema: &arrow::datatypes::Schema, batch: &Batch, max: usize) -> Result<u32> {
    let buf = ipc_bytes(schema, batch)?;
    if buf.len() <= max {
        return Ok(1);
    }
    drop(buf);
    split_spill_batch(batch, |left, right| {
        let n = count_ipc_frames(schema, left, max)?;
        n.checked_add(count_ipc_frames(schema, right, max)?)
            .ok_or_else(|| Error::Io("too many spill frames".into()))
    })
}

fn append_ipc_frames<W: std::io::Write>(
    out: &mut W,
    key: &SpillKey,
    schema: &arrow::datatypes::Schema,
    batch: &Batch,
    header: &SpillHeader,
    index: &mut u32,
    max: usize,
) -> Result<()> {
    let buf = ipc_bytes(schema, batch)?;
    if buf.len() <= max {
        append_spill_frame(out, key, &buf, header, *index)?;
        *index = index
            .checked_add(1)
            .ok_or_else(|| Error::Io("too many spill frames".into()))?;
        return Ok(());
    }
    drop(buf);
    split_spill_batch(batch, |left, right| {
        append_ipc_frames(out, key, schema, left, header, index, max)?;
        append_ipc_frames(out, key, schema, right, header, index, max)
    })
}

fn split_spill_batch<T>(
    batch: &Batch,
    then: impl FnOnce(&Batch, &Batch) -> Result<T>,
) -> Result<T> {
    if batch.num_rows() == 1 {
        return Err(Error::Io("spill frame too large".into()));
    }
    let mid = batch.num_rows() / 2;
    then(
        &batch.slice(0, mid)?,
        &batch.slice(mid, batch.num_rows() - mid)?,
    )
}

pub fn write_spill(path: &Path, schema: SchemaRef, batches: &[Batch]) -> Result<()> {
    write_spill_with(path, schema, batches, None)
}

fn write_spill_with(
    path: &Path,
    schema: SchemaRef,
    batches: &[Batch],
    key: Option<&SpillKey>,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        mkdir_private_all(parent).map_err(|e| Error::Io(e.to_string()))?;
    }
    match key {
        Some(key) => {
            let mut frames = 0u32;
            for batch in batches {
                frames = frames
                    .checked_add(count_spill_frames_limited(
                        schema.as_ref(),
                        batch,
                        SPILL_PLAINTEXT_MAX,
                    )?)
                    .ok_or_else(|| Error::Io("too many spill frames".into()))?;
            }
            let mut file = create_private_file(path).map_err(|e| Error::Io(e.to_string()))?;
            let header = write_spill_header(&mut file, key, frames)?;
            let mut index = 0u32;
            for batch in batches {
                append_spill_frames_limited(
                    &mut file,
                    key,
                    schema.as_ref(),
                    batch,
                    &header,
                    &mut index,
                    SPILL_PLAINTEXT_MAX,
                )?;
            }
            Ok(())
        }
        None => {
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
    }
}

pub fn read_spill(path: &Path, schema: SchemaRef) -> Result<Vec<Batch>> {
    read_spill_with(path, schema, None)
}

fn read_spill_with(
    path: &Path,
    schema: SchemaRef,
    key: Option<Arc<SpillKey>>,
) -> Result<Vec<Batch>> {
    let mut out = Vec::new();
    for batch in spill_batches_with(path, key)? {
        out.push(batch?);
    }
    if out.is_empty() {
        out.push(Batch::empty(schema)?);
    }
    Ok(out)
}

/// One Arrow IPC record at a time so a multi-batch spill file is not all live.
fn spill_batches_with(path: &Path, key: Option<Arc<SpillKey>>) -> Result<SpillIter> {
    if !path.exists() {
        return Ok(SpillIter {
            file: None,
            sealed: None,
        });
    }
    let mut file = File::open(path).map_err(|e| Error::Io(e.to_string()))?;
    let mut magic = [0u8; 4];
    let n = file
        .read(&mut magic)
        .map_err(|e| Error::Io(e.to_string()))?;
    if n >= 4 && is_sealed_spill(&magic) {
        let key = key.ok_or_else(|| Error::Io("spill file is encrypted".into()))?;
        let header = read_spill_header(&mut file, &key)?;
        return Ok(SpillIter {
            file: None,
            sealed: Some(SealedFrames {
                file,
                key,
                header,
                next_index: 0,
                current: None,
            }),
        });
    }
    if key.is_some() {
        return Err(Error::Io("spill file is not encrypted".into()));
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|e| Error::Io(e.to_string()))?;
    let reader = FileReader::try_new(file, None).map_err(|e| Error::Io(e.to_string()))?;
    Ok(SpillIter {
        file: Some(reader),
        sealed: None,
    })
}

pub(super) struct SpillIter {
    file: Option<FileReader<File>>,
    sealed: Option<SealedFrames>,
}

struct SealedFrames {
    file: File,
    key: Arc<SpillKey>,
    header: SpillHeader,
    next_index: u32,
    current: Option<FileReader<Cursor<Vec<u8>>>>,
}

impl SealedFrames {
    fn next_batch(&mut self) -> Option<Result<Batch>> {
        loop {
            if let Some(item) = next_ipc(self.current.as_mut()) {
                return Some(item);
            }
            self.current = None;
            if self.next_index == self.header.frame_count() {
                let mut extra = [0u8; 1];
                return match self.file.read(&mut extra) {
                    Ok(0) => None,
                    Ok(_) => Some(Err(Error::Io("trailing spill data".into()))),
                    Err(e) => Some(Err(Error::Io(e.to_string()))),
                };
            }
            match read_spill_frame(&mut self.file, &self.key, &self.header, self.next_index) {
                Ok(plain) => {
                    self.next_index += 1;
                    match FileReader::try_new(Cursor::new(plain), None) {
                        Ok(reader) => self.current = Some(reader),
                        Err(e) => return Some(Err(Error::Io(e.to_string()))),
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

impl Iterator for SpillIter {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        next_ipc(self.file.as_mut())
            .or_else(|| self.sealed.as_mut().and_then(SealedFrames::next_batch))
    }
}

fn next_ipc<R: Read + Seek>(reader: Option<&mut FileReader<R>>) -> Option<Result<Batch>> {
    let reader = reader?;
    loop {
        match reader.next() {
            None => return None,
            Some(Err(e)) => return Some(Err(Error::Io(e.to_string()))),
            Some(Ok(batch)) if batch.num_rows() == 0 => continue,
            Some(Ok(batch)) => return Some(Batch::from_arrow(batch)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_memtotal_skips_leading_lines() {
        assert_eq!(
            parse_memtotal("Buffers: 1 kB\nMemTotal: 2048 kB\nMemFree: 1 kB\n"),
            Some(2048 * 1024)
        );
        assert_eq!(parse_memtotal("MemFree: 1 kB\n"), None);
    }

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

    #[test]
    fn sealed_spill_hides_plaintext() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        let root = std::env::temp_dir().join(format!(
            "mizuchi-pool-enc-{}-{}",
            std::process::id(),
            JOB_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        let mut pool = MemoryPool::new(None, Some(root.clone())).unwrap();
        pool.set_spill_key(SpillKey::generate());
        let schema = Arc::new(Schema::new(vec![Field::new("ssn", DataType::Utf8, true)]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![Some(
                "employee_ssn=123-45-6789",
            )]))],
        )
        .unwrap();
        let path = pool.write_spill_file(schema.clone(), &[batch]).unwrap();
        let bytes = fs::read(&path).unwrap();
        let needle = b"employee_ssn=123-45-6789";
        assert!(
            !bytes.windows(needle.len()).any(|w| w == needle),
            "plaintext leaked into spill file"
        );
        assert!(is_sealed_spill(&bytes));
        let got = pool.read_spill(&path, schema).unwrap();
        assert_eq!(got[0].num_rows(), 1);
        drop(pool);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn sealed_spill_iterates_one_frame_at_a_time() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        let root = std::env::temp_dir().join(format!(
            "mizuchi-pool-enc-frames-{}-{}",
            std::process::id(),
            JOB_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        let mut pool = MemoryPool::new(None, Some(root.clone())).unwrap();
        pool.set_spill_key(SpillKey::generate());
        let schema = Arc::new(Schema::new(vec![Field::new("ssn", DataType::Utf8, true)]));
        let batch_a = Batch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![Some("frame-a-secret")]))],
        )
        .unwrap();
        let batch_b = Batch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![Some("frame-b-secret")]))],
        )
        .unwrap();
        let path = pool
            .write_spill_file(schema.clone(), &[batch_a, batch_b])
            .unwrap();
        let bytes = fs::read(&path).unwrap();
        assert!(is_sealed_spill(&bytes));
        for needle in [b"frame-a-secret".as_slice(), b"frame-b-secret".as_slice()] {
            assert!(
                !bytes.windows(needle.len()).any(|w| w == needle),
                "plaintext leaked into spill file"
            );
        }
        let got: Vec<_> = pool
            .spill_iter(&path)
            .unwrap()
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].num_rows(), 1);
        assert_eq!(got[1].num_rows(), 1);
        drop(pool);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn oversize_ipc_batch_splits_into_multiple_frames() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let payload = "x".repeat(200);
        let values: Vec<Option<&str>> = (0..80).map(|_| Some(payload.as_str())).collect();
        let batch =
            Batch::try_new(schema.clone(), vec![Arc::new(StringArray::from(values))]).unwrap();
        const MAX: usize = 8 * 1024;
        let frames = count_spill_frames_limited(schema.as_ref(), &batch, MAX).unwrap();
        assert!(frames > 1, "expected split, got {frames} frames");
        assert!(
            frames < 40,
            "zero-copy slices must not collapse to one row per frame, got {frames}"
        );
        let key = SpillKey::generate();
        let mut sealed = Vec::new();
        let header = write_spill_header(&mut sealed, &key, frames).unwrap();
        let mut index = 0u32;
        append_spill_frames_limited(
            &mut sealed,
            &key,
            schema.as_ref(),
            &batch,
            &header,
            &mut index,
            MAX,
        )
        .unwrap();
        assert_eq!(index, frames);
        let mut cur = std::io::Cursor::new(&sealed[4..]);
        let header = read_spill_header(&mut cur, &key).unwrap();
        let mut rows = 0usize;
        for i in 0..header.frame_count() {
            let plain = read_spill_frame(&mut cur, &key, &header, i).unwrap();
            assert!(plain.len() <= MAX);
            let reader = FileReader::try_new(std::io::Cursor::new(plain), None).unwrap();
            for rec in reader {
                rows += rec.unwrap().num_rows();
            }
        }
        assert_eq!(rows, 80);
    }

    #[test]
    fn truncated_sealed_spill_fails_closed() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        let root = std::env::temp_dir().join(format!(
            "mizuchi-pool-enc-trunc-{}-{}",
            std::process::id(),
            JOB_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        let mut pool = MemoryPool::new(None, Some(root.clone())).unwrap();
        pool.set_spill_key(SpillKey::generate());
        let schema = Arc::new(Schema::new(vec![Field::new("ssn", DataType::Utf8, true)]));
        let batch_a = Batch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![Some("frame-a-secret")]))],
        )
        .unwrap();
        let batch_b = Batch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![Some("frame-b-secret")]))],
        )
        .unwrap();
        let path = pool.write_spill_file(schema, &[batch_a, batch_b]).unwrap();
        let bytes = fs::read(&path).unwrap();
        // magic(4) + file id(16) + count(4) + header tag(28) + first frame length(4)
        let first_len = u32::from_be_bytes(bytes[52..56].try_into().unwrap()) as usize;
        fs::write(&path, &bytes[..56 + first_len]).unwrap();
        let err = pool
            .spill_iter(&path)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap_err();
        assert!(err.to_string().contains("truncated"), "{err}");
        drop(pool);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn forged_empty_spill_header_fails() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        let root = std::env::temp_dir().join(format!(
            "mizuchi-pool-enc-forge-{}-{}",
            std::process::id(),
            JOB_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        let mut pool = MemoryPool::new(None, Some(root.clone())).unwrap();
        pool.set_spill_key(SpillKey::generate());
        let schema = Arc::new(Schema::new(vec![Field::new("ssn", DataType::Utf8, true)]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![Some("frame-a-secret")]))],
        )
        .unwrap();
        let path = pool.write_spill_file(schema, &[batch]).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        bytes[20..24].copy_from_slice(&0u32.to_be_bytes());
        fs::write(&path, &bytes).unwrap();
        let err = match pool.spill_iter(&path) {
            Ok(iter) => iter.collect::<Result<Vec<_>>>().unwrap_err(),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("spill decrypt failed")
                || err.to_string().contains("truncated"),
            "{err}"
        );
        drop(pool);
        let _ = fs::remove_dir_all(&root);
    }
}
