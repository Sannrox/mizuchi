//! Per-query memory pool and Arrow IPC spill files.
//!
//! Operators reserve working-set bytes before they grow. When a reservation
//! cannot fit and a spill directory was given, they write Arrow IPC runs and
//! release the reservation. Without a directory, the engine fails closed.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;

use crate::batch::Batch;
use crate::error::{Error, Result};

/// Bytes held by `batch` according to Arrow's array size accounting.
pub(super) fn batch_bytes(batch: &Batch) -> u64 {
    batch.as_arrow().get_array_memory_size() as u64
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
    spill_dir: Option<PathBuf>,
    seq: AtomicU64,
}

static JOB_SEQ: AtomicU64 = AtomicU64::new(1);

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
            Some(root) => {
                let dir = root.join(format!("mem-{}", JOB_SEQ.fetch_add(1, Ordering::Relaxed)));
                fs::create_dir_all(&dir).map_err(|e| Error::Io(e.to_string()))?;
                Some(dir)
            }
            None => None,
        };
        Ok(Self {
            limit,
            used,
            peak: AtomicU64::new(0),
            copied: Arc::new(AtomicU64::new(0)),
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

    pub(super) fn try_reserve(&self, bytes: u64) -> Result<Reservation<'_>> {
        if bytes == 0 {
            return Ok(Reservation {
                pool: self,
                bytes: 0,
            });
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

pub(super) fn write_spill(path: &Path, schema: SchemaRef, batches: &[Batch]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| Error::Io(e.to_string()))?;
    }
    let file = File::create(path).map_err(|e| Error::Io(e.to_string()))?;
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

pub(super) fn read_spill(path: &Path, schema: SchemaRef) -> Result<Vec<Batch>> {
    if !path.exists() {
        return Ok(vec![Batch::empty(schema)?]);
    }
    let file = File::open(path).map_err(|e| Error::Io(e.to_string()))?;
    let reader = FileReader::try_new(file, None).map_err(|e| Error::Io(e.to_string()))?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| Error::Io(e.to_string()))?;
        if batch.num_rows() > 0 {
            out.push(Batch::from_arrow(batch)?);
        }
    }
    if out.is_empty() {
        out.push(Batch::empty(schema)?);
    }
    Ok(out)
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
}
