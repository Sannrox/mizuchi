//! In-process cluster coordinator: workers, resources, dynamic allocation.
//!
//! The engine opens TCP only when the embedder grants network. In-process
//! slots stay here; process workers use Arrow Flight (`RemoteCluster`).
//! A runner maps workers onto processes or pods using `deploy/`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use super::Session;
use mizuchi_core::error::{Error, Result};

/// CPU, memory, and spill grant for one worker slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerResources {
    /// Slot id assigned by the coordinator.
    pub id: usize,
    /// Morsel and stage threads on this worker.
    pub threads: usize,
    /// Working-set cap in bytes.
    pub memory: u64,
    /// Spill directory for this worker, when set.
    pub spill_dir: Option<PathBuf>,
}

/// How a standalone cluster is sized.
#[derive(Clone, Debug)]
pub struct ClusterConfig {
    /// Workers to start with. Must be at least 1.
    pub workers: usize,
    /// Threads granted to each worker.
    pub threads_per_worker: usize,
    /// Memory granted to each worker, in bytes.
    pub memory_per_worker: u64,
    /// Shared spill root. Each worker uses a subdirectory.
    pub spill_dir: Option<PathBuf>,
    /// When true, a second copy of each stage task may run; the first
    /// commit wins and the other result is discarded.
    pub speculate: bool,
    /// Floor for [`Cluster::release_idle`].
    pub min_workers: usize,
    /// Ceiling for [`Cluster::add_worker`].
    pub max_workers: usize,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            workers: 1,
            threads_per_worker: 1,
            memory_per_worker: 32 * 1024 * 1024,
            spill_dir: None,
            speculate: false,
            min_workers: 1,
            max_workers: 8,
        }
    }
}

struct Slot {
    id: usize,
    idle: bool,
}

struct Inner {
    slots: Mutex<Vec<Slot>>,
    next_id: Mutex<usize>,
    threads_per_worker: usize,
    memory_per_worker: u64,
    spill_dir: Option<PathBuf>,
    speculate: bool,
    min_workers: usize,
    max_workers: usize,
}

/// Coordinator that places sessions onto a pool of resource-accounted workers.
#[derive(Clone, Debug)]
pub struct Cluster {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("threads_per_worker", &self.threads_per_worker)
            .field("memory_per_worker", &self.memory_per_worker)
            .field("speculate", &self.speculate)
            .field("min_workers", &self.min_workers)
            .field("max_workers", &self.max_workers)
            .finish()
    }
}

impl Cluster {
    /// Start a standalone in-process cluster from `cfg`.
    pub fn standalone(cfg: ClusterConfig) -> Result<Self> {
        if cfg.workers == 0 || cfg.min_workers == 0 {
            return Err(Error::Plan(
                "cluster workers must be greater than zero".into(),
            ));
        }
        if cfg.threads_per_worker == 0 {
            return Err(Error::Plan(
                "cluster threads_per_worker must be greater than zero".into(),
            ));
        }
        if cfg.memory_per_worker == 0 {
            return Err(Error::Plan(
                "cluster memory_per_worker must be greater than zero".into(),
            ));
        }
        if cfg.workers > cfg.max_workers {
            return Err(Error::Plan("cluster workers exceed max_workers".into()));
        }
        if cfg.min_workers > cfg.max_workers {
            return Err(Error::Plan("cluster min_workers exceed max_workers".into()));
        }
        let mut slots = Vec::with_capacity(cfg.workers);
        for id in 0..cfg.workers {
            slots.push(Slot { id, idle: true });
        }
        Ok(Self {
            inner: Arc::new(Inner {
                slots: Mutex::new(slots),
                next_id: Mutex::new(cfg.workers),
                threads_per_worker: cfg.threads_per_worker,
                memory_per_worker: cfg.memory_per_worker,
                spill_dir: cfg.spill_dir,
                speculate: cfg.speculate,
                min_workers: cfg.min_workers,
                max_workers: cfg.max_workers,
            }),
        })
    }

    fn lock_slots(&self) -> Result<std::sync::MutexGuard<'_, Vec<Slot>>> {
        self.inner
            .slots
            .lock()
            .map_err(|_| Error::Plan("cluster lock poisoned".into()))
    }

    /// Live worker slots.
    pub fn worker_count(&self) -> Result<usize> {
        Ok(self.lock_slots()?.len())
    }

    /// Per-worker resource grants currently registered.
    pub fn workers(&self) -> Result<Vec<WorkerResources>> {
        let slots = self.lock_slots()?;
        Ok(slots
            .iter()
            .map(|s| WorkerResources {
                id: s.id,
                threads: self.inner.threads_per_worker,
                memory: self.inner.memory_per_worker,
                spill_dir: self
                    .inner
                    .spill_dir
                    .as_ref()
                    .map(|root| root.join(format!("w-{}", s.id))),
            })
            .collect())
    }

    /// Add one worker if under `max_workers`.
    pub fn add_worker(&self) -> Result<WorkerResources> {
        let mut slots = self.lock_slots()?;
        if slots.len() >= self.inner.max_workers {
            return Err(Error::Plan("cluster is at max_workers".into()));
        }
        let mut next = self
            .inner
            .next_id
            .lock()
            .map_err(|_| Error::Plan("cluster lock poisoned".into()))?;
        let id = *next;
        *next += 1;
        slots.push(Slot { id, idle: true });
        Ok(WorkerResources {
            id,
            threads: self.inner.threads_per_worker,
            memory: self.inner.memory_per_worker,
            spill_dir: self
                .inner
                .spill_dir
                .as_ref()
                .map(|root| root.join(format!("w-{id}"))),
        })
    }

    /// Release idle workers down to `min_workers`. Returns how many left.
    pub fn release_idle(&self) -> Result<usize> {
        let mut slots = self.lock_slots()?;
        let mut released = 0usize;
        while slots.len() > self.inner.min_workers {
            if let Some(i) = slots.iter().rposition(|s| s.idle) {
                slots.remove(i);
                released += 1;
            } else {
                break;
            }
        }
        Ok(released)
    }

    /// Grow or shrink toward `n`, honoring min/max.
    pub fn scale_to(&self, n: usize) -> Result<usize> {
        let n = n.clamp(self.inner.min_workers, self.inner.max_workers);
        while self.worker_count()? < n {
            self.add_worker()?;
        }
        if self.worker_count()? > n {
            let mut slots = self.lock_slots()?;
            while slots.len() > n {
                if let Some(i) = slots.iter().rposition(|s| s.idle) {
                    slots.remove(i);
                } else {
                    break;
                }
            }
        }
        self.worker_count()
    }

    /// Open a session whose threads and memory equal the live worker grants.
    pub fn session(&self) -> Result<Session> {
        let n = self.worker_count()?;
        let threads = n.saturating_mul(self.inner.threads_per_worker).max(1);
        let memory = (n as u64)
            .saturating_mul(self.inner.memory_per_worker)
            .max(1);
        let mut session = Session::new()
            .with_workers(threads)?
            .with_memory_limit(memory);
        if let Some(dir) = &self.inner.spill_dir {
            session = session.with_spill_dir(dir.clone());
        }
        Ok(session.with_speculate(self.inner.speculate))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use mizuchi_core::batch::Batch;

    fn numbered(n: i64) -> (arrow::datatypes::SchemaRef, Vec<Batch>) {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(
                (0..n).map(Some).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        (schema, vec![Batch::from_arrow(batch).unwrap()])
    }

    #[test]
    fn scale_up_then_release_idle() {
        let c = Cluster::standalone(ClusterConfig {
            workers: 1,
            min_workers: 1,
            max_workers: 4,
            ..ClusterConfig::default()
        })
        .unwrap();
        assert_eq!(c.worker_count().unwrap(), 1);
        c.add_worker().unwrap();
        c.add_worker().unwrap();
        assert_eq!(c.worker_count().unwrap(), 3);
        assert_eq!(c.release_idle().unwrap(), 2);
        assert_eq!(c.worker_count().unwrap(), 1);
        let err = Cluster::standalone(ClusterConfig {
            workers: 1,
            max_workers: 1,
            ..ClusterConfig::default()
        })
        .unwrap()
        .add_worker()
        .unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
    }

    #[test]
    fn two_worker_session_matches_one() {
        let (schema, batches) = numbered(16);
        let one = Cluster::standalone(ClusterConfig {
            workers: 1,
            threads_per_worker: 1,
            ..ClusterConfig::default()
        })
        .unwrap();
        let two = Cluster::standalone(ClusterConfig {
            workers: 2,
            threads_per_worker: 1,
            ..ClusterConfig::default()
        })
        .unwrap();
        let mut s1 = one.session().unwrap();
        let mut s2 = two.session().unwrap();
        s1.register("t", schema.clone(), batches.clone()).unwrap();
        s2.register("t", schema, batches).unwrap();
        let sql = "SELECT a FROM t WHERE a >= 4";
        let a = s1.execute_sql(sql).unwrap();
        let b = s2.execute_sql(sql).unwrap();
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
    fn scale_to_clamps_and_session_sees_grants() {
        let c = Cluster::standalone(ClusterConfig {
            workers: 1,
            threads_per_worker: 2,
            memory_per_worker: 8 * 1024 * 1024,
            min_workers: 1,
            max_workers: 3,
            ..ClusterConfig::default()
        })
        .unwrap();
        assert_eq!(c.scale_to(8).unwrap(), 3);
        assert_eq!(c.workers().unwrap().len(), 3);
        let s = c.session().unwrap();
        assert_eq!(s.workers(), 6);
    }
}
