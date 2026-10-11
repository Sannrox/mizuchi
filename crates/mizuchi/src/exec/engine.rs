//! Per-owner memory and worker shares on one engine.
//!
//! Shares are reserved when a session is opened. Under contention each owner
//! is capped at its share, so concurrent owners cannot starve each other.

use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::exec::Session;
use crate::exec::memory::Budget;

#[derive(Debug)]
struct Owner {
    memory: u64,
    workers: usize,
    budget: Arc<Budget>,
}

#[derive(Debug)]
struct Inner {
    memory_limit: u64,
    workers: usize,
    owners: Mutex<BTreeMap<String, Owner>>,
}

/// Shared engine. Owner sessions draw from reserved memory and worker shares.
#[derive(Clone, Debug)]
pub struct Engine {
    inner: Arc<Inner>,
}

impl Engine {
    /// `memory_limit` bytes and `workers` threads in total across owners.
    pub fn new(memory_limit: u64, workers: usize) -> Result<Self> {
        if memory_limit == 0 {
            return Err(Error::Plan(
                "engine memory_limit must be greater than zero".into(),
            ));
        }
        if workers == 0 {
            return Err(Error::Plan(
                "engine workers must be greater than zero".into(),
            ));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                memory_limit,
                workers,
                owners: Mutex::new(BTreeMap::new()),
            }),
        })
    }

    /// Open a session for `owner` with a reserved memory and worker share.
    ///
    /// Repeating the same owner and shares reuses the budget. A second owner
    /// whose shares would exceed the engine totals fails closed.
    pub fn session(
        &self,
        owner: impl Into<String>,
        memory: u64,
        workers: usize,
    ) -> Result<Session> {
        if memory == 0 {
            return Err(Error::Plan(
                "owner memory share must be greater than zero".into(),
            ));
        }
        if workers == 0 {
            return Err(Error::Plan(
                "owner workers must be greater than zero".into(),
            ));
        }
        let owner = owner.into();
        let budget = {
            let mut owners = self
                .inner
                .owners
                .lock()
                .map_err(|_| Error::Plan("engine lock poisoned".into()))?;
            if let Some(existing) = owners.get(&owner) {
                if existing.memory != memory || existing.workers != workers {
                    return Err(Error::Plan(format!(
                        "owner {owner} already has memory {} workers {}",
                        existing.memory, existing.workers
                    )));
                }
                existing.budget.clone()
            } else {
                let used_mem: u64 = owners.values().map(|o| o.memory).sum();
                let used_workers: usize = owners.values().map(|o| o.workers).sum();
                let mem_next = used_mem.saturating_add(memory);
                let workers_next = used_workers.saturating_add(workers);
                if mem_next > self.inner.memory_limit {
                    return Err(Error::Memory(format!(
                        "owner {owner} memory {memory} would use {mem_next} of engine {}",
                        self.inner.memory_limit
                    )));
                }
                if workers_next > self.inner.workers {
                    return Err(Error::Plan(format!(
                        "owner {owner} workers {workers} would use {workers_next} of engine {}",
                        self.inner.workers
                    )));
                }
                let budget = Arc::new(Budget {
                    limit: memory,
                    used: Arc::new(AtomicU64::new(0)),
                });
                owners.insert(
                    owner.clone(),
                    Owner {
                        memory,
                        workers,
                        budget: budget.clone(),
                    },
                );
                budget
            }
        };
        Ok(Session::with_owner_share(workers, memory, budget))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::batch::Batch;
    use crate::error::Error;

    fn numbered(n_batches: usize, rows: usize) -> (arrow::datatypes::SchemaRef, Vec<Batch>) {
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

    fn tight_limit(batches: &[Batch]) -> u64 {
        let sz = batches[0].as_arrow().get_array_memory_size() as u64;
        sz + sz / 2
    }

    #[test]
    fn shares_that_exceed_engine_fail_closed() {
        let engine = Engine::new(1000, 2).unwrap();
        engine.session("a", 600, 1).unwrap();
        let err = engine.session("b", 500, 1).unwrap_err();
        assert!(matches!(err, Error::Memory(_)), "{err:?}");
        let err = engine.session("c", 100, 2).unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
    }

    #[test]
    fn owner_cannot_use_another_owners_idle_memory() {
        let (schema, batches) = numbered(12, 32);
        let cap = tight_limit(&batches);
        let engine = Engine::new(cap + 32 * 1024 * 1024, 2).unwrap();
        let mut small = engine.session("small", cap, 1).unwrap();
        let mut large = engine.session("large", 32 * 1024 * 1024, 1).unwrap();
        small
            .register("t", schema.clone(), batches.clone())
            .unwrap();
        large.register("t", schema, batches).unwrap();
        let err = small.execute_sql("SELECT a FROM t ORDER BY a");
        assert!(matches!(err, Err(Error::Memory(_))), "{err:?}");
        large
            .execute_sql("SELECT a FROM t ORDER BY a")
            .expect("large owner has room");
    }

    #[test]
    fn two_owners_each_complete_under_contention() {
        let (schema, batches) = numbered(4, 16);
        let engine = Engine::new(64 * 1024 * 1024, 4).unwrap();
        let mut a = engine.session("a", 32 * 1024 * 1024, 2).unwrap();
        let mut b = engine.session("b", 32 * 1024 * 1024, 2).unwrap();
        a.register("t", schema.clone(), batches.clone()).unwrap();
        b.register("t", schema, batches).unwrap();
        let ta = thread::spawn(move || {
            for _ in 0..8 {
                a.execute_sql("SELECT COUNT(*) AS c FROM t").unwrap();
            }
        });
        let tb = thread::spawn(move || {
            for _ in 0..8 {
                b.execute_sql("SELECT COUNT(*) AS c FROM t").unwrap();
            }
        });
        ta.join().expect("owner a");
        tb.join().expect("owner b");
    }

    #[test]
    fn same_owner_reuses_share() {
        let engine = Engine::new(1000, 2).unwrap();
        let _ = engine.session("a", 400, 1).unwrap();
        let _ = engine.session("a", 400, 1).unwrap();
        let err = engine.session("a", 500, 1).unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
    }
}
