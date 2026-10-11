//! Recursive CTE: execute the seeds, then the step over a working table
//! until the step produces no rows.

use super::memory::{self, MemoryPool, batch_bytes};
use super::{Session, collect};
use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::plan::LogicalPlan;
use arrow::datatypes::SchemaRef;

/// Stop a runaway recursive step that never goes empty.
const MAX_ITERS: u32 = 1000;

pub(super) fn run(
    name: &str,
    anchors: &[LogicalPlan],
    step: &LogicalPlan,
    _schema: &SchemaRef,
    session: &Session,
    pool: &MemoryPool,
    analyze: bool,
) -> Result<Vec<Batch>> {
    let mut working = Vec::new();
    for anchor in anchors {
        working.extend(collect(
            super::physical_with_work(anchor, session, pool, analyze, None)?,
            &session.cancel,
        )?);
    }
    // Seed batches stay live as the first working table while `out` holds a copy.
    let mut holds = Vec::new();
    holds.push(reserve_batches(pool, &working)?);
    let mut out = working.clone();
    let mut iter = 0u32;
    loop {
        session.cancel.check()?;
        if working.iter().all(|b| b.num_rows() == 0) {
            break;
        }
        iter = iter.saturating_add(1);
        if iter > MAX_ITERS {
            return Err(Error::Eval("recursive CTE iteration limit".into()));
        }
        working = collect(
            super::physical_with_work(step, session, pool, analyze, Some((name, &working)))?,
            &session.cancel,
        )?;
        // The step result stays live as the next working table; reserve the
        // extra copy that `out` will hold.
        holds.push(reserve_batches(pool, &working)?);
        for batch in &working {
            if batch.num_rows() > 0 {
                out.push(batch.clone());
            }
        }
    }
    drop(holds);
    Ok(out)
}

fn reserve_batches<'a>(pool: &'a MemoryPool, batches: &[Batch]) -> Result<memory::Reservation<'a>> {
    let bytes = batches
        .iter()
        .map(batch_bytes)
        .fold(0u64, u64::saturating_add);
    pool.try_reserve(bytes)
}
