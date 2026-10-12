//! UNION, INTERSECT, and EXCEPT over Arrow batches.
//!
//! UNION ALL concatenates inputs after relabeling each batch to the left
//! schema. UNION DISTINCT hashes whole rows and keeps first-seen order.
//! INTERSECT and EXCEPT probe the right input: DISTINCT uses a set of keys,
//! ALL uses per-key bag counts. Nulls compare equal. Hash tables do not spill.

use std::collections::{HashMap, HashSet};

use arrow::array::UInt32Array;
use arrow::compute::take;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::key::{Scalar, extract_key};
use super::memory::{self, MemoryPool, batch_bytes};
use super::{CancelToken, Operator};
use mizuchi_core::batch::{Batch, BatchPolicy};
use mizuchi_core::error::{Error, Result};

pub(super) fn union(
    inputs: Vec<Box<dyn Operator>>,
    schema: SchemaRef,
    all: bool,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let mut batches = Vec::new();
    let mut holds = Vec::new();
    for input in inputs {
        let (part, part_holds) = collect_side(input, schema.clone(), pool, cancel)?;
        batches.extend(part);
        holds.extend(part_holds);
    }
    let out = if all {
        batches
    } else {
        distinct_first_seen(&batches, schema, policy, cancel)?
    };
    drop(holds);
    Ok(out)
}

pub(super) fn intersect(
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    schema: SchemaRef,
    all: bool,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let (right_batches, right_holds) = collect_side(right, schema.clone(), pool, cancel)?;
    let (left_batches, left_holds) = collect_side(left, schema.clone(), pool, cancel)?;
    let out = if all {
        probe_bag(&left_batches, &right_batches, schema, policy, cancel, true)?
    } else {
        probe_set(&left_batches, &right_batches, schema, policy, cancel, true)?
    };
    drop(right_holds);
    drop(left_holds);
    Ok(out)
}

pub(super) fn except(
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    schema: SchemaRef,
    all: bool,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let (right_batches, right_holds) = collect_side(right, schema.clone(), pool, cancel)?;
    let (left_batches, left_holds) = collect_side(left, schema.clone(), pool, cancel)?;
    let out = if all {
        probe_bag(&left_batches, &right_batches, schema, policy, cancel, false)?
    } else {
        probe_set(&left_batches, &right_batches, schema, policy, cancel, false)?
    };
    drop(right_holds);
    drop(left_holds);
    Ok(out)
}

fn collect_side<'a>(
    mut exec: Box<dyn Operator>,
    schema: SchemaRef,
    pool: &'a MemoryPool,
    cancel: &CancelToken,
) -> Result<(Vec<Batch>, Vec<memory::Reservation<'a>>)> {
    let mut batches = Vec::new();
    let mut holds = Vec::new();
    while let Some(batch) = exec.next_batch(cancel)? {
        cancel.check()?;
        if batch.num_rows() == 0 {
            continue;
        }
        let batch = relabel(batch, schema.clone())?;
        if pool.has_limit() {
            holds.push(pool.try_reserve(batch_bytes(&batch))?);
        }
        batches.push(batch);
    }
    Ok((batches, holds))
}

fn relabel(batch: Batch, schema: SchemaRef) -> Result<Batch> {
    if batch.schema().as_ref() == schema.as_ref() {
        return Ok(batch);
    }
    let inner = RecordBatch::try_new(schema, batch.as_arrow().columns().to_vec())
        .map_err(|e| Error::Eval(e.to_string()))?;
    Batch::from_arrow(inner)
}

fn distinct_first_seen(
    batches: &[Batch],
    schema: SchemaRef,
    policy: &BatchPolicy,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let mut seen: HashSet<Vec<Scalar>> = HashSet::new();
    let mut out = Vec::new();
    for batch in batches {
        cancel.check()?;
        let mut keep = Vec::new();
        for row in 0..batch.num_rows() {
            let key = row_key(batch, row)?;
            if seen.insert(key) {
                keep.push(row as u32);
            }
        }
        out.extend(take_kept(batch, keep, schema.clone(), policy)?);
    }
    Ok(out)
}

fn probe_set(
    left: &[Batch],
    right: &[Batch],
    schema: SchemaRef,
    policy: &BatchPolicy,
    cancel: &CancelToken,
    keep_hit: bool,
) -> Result<Vec<Batch>> {
    let mut right_keys: HashSet<Vec<Scalar>> = HashSet::new();
    for batch in right {
        cancel.check()?;
        for row in 0..batch.num_rows() {
            right_keys.insert(row_key(batch, row)?);
        }
    }
    let mut seen: HashSet<Vec<Scalar>> = HashSet::new();
    let mut out = Vec::new();
    for batch in left {
        cancel.check()?;
        let mut keep = Vec::new();
        for row in 0..batch.num_rows() {
            let key = row_key(batch, row)?;
            let hit = right_keys.contains(&key);
            if hit == keep_hit && seen.insert(key) {
                keep.push(row as u32);
            }
        }
        out.extend(take_kept(batch, keep, schema.clone(), policy)?);
    }
    Ok(out)
}

fn probe_bag(
    left: &[Batch],
    right: &[Batch],
    schema: SchemaRef,
    policy: &BatchPolicy,
    cancel: &CancelToken,
    keep_hit: bool,
) -> Result<Vec<Batch>> {
    let mut counts: HashMap<Vec<Scalar>, usize> = HashMap::new();
    for batch in right {
        cancel.check()?;
        for row in 0..batch.num_rows() {
            *counts.entry(row_key(batch, row)?).or_insert(0) += 1;
        }
    }
    let mut out = Vec::new();
    for batch in left {
        cancel.check()?;
        let mut keep = Vec::new();
        for row in 0..batch.num_rows() {
            let key = row_key(batch, row)?;
            if keep_hit {
                if let Some(c) = counts.get_mut(&key)
                    && *c > 0
                {
                    *c -= 1;
                    keep.push(row as u32);
                }
            } else if let Some(c) = counts.get_mut(&key)
                && *c > 0
            {
                *c -= 1;
            } else {
                keep.push(row as u32);
            }
        }
        out.extend(take_kept(batch, keep, schema.clone(), policy)?);
    }
    Ok(out)
}

fn row_key(batch: &Batch, row: usize) -> Result<Vec<Scalar>> {
    extract_key(batch.as_arrow().columns(), row)
}

fn take_kept(
    batch: &Batch,
    keep: Vec<u32>,
    schema: SchemaRef,
    policy: &BatchPolicy,
) -> Result<Vec<Batch>> {
    if keep.is_empty() {
        return Ok(Vec::new());
    }
    let idx = UInt32Array::from(keep);
    let mut columns = Vec::with_capacity(batch.num_columns());
    for col in batch.as_arrow().columns() {
        columns.push(take(col.as_ref(), &idx, None).map_err(|e| Error::Eval(e.to_string()))?);
    }
    let out = Batch::try_new(schema, columns)?;
    Ok(out
        .split_by_policy(policy)
        .into_iter()
        .filter(|b| b.num_rows() > 0)
        .collect())
}
