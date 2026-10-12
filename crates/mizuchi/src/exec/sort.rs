//! Sort, top-k, and limit over Arrow batches.
//!
//! A limit without a sort walks the child and slices rows in input order. A
//! sort materializes the child, evaluates the keys, and orders them with Arrow
//! lexicographic sort. LIMIT after ORDER BY asks that sort for only the first
//! `skip + fetch` keys, then drops the offset.

use arrow::array::{Array, RecordBatch, UInt32Array};
use arrow::compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take};
use arrow::datatypes::SchemaRef;

use super::CancelToken;
use super::memory::{self, MemoryPool, batch_bytes};
use super::{Exec, Operator};
use crate::batch::{Batch, BatchPolicy};
use crate::error::{Error, Result};
use crate::plan::SortExpr;

/// Sort `child` by `order`, then apply `skip` / `fetch` (top-k when `fetch` is set).
#[allow(clippy::too_many_arguments)]
pub(super) fn ordered(
    mut child: Box<dyn Operator>,
    schema: SchemaRef,
    order: &[SortExpr],
    skip: u64,
    fetch: Option<u64>,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    if matches!(fetch, Some(0)) {
        return Ok(Vec::new());
    }
    if !pool.has_limit() {
        return sort_concat(&mut child, schema, order, skip, fetch, policy, pool, cancel);
    }
    let mut run: Vec<Batch> = Vec::new();
    let mut held: Vec<memory::Reservation<'_>> = Vec::new();
    let mut spills = Vec::new();
    while let Some(batch) = child.next_batch(cancel)? {
        if batch.num_rows() == 0 {
            continue;
        }
        let sz = batch_bytes(&batch);
        match pool.try_reserve(sz) {
            Ok(r) => {
                run.push(batch);
                held.push(r);
            }
            Err(e) => {
                if !pool.can_spill() {
                    return Err(e);
                }
                if run.is_empty() {
                    return Err(Error::Memory(
                        "a single sort batch exceeds the memory limit".into(),
                    ));
                }
                spills.push(spill_sorted_run(&run, schema.clone(), order, pool, cancel)?);
                held.clear();
                run.clear();
                let r = pool.try_reserve(sz).map_err(|_| {
                    Error::Memory("a single sort batch exceeds the memory limit".into())
                })?;
                run.push(batch);
                held.push(r);
            }
        }
    }
    if spills.is_empty() {
        drop(held);
        let mut concat = batches_to_exec(run, schema.clone());
        return sort_concat(
            &mut concat,
            schema,
            order,
            skip,
            fetch,
            policy,
            pool,
            cancel,
        );
    }
    if !run.is_empty() {
        spills.push(spill_sorted_run(&run, schema.clone(), order, pool, cancel)?);
        held.clear();
        run.clear();
    }
    let merged = merge_spilled(spills, schema.clone(), order, pool, cancel)?;
    apply_skip_fetch(merged, schema, skip, fetch, policy, pool, cancel)
}

#[allow(clippy::too_many_arguments)]
fn sort_concat(
    child: &mut dyn Operator,
    schema: SchemaRef,
    order: &[SortExpr],
    skip: u64,
    fetch: Option<u64>,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let batch = concat_exec(child, schema.clone(), pool, cancel)?;
    sort_batch(&batch, order, skip, fetch, policy, pool)
}

fn sort_batch(
    batch: &Batch,
    order: &[SortExpr],
    skip: u64,
    fetch: Option<u64>,
    policy: &BatchPolicy,
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    let n = batch.num_rows();
    let skip = clamp_rows(skip, n);
    if skip >= n || n == 0 {
        return empty_out(batch.schema(), policy);
    }
    if n > u32::MAX as usize {
        return Err(Error::Eval("sort input exceeds u32 row indices".into()));
    }
    let mut columns = Vec::with_capacity(order.len());
    for key in order {
        let values = key.expr.eval(batch)?;
        columns.push(SortColumn {
            values,
            options: Some(SortOptions {
                descending: !key.asc,
                nulls_first: key.nulls_first,
            }),
        });
    }
    let k = fetch.map(|f| skip.saturating_add(clamp_rows(f, n)).min(n));
    let indices = lexsort_to_indices(&columns, k).map_err(|e| Error::Eval(e.to_string()))?;
    let start = skip.min(indices.len());
    let len = indices.len() - start;
    take_rows(batch, &indices.slice(start, len), policy, pool)
}

fn spill_sorted_run(
    run: &[Batch],
    schema: SchemaRef,
    order: &[SortExpr],
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<std::path::PathBuf> {
    let mut child = batches_to_exec(run.to_vec(), schema.clone());
    let sorted = sort_concat(
        &mut child,
        schema.clone(),
        order,
        0,
        None,
        &BatchPolicy::unlimited(),
        pool,
        cancel,
    )?;
    let path = pool.spill_path()?;
    memory::write_spill(&path, schema, &sorted)?;
    Ok(path)
}

fn batches_to_exec(batches: Vec<Batch>, _schema: SchemaRef) -> Box<dyn Operator> {
    Box::new(Exec::Scan { batches, index: 0 })
}

fn merge_spilled(
    mut runs: Vec<std::path::PathBuf>,
    schema: SchemaRef,
    order: &[SortExpr],
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    while runs.len() > 1 {
        let a = runs.remove(0);
        let b = runs.remove(0);
        let left = memory::read_spill(&a, schema.clone())?;
        let right = memory::read_spill(&b, schema.clone())?;
        let merged = merge_sorted_batches(left, right, schema.clone(), order, pool, cancel)?;
        let path = pool.spill_path()?;
        memory::write_spill(&path, schema.clone(), &merged)?;
        runs.push(path);
    }
    match runs.pop() {
        Some(path) => memory::read_spill(&path, schema),
        None => Ok(vec![Batch::empty(schema)?]),
    }
}

fn merge_sorted_batches(
    left: Vec<Batch>,
    right: Vec<Batch>,
    schema: SchemaRef,
    order: &[SortExpr],
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let mut child = batches_to_exec(left.into_iter().chain(right).collect(), schema.clone());
    sort_concat(
        &mut child,
        schema,
        order,
        0,
        None,
        &BatchPolicy::unlimited(),
        pool,
        cancel,
    )
}

fn apply_skip_fetch(
    batches: Vec<Batch>,
    schema: SchemaRef,
    skip: u64,
    fetch: Option<u64>,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let mut child = batches_to_exec(batches, schema.clone());
    let batch = concat_exec(&mut child, schema, pool, cancel)?;
    let n = batch.num_rows();
    let skip = clamp_rows(skip, n);
    if skip >= n || n == 0 {
        return empty_out(batch.schema(), policy);
    }
    let take_n = fetch
        .map(|f| clamp_rows(f, n.saturating_sub(skip)))
        .unwrap_or(n - skip);
    let sliced = batch.slice(skip, take_n)?;
    Ok(sliced
        .split_by_policy(policy)
        .into_iter()
        .filter(|b| b.num_rows() > 0)
        .collect())
}

fn concat_exec(
    exec: &mut dyn Operator,
    schema: SchemaRef,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Batch> {
    let mut batches = Vec::new();
    while let Some(batch) = exec.next_batch(cancel)? {
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }
    match batches.len() {
        0 => Batch::empty(schema),
        1 => Ok(batches.pop().expect("len is 1")),
        _ => {
            let arrows: Vec<RecordBatch> = batches.into_iter().map(Batch::into_arrow).collect();
            let inner = concat_batches(&schema, &arrows).map_err(|e| Error::Eval(e.to_string()))?;
            let out = Batch::from_arrow(inner)?;
            pool.record_copy(batch_bytes(&out));
            Ok(out)
        }
    }
}

fn take_rows(
    batch: &Batch,
    indices: &dyn Array,
    policy: &BatchPolicy,
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    if indices.is_empty() {
        return empty_out(batch.schema(), policy);
    }
    let out = if let Some(idx) = indices.as_any().downcast_ref::<UInt32Array>() {
        if idx.null_count() == 0 {
            let values = idx.values();
            let gathered = batch.gather(values)?;
            if crate::batch::contiguous_indices(values).is_none() {
                pool.record_copy(batch_bytes(&gathered));
            }
            gathered
        } else {
            take_all_columns(batch, indices, pool)?
        }
    } else {
        take_all_columns(batch, indices, pool)?
    };
    Ok(out
        .split_by_policy(policy)
        .into_iter()
        .filter(|b| b.num_rows() > 0)
        .collect())
}

fn take_all_columns(batch: &Batch, indices: &dyn Array, pool: &MemoryPool) -> Result<Batch> {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for i in 0..batch.num_columns() {
        columns.push(
            take(batch.column(i).as_ref(), indices, None)
                .map_err(|e| Error::Eval(e.to_string()))?,
        );
    }
    let out = Batch::try_new(batch.schema(), columns)?;
    pool.record_copy(batch_bytes(&out));
    Ok(out)
}

fn empty_out(schema: SchemaRef, policy: &BatchPolicy) -> Result<Vec<Batch>> {
    Ok(Batch::empty(schema)?
        .split_by_policy(policy)
        .into_iter()
        .filter(|b| b.num_rows() > 0)
        .collect())
}

fn clamp_rows(n: u64, len: usize) -> usize {
    (n as usize).min(len)
}
