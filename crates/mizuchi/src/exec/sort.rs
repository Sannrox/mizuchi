//! Sort, top-k, and limit over Arrow batches.
//!
//! A limit without a sort walks the child and slices rows in input order. A
//! sort materializes the child, evaluates the keys, and orders them with Arrow
//! lexicographic sort. LIMIT after ORDER BY asks that sort for only the first
//! `skip + fetch` keys, then drops the offset.

use arrow::array::{Array, RecordBatch};
use arrow::compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take};
use arrow::datatypes::SchemaRef;

use super::Exec;
use super::memory::{self, MemoryPool, batch_bytes};
use crate::batch::{Batch, BatchPolicy};
use crate::error::{Error, Result};
use crate::plan::SortExpr;

/// Sort `child` by `order`, then apply `skip` / `fetch` (top-k when `fetch` is set).
pub(super) fn ordered(
    mut child: Exec,
    schema: SchemaRef,
    order: &[SortExpr],
    skip: u64,
    fetch: Option<u64>,
    policy: &BatchPolicy,
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    if matches!(fetch, Some(0)) {
        return Ok(Vec::new());
    }
    if !pool.has_limit() {
        return sort_concat(&mut child, schema, order, skip, fetch, policy);
    }
    let mut run: Vec<Batch> = Vec::new();
    let mut held: Vec<memory::Reservation<'_>> = Vec::new();
    let mut spills = Vec::new();
    while let Some(batch) = child.next_batch()? {
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
                spills.push(spill_sorted_run(&run, schema.clone(), order, pool)?);
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
        return sort_concat(&mut concat, schema, order, skip, fetch, policy);
    }
    if !run.is_empty() {
        spills.push(spill_sorted_run(&run, schema.clone(), order, pool)?);
        held.clear();
        run.clear();
    }
    let merged = merge_spilled(spills, schema.clone(), order, pool)?;
    apply_skip_fetch(merged, schema, skip, fetch, policy)
}

fn sort_concat(
    child: &mut Exec,
    schema: SchemaRef,
    order: &[SortExpr],
    skip: u64,
    fetch: Option<u64>,
    policy: &BatchPolicy,
) -> Result<Vec<Batch>> {
    let batch = concat_exec(child, schema.clone())?;
    sort_batch(&batch, order, skip, fetch, policy)
}

fn sort_batch(
    batch: &Batch,
    order: &[SortExpr],
    skip: u64,
    fetch: Option<u64>,
    policy: &BatchPolicy,
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
    take_rows(batch, &indices.slice(start, len), policy)
}

fn spill_sorted_run(
    run: &[Batch],
    schema: SchemaRef,
    order: &[SortExpr],
    pool: &MemoryPool,
) -> Result<std::path::PathBuf> {
    let mut child = batches_to_exec(run.to_vec(), schema.clone());
    let sorted = sort_concat(
        &mut child,
        schema.clone(),
        order,
        0,
        None,
        &BatchPolicy::unlimited(),
    )?;
    let path = pool.spill_path()?;
    memory::write_spill(&path, schema, &sorted)?;
    Ok(path)
}

fn batches_to_exec(batches: Vec<Batch>, _schema: SchemaRef) -> Exec {
    Exec::Scan { batches, index: 0 }
}

fn merge_spilled(
    mut runs: Vec<std::path::PathBuf>,
    schema: SchemaRef,
    order: &[SortExpr],
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    while runs.len() > 1 {
        let a = runs.remove(0);
        let b = runs.remove(0);
        let left = memory::read_spill(&a, schema.clone())?;
        let right = memory::read_spill(&b, schema.clone())?;
        let merged = merge_sorted_batches(left, right, schema.clone(), order)?;
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
) -> Result<Vec<Batch>> {
    let mut child = batches_to_exec(left.into_iter().chain(right).collect(), schema.clone());
    sort_concat(
        &mut child,
        schema,
        order,
        0,
        None,
        &BatchPolicy::unlimited(),
    )
}

fn apply_skip_fetch(
    batches: Vec<Batch>,
    schema: SchemaRef,
    skip: u64,
    fetch: Option<u64>,
    policy: &BatchPolicy,
) -> Result<Vec<Batch>> {
    let mut child = batches_to_exec(batches, schema.clone());
    let batch = concat_exec(&mut child, schema)?;
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

fn concat_exec(exec: &mut Exec, schema: SchemaRef) -> Result<Batch> {
    let mut batches = Vec::new();
    while let Some(batch) = exec.next_batch()? {
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }
    if batches.is_empty() {
        return Batch::empty(schema);
    }
    let arrows: Vec<RecordBatch> = batches.into_iter().map(Batch::into_arrow).collect();
    let inner = concat_batches(&schema, &arrows).map_err(|e| Error::Eval(e.to_string()))?;
    Batch::from_arrow(inner)
}

fn take_rows(batch: &Batch, indices: &dyn Array, policy: &BatchPolicy) -> Result<Vec<Batch>> {
    if indices.is_empty() {
        return empty_out(batch.schema(), policy);
    }
    let mut columns = Vec::with_capacity(batch.num_columns());
    for i in 0..batch.num_columns() {
        columns.push(
            take(batch.column(i).as_ref(), indices, None)
                .map_err(|e| Error::Eval(e.to_string()))?,
        );
    }
    let out = Batch::try_new(batch.schema(), columns)?;
    Ok(out
        .split_by_policy(policy)
        .into_iter()
        .filter(|b| b.num_rows() > 0)
        .collect())
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
