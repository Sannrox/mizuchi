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
use mizuchi_core::batch::{Batch, BatchPolicy};
use mizuchi_core::error::{Error, Result};
use mizuchi_plan::SortExpr;

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
    if let Some(fetch) = fetch
        && skip.saturating_add(fetch) <= TOP_K_MAX_ROWS
    {
        return top_k(child, schema, order, skip, fetch, policy, pool, cancel);
    }
    external_sort(child, schema, order, skip, fetch, policy, pool, cancel)
}

/// Memory-limited sort: reserve each batch, spill sorted runs when the next
/// batch does not fit, then merge the runs.
#[allow(clippy::too_many_arguments)]
fn external_sort(
    mut child: Box<dyn Operator>,
    schema: SchemaRef,
    order: &[SortExpr],
    skip: u64,
    fetch: Option<u64>,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
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

/// Largest `skip + fetch` a memory-limited sort keeps as a running top-k.
const TOP_K_MAX_ROWS: u64 = 65_536;

/// Memory-limited ORDER BY ... LIMIT: fold each input batch into the best
/// `skip + fetch` rows seen so far. The kept rows are copied out of their
/// input, so the reservation covers exactly what is retained and an input
/// batch (often a slice of a much larger parent) is charged only while it is
/// merged. When a reservation fails and spilling is available, the kept rows
/// and the rest of the input go to [`external_sort`]; without spilling the
/// pool error is returned, as the external sort would.
#[allow(clippy::too_many_arguments)]
fn top_k(
    mut child: Box<dyn Operator>,
    schema: SchemaRef,
    order: &[SortExpr],
    skip: u64,
    fetch: u64,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let k = skip.saturating_add(fetch);
    let unlimited = BatchPolicy::unlimited();
    let mut kept: Option<Batch> = None;
    let mut kept_held: Option<memory::Reservation<'_>> = None;
    // Batches still owned by this sort when a reservation fails.
    let pending: Vec<Batch> = loop {
        let Some(batch) = child.next_batch(cancel)? else {
            return match kept {
                Some(kept) => sort_batch(&kept, order, skip, Some(fetch), policy, pool),
                None => empty_out(schema, policy),
            };
        };
        if batch.num_rows() == 0 {
            continue;
        }
        let incoming = match pool.try_reserve(batch_bytes(&batch)) {
            Ok(r) => r,
            Err(e) if !pool.can_spill() => return Err(e),
            Err(_) => break kept.take().into_iter().chain([batch]).collect(),
        };
        let mut candidates = batches_to_exec(
            kept.take().into_iter().chain([batch]).collect(),
            schema.clone(),
        );
        let best = sort_concat(
            &mut *candidates,
            schema.clone(),
            order,
            0,
            Some(k),
            &unlimited,
            pool,
            cancel,
        )?;
        let best = compact(best, schema.clone(), pool)?;
        drop(candidates);
        // The consumed input and the old kept rows are released before the
        // new kept rows are reserved: the charge moves, it is never doubled.
        drop(incoming);
        drop(kept_held.take());
        match pool.try_reserve(batch_bytes(&best)) {
            Ok(r) => kept_held = Some(r),
            Err(e) if !pool.can_spill() => return Err(e),
            Err(_) => break vec![best],
        }
        kept = Some(best);
    };
    drop(kept_held);
    let rest = Box::new(Chain {
        front: pending.into_iter(),
        rest: child,
    });
    external_sort(rest, schema, order, skip, Some(fetch), policy, pool, cancel)
}

/// Yields `front`, then every batch of `rest`.
struct Chain {
    front: std::vec::IntoIter<Batch>,
    rest: Box<dyn Operator>,
}

impl Operator for Chain {
    fn next_batch(&mut self, cancel: &CancelToken) -> Result<Option<Batch>> {
        match self.front.next() {
            Some(batch) => Ok(Some(batch)),
            None => self.rest.next_batch(cancel),
        }
    }
}

/// One freshly allocated batch holding `batches`' rows, so nothing it keeps
/// shares a buffer with a larger parent.
fn compact(batches: Vec<Batch>, schema: SchemaRef, pool: &MemoryPool) -> Result<Batch> {
    let arrows: Vec<RecordBatch> = batches.into_iter().map(Batch::into_arrow).collect();
    let joined = Batch::from_arrow(
        concat_batches(&schema, &arrows).map_err(|e| Error::Eval(e.to_string()))?,
    )?;
    let n = u32::try_from(joined.num_rows())
        .map_err(|_| Error::Eval("top-k exceeds u32 row indices".into()))?;
    take_all_columns(&joined, &UInt32Array::from_iter_values(0..n), pool)
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
    pool.write_spill_file(schema, &sorted)
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
        let left = pool.read_spill(&a, schema.clone())?;
        let right = pool.read_spill(&b, schema.clone())?;
        let merged = merge_sorted_batches(left, right, schema.clone(), order, pool, cancel)?;
        let path = pool.write_spill_file(schema.clone(), &merged)?;
        runs.push(path);
    }
    match runs.pop() {
        Some(path) => pool.read_spill(&path, schema),
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
            if mizuchi_core::batch::contiguous_indices(values).is_none() {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{AsArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use mizuchi_core::expr::Expr;

    use super::*;

    #[test]
    fn limited_top_k_charges_kept_rows_not_each_slice_parent() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let parent = Batch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(0..65_536))],
        )
        .unwrap();
        // Sixteen zero-copy slices, each charged for the whole parent.
        let slices = parent.split_by_policy(&BatchPolicy::default());
        assert_eq!(slices.len(), 16);
        let cap = 3 * batch_bytes(&parent);
        let pool = MemoryPool::new(Some(cap), None).unwrap();
        let order = [SortExpr {
            expr: Expr::nth(0),
            asc: false,
            nulls_first: false,
        }];
        let out = ordered(
            batches_to_exec(slices, schema.clone()),
            schema,
            &order,
            2,
            Some(3),
            &BatchPolicy::default(),
            &pool,
            &CancelToken::new(),
        )
        .unwrap();
        let values: Vec<i64> = out
            .iter()
            .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
            .collect();
        assert_eq!(values, [65_533, 65_532, 65_531]);
        // One slice in flight plus the kept rows, never all sixteen parents.
        assert!(pool.peak() <= cap, "peak {} over cap {cap}", pool.peak());
    }

    /// Eight separately allocated 4,096-row batches of `v` in ascending order.
    fn wide_batches(schema: &SchemaRef) -> Vec<Batch> {
        (0..8)
            .map(|i| {
                Batch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int64Array::from_iter_values(
                        i * 4096..(i + 1) * 4096,
                    ))],
                )
                .unwrap()
            })
            .collect()
    }

    fn top_desc(
        batches: Vec<Batch>,
        schema: SchemaRef,
        fetch: u64,
        pool: &MemoryPool,
    ) -> Result<Vec<i64>> {
        let order = [SortExpr {
            expr: Expr::nth(0),
            asc: false,
            nulls_first: false,
        }];
        let out = ordered(
            batches_to_exec(batches, schema.clone()),
            schema,
            &order,
            0,
            Some(fetch),
            &BatchPolicy::default(),
            pool,
            &CancelToken::new(),
        )?;
        Ok(out
            .iter()
            .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
            .collect())
    }

    #[test]
    fn limited_top_k_falls_back_to_spilling_when_kept_rows_and_input_overflow() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batches = wide_batches(&schema);
        // 6,000 kept rows plus one more batch do not fit; two batches do.
        let cap = batch_bytes(&batches[0]) * 22 / 10;
        let root = std::env::temp_dir().join(format!(
            "mizuchi-topk-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let pool = MemoryPool::new(Some(cap), Some(root.clone())).unwrap();
        let values = top_desc(batches.clone(), schema.clone(), 6_000, &pool);
        let spilled = pool.spilled();
        drop(pool);
        let _ = std::fs::remove_dir_all(&root);
        let expected: Vec<i64> = (8 * 4096 - 6_000..8 * 4096).rev().collect();
        assert_eq!(values.unwrap(), expected);
        assert!(spilled > 0, "expected the external sort to spill");
        // Without a spill directory the pool error is kept.
        let pool = MemoryPool::new(Some(cap), None).unwrap();
        assert!(matches!(
            top_desc(batches, schema, 6_000, &pool),
            Err(Error::Memory(_))
        ));
    }

    #[test]
    fn limited_top_k_accepts_a_batch_that_uses_the_whole_budget() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = wide_batches(&schema).remove(0);
        let pool = MemoryPool::new(Some(batch_bytes(&batch)), None).unwrap();
        assert_eq!(
            top_desc(vec![batch], schema, 3, &pool).unwrap(),
            [4095, 4094, 4093]
        );
    }
}
