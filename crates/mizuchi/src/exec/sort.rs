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
) -> Result<Vec<Batch>> {
    if matches!(fetch, Some(0)) {
        return Ok(Vec::new());
    }
    let batch = concat_exec(&mut child, schema.clone())?;
    let n = batch.num_rows();
    let skip = clamp_rows(skip, n);
    if skip >= n || n == 0 {
        return empty_out(schema, policy);
    }
    if n > u32::MAX as usize {
        return Err(Error::Eval("sort input exceeds u32 row indices".into()));
    }
    let mut columns = Vec::with_capacity(order.len());
    for key in order {
        let values = key.expr.eval(&batch)?;
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
    take_rows(&batch, &indices.slice(start, len), policy)
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
