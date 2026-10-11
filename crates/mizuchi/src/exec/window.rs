//! Window functions over sorted Arrow batches.
//!
//! The input is sorted by partition keys then order keys. Each partition is
//! scanned in order. Ranking uses order-key peers. Aggregates honour the
//! bound frame: a whole partition, or a running `ROWS` prefix.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, SchemaRef};

use super::CancelToken;
use super::Operator;
use super::key::{Scalar, eval_all, extract_key, scalar_at};
use super::memory::{self, MemoryPool, batch_bytes};
use super::sort;
use crate::batch::{Batch, BatchPolicy};
use crate::error::{Error, Result};
use crate::expr::Expr;
use crate::plan::{AggCall, FrameBound, FrameUnits, SortExpr, WindowCall, WindowFn, WindowFrame};

#[allow(clippy::too_many_arguments)]
pub(super) fn run(
    child: Box<dyn Operator>,
    input_schema: SchemaRef,
    partition: &[Expr],
    order: &[SortExpr],
    frame: WindowFrame,
    fns: &[WindowCall],
    schema: SchemaRef,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let sorted = sort_input(
        child,
        input_schema.clone(),
        partition,
        order,
        policy,
        pool,
        cancel,
    )?;
    if sorted.is_empty() {
        return Ok(Vec::new());
    }
    let input_bytes = sorted
        .iter()
        .map(batch_bytes)
        .fold(0u64, u64::saturating_add);
    let extra = (fns.len() as u64)
        .saturating_mul(sorted.iter().map(Batch::num_rows).sum::<usize>() as u64)
        .saturating_mul(8);
    // concat copies the input while those batches are still live
    let _hold = pool.try_reserve(
        input_bytes
            .saturating_add(input_bytes)
            .saturating_add(extra),
    )?;
    let batch = concat_input(&sorted, &input_schema)?;
    drop(sorted);
    let n = batch.num_rows();
    if n == 0 {
        return Ok(Vec::new());
    }
    cancel.check()?;
    let part_cols = eval_all(partition, &batch)?;
    let order_exprs: Vec<Expr> = order.iter().map(|s| s.expr.clone()).collect();
    let order_cols = eval_all(&order_exprs, &batch)?;
    let mut out_cols: Vec<ArrayRef> = Vec::with_capacity(fns.len());
    for call in fns {
        cancel.check()?;
        out_cols.push(eval_fn(
            &batch,
            n,
            &part_cols,
            &order_cols,
            frame,
            &call.func,
            cancel,
        )?);
    }
    let mut arrays = batch.as_arrow().columns().to_vec();
    arrays.extend(out_cols);
    let out = Batch::try_new(schema, arrays)?;
    Ok(out.split_by_policy(policy))
}

#[allow(clippy::too_many_arguments)]
fn sort_input(
    child: Box<dyn Operator>,
    schema: SchemaRef,
    partition: &[Expr],
    order: &[SortExpr],
    policy: &BatchPolicy,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let mut keys = Vec::new();
    for e in partition {
        keys.push(SortExpr {
            expr: e.clone(),
            asc: true,
            nulls_first: false,
        });
    }
    keys.extend(order.iter().cloned());
    if keys.is_empty() {
        return collect_all(child, pool, cancel);
    }
    sort::ordered(child, schema, &keys, 0, None, policy, pool, cancel)
}

fn collect_all(
    mut child: Box<dyn Operator>,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let mut out = Vec::new();
    let mut held: Vec<memory::Reservation<'_>> = Vec::new();
    while let Some(batch) = child.next_batch(cancel)? {
        if batch.num_rows() == 0 {
            continue;
        }
        let sz = batch_bytes(&batch);
        match pool.try_reserve(sz) {
            Ok(r) => {
                out.push(batch);
                held.push(r);
            }
            Err(e) => {
                if !pool.can_spill() {
                    return Err(e);
                }
                return Err(Error::Memory(
                    "window working set exceeds the memory limit".into(),
                ));
            }
        }
    }
    drop(held);
    Ok(out)
}

fn concat_input(batches: &[Batch], schema: &SchemaRef) -> Result<Batch> {
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }
    let rbs: Vec<_> = batches.iter().map(|b| b.as_arrow().clone()).collect();
    let concat = concat_batches(schema, &rbs).map_err(|e| Error::Eval(e.to_string()))?;
    Batch::from_arrow(concat)
}

fn eval_fn(
    batch: &Batch,
    n: usize,
    part_cols: &[ArrayRef],
    order_cols: &[ArrayRef],
    frame: WindowFrame,
    func: &WindowFn,
    cancel: &CancelToken,
) -> Result<ArrayRef> {
    match func {
        WindowFn::RowNumber | WindowFn::Rank | WindowFn::DenseRank => {
            ranking(n, part_cols, order_cols, func, cancel)
        }
        WindowFn::Agg(call) => aggregate(batch, n, part_cols, order_cols, frame, call, cancel),
    }
}

fn ranking(
    n: usize,
    part_cols: &[ArrayRef],
    order_cols: &[ArrayRef],
    func: &WindowFn,
    cancel: &CancelToken,
) -> Result<ArrayRef> {
    let mut values = vec![0i64; n];
    let mut i = 0;
    while i < n {
        cancel.check()?;
        let end = partition_end(part_cols, i, n)?;
        let mut rank = 1i64;
        let mut dense = 1i64;
        let mut prev_order: Option<Vec<Scalar>> = None;
        for (offset, row) in (i..end).enumerate() {
            let order = extract_key(order_cols, row)?;
            if offset > 0 && prev_order.as_ref() != Some(&order) {
                rank = offset as i64 + 1;
                dense += 1;
            }
            values[row] = match func {
                WindowFn::RowNumber => offset as i64 + 1,
                WindowFn::Rank => rank,
                WindowFn::DenseRank => dense,
                WindowFn::Agg(_) => unreachable!(),
            };
            prev_order = Some(order);
        }
        i = end;
    }
    Ok(Arc::new(Int64Array::from(values)))
}

fn aggregate(
    batch: &Batch,
    n: usize,
    part_cols: &[ArrayRef],
    order_cols: &[ArrayRef],
    frame: WindowFrame,
    call: &AggCall,
    cancel: &CancelToken,
) -> Result<ArrayRef> {
    let arg_expr = match call {
        AggCall::CountStar => None,
        AggCall::Count(e) | AggCall::Sum(e) => Some(e),
        other => {
            return Err(Error::NotImplemented(match other {
                AggCall::Avg(_) => "AVG window",
                AggCall::Min(_) => "MIN window",
                AggCall::Max(_) => "MAX window",
                AggCall::CountDistinct(_) => "COUNT DISTINCT window",
                AggCall::User { .. } => "user aggregate window",
                AggCall::CountStar | AggCall::Count(_) | AggCall::Sum(_) => unreachable!(),
            }));
        }
    };
    let arg = match arg_expr {
        Some(e) => Some(e.eval(batch)?),
        None => None,
    };
    let whole = frame.start == FrameBound::UnboundedPreceding
        && frame.end == FrameBound::UnboundedFollowing;
    let running =
        frame.start == FrameBound::UnboundedPreceding && frame.end == FrameBound::CurrentRow;
    if !whole && !running {
        return Err(Error::NotImplemented("window frame"));
    }
    let range_peers = running && frame.units == FrameUnits::Range;
    let dt = call.result_type(batch.schema().as_ref())?;
    let mut values: Vec<Option<i64>> = vec![None; n];
    let mut i = 0;
    while i < n {
        cancel.check()?;
        let end = partition_end(part_cols, i, n)?;
        if whole {
            let mut acc = Accum::new(call);
            for row in i..end {
                acc.update(arg.as_deref(), row)?;
            }
            let v = acc.finish();
            for slot in &mut values[i..end] {
                *slot = v;
            }
        } else if range_peers {
            let mut acc = Accum::new(call);
            let mut j = i;
            while j < end {
                let peer = partition_end(order_cols, j, end)?;
                for row in j..peer {
                    acc.update(arg.as_deref(), row)?;
                }
                let v = acc.finish();
                for slot in &mut values[j..peer] {
                    *slot = v;
                }
                j = peer;
            }
        } else {
            let mut acc = Accum::new(call);
            for (offset, slot) in values[i..end].iter_mut().enumerate() {
                acc.update(arg.as_deref(), i + offset)?;
                *slot = acc.finish();
            }
        }
        i = end;
    }
    finish_array(&values, &dt)
}

fn partition_end(part_cols: &[ArrayRef], start: usize, n: usize) -> Result<usize> {
    if part_cols.is_empty() {
        return Ok(n);
    }
    let key = extract_key(part_cols, start)?;
    let mut end = start + 1;
    while end < n && extract_key(part_cols, end)? == key {
        end += 1;
    }
    Ok(end)
}

struct Accum {
    count: i64,
    sum: i64,
    seen: bool,
    is_count: bool,
}

impl Accum {
    fn new(call: &AggCall) -> Self {
        Self {
            count: 0,
            sum: 0,
            seen: false,
            is_count: matches!(call, AggCall::CountStar | AggCall::Count(_)),
        }
    }

    fn update(&mut self, arg: Option<&dyn Array>, row: usize) -> Result<()> {
        if self.is_count {
            if let Some(array) = arg {
                if !array.is_null(row) {
                    self.count += 1;
                }
            } else {
                self.count += 1;
            }
            return Ok(());
        }
        let array = arg.ok_or_else(|| Error::Eval("SUM window needs an argument".into()))?;
        if array.is_null(row) {
            return Ok(());
        }
        self.sum = self
            .sum
            .checked_add(int_value(array, row)?)
            .ok_or_else(|| Error::Eval("SUM overflow".into()))?;
        self.seen = true;
        Ok(())
    }

    fn finish(&self) -> Option<i64> {
        if self.is_count {
            Some(self.count)
        } else if self.seen {
            Some(self.sum)
        } else {
            None
        }
    }
}

fn int_value(array: &dyn Array, row: usize) -> Result<i64> {
    scalar_at(array, row).and_then(|s| match s {
        Scalar::I8(v) => Ok(i64::from(v)),
        Scalar::I16(v) => Ok(i64::from(v)),
        Scalar::I32(v) => Ok(i64::from(v)),
        Scalar::I64(v) => Ok(v),
        Scalar::U8(v) => Ok(i64::from(v)),
        Scalar::U16(v) => Ok(i64::from(v)),
        Scalar::U32(v) => Ok(i64::from(v)),
        Scalar::U64(v) => i64::try_from(v).map_err(|_| Error::Eval("SUM overflow".into())),
        _ => Err(Error::Eval("SUM window expects an integer".into())),
    })
}

fn finish_array(values: &[Option<i64>], dt: &DataType) -> Result<ArrayRef> {
    match dt {
        DataType::Int64 => Ok(Arc::new(Int64Array::from(values.to_vec()))),
        DataType::Int32 => {
            let mut out = Vec::with_capacity(values.len());
            for v in values {
                out.push(match v {
                    None => None,
                    Some(n) => {
                        Some(i32::try_from(*n).map_err(|_| Error::Eval("SUM overflow".into()))?)
                    }
                });
            }
            Ok(Arc::new(Int32Array::from(out)))
        }
        other => Err(Error::Eval(format!(
            "unsupported window aggregate type {other}"
        ))),
    }
}
