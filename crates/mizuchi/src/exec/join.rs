//! Build/probe hash join over Arrow batches.
//!
//! Equality conjuncts in `ON` become hash keys. The right input is the build
//! side, the left input is probed. Null keys never match. Non-equality
//! conjuncts are a residual filter on candidate pairs. Joins without an
//! equality key (including CROSS) use a nested loop. LEFT SEMI / ANTI emit
//! the left schema only.

use std::collections::HashMap;

use arrow::array::{ArrayRef, BooleanArray, RecordBatch, UInt32Array, new_null_array};
use arrow::compute::kernels::zip::zip;
use arrow::compute::{concat_batches, take};
use arrow::datatypes::SchemaRef;

use super::key::{Scalar, extract_key};
use super::memory::{self, MemoryPool, batch_bytes};
use super::{CancelToken, Operator, filter_batch};
use crate::batch::{Batch, BatchPolicy};
use crate::error::{Error, Result};
use crate::expr::{BinaryOp, Expr};
use crate::plan::JoinType;

pub(super) struct JoinSpec<'a> {
    pub join_type: JoinType,
    pub on: Option<&'a Expr>,
    pub left_schema: SchemaRef,
    pub right_schema: SchemaRef,
    pub schema: SchemaRef,
}

pub(super) fn run(
    mut left: Box<dyn Operator>,
    mut right: Box<dyn Operator>,
    spec: JoinSpec<'_>,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let left_chunks = collect_chunks(&mut left, spec.left_schema.clone(), pool, cancel)?;
    let right_chunks = collect_chunks(&mut right, spec.right_schema.clone(), pool, cancel)?;
    let spilled = left_chunks.len() > 1 || right_chunks.len() > 1;
    if spilled
        && matches!(
            spec.join_type,
            JoinType::Left | JoinType::Right | JoinType::Full
        )
    {
        return Err(Error::Memory("outer join exceeded the memory limit".into()));
    }
    if matches!(spec.join_type, JoinType::Semi | JoinType::Anti) {
        return semi_anti_chunks(&left_chunks, &right_chunks, &spec, policy);
    }
    let mut parts = Vec::new();
    for l in &left_chunks {
        let l = load_chunk(l, spec.left_schema.clone())?;
        for r in &right_chunks {
            let r = load_chunk(r, spec.right_schema.clone())?;
            parts.push(join_pair(&l, &r, &spec, policy)?);
        }
    }
    Ok(parts
        .into_iter()
        .flatten()
        .filter(|b| b.num_rows() > 0)
        .collect())
}

enum Chunk {
    Mem(Batch),
    File(std::path::PathBuf),
}

fn load_chunk(chunk: &Chunk, schema: SchemaRef) -> Result<Batch> {
    match chunk {
        Chunk::Mem(b) => Ok(b.clone()),
        Chunk::File(p) => {
            let batches = memory::read_spill(p, schema.clone())?;
            concat_chunk(&batches, schema)
        }
    }
}

fn collect_chunks(
    exec: &mut dyn Operator,
    schema: SchemaRef,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Chunk>> {
    if !pool.has_limit() {
        return Ok(vec![Chunk::Mem(concat_exec(exec, schema, cancel)?)]);
    }
    let mut out = Vec::new();
    let mut cur: Vec<Batch> = Vec::new();
    let mut held: Vec<super::memory::Reservation<'_>> = Vec::new();
    while let Some(batch) = exec.next_batch(cancel)? {
        if batch.num_rows() == 0 {
            continue;
        }
        let sz = batch_bytes(&batch);
        match pool.try_reserve(sz) {
            Ok(r) => {
                cur.push(batch);
                held.push(r);
            }
            Err(e) => {
                if !pool.can_spill() {
                    return Err(e);
                }
                if cur.is_empty() {
                    return Err(Error::Memory(
                        "a single join batch exceeds the memory limit".into(),
                    ));
                }
                out.push(spill_chunk(&cur, schema.clone(), pool)?);
                held.clear();
                cur.clear();
                let r = pool.try_reserve(sz).map_err(|_| {
                    Error::Memory("a single join batch exceeds the memory limit".into())
                })?;
                cur.push(batch);
                held.push(r);
            }
        }
    }
    if !cur.is_empty() {
        if out.is_empty() {
            out.push(Chunk::Mem(concat_chunk(&cur, schema)?));
        } else {
            out.push(spill_chunk(&cur, schema, pool)?);
        }
    } else if out.is_empty() {
        out.push(Chunk::Mem(Batch::empty(schema)?));
    }
    Ok(out)
}

fn spill_chunk(cur: &[Batch], schema: SchemaRef, pool: &MemoryPool) -> Result<Chunk> {
    let batch = concat_chunk(cur, schema.clone())?;
    let path = pool.spill_path()?;
    memory::write_spill(&path, schema, std::slice::from_ref(&batch))?;
    Ok(Chunk::File(path))
}

fn concat_chunk(batches: &[Batch], schema: SchemaRef) -> Result<Batch> {
    if batches.is_empty() {
        return Batch::empty(schema);
    }
    let arrows: Vec<RecordBatch> = batches.iter().map(|b| b.as_arrow().clone()).collect();
    let inner = concat_batches(&schema, &arrows).map_err(|e| Error::Eval(e.to_string()))?;
    Batch::from_arrow(inner)
}

fn semi_anti_chunks(
    left_chunks: &[Chunk],
    right_chunks: &[Chunk],
    spec: &JoinSpec<'_>,
    policy: &BatchPolicy,
) -> Result<Vec<Batch>> {
    let keep_hit = spec.join_type == JoinType::Semi;
    let mut parts = Vec::new();
    for l in left_chunks {
        let left = load_chunk(l, spec.left_schema.clone())?;
        let mut hits = vec![false; left.num_rows()];
        for r in right_chunks {
            let right = load_chunk(r, spec.right_schema.clone())?;
            or_left_hits(&left, &right, spec, &mut hits)?;
        }
        let indices: Vec<u32> = hits
            .iter()
            .enumerate()
            .filter(|(_, hit)| **hit == keep_hit)
            .map(|(i, _)| i as u32)
            .collect();
        let out = take_left(&left, &indices, spec.schema.clone())?;
        parts.extend(
            out.split_by_policy(policy)
                .into_iter()
                .filter(|b| b.num_rows() > 0),
        );
    }
    Ok(parts)
}

fn or_left_hits(left: &Batch, right: &Batch, spec: &JoinSpec<'_>, hits: &mut [bool]) -> Result<()> {
    let split = match spec.on {
        Some(pred) => split_equi(pred, left.num_columns())?,
        None => EquiSplit {
            keys: Vec::new(),
            residual: None,
        },
    };
    if split.keys.is_empty() {
        let cartesian = split.residual.is_none();
        for (lrow, hit) in hits.iter_mut().enumerate() {
            if *hit {
                continue;
            }
            for rrow in 0..right.num_rows() {
                if cartesian || residual_ok(left, right, lrow, rrow, split.residual.as_ref())? {
                    *hit = true;
                    break;
                }
            }
        }
        return Ok(());
    }
    let left_keys: Vec<ArrayRef> = split
        .keys
        .iter()
        .map(|(l, _)| l.eval(left))
        .collect::<Result<_>>()?;
    let right_keys: Vec<ArrayRef> = split
        .keys
        .iter()
        .map(|(_, r)| r.eval(right))
        .collect::<Result<_>>()?;
    let mut table: HashMap<Vec<Scalar>, Vec<u32>> = HashMap::new();
    for row in 0..right.num_rows() {
        let key = extract_key(&right_keys, row)?;
        if key.contains(&Scalar::Null) {
            continue;
        }
        table.entry(key).or_default().push(row as u32);
    }
    for (lrow, hit) in hits.iter_mut().enumerate() {
        if *hit {
            continue;
        }
        let key = extract_key(&left_keys, lrow)?;
        if key.contains(&Scalar::Null) {
            continue;
        }
        if let Some(rows) = table.get(&key) {
            for &rrow in rows {
                if residual_ok(left, right, lrow, rrow as usize, split.residual.as_ref())? {
                    *hit = true;
                    break;
                }
            }
        }
    }
    Ok(())
}

fn join_pair(
    left_batch: &Batch,
    right_batch: &Batch,
    spec: &JoinSpec<'_>,
    policy: &BatchPolicy,
) -> Result<Vec<Batch>> {
    let left_ncols = left_batch.num_columns();
    let split = match spec.on {
        Some(pred) => split_equi(pred, left_ncols)?,
        None => EquiSplit {
            keys: Vec::new(),
            residual: None,
        },
    };
    let out = if split.keys.is_empty() {
        nested_loop(
            left_batch,
            right_batch,
            spec.join_type,
            split.residual.as_ref(),
            spec.schema.clone(),
        )?
    } else {
        hash_join(
            left_batch,
            right_batch,
            spec.join_type,
            &split.keys,
            split.residual.as_ref(),
            spec.schema.clone(),
        )?
    };
    Ok(out
        .split_by_policy(policy)
        .into_iter()
        .filter(|b| b.num_rows() > 0)
        .collect())
}

fn concat_exec(exec: &mut dyn Operator, schema: SchemaRef, cancel: &CancelToken) -> Result<Batch> {
    let mut batches = Vec::new();
    while let Some(batch) = exec.next_batch(cancel)? {
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

fn hash_join(
    left: &Batch,
    right: &Batch,
    join_type: JoinType,
    eq_keys: &[(Expr, Expr)],
    residual: Option<&Expr>,
    schema: SchemaRef,
) -> Result<Batch> {
    let left_keys: Vec<ArrayRef> = eq_keys
        .iter()
        .map(|(l, _)| l.eval(left))
        .collect::<Result<_>>()?;
    let right_keys: Vec<ArrayRef> = eq_keys
        .iter()
        .map(|(_, r)| r.eval(right))
        .collect::<Result<_>>()?;

    let mut table: HashMap<Vec<Scalar>, Vec<u32>> = HashMap::new();
    for row in 0..right.num_rows() {
        let key = extract_key(&right_keys, row)?;
        if key.contains(&Scalar::Null) {
            continue;
        }
        table.entry(key).or_default().push(row as u32);
    }

    let mut left_idx = Vec::new();
    let mut right_idx = Vec::new();
    let mut unmatched_left = Vec::new();
    let mut build_hit = vec![false; right.num_rows()];

    for lrow in 0..left.num_rows() {
        let key = extract_key(&left_keys, lrow)?;
        let hits = if key.contains(&Scalar::Null) {
            None
        } else {
            table.get(&key)
        };
        let mut matched = false;
        if let Some(hits) = hits {
            for &rrow in hits {
                if residual_ok(left, right, lrow, rrow as usize, residual)? {
                    matched = true;
                    build_hit[rrow as usize] = true;
                    left_idx.push(lrow as u32);
                    right_idx.push(rrow);
                }
            }
        }
        if !matched {
            unmatched_left.push(lrow as u32);
        }
    }

    match join_type {
        JoinType::Inner => hconcat_take(left, right, &left_idx, &right_idx, schema),
        JoinType::Left => {
            append_unmatched_left(&mut left_idx, &mut right_idx, &unmatched_left);
            hconcat_take(left, right, &left_idx, &right_idx, schema)
        }
        JoinType::Right => {
            append_unmatched_right(&mut left_idx, &mut right_idx, &build_hit);
            hconcat_take(left, right, &left_idx, &right_idx, schema)
        }
        JoinType::Full => {
            append_unmatched_left(&mut left_idx, &mut right_idx, &unmatched_left);
            append_unmatched_right(&mut left_idx, &mut right_idx, &build_hit);
            hconcat_take(left, right, &left_idx, &right_idx, schema)
        }
        JoinType::Semi => take_left(
            left,
            &matched_once(&unmatched_left, left.num_rows()),
            schema,
        ),
        JoinType::Anti => take_left(left, &unmatched_left, schema),
        JoinType::Cross => Err(Error::Eval(
            "internal: CROSS JOIN used the hash path".into(),
        )),
    }
}

fn nested_loop(
    left: &Batch,
    right: &Batch,
    join_type: JoinType,
    residual: Option<&Expr>,
    schema: SchemaRef,
) -> Result<Batch> {
    let mut left_idx = Vec::new();
    let mut right_idx = Vec::new();
    let mut unmatched_left = Vec::new();
    let mut build_hit = vec![false; right.num_rows()];
    let cartesian = residual.is_none();

    for lrow in 0..left.num_rows() {
        let mut matched = false;
        for (rrow, hit) in build_hit.iter_mut().enumerate() {
            if cartesian || residual_ok(left, right, lrow, rrow, residual)? {
                matched = true;
                *hit = true;
                left_idx.push(lrow as u32);
                right_idx.push(rrow as u32);
            }
        }
        if !matched {
            unmatched_left.push(lrow as u32);
        }
    }

    match join_type {
        JoinType::Inner | JoinType::Cross => {
            hconcat_take(left, right, &left_idx, &right_idx, schema)
        }
        JoinType::Left => {
            append_unmatched_left(&mut left_idx, &mut right_idx, &unmatched_left);
            hconcat_take(left, right, &left_idx, &right_idx, schema)
        }
        JoinType::Right => {
            append_unmatched_right(&mut left_idx, &mut right_idx, &build_hit);
            hconcat_take(left, right, &left_idx, &right_idx, schema)
        }
        JoinType::Full => {
            append_unmatched_left(&mut left_idx, &mut right_idx, &unmatched_left);
            append_unmatched_right(&mut left_idx, &mut right_idx, &build_hit);
            hconcat_take(left, right, &left_idx, &right_idx, schema)
        }
        JoinType::Semi => take_left(
            left,
            &matched_once(&unmatched_left, left.num_rows()),
            schema,
        ),
        JoinType::Anti => take_left(left, &unmatched_left, schema),
    }
}

fn matched_once(unmatched_left: &[u32], n_left: usize) -> Vec<u32> {
    let mut unmatched = vec![false; n_left];
    for &i in unmatched_left {
        unmatched[i as usize] = true;
    }
    (0..n_left as u32)
        .filter(|i| !unmatched[*i as usize])
        .collect()
}

fn append_unmatched_left(left_idx: &mut Vec<u32>, right_idx: &mut Vec<u32>, unmatched: &[u32]) {
    for &l in unmatched {
        left_idx.push(l);
        right_idx.push(u32::MAX);
    }
}

fn append_unmatched_right(left_idx: &mut Vec<u32>, right_idx: &mut Vec<u32>, build_hit: &[bool]) {
    for (r, hit) in build_hit.iter().enumerate() {
        if !hit {
            left_idx.push(u32::MAX);
            right_idx.push(r as u32);
        }
    }
}

fn residual_ok(
    left: &Batch,
    right: &Batch,
    lrow: usize,
    rrow: usize,
    residual: Option<&Expr>,
) -> Result<bool> {
    let Some(pred) = residual else {
        return Ok(true);
    };
    let pair = hconcat_take(
        left,
        right,
        &[lrow as u32],
        &[rrow as u32],
        join_pair_schema(left, right),
    )?;
    Ok(filter_batch(&pair, pred)?.is_some())
}

fn join_pair_schema(left: &Batch, right: &Batch) -> SchemaRef {
    crate::plan::join_schema(&left.schema(), &right.schema())
}

fn hconcat_take(
    left: &Batch,
    right: &Batch,
    left_idx: &[u32],
    right_idx: &[u32],
    schema: SchemaRef,
) -> Result<Batch> {
    debug_assert_eq!(left_idx.len(), right_idx.len());
    let n = left_idx.len();
    if n == 0 {
        return Batch::empty(schema);
    }
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    columns.extend(take_or_null(left, left_idx)?);
    columns.extend(take_or_null(right, right_idx)?);
    Batch::try_new(schema, columns)
}

fn take_or_null(batch: &Batch, indices: &[u32]) -> Result<Vec<ArrayRef>> {
    let mut out = Vec::with_capacity(batch.num_columns());
    for col in batch.as_arrow().columns() {
        out.push(take_with_nulls(col, indices)?);
    }
    Ok(out)
}

fn take_with_nulls(col: &ArrayRef, indices: &[u32]) -> Result<ArrayRef> {
    let mut builder_valid = Vec::with_capacity(indices.len());
    let mut take_idx = Vec::with_capacity(indices.len());
    for &i in indices {
        if i == u32::MAX {
            builder_valid.push(false);
            take_idx.push(0);
        } else {
            builder_valid.push(true);
            take_idx.push(i);
        }
    }
    if col.is_empty() {
        return Ok(new_null_array(col.data_type(), indices.len()));
    }
    let taken = take(col.as_ref(), &UInt32Array::from(take_idx), None)
        .map_err(|e| Error::Eval(e.to_string()))?;
    if builder_valid.iter().all(|v| *v) {
        return Ok(taken);
    }
    let mask = BooleanArray::from(builder_valid);
    let nulls = new_null_array(col.data_type(), indices.len());
    zip(&mask, &taken, &nulls).map_err(|e| Error::Eval(e.to_string()))
}

fn take_left(left: &Batch, indices: &[u32], schema: SchemaRef) -> Result<Batch> {
    if indices.is_empty() {
        return Batch::empty(schema);
    }
    let idx = UInt32Array::from(indices.to_vec());
    let mut columns = Vec::with_capacity(left.num_columns());
    for col in left.as_arrow().columns() {
        columns.push(take(col.as_ref(), &idx, None).map_err(|e| Error::Eval(e.to_string()))?);
    }
    Batch::try_new(schema, columns)
}

struct EquiSplit {
    keys: Vec<(Expr, Expr)>,
    residual: Option<Expr>,
}

pub(crate) fn equi_key_pairs(pred: &Expr, left_ncols: usize) -> Result<Vec<(Expr, Expr)>> {
    Ok(split_equi(pred, left_ncols)?.keys)
}

fn split_equi(pred: &Expr, left_ncols: usize) -> Result<EquiSplit> {
    let mut keys = Vec::new();
    let mut residuals = Vec::new();
    for conjunct in flatten_and(pred) {
        if let Expr::Binary(BinaryOp::Eq, l, r) = conjunct {
            let sl = expr_side(l, left_ncols)?;
            let sr = expr_side(r, left_ncols)?;
            match (sl, sr) {
                (Side::Left, Side::Right) => {
                    keys.push((l.as_ref().clone(), shift_right(r, left_ncols)));
                }
                (Side::Right, Side::Left) => {
                    keys.push((r.as_ref().clone(), shift_right(l, left_ncols)));
                }
                _ => residuals.push(conjunct.clone()),
            }
        } else {
            residuals.push(conjunct.clone());
        }
    }
    let residual = residuals
        .into_iter()
        .reduce(|a, b| Expr::Binary(BinaryOp::And, Box::new(a), Box::new(b)));
    Ok(EquiSplit { keys, residual })
}

fn flatten_and(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::Binary(BinaryOp::And, l, r) => {
            let mut out = flatten_and(l);
            out.extend(flatten_and(r));
            out
        }
        other => vec![other],
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Left,
    Right,
    Both,
    None,
}

fn combine(a: Side, b: Side) -> Side {
    match (a, b) {
        (Side::None, x) | (x, Side::None) => x,
        (Side::Left, Side::Left) => Side::Left,
        (Side::Right, Side::Right) => Side::Right,
        _ => Side::Both,
    }
}

fn expr_side(expr: &Expr, left_ncols: usize) -> Result<Side> {
    Ok(match expr {
        Expr::Nth(i) if *i < left_ncols => Side::Left,
        Expr::Nth(_) => Side::Right,
        Expr::Column(_) => Side::Both,
        Expr::Literal(_) => Side::None,
        Expr::Binary(_, l, r) => combine(expr_side(l, left_ncols)?, expr_side(r, left_ncols)?),
        Expr::Unary(_, e) | Expr::Cast { expr: e, .. } | Expr::Substring { expr: e, .. } => {
            expr_side(e, left_ncols)?
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let mut s = operand
                .as_deref()
                .map(|e| expr_side(e, left_ncols))
                .transpose()?
                .unwrap_or(Side::None);
            for (w, t) in when_then {
                s = combine(s, expr_side(w, left_ncols)?);
                s = combine(s, expr_side(t, left_ncols)?);
            }
            if let Some(e) = else_expr {
                s = combine(s, expr_side(e, left_ncols)?);
            }
            s
        }
        Expr::Coalesce(args) | Expr::Concat(args) | Expr::Array(args) => {
            let mut s = Side::None;
            for a in args {
                s = combine(s, expr_side(a, left_ncols)?);
            }
            s
        }
    })
}

fn shift_right(expr: &Expr, left_ncols: usize) -> Expr {
    match expr {
        Expr::Nth(i) if *i >= left_ncols => Expr::nth(*i - left_ncols),
        Expr::Nth(i) => Expr::nth(*i),
        Expr::Column(name) => Expr::column(name.clone()),
        Expr::Literal(v) => Expr::Literal(v.clone()),
        Expr::Binary(op, l, r) => Expr::Binary(
            *op,
            Box::new(shift_right(l, left_ncols)),
            Box::new(shift_right(r, left_ncols)),
        ),
        Expr::Unary(op, e) => Expr::Unary(*op, Box::new(shift_right(e, left_ncols))),
        Expr::Cast { expr, data_type } => Expr::Cast {
            expr: Box::new(shift_right(expr, left_ncols)),
            data_type: data_type.clone(),
        },
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => Expr::Case {
            operand: operand
                .as_deref()
                .map(|e| Box::new(shift_right(e, left_ncols))),
            when_then: when_then
                .iter()
                .map(|(w, t)| (shift_right(w, left_ncols), shift_right(t, left_ncols)))
                .collect(),
            else_expr: else_expr
                .as_deref()
                .map(|e| Box::new(shift_right(e, left_ncols))),
        },
        Expr::Coalesce(args) => {
            Expr::Coalesce(args.iter().map(|e| shift_right(e, left_ncols)).collect())
        }
        Expr::Concat(args) => {
            Expr::Concat(args.iter().map(|e| shift_right(e, left_ncols)).collect())
        }
        Expr::Substring {
            expr,
            start,
            length,
        } => Expr::Substring {
            expr: Box::new(shift_right(expr, left_ncols)),
            start: *start,
            length: *length,
        },
        Expr::Array(args) => Expr::Array(args.iter().map(|e| shift_right(e, left_ncols)).collect()),
    }
}
