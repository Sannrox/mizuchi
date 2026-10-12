//! Build/probe hash join over Arrow batches.
//!
//! Equality conjuncts in `ON` become hash keys. Inner joins build the smaller
//! collected side. A single integer column uses a compact range map when keys
//! sit in a tight span, otherwise a cheap hash map. Unique i64 keys probe
//! with a direct index. Other join types probe left and build right. Inner
//! probe ranges run on `Session::workers` threads. Null keys never match.
//! Non-equality conjuncts are a residual filter on candidate pairs. Joins
//! without an equality key (including CROSS) use a nested loop. LEFT SEMI /
//! ANTI emit the left schema only.

use std::hash::Hash;
use std::sync::Mutex;
use std::thread;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, PrimitiveArray, RecordBatch, UInt32Array,
    new_null_array,
};
use arrow::compute::kernels::zip::zip;
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Float64Type, Int32Type, Int64Type, SchemaRef, UInt32Type,
    UInt64Type,
};

use super::agg;
use super::key::{
    FastMap, IdentityMap, fast_map_with_capacity, hash_row, identity_map_with_capacity,
    row_has_null, rows_equal,
};
use super::memory::{self, MemoryPool, batch_bytes};
use super::{CancelToken, Exec, Operator, Session, filter_batch};
use crate::batch::{Batch, BatchPolicy};
use crate::error::{Error, Result};
use crate::expr::{BinaryOp, Expr};
use crate::plan::{AggCall, JoinType, NamedExpr};

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
    workers: usize,
) -> Result<Vec<Batch>> {
    let left_chunks = collect_chunks(&mut left, spec.left_schema.clone(), pool, cancel)?;
    let right_chunks = collect_chunks(&mut right, spec.right_schema.clone(), pool, cancel)?;
    let spilled = chunks_on_disk(&left_chunks) || chunks_on_disk(&right_chunks);
    if spilled
        && matches!(
            spec.join_type,
            JoinType::Left | JoinType::Right | JoinType::Full
        )
    {
        return Err(Error::Memory("outer join exceeded the memory limit".into()));
    }
    if matches!(
        spec.join_type,
        JoinType::Semi | JoinType::Anti | JoinType::NullAwareAnti
    ) {
        return semi_anti_chunks(&left_chunks, &right_chunks, &spec, policy, pool);
    }
    if spec.join_type == JoinType::Inner {
        if spilled {
            let mut parts = Vec::new();
            for l in &left_chunks {
                let l = load_chunk(l, spec.left_schema.clone(), pool)?;
                for r in &right_chunks {
                    let r = load_chunk(r, spec.right_schema.clone(), pool)?;
                    parts.push(join_pair(&l, &r, &spec, policy, workers, pool)?);
                }
            }
            return Ok(parts
                .into_iter()
                .flatten()
                .filter(|b| b.num_rows() > 0)
                .collect());
        }
        let left = take_batches(left_chunks, spec.left_schema.clone())?;
        let right = take_batches(right_chunks, spec.right_schema.clone())?;
        if left.len() == 1 && right.len() == 1 {
            return join_pair(&left[0], &right[0], &spec, policy, workers, pool);
        }
        return hash_join_streamed(&left, &right, &spec, policy, workers, pool);
    }
    // Unmatched rows for LEFT/RIGHT/FULL are relative to the whole opposite
    // side. Concatenate once, then emit unmatched rows a single time.
    let left = concat_chunk(
        &take_batches(left_chunks, spec.left_schema.clone())?,
        spec.left_schema.clone(),
        pool,
    )?;
    let right = concat_chunk(
        &take_batches(right_chunks, spec.right_schema.clone())?,
        spec.right_schema.clone(),
        pool,
    )?;
    join_pair(&left, &right, &spec, policy, workers, pool)
}

/// Inner join whose output is aggregated immediately so the wide join
/// result is never retained.
#[allow(clippy::too_many_arguments)]
pub(super) fn run_then_agg(
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    spec: JoinSpec<'_>,
    group: &[NamedExpr],
    aggs: &[(String, AggCall)],
    out_schema: SchemaRef,
    session: &Session,
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    let mut left = left;
    let mut right = right;
    let left_chunks = collect_chunks(&mut left, spec.left_schema.clone(), pool, &session.cancel)?;
    let right_chunks =
        collect_chunks(&mut right, spec.right_schema.clone(), pool, &session.cancel)?;
    if pool.has_limit() {
        let mut joined = Vec::new();
        for l in &left_chunks {
            let l = load_chunk(l, spec.left_schema.clone(), pool)?;
            for r in &right_chunks {
                let r = load_chunk(r, spec.right_schema.clone(), pool)?;
                joined.extend(join_pair(
                    &l,
                    &r,
                    &spec,
                    &session.policy,
                    session.workers,
                    pool,
                )?);
            }
        }
        if joined.is_empty() {
            return agg::merge_finish(Vec::new(), group, aggs, out_schema, &session.policy);
        }
        return agg::run(
            Box::new(Exec::Scan {
                batches: joined,
                index: 0,
            }),
            group,
            aggs,
            out_schema,
            &session.policy,
            pool,
            &session.cancel,
        );
    }
    let left = take_batches(left_chunks, spec.left_schema.clone())?;
    let right = take_batches(right_chunks, spec.right_schema.clone())?;
    if spec.join_type == JoinType::Inner
        && let Some(out) = fold_unique_i64_join(
            &left,
            &right,
            &spec,
            group,
            aggs,
            out_schema.clone(),
            session,
            pool,
        )?
    {
        return Ok(out);
    }
    let mut joined = Vec::new();
    for l in &left {
        for r in &right {
            joined.extend(join_pair(
                l,
                r,
                &spec,
                &session.policy,
                session.workers,
                pool,
            )?);
        }
    }
    if joined.is_empty() {
        return agg::merge_finish(Vec::new(), group, aggs, out_schema, &session.policy);
    }
    let n = session.workers.max(1).min(joined.len());
    let mut chunks = vec![Vec::new(); n];
    for (i, batch) in joined.into_iter().enumerate() {
        chunks[i % n].push(batch);
    }
    let tables = Mutex::new(Vec::new());
    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for chunk in chunks {
            let tables = &tables;
            let out_schema = out_schema.clone();
            handles.push(scope.spawn(move || -> Result<()> {
                session.cancel.check()?;
                let table = agg::fold_batches(chunk, group, aggs, out_schema)?;
                tables
                    .lock()
                    .map_err(|_| Error::Eval("agg combine lock poisoned".into()))?
                    .push(table);
                Ok(())
            }));
        }
        for h in handles {
            h.join()
                .map_err(|_| Error::Eval("worker panicked".into()))??;
        }
        Ok(())
    })?;
    let tables = tables
        .into_inner()
        .map_err(|_| Error::Eval("agg combine lock poisoned".into()))?;
    agg::merge_finish(tables, group, aggs, out_schema, &session.policy)
}

#[allow(clippy::too_many_arguments)]
fn fold_unique_i64_join(
    left: &[Batch],
    right: &[Batch],
    spec: &JoinSpec<'_>,
    group: &[NamedExpr],
    aggs: &[(String, AggCall)],
    out_schema: SchemaRef,
    session: &Session,
    pool: &MemoryPool,
) -> Result<Option<Vec<Batch>>> {
    let left_ncols = spec.left_schema.fields().len();
    let split = match spec.on {
        Some(pred) => split_equi(pred, left_ncols)?,
        None => return Ok(None),
    };
    if split.keys.is_empty() || split.residual.is_some() {
        return Ok(None);
    }
    let ln: usize = left.iter().map(Batch::num_rows).sum();
    let rn: usize = right.iter().map(Batch::num_rows).sum();
    if ln == 0 || rn == 0 {
        return Ok(None);
    }
    let build_left = ln < rn;
    let (build_batches, probe_batches, build_schema) = if build_left {
        (left, right, spec.left_schema.clone())
    } else {
        (right, left, spec.right_schema.clone())
    };
    let build = concat_chunk(build_batches, build_schema, pool)?;
    let n_build = build.num_rows();
    let build_keys: Vec<ArrayRef> = if build_left {
        split
            .keys
            .iter()
            .map(|(l, _)| l.eval(&build))
            .collect::<Result<_>>()?
    } else {
        split
            .keys
            .iter()
            .map(|(_, r)| r.eval(&build))
            .collect::<Result<_>>()?
    };
    let probe_key_exprs: Vec<&Expr> = if build_left {
        split.keys.iter().map(|(_, r)| r).collect()
    } else {
        split.keys.iter().map(|(l, _)| l).collect()
    };
    let Some(sample) = probe_batches.iter().find(|b| b.num_rows() > 0) else {
        return Ok(None);
    };
    let sample_keys: Vec<ArrayRef> = probe_key_exprs
        .iter()
        .map(|e| e.eval(sample))
        .collect::<Result<_>>()?;
    let table = BuildTable::new(&build_keys, &sample_keys, n_build)?;
    if !table.has_unique_i64(&sample_keys) {
        return Ok(None);
    }
    fold_into_unique_slots(
        &build,
        probe_batches,
        &table,
        &probe_key_exprs,
        build_left,
        left_ncols,
        &split,
        group,
        aggs,
        out_schema,
        session,
        n_build,
    )
}

/// When grouping keys are determined by the unique build row and aggregate
/// arguments live on the probe side, update accumulators in those build
/// slots and never assemble the join result. The smaller side is concatenated;
/// probe morsels stay streamed.
#[allow(clippy::too_many_arguments)]
fn fold_into_unique_slots(
    build: &Batch,
    probe_batches: &[Batch],
    table: &BuildTable,
    probe_key_exprs: &[&Expr],
    build_left: bool,
    left_ncols: usize,
    split: &EquiSplit,
    group: &[NamedExpr],
    aggs: &[(String, AggCall)],
    out_schema: SchemaRef,
    session: &Session,
    n_build: usize,
) -> Result<Option<Vec<Batch>>> {
    let Some((min, slots)) = table.unique_i64_slots() else {
        return Ok(None);
    };
    let mut build_group = Vec::with_capacity(group.len());
    for g in group {
        let Some(expr) = onto_build(&g.expr, left_ncols, build_left, split) else {
            return Ok(None);
        };
        build_group.push(expr);
    }
    let mut probe_aggs = Vec::with_capacity(aggs.len());
    for (_, call) in aggs {
        let Some(rewritten) = agg_onto_probe(call, left_ncols, build_left) else {
            return Ok(None);
        };
        probe_aggs.push(rewritten);
    }
    let group_cols = if build_group.is_empty() {
        Vec::new()
    } else {
        super::key::eval_all(&build_group, build)?
    };
    let unique_groups = split.keys.len() == 1 && {
        let build_key = if build_left {
            &split.keys[0].0
        } else {
            &split.keys[0].1
        };
        build_group.iter().any(|e| expr_same(e, build_key))
            || group.iter().any(|g| expr_same(&g.expr, build_key))
    };
    let n = session.workers.max(1).min(probe_batches.len().max(1));
    let queue = Mutex::new((0..probe_batches.len()).collect::<Vec<usize>>());
    let group_cols = &group_cols;
    let probe_aggs = &probe_aggs;
    let one_f64_sum = unique_groups
        && aggs.len() == 1
        && matches!(aggs[0].1, AggCall::Sum(_))
        && out_schema.field(group.len()).data_type() == &DataType::Float64;
    if one_f64_sum {
        let mut sums = agg::BuildF64Sums::new(out_schema.clone(), group.len(), n_build);
        for probe in probe_batches {
            session.cancel.check()?;
            if probe.num_rows() == 0 {
                continue;
            }
            let probe_keys: Vec<ArrayRef> = probe_key_exprs
                .iter()
                .map(|e| e.eval(probe))
                .collect::<Result<_>>()?;
            let keys = probe_keys[0].as_primitive::<Int64Type>();
            let arg = match &probe_aggs[0] {
                AggCall::Sum(e) => e.eval(probe)?,
                _ => unreachable!("one_f64_sum"),
            };
            let vals = arg.as_primitive::<Float64Type>();
            for prow in 0..probe.num_rows() {
                let Some(brow) = unique_i64_brow(min, slots, keys, prow) else {
                    continue;
                };
                if !vals.is_null(prow) {
                    sums.add(brow as usize, vals.value(prow));
                }
            }
        }
        return Ok(Some(sums.finish(group_cols, &session.policy)?));
    }
    if unique_groups {
        let parts = Mutex::new(Vec::new());
        let parts_ref = &parts;
        thread::scope(|scope| -> Result<()> {
            let mut handles = Vec::new();
            for _ in 0..n {
                let out_schema = out_schema.clone();
                let queue = &queue;
                handles.push(scope.spawn(move || -> Result<()> {
                    session.cancel.check()?;
                    let mut slots_acc = agg::BuildSlots::new(group, aggs, out_schema, n_build)?;
                    drain_probe_queue(
                        queue,
                        probe_batches,
                        probe_key_exprs,
                        probe_aggs,
                        min,
                        slots,
                        |brow, arg_cols, prow| slots_acc.add(brow, arg_cols, prow),
                    )?;
                    parts_ref
                        .lock()
                        .map_err(|_| Error::Eval("agg combine lock poisoned".into()))?
                        .push(slots_acc);
                    Ok(())
                }));
            }
            for h in handles {
                h.join()
                    .map_err(|_| Error::Eval("worker panicked".into()))??;
            }
            Ok(())
        })?;
        let mut parts = parts
            .into_inner()
            .map_err(|_| Error::Eval("agg combine lock poisoned".into()))?;
        let Some(mut acc) = parts.pop() else {
            return Ok(Some(Vec::new()));
        };
        for part in parts {
            acc.merge_from(part)?;
        }
        return Ok(Some(acc.finish(group_cols, &session.policy)?));
    }
    let tables = Mutex::new(Vec::new());
    let tables_ref = &tables;
    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for _ in 0..n {
            let out_schema = out_schema.clone();
            let queue = &queue;
            handles.push(scope.spawn(move || -> Result<()> {
                session.cancel.check()?;
                let mut fold = agg::BuildFold::new(group, aggs, out_schema, n_build)?;
                drain_probe_queue(
                    queue,
                    probe_batches,
                    probe_key_exprs,
                    probe_aggs,
                    min,
                    slots,
                    |brow, arg_cols, prow| fold.add(group_cols, brow, arg_cols, prow),
                )?;
                tables_ref
                    .lock()
                    .map_err(|_| Error::Eval("agg combine lock poisoned".into()))?
                    .push(fold.into_table());
                Ok(())
            }));
        }
        for h in handles {
            h.join()
                .map_err(|_| Error::Eval("worker panicked".into()))??;
        }
        Ok(())
    })?;
    let tables = tables
        .into_inner()
        .map_err(|_| Error::Eval("agg combine lock poisoned".into()))?;
    Ok(Some(agg::merge_finish(
        tables,
        group,
        aggs,
        out_schema,
        &session.policy,
    )?))
}

fn drain_probe_queue(
    queue: &Mutex<Vec<usize>>,
    probe_batches: &[Batch],
    probe_key_exprs: &[&Expr],
    probe_aggs: &[AggCall],
    min: i128,
    slots: &[u32],
    mut on_match: impl FnMut(usize, &[Option<ArrayRef>], usize) -> Result<()>,
) -> Result<()> {
    loop {
        let i = {
            let mut q = queue
                .lock()
                .map_err(|_| Error::Eval("agg combine lock poisoned".into()))?;
            q.pop()
        };
        let Some(i) = i else {
            return Ok(());
        };
        let probe = &probe_batches[i];
        if probe.num_rows() == 0 {
            continue;
        }
        let probe_keys: Vec<ArrayRef> = probe_key_exprs
            .iter()
            .map(|e| e.eval(probe))
            .collect::<Result<_>>()?;
        let arr = probe_keys[0].as_primitive::<Int64Type>();
        let mut arg_cols: Vec<Option<ArrayRef>> = Vec::with_capacity(probe_aggs.len());
        for call in probe_aggs {
            arg_cols.push(match call {
                AggCall::CountStar => None,
                AggCall::Count(e)
                | AggCall::CountDistinct(e)
                | AggCall::Sum(e)
                | AggCall::Avg(e)
                | AggCall::Min(e)
                | AggCall::Max(e)
                | AggCall::User { arg: e, .. } => Some(e.eval(probe)?),
            });
        }
        for prow in 0..probe.num_rows() {
            let Some(brow) = unique_i64_brow(min, slots, arr, prow) else {
                continue;
            };
            on_match(brow as usize, &arg_cols, prow)?;
        }
    }
}

enum Chunk {
    Mem(Batch),
    File(std::path::PathBuf),
}

fn chunks_on_disk(chunks: &[Chunk]) -> bool {
    chunks.iter().any(|c| matches!(c, Chunk::File(_)))
}

fn load_chunk(chunk: &Chunk, schema: SchemaRef, pool: &MemoryPool) -> Result<Batch> {
    match chunk {
        Chunk::Mem(b) => Ok(b.clone()),
        Chunk::File(p) => {
            let batches = memory::read_spill(p, schema.clone())?;
            concat_chunk(&batches, schema, pool)
        }
    }
}

fn take_batches(chunks: Vec<Chunk>, schema: SchemaRef) -> Result<Vec<Batch>> {
    let mut out = Vec::new();
    for chunk in chunks {
        match chunk {
            Chunk::Mem(b) => {
                if b.num_rows() > 0 {
                    out.push(b);
                }
            }
            Chunk::File(p) => {
                for b in memory::read_spill(&p, schema.clone())? {
                    if b.num_rows() > 0 {
                        out.push(b);
                    }
                }
            }
        }
    }
    if out.is_empty() {
        out.push(Batch::empty(schema)?);
    }
    Ok(out)
}

fn collect_chunks(
    exec: &mut dyn Operator,
    schema: SchemaRef,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Chunk>> {
    if !pool.has_limit() {
        let mut out = Vec::new();
        while let Some(batch) = exec.next_batch(cancel)? {
            if batch.num_rows() > 0 {
                out.push(Chunk::Mem(batch));
            }
        }
        if out.is_empty() {
            out.push(Chunk::Mem(Batch::empty(schema)?));
        }
        return Ok(out);
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
            out.push(Chunk::Mem(concat_chunk(&cur, schema, pool)?));
        } else {
            out.push(spill_chunk(&cur, schema, pool)?);
        }
    } else if out.is_empty() {
        out.push(Chunk::Mem(Batch::empty(schema)?));
    }
    Ok(out)
}

fn spill_chunk(cur: &[Batch], schema: SchemaRef, pool: &MemoryPool) -> Result<Chunk> {
    let batch = concat_chunk(cur, schema.clone(), pool)?;
    let path = pool.spill_path()?;
    memory::write_spill(&path, schema, std::slice::from_ref(&batch))?;
    Ok(Chunk::File(path))
}

fn concat_chunk(batches: &[Batch], schema: SchemaRef, pool: &MemoryPool) -> Result<Batch> {
    match batches {
        [] => Batch::empty(schema),
        [one] if one.num_columns() == 0 => Ok(one.clone()),
        [one] => Batch::try_new(schema, one.as_arrow().columns().to_vec()),
        _ => {
            let arrows: Vec<RecordBatch> = batches.iter().map(|b| b.as_arrow().clone()).collect();
            let inner = concat_batches(&schema, &arrows).map_err(|e| Error::Eval(e.to_string()))?;
            let out = Batch::from_arrow(inner)?;
            pool.record_copy(batch_bytes(&out));
            Ok(out)
        }
    }
}

fn semi_anti_chunks(
    left_chunks: &[Chunk],
    right_chunks: &[Chunk],
    spec: &JoinSpec<'_>,
    policy: &BatchPolicy,
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    let keep_hit = spec.join_type == JoinType::Semi;
    let naaj = spec.join_type == JoinType::NullAwareAnti;
    let mut right_rows = 0usize;
    if naaj {
        for r in right_chunks {
            let right = load_chunk(r, spec.right_schema.clone(), pool)?;
            right_rows += right.num_rows();
            if right_has_null_key(&right, spec)? {
                return Ok(Vec::new());
            }
        }
    }
    let mut parts = Vec::new();
    for l in left_chunks {
        let left = load_chunk(l, spec.left_schema.clone(), pool)?;
        let mut hits = vec![false; left.num_rows()];
        for r in right_chunks {
            let right = load_chunk(r, spec.right_schema.clone(), pool)?;
            or_left_hits(&left, &right, spec, &mut hits, pool)?;
        }
        if naaj && right_rows > 0 {
            mark_null_left_keys(&left, spec, &mut hits)?;
        }
        let indices: Vec<u32> = hits
            .iter()
            .enumerate()
            .filter(|(_, hit)| **hit == keep_hit)
            .map(|(i, _)| i as u32)
            .collect();
        let out = take_left(&left, &indices, spec.schema.clone(), pool)?;
        parts.extend(
            out.split_by_policy(policy)
                .into_iter()
                .filter(|b| b.num_rows() > 0),
        );
    }
    Ok(parts)
}

fn or_left_hits(
    left: &Batch,
    right: &Batch,
    spec: &JoinSpec<'_>,
    hits: &mut [bool],
    pool: &MemoryPool,
) -> Result<()> {
    if spec.join_type == JoinType::NullAwareAnti {
        let pairs = naaj_pairs(spec)?;
        if !pairs.is_empty() {
            return probe_equi_hits(left, right, &pairs, None, hits, pool);
        }
    }
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
                if cartesian || residual_ok(left, right, lrow, rrow, split.residual.as_ref(), pool)?
                {
                    *hit = true;
                    break;
                }
            }
        }
        return Ok(());
    }
    probe_equi_hits(
        left,
        right,
        &split.keys,
        split.residual.as_ref(),
        hits,
        pool,
    )
}

fn probe_equi_hits(
    left: &Batch,
    right: &Batch,
    keys: &[(Expr, Expr)],
    residual: Option<&Expr>,
    hits: &mut [bool],
    pool: &MemoryPool,
) -> Result<()> {
    let left_keys: Vec<ArrayRef> = keys
        .iter()
        .map(|(l, _)| l.eval(left))
        .collect::<Result<_>>()?;
    let right_keys: Vec<ArrayRef> = keys
        .iter()
        .map(|(_, r)| r.eval(right))
        .collect::<Result<_>>()?;
    let table = BuildTable::new(&right_keys, &left_keys, right.num_rows())?;
    for (lrow, hit) in hits.iter_mut().enumerate() {
        if *hit {
            continue;
        }
        table.for_matches(&left_keys, lrow, |rrow| {
            if residual_ok(left, right, lrow, rrow as usize, residual, pool)? {
                *hit = true;
                Ok(false)
            } else {
                Ok(true)
            }
        })?;
    }
    Ok(())
}

fn naaj_pairs(spec: &JoinSpec<'_>) -> Result<Vec<(Expr, Expr)>> {
    let Some(pred) = spec.on else {
        return Ok(Vec::new());
    };
    let left_ncols = spec.left_schema.fields().len();
    let mut pairs = Vec::new();
    for conjunct in flatten_and(pred) {
        let Expr::Binary(BinaryOp::Eq, l, r) = conjunct else {
            continue;
        };
        let sl = expr_side(l, left_ncols)?;
        let sr = expr_side(r, left_ncols)?;
        match (sl, sr) {
            (Side::Left | Side::None, Side::Right) => {
                pairs.push((l.as_ref().clone(), shift_right(r, left_ncols)));
            }
            (Side::Right, Side::Left | Side::None) => {
                pairs.push((r.as_ref().clone(), shift_right(l, left_ncols)));
            }
            _ => {}
        }
    }
    Ok(pairs)
}

fn right_has_null_key(right: &Batch, spec: &JoinSpec<'_>) -> Result<bool> {
    let pairs = naaj_pairs(spec)?;
    if pairs.is_empty() {
        return Ok(false);
    }
    let right_keys: Vec<ArrayRef> = pairs
        .iter()
        .map(|(_, r)| r.eval(right))
        .collect::<Result<_>>()?;
    for row in 0..right.num_rows() {
        if row_has_null(&right_keys, row) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn mark_null_left_keys(left: &Batch, spec: &JoinSpec<'_>, hits: &mut [bool]) -> Result<()> {
    let pairs = naaj_pairs(spec)?;
    if pairs.is_empty() {
        return Ok(());
    }
    let left_keys: Vec<ArrayRef> = pairs
        .iter()
        .map(|(l, _)| l.eval(left))
        .collect::<Result<_>>()?;
    for (lrow, hit) in hits.iter_mut().enumerate() {
        if *hit {
            continue;
        }
        if row_has_null(&left_keys, lrow) {
            *hit = true;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn join_pair(
    left_batch: &Batch,
    right_batch: &Batch,
    spec: &JoinSpec<'_>,
    policy: &BatchPolicy,
    workers: usize,
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    let left_ncols = left_batch.num_columns();
    let split = match spec.on {
        Some(pred) => split_equi(pred, left_ncols)?,
        None => EquiSplit {
            keys: Vec::new(),
            residual: None,
        },
    };
    if split.keys.is_empty() {
        let out = nested_loop(
            left_batch,
            right_batch,
            spec.join_type,
            split.residual.as_ref(),
            spec.schema.clone(),
            pool,
        )?;
        return Ok(out
            .split_by_policy(policy)
            .into_iter()
            .filter(|b| b.num_rows() > 0)
            .collect());
    }
    if spec.join_type == JoinType::Inner {
        let left_keys: Vec<ArrayRef> = split
            .keys
            .iter()
            .map(|(l, _)| l.eval(left_batch))
            .collect::<Result<_>>()?;
        let right_keys: Vec<ArrayRef> = split
            .keys
            .iter()
            .map(|(_, r)| r.eval(right_batch))
            .collect::<Result<_>>()?;
        return inner_hash_join(
            left_batch,
            right_batch,
            &left_keys,
            &right_keys,
            split.residual.as_ref(),
            spec.schema.clone(),
            left_batch.num_rows() < right_batch.num_rows(),
            workers,
            policy,
            pool,
        );
    }
    let out = hash_join(
        left_batch,
        right_batch,
        spec.join_type,
        &split.keys,
        split.residual.as_ref(),
        spec.schema.clone(),
        workers,
        pool,
    )?;
    Ok(out
        .split_by_policy(policy)
        .into_iter()
        .filter(|b| b.num_rows() > 0)
        .collect())
}

/// Inner equi-join: concat only the smaller side, probe each morsel of the
/// other side. Probe morsels run on `workers` threads.
fn hash_join_streamed(
    left: &[Batch],
    right: &[Batch],
    spec: &JoinSpec<'_>,
    policy: &BatchPolicy,
    workers: usize,
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    let left_ncols = spec.left_schema.fields().len();
    let split = match spec.on {
        Some(pred) => split_equi(pred, left_ncols)?,
        None => EquiSplit {
            keys: Vec::new(),
            residual: None,
        },
    };
    if split.keys.is_empty() {
        let l = concat_chunk(left, spec.left_schema.clone(), pool)?;
        let r = concat_chunk(right, spec.right_schema.clone(), pool)?;
        return join_pair(&l, &r, spec, policy, workers, pool);
    }
    let ln: usize = left.iter().map(Batch::num_rows).sum();
    let rn: usize = right.iter().map(Batch::num_rows).sum();
    let build_left = ln < rn;
    let (build_batches, probe_batches, build_schema) = if build_left {
        (left, right, spec.left_schema.clone())
    } else {
        (right, left, spec.right_schema.clone())
    };
    let build = concat_chunk(build_batches, build_schema, pool)?;
    let build_keys: Vec<ArrayRef> = if build_left {
        split
            .keys
            .iter()
            .map(|(l, _)| l.eval(&build))
            .collect::<Result<_>>()?
    } else {
        split
            .keys
            .iter()
            .map(|(_, r)| r.eval(&build))
            .collect::<Result<_>>()?
    };
    let probe_key_exprs: Vec<&Expr> = if build_left {
        split.keys.iter().map(|(_, r)| r).collect()
    } else {
        split.keys.iter().map(|(l, _)| l).collect()
    };
    let sample_probe = match probe_batches.iter().find(|b| b.num_rows() > 0) {
        Some(b) => b.clone(),
        None => {
            if build_left {
                Batch::empty(spec.right_schema.clone())?
            } else {
                Batch::empty(spec.left_schema.clone())?
            }
        }
    };
    let sample_probe_keys: Vec<ArrayRef> = probe_key_exprs
        .iter()
        .map(|e| e.eval(&sample_probe))
        .collect::<Result<_>>()?;
    let table = BuildTable::new(&build_keys, &sample_probe_keys, build.num_rows())?;
    let residual = split.residual.as_ref();
    let n = workers.max(1).min(probe_batches.len().max(1));
    if n <= 1 {
        let mut out = Vec::new();
        for probe in probe_batches {
            out.extend(probe_inner_batch(
                &table,
                &build,
                probe,
                &probe_key_exprs,
                residual,
                build_left,
                spec.schema.clone(),
                policy,
                pool,
            )?);
        }
        return Ok(out);
    }
    let queue = Mutex::new((0..probe_batches.len()).collect::<Vec<usize>>());
    let parts = Mutex::new(Vec::new());
    let table = &table;
    let build = &build;
    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for _ in 0..n {
            let parts = &parts;
            let queue = &queue;
            let probe_key_exprs = &probe_key_exprs;
            handles.push(scope.spawn(move || -> Result<()> {
                loop {
                    let i = {
                        let mut q = queue
                            .lock()
                            .map_err(|_| Error::Eval("join probe lock poisoned".into()))?;
                        q.pop()
                    };
                    let Some(i) = i else {
                        return Ok(());
                    };
                    let batches = probe_inner_batch(
                        table,
                        build,
                        &probe_batches[i],
                        probe_key_exprs,
                        residual,
                        build_left,
                        spec.schema.clone(),
                        policy,
                        pool,
                    )?;
                    parts
                        .lock()
                        .map_err(|_| Error::Eval("join probe lock poisoned".into()))?
                        .push((i, batches));
                }
            }));
        }
        for h in handles {
            h.join()
                .map_err(|_| Error::Eval("worker panicked".into()))??;
        }
        Ok(())
    })?;
    let mut parts = parts
        .into_inner()
        .map_err(|_| Error::Eval("join probe lock poisoned".into()))?;
    parts.sort_by_key(|(i, _)| *i);
    Ok(parts.into_iter().flat_map(|(_, b)| b).collect())
}

#[allow(clippy::too_many_arguments)]
fn probe_inner_batch(
    table: &BuildTable,
    build: &Batch,
    probe: &Batch,
    probe_key_exprs: &[&Expr],
    residual: Option<&Expr>,
    build_left: bool,
    schema: SchemaRef,
    policy: &BatchPolicy,
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    if probe.num_rows() == 0 {
        return Ok(Vec::new());
    }
    let probe_keys: Vec<ArrayRef> = probe_key_exprs
        .iter()
        .map(|e| e.eval(probe))
        .collect::<Result<_>>()?;
    let (left, right) = if build_left {
        (build, probe)
    } else {
        (probe, build)
    };
    let (left_idx, right_idx) = if residual.is_none() {
        if let Some(pair) = table.probe_unique_i64(&probe_keys, build_left)? {
            pair
        } else {
            probe_inner_range(
                table,
                &probe_keys,
                left,
                right,
                residual,
                build_left,
                0,
                probe.num_rows(),
                pool,
            )?
        }
    } else {
        probe_inner_range(
            table,
            &probe_keys,
            left,
            right,
            residual,
            build_left,
            0,
            probe.num_rows(),
            pool,
        )?
    };
    if left_idx.is_empty() {
        return Ok(Vec::new());
    }
    let joined = hconcat_take(left, right, &left_idx, &right_idx, schema, pool)?;
    Ok(joined
        .split_by_policy(policy)
        .into_iter()
        .filter(|b| b.num_rows() > 0)
        .collect())
}

#[allow(clippy::too_many_arguments)]
fn hash_join(
    left: &Batch,
    right: &Batch,
    join_type: JoinType,
    eq_keys: &[(Expr, Expr)],
    residual: Option<&Expr>,
    schema: SchemaRef,
    _workers: usize,
    pool: &MemoryPool,
) -> Result<Batch> {
    let left_keys: Vec<ArrayRef> = eq_keys
        .iter()
        .map(|(l, _)| l.eval(left))
        .collect::<Result<_>>()?;
    let right_keys: Vec<ArrayRef> = eq_keys
        .iter()
        .map(|(_, r)| r.eval(right))
        .collect::<Result<_>>()?;

    let table = BuildTable::new(&right_keys, &left_keys, right.num_rows())?;
    let mut left_idx = Vec::new();
    let mut right_idx = Vec::new();
    let mut unmatched_left = Vec::new();
    let mut build_hit = vec![false; right.num_rows()];

    for lrow in 0..left.num_rows() {
        let mut matched = false;
        table.for_matches(&left_keys, lrow, |rrow| {
            if residual_ok(left, right, lrow, rrow as usize, residual, pool)? {
                matched = true;
                build_hit[rrow as usize] = true;
                left_idx.push(lrow as u32);
                right_idx.push(rrow);
            }
            Ok(true)
        })?;
        if !matched {
            unmatched_left.push(lrow as u32);
        }
    }

    match join_type {
        JoinType::Inner => unreachable!("inner uses inner_hash_join"),
        JoinType::Left => {
            append_unmatched_left(&mut left_idx, &mut right_idx, &unmatched_left);
            hconcat_take(left, right, &left_idx, &right_idx, schema, pool)
        }
        JoinType::Right => {
            append_unmatched_right(&mut left_idx, &mut right_idx, &build_hit);
            hconcat_take(left, right, &left_idx, &right_idx, schema, pool)
        }
        JoinType::Full => {
            append_unmatched_left(&mut left_idx, &mut right_idx, &unmatched_left);
            append_unmatched_right(&mut left_idx, &mut right_idx, &build_hit);
            hconcat_take(left, right, &left_idx, &right_idx, schema, pool)
        }
        JoinType::Semi => take_left(
            left,
            &matched_once(&unmatched_left, left.num_rows()),
            schema,
            pool,
        ),
        JoinType::Anti => take_left(left, &unmatched_left, schema, pool),
        JoinType::NullAwareAnti => Err(Error::Eval(
            "internal: null-aware anti used the hash path".into(),
        )),
        JoinType::Cross => Err(Error::Eval(
            "internal: CROSS JOIN used the hash path".into(),
        )),
    }
}

fn emit_join_chunks(
    left: &Batch,
    right: &Batch,
    left_idx: &[u32],
    right_idx: &[u32],
    schema: SchemaRef,
    policy: &BatchPolicy,
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    if left_idx.is_empty() {
        return Ok(Vec::new());
    }
    let cap = policy.max_rows().unwrap_or(left_idx.len()).max(1);
    let mut out = Vec::new();
    let mut start = 0;
    while start < left_idx.len() {
        let end = (start + cap).min(left_idx.len());
        out.push(hconcat_take(
            left,
            right,
            &left_idx[start..end],
            &right_idx[start..end],
            schema.clone(),
            pool,
        )?);
        start = end;
    }
    Ok(out.into_iter().filter(|b| b.num_rows() > 0).collect())
}

#[allow(clippy::too_many_arguments)]
fn inner_hash_join(
    left: &Batch,
    right: &Batch,
    left_keys: &[ArrayRef],
    right_keys: &[ArrayRef],
    residual: Option<&Expr>,
    schema: SchemaRef,
    build_left: bool,
    workers: usize,
    policy: &BatchPolicy,
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    let (build_keys, probe_keys, n_build, n_probe) = if build_left {
        (left_keys, right_keys, left.num_rows(), right.num_rows())
    } else {
        (right_keys, left_keys, right.num_rows(), left.num_rows())
    };
    let table = BuildTable::new(build_keys, probe_keys, n_build)?;
    let n = workers.max(1).min(n_probe.max(1));
    if residual.is_none() && table.has_unique_i64(probe_keys) {
        return unique_i64_join(
            &table, probe_keys, left, right, build_left, schema, n, policy, pool,
        );
    }
    if n <= 1 {
        let (left_idx, right_idx) = probe_inner_range(
            &table, probe_keys, left, right, residual, build_left, 0, n_probe, pool,
        )?;
        return emit_join_chunks(left, right, &left_idx, &right_idx, schema, policy, pool);
    }
    let chunk = n_probe.div_ceil(n);
    let parts = Mutex::new(Vec::new());
    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for t in 0..n {
            let start = t * chunk;
            let end = (start + chunk).min(n_probe);
            if start >= end {
                continue;
            }
            let table = &table;
            let parts = &parts;
            handles.push(scope.spawn(move || -> Result<()> {
                let pair = probe_inner_range(
                    table, probe_keys, left, right, residual, build_left, start, end, pool,
                )?;
                parts
                    .lock()
                    .map_err(|_| Error::Eval("join probe lock poisoned".into()))?
                    .push((start, pair));
                Ok(())
            }));
        }
        for h in handles {
            h.join()
                .map_err(|_| Error::Eval("worker panicked".into()))??;
        }
        Ok(())
    })?;
    let mut parts = parts
        .into_inner()
        .map_err(|_| Error::Eval("join probe lock poisoned".into()))?;
    parts.sort_by_key(|(start, _)| *start);
    let mut left_idx = Vec::new();
    let mut right_idx = Vec::new();
    for (_, (l, r)) in parts {
        left_idx.extend(l);
        right_idx.extend(r);
    }
    emit_join_chunks(left, right, &left_idx, &right_idx, schema, policy, pool)
}

#[allow(clippy::too_many_arguments)]
fn unique_i64_join(
    table: &BuildTable,
    probe_keys: &[ArrayRef],
    left: &Batch,
    right: &Batch,
    build_left: bool,
    schema: SchemaRef,
    workers: usize,
    policy: &BatchPolicy,
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    let n_probe = if build_left {
        right.num_rows()
    } else {
        left.num_rows()
    };
    let n = workers.max(1).min(n_probe.max(1));
    if n <= 1 {
        let (left_idx, right_idx) = table
            .probe_unique_i64(probe_keys, build_left)?
            .ok_or_else(|| Error::Eval("expected unique i64 join map".into()))?;
        return emit_join_chunks(left, right, &left_idx, &right_idx, schema, policy, pool);
    }
    let chunk = n_probe.div_ceil(n);
    let parts = Mutex::new(Vec::new());
    let parts_ref = &parts;
    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for t in 0..n {
            let start = t * chunk;
            let end = (start + chunk).min(n_probe);
            if start >= end {
                continue;
            }
            handles.push(scope.spawn(move || -> Result<()> {
                let pair = probe_unique_i64_range(table, probe_keys, build_left, start, end)?;
                parts_ref
                    .lock()
                    .map_err(|_| Error::Eval("join probe lock poisoned".into()))?
                    .push((start, pair));
                Ok(())
            }));
        }
        for h in handles {
            h.join()
                .map_err(|_| Error::Eval("worker panicked".into()))??;
        }
        Ok(())
    })?;
    let mut parts = parts
        .into_inner()
        .map_err(|_| Error::Eval("join probe lock poisoned".into()))?;
    parts.sort_by_key(|(start, _)| *start);
    let mut left_idx = Vec::new();
    let mut right_idx = Vec::new();
    for (_, (l, r)) in parts {
        left_idx.extend(l);
        right_idx.extend(r);
    }
    emit_join_chunks(left, right, &left_idx, &right_idx, schema, policy, pool)
}

fn unique_i64_brow(
    min: i128,
    slots: &[u32],
    arr: &PrimitiveArray<Int64Type>,
    prow: usize,
) -> Option<u32> {
    if arr.is_null(prow) {
        return None;
    }
    let idx = i128::from(arr.value(prow)) - min;
    if idx < 0 || idx >= slots.len() as i128 {
        return None;
    }
    let brow = slots[idx as usize];
    if brow == u32::MAX { None } else { Some(brow) }
}

fn probe_unique_i64_range(
    table: &BuildTable,
    probe_keys: &[ArrayRef],
    build_left: bool,
    start: usize,
    end: usize,
) -> Result<(Vec<u32>, Vec<u32>)> {
    let IntRelation::Unique { min, slots } = (match table {
        BuildTable::I64(rel) => rel,
        _ => return Err(Error::Eval("expected unique i64 join map".into())),
    }) else {
        return Err(Error::Eval("expected unique i64 join map".into()));
    };
    let arr = probe_keys[0].as_primitive::<Int64Type>();
    let mut left_idx = Vec::with_capacity(end.saturating_sub(start));
    let mut right_idx = Vec::with_capacity(end.saturating_sub(start));
    let min = *min;
    let slots = slots.as_slice();
    let span = slots.len() as i128;
    for prow in start..end {
        if arr.is_null(prow) {
            continue;
        }
        let idx = i128::from(arr.value(prow)) - min;
        if idx < 0 || idx >= span {
            continue;
        }
        let brow = slots[idx as usize];
        if brow == u32::MAX {
            continue;
        }
        if build_left {
            left_idx.push(brow);
            right_idx.push(prow as u32);
        } else {
            left_idx.push(prow as u32);
            right_idx.push(brow);
        }
    }
    Ok((left_idx, right_idx))
}

#[allow(clippy::too_many_arguments)]
fn probe_inner_range(
    table: &BuildTable,
    probe_keys: &[ArrayRef],
    left: &Batch,
    right: &Batch,
    residual: Option<&Expr>,
    build_left: bool,
    start: usize,
    end: usize,
    pool: &MemoryPool,
) -> Result<(Vec<u32>, Vec<u32>)> {
    let mut left_idx = Vec::new();
    let mut right_idx = Vec::new();
    for prow in start..end {
        table.for_matches(probe_keys, prow, |brow| {
            let (lrow, rrow) = if build_left {
                (brow, prow as u32)
            } else {
                (prow as u32, brow)
            };
            if residual_ok(left, right, lrow as usize, rrow as usize, residual, pool)? {
                left_idx.push(lrow);
                right_idx.push(rrow);
            }
            Ok(true)
        })?;
    }
    Ok((left_idx, right_idx))
}

/// Build-side lookup. A single integer column uses a compact range map when
/// keys sit in a tight span; otherwise a cheap hash map. Multi-column keys
/// hash in place and collisions compare cells.
enum BuildTable {
    I64(IntRelation<i64>),
    I32(IntRelation<i32>),
    U64(IntRelation<u64>),
    U32(IntRelation<u32>),
    Hashed {
        slots: IdentityMap<Vec<u32>>,
        keys: Vec<ArrayRef>,
    },
}

/// Integer key → build rows. Unique keys in a tight min/max span are one
/// array slot per key; duplicates keep a per-row chain; sparse keys hash.
#[derive(Debug)]
enum IntRelation<T> {
    Hash(FastMap<T, Vec<u32>>),
    Unique {
        min: i128,
        slots: Vec<u32>,
    },
    Multi {
        min: i128,
        heads: Vec<u32>,
        next: Vec<u32>,
    },
}

impl BuildTable {
    fn new(build_keys: &[ArrayRef], probe_keys: &[ArrayRef], n_build: usize) -> Result<Self> {
        if build_keys.len() == 1
            && probe_keys.len() == 1
            && build_keys[0].data_type() == probe_keys[0].data_type()
        {
            match build_keys[0].data_type() {
                DataType::Int64 => {
                    return Ok(Self::I64(int_relation::<Int64Type>(
                        build_keys[0].as_primitive(),
                        n_build,
                    )));
                }
                DataType::Int32 => {
                    return Ok(Self::I32(int_relation::<Int32Type>(
                        build_keys[0].as_primitive(),
                        n_build,
                    )));
                }
                DataType::UInt64 => {
                    return Ok(Self::U64(int_relation::<UInt64Type>(
                        build_keys[0].as_primitive(),
                        n_build,
                    )));
                }
                DataType::UInt32 => {
                    return Ok(Self::U32(int_relation::<UInt32Type>(
                        build_keys[0].as_primitive(),
                        n_build,
                    )));
                }
                _ => {}
            }
        }
        let mut slots = identity_map_with_capacity::<Vec<u32>>(n_build);
        for row in 0..n_build {
            if row_has_null(build_keys, row) {
                continue;
            }
            let h = hash_row(build_keys, row)?;
            slots.entry(h).or_default().push(row as u32);
        }
        Ok(Self::Hashed {
            slots,
            keys: build_keys.to_vec(),
        })
    }

    fn has_unique_i64(&self, probe_keys: &[ArrayRef]) -> bool {
        matches!(self, Self::I64(IntRelation::Unique { .. }))
            && probe_keys.len() == 1
            && probe_keys[0].data_type() == &DataType::Int64
    }

    fn unique_i64_slots(&self) -> Option<(i128, &[u32])> {
        match self {
            Self::I64(IntRelation::Unique { min, slots }) => Some((*min, slots.as_slice())),
            _ => None,
        }
    }

    /// Tight unique-i64 probe: one array index per key, no closure.
    fn probe_unique_i64(
        &self,
        probe_keys: &[ArrayRef],
        build_left: bool,
    ) -> Result<Option<(Vec<u32>, Vec<u32>)>> {
        let IntRelation::Unique { min, slots } = (match self {
            Self::I64(rel) => rel,
            _ => return Ok(None),
        }) else {
            return Ok(None);
        };
        if probe_keys.len() != 1 || probe_keys[0].data_type() != &DataType::Int64 {
            return Ok(None);
        }
        let arr = probe_keys[0].as_primitive::<Int64Type>();
        let n = arr.len();
        let mut left_idx = Vec::with_capacity(n);
        let mut right_idx = Vec::with_capacity(n);
        let min = *min;
        let slots = slots.as_slice();
        let span = slots.len() as i128;
        for prow in 0..n {
            if arr.is_null(prow) {
                continue;
            }
            let idx = i128::from(arr.value(prow)) - min;
            if idx < 0 || idx >= span {
                continue;
            }
            let brow = slots[idx as usize];
            if brow == u32::MAX {
                continue;
            }
            if build_left {
                left_idx.push(brow);
                right_idx.push(prow as u32);
            } else {
                left_idx.push(prow as u32);
                right_idx.push(brow);
            }
        }
        Ok(Some((left_idx, right_idx)))
    }

    /// Visit matching build rows. `visit` returns whether to keep scanning
    /// (`true`) or stop (`false`).
    fn for_matches(
        &self,
        probe_keys: &[ArrayRef],
        prow: usize,
        mut visit: impl FnMut(u32) -> Result<bool>,
    ) -> Result<()> {
        if row_has_null(probe_keys, prow) {
            return Ok(());
        }
        match self {
            Self::I64(rel) => {
                int_probe::<Int64Type>(rel, probe_keys[0].as_primitive(), prow, visit)
            }
            Self::I32(rel) => {
                int_probe::<Int32Type>(rel, probe_keys[0].as_primitive(), prow, visit)
            }
            Self::U64(rel) => {
                int_probe::<UInt64Type>(rel, probe_keys[0].as_primitive(), prow, visit)
            }
            Self::U32(rel) => {
                int_probe::<UInt32Type>(rel, probe_keys[0].as_primitive(), prow, visit)
            }
            Self::Hashed { slots, keys } => {
                let h = hash_row(probe_keys, prow)?;
                let Some(cands) = slots.get(&h) else {
                    return Ok(());
                };
                for &brow in cands {
                    if rows_equal(keys, brow as usize, probe_keys, prow)? && !visit(brow)? {
                        return Ok(());
                    }
                }
                Ok(())
            }
        }
    }
}

trait IntKey: Copy + Hash + Eq {
    fn as_i128(self) -> i128;
}

impl IntKey for i64 {
    fn as_i128(self) -> i128 {
        i128::from(self)
    }
}
impl IntKey for i32 {
    fn as_i128(self) -> i128 {
        i128::from(self)
    }
}
impl IntKey for u64 {
    fn as_i128(self) -> i128 {
        i128::from(self)
    }
}
impl IntKey for u32 {
    fn as_i128(self) -> i128 {
        i128::from(self)
    }
}

fn dense_len(min: i128, max: i128, n: usize) -> Option<usize> {
    let range = max.checked_sub(min)?.checked_add(1)?;
    if range <= 0 || range > 64 * 1024 * 1024 {
        return None;
    }
    let range = range as usize;
    // Allow ~32 empty slots per live key so a filtered unique key
    // (200k hits in a 2M domain) still uses a direct index.
    if range > n.saturating_mul(32) && range > 1_048_576 {
        return None;
    }
    Some(range)
}

#[allow(clippy::needless_range_loop)]
fn int_relation<T: ArrowPrimitiveType>(arr: &PrimitiveArray<T>, n: usize) -> IntRelation<T::Native>
where
    T::Native: IntKey,
{
    let mut min = i128::MAX;
    let mut max = i128::MIN;
    let mut count = 0usize;
    for row in 0..n {
        if arr.is_null(row) {
            continue;
        }
        let v = arr.value(row).as_i128();
        min = min.min(v);
        max = max.max(v);
        count += 1;
    }
    if count == 0 {
        return IntRelation::Hash(fast_map_with_capacity(0));
    }
    if let Some(range) = dense_len(min, max, count) {
        let mut slots = vec![u32::MAX; range];
        let mut unique = true;
        for row in 0..n {
            if arr.is_null(row) {
                continue;
            }
            let idx = (arr.value(row).as_i128() - min) as usize;
            if slots[idx] != u32::MAX {
                unique = false;
                break;
            }
            slots[idx] = row as u32;
        }
        if unique {
            return IntRelation::Unique { min, slots };
        }
        let mut heads = vec![u32::MAX; range];
        let mut next = vec![u32::MAX; n];
        for row in 0..n {
            if arr.is_null(row) {
                continue;
            }
            let idx = (arr.value(row).as_i128() - min) as usize;
            next[row] = heads[idx];
            heads[idx] = row as u32;
        }
        return IntRelation::Multi { min, heads, next };
    }
    let mut map = fast_map_with_capacity::<T::Native, Vec<u32>>(count);
    for row in 0..n {
        if arr.is_null(row) {
            continue;
        }
        map.entry(arr.value(row)).or_default().push(row as u32);
    }
    IntRelation::Hash(map)
}

fn int_probe<T: ArrowPrimitiveType>(
    rel: &IntRelation<T::Native>,
    arr: &PrimitiveArray<T>,
    prow: usize,
    mut visit: impl FnMut(u32) -> Result<bool>,
) -> Result<()>
where
    T::Native: IntKey,
{
    if arr.is_null(prow) {
        return Ok(());
    }
    let key = arr.value(prow);
    match rel {
        IntRelation::Hash(map) => {
            let Some(hits) = map.get(&key) else {
                return Ok(());
            };
            for &brow in hits {
                if !visit(brow)? {
                    return Ok(());
                }
            }
            Ok(())
        }
        IntRelation::Unique { min, slots } => {
            let idx = key.as_i128() - *min;
            if idx < 0 || idx >= slots.len() as i128 {
                return Ok(());
            }
            let brow = slots[idx as usize];
            if brow == u32::MAX {
                return Ok(());
            }
            visit(brow)?;
            Ok(())
        }
        IntRelation::Multi { min, heads, next } => {
            let idx = key.as_i128() - *min;
            if idx < 0 || idx >= heads.len() as i128 {
                return Ok(());
            }
            let mut brow = heads[idx as usize];
            while brow != u32::MAX {
                if !visit(brow)? {
                    return Ok(());
                }
                brow = next[brow as usize];
            }
            Ok(())
        }
    }
}

fn nested_loop(
    left: &Batch,
    right: &Batch,
    join_type: JoinType,
    residual: Option<&Expr>,
    schema: SchemaRef,
    pool: &MemoryPool,
) -> Result<Batch> {
    let mut left_idx = Vec::new();
    let mut right_idx = Vec::new();
    let mut unmatched_left = Vec::new();
    let mut build_hit = vec![false; right.num_rows()];
    let cartesian = residual.is_none();

    for lrow in 0..left.num_rows() {
        let mut matched = false;
        for (rrow, hit) in build_hit.iter_mut().enumerate() {
            if cartesian || residual_ok(left, right, lrow, rrow, residual, pool)? {
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
            hconcat_take(left, right, &left_idx, &right_idx, schema, pool)
        }
        JoinType::Left => {
            append_unmatched_left(&mut left_idx, &mut right_idx, &unmatched_left);
            hconcat_take(left, right, &left_idx, &right_idx, schema, pool)
        }
        JoinType::Right => {
            append_unmatched_right(&mut left_idx, &mut right_idx, &build_hit);
            hconcat_take(left, right, &left_idx, &right_idx, schema, pool)
        }
        JoinType::Full => {
            append_unmatched_left(&mut left_idx, &mut right_idx, &unmatched_left);
            append_unmatched_right(&mut left_idx, &mut right_idx, &build_hit);
            hconcat_take(left, right, &left_idx, &right_idx, schema, pool)
        }
        JoinType::Semi => take_left(
            left,
            &matched_once(&unmatched_left, left.num_rows()),
            schema,
            pool,
        ),
        JoinType::Anti => take_left(left, &unmatched_left, schema, pool),
        JoinType::NullAwareAnti => Err(Error::Eval(
            "internal: null-aware anti used the nested-loop path".into(),
        )),
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
    pool: &MemoryPool,
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
        pool,
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
    pool: &MemoryPool,
) -> Result<Batch> {
    debug_assert_eq!(left_idx.len(), right_idx.len());
    let n = left_idx.len();
    if n == 0 {
        return Batch::empty(schema);
    }
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    columns.extend(take_or_null(left, left_idx, pool)?);
    columns.extend(take_or_null(right, right_idx, pool)?);
    Batch::try_new(schema, columns)
}

fn take_or_null(batch: &Batch, indices: &[u32], pool: &MemoryPool) -> Result<Vec<ArrayRef>> {
    if !indices.contains(&u32::MAX)
        && let Some((offset, length)) = crate::batch::contiguous_indices(indices)
    {
        return Ok(batch
            .as_arrow()
            .columns()
            .iter()
            .map(|c| c.slice(offset, length))
            .collect());
    }
    let mut out = Vec::with_capacity(batch.num_columns());
    for col in batch.as_arrow().columns() {
        out.push(take_with_nulls(col, indices)?);
    }
    let bytes: u64 = out.iter().map(|c| c.get_array_memory_size() as u64).sum();
    pool.record_copy(bytes);
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

fn take_left(left: &Batch, indices: &[u32], schema: SchemaRef, pool: &MemoryPool) -> Result<Batch> {
    let gathered = left.gather(indices)?;
    if !indices.is_empty() && crate::batch::contiguous_indices(indices).is_none() {
        pool.record_copy(batch_bytes(&gathered));
    }
    if gathered.num_columns() == 0 {
        return Ok(gathered);
    }
    Batch::try_new(schema, gathered.as_arrow().columns().to_vec())
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

fn onto_build(expr: &Expr, left_ncols: usize, build_left: bool, split: &EquiSplit) -> Option<Expr> {
    if let Some(rewritten) = onto_side(expr, left_ncols, build_left) {
        return Some(rewritten);
    }
    if split.keys.len() != 1 {
        return None;
    }
    let (left_key, right_key) = &split.keys[0];
    let probe_on_output = if build_left {
        lift_nth(right_key, left_ncols)
    } else {
        left_key.clone()
    };
    if !expr_same(expr, &probe_on_output) {
        return None;
    }
    Some(if build_left {
        left_key.clone()
    } else {
        right_key.clone()
    })
}

fn onto_side(expr: &Expr, left_ncols: usize, want_left: bool) -> Option<Expr> {
    match expr_side(expr, left_ncols).ok()? {
        Side::None => Some(expr.clone()),
        Side::Left if want_left => Some(expr.clone()),
        Side::Right if !want_left => Some(shift_right(expr, left_ncols)),
        _ => None,
    }
}

fn agg_onto_probe(call: &AggCall, left_ncols: usize, build_left: bool) -> Option<AggCall> {
    let onto = |e: &Expr| onto_side(e, left_ncols, !build_left);
    match call {
        AggCall::CountStar => Some(AggCall::CountStar),
        AggCall::Count(e) => Some(AggCall::Count(onto(e)?)),
        AggCall::CountDistinct(e) => Some(AggCall::CountDistinct(onto(e)?)),
        AggCall::Sum(e) => Some(AggCall::Sum(onto(e)?)),
        AggCall::Avg(e) => Some(AggCall::Avg(onto(e)?)),
        AggCall::Min(e) => Some(AggCall::Min(onto(e)?)),
        AggCall::Max(e) => Some(AggCall::Max(onto(e)?)),
        AggCall::User { name, arg, agg } => Some(AggCall::User {
            name: name.clone(),
            arg: onto(arg)?,
            agg: agg.clone(),
        }),
    }
}

fn lift_nth(expr: &Expr, left_ncols: usize) -> Expr {
    match expr {
        Expr::Nth(i) => Expr::nth(*i + left_ncols),
        Expr::Column(name) => Expr::column(name.clone()),
        Expr::Literal(v) => Expr::Literal(v.clone()),
        Expr::Binary(op, l, r) => Expr::Binary(
            *op,
            Box::new(lift_nth(l, left_ncols)),
            Box::new(lift_nth(r, left_ncols)),
        ),
        Expr::Unary(op, e) => Expr::Unary(*op, Box::new(lift_nth(e, left_ncols))),
        Expr::Cast { expr, data_type } => Expr::Cast {
            expr: Box::new(lift_nth(expr, left_ncols)),
            data_type: data_type.clone(),
        },
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => Expr::Case {
            operand: operand
                .as_deref()
                .map(|e| Box::new(lift_nth(e, left_ncols))),
            when_then: when_then
                .iter()
                .map(|(w, t)| (lift_nth(w, left_ncols), lift_nth(t, left_ncols)))
                .collect(),
            else_expr: else_expr
                .as_deref()
                .map(|e| Box::new(lift_nth(e, left_ncols))),
        },
        Expr::Coalesce(args) => {
            Expr::Coalesce(args.iter().map(|e| lift_nth(e, left_ncols)).collect())
        }
        Expr::Concat(args) => Expr::Concat(args.iter().map(|e| lift_nth(e, left_ncols)).collect()),
        Expr::Substring {
            expr,
            start,
            length,
        } => Expr::Substring {
            expr: Box::new(lift_nth(expr, left_ncols)),
            start: *start,
            length: *length,
        },
        Expr::Array(args) => Expr::Array(args.iter().map(|e| lift_nth(e, left_ncols)).collect()),
    }
}

fn expr_same(a: &Expr, b: &Expr) -> bool {
    match (a, b) {
        (Expr::Nth(i), Expr::Nth(j)) => i == j,
        (Expr::Column(x), Expr::Column(y)) => x == y,
        (Expr::Binary(op1, l1, r1), Expr::Binary(op2, l2, r2)) => {
            op1 == op2 && expr_same(l1, l2) && expr_same(r1, r2)
        }
        (Expr::Unary(op1, e1), Expr::Unary(op2, e2)) => op1 == op2 && expr_same(e1, e2),
        (
            Expr::Cast {
                expr: e1,
                data_type: d1,
            },
            Expr::Cast {
                expr: e2,
                data_type: d2,
            },
        ) => d1 == d2 && expr_same(e1, e2),
        _ => false,
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    fn i64_array(values: &[i64]) -> PrimitiveArray<Int64Type> {
        Int64Array::from(values.to_vec())
    }

    #[test]
    fn moderately_sparse_unique_keys_still_use_range_slots() {
        let values: Vec<i64> = (0..200_000).map(|i| i * 10).collect();
        let arr = Int64Array::from(values);
        match int_relation::<Int64Type>(&arr, 200_000) {
            IntRelation::Unique { min, slots } => {
                assert_eq!(min, 0);
                assert_eq!(slots.len(), 1_999_991);
                assert_eq!(slots[0], 0);
                assert_eq!(slots[10], 1);
            }
            other => panic!("expected unique range map, got {other:?}"),
        }
    }

    #[test]
    fn compact_unique_integer_keys_use_range_slots() {
        let arr = i64_array(&[10, 11, 12]);
        match int_relation::<Int64Type>(&arr, 3) {
            IntRelation::Unique { min, slots } => {
                assert_eq!(min, 10);
                assert_eq!(slots.len(), 3);
                assert_eq!(slots[0], 0);
                assert_eq!(slots[2], 2);
            }
            other => panic!("expected unique range map, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_integer_keys_use_range_chains() {
        let arr = i64_array(&[5, 6, 5]);
        match int_relation::<Int64Type>(&arr, 3) {
            IntRelation::Multi { min, heads, next } => {
                assert_eq!(min, 5);
                let mut seen = Vec::new();
                let mut row = heads[0];
                while row != u32::MAX {
                    seen.push(row);
                    row = next[row as usize];
                }
                seen.sort();
                assert_eq!(seen, vec![0, 2]);
            }
            other => panic!("expected multi range map, got {other:?}"),
        }
    }

    #[test]
    fn sparse_integer_keys_use_hash_map() {
        let arr = i64_array(&[0, 1_000_000_000]);
        match int_relation::<Int64Type>(&arr, 2) {
            IntRelation::Hash(map) => {
                assert_eq!(map.get(&0).map(Vec::as_slice), Some(&[0][..]));
                assert_eq!(map.get(&1_000_000_000).map(Vec::as_slice), Some(&[1][..]));
            }
            other => panic!("expected hash map, got {other:?}"),
        }
    }

    #[test]
    fn empty_build_is_hash_map() {
        let arr = Int64Array::from(Vec::<i64>::new());
        assert!(matches!(
            int_relation::<Int64Type>(&arr, 0),
            IntRelation::Hash(_)
        ));
    }

    #[test]
    fn concat_chunk_reuses_a_single_batch() {
        use std::sync::Arc;

        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
        )
        .unwrap();
        let pool = MemoryPool::new(None, None).unwrap();
        let out = concat_chunk(std::slice::from_ref(&batch), schema, &pool).unwrap();
        assert_eq!(pool.copied(), 0);
        assert!(Arc::ptr_eq(out.column(0), batch.column(0)));
    }

    #[test]
    fn concat_chunk_records_bytes_when_merging() {
        use std::sync::Arc;

        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let a = Batch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();
        let b = Batch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![2_i64]))],
        )
        .unwrap();
        let pool = MemoryPool::new(None, None).unwrap();
        let out = concat_chunk(&[a, b], schema, &pool).unwrap();
        assert_eq!(out.num_rows(), 2);
        assert!(pool.copied() > 0);
    }
}
