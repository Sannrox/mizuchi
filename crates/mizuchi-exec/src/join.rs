//! Build/probe hash join over Arrow batches.
//!
//! Equality conjuncts in `ON` become hash keys. Inner joins build the smaller
//! collected side, or the right side when the probe operator is streamed
//! (optimizer places the smaller estimated input on the right). Workers scatter
//! build morsels into hash partitions and finalise each partition's table with
//! no insert lock; a single integer key in a dense span skips partitioning
//! and builds one direct index. A single integer column uses a compact range
//! map when keys sit in a tight span, otherwise a cheap hash map. Unique i64
//! keys probe with a direct index. Probe batches are never rewritten: only
//! matched rows are gathered. Other join types probe left and build right.
//! Inner probe ranges run on `Session::workers` threads. Null keys never
//! match. Non-equality conjuncts are a residual filter on candidate pairs.
//! Joins without an equality key (including CROSS) use a nested loop. LEFT
//! SEMI / ANTI emit the left schema only. Under a memory limit, an inner
//! equi-join whose build side stays in memory streams the probe side; when
//! the build spills, both sides hash-partition and each partition joins once.

use std::hash::Hash;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
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
use super::memory::{MemoryPool, batch_bytes};
use super::{CancelToken, Exec, Operator, Session, filter_batch};
use mizuchi_core::batch::{Batch, BatchPolicy};
use mizuchi_core::error::{Error, Result};
use mizuchi_core::expr::{BinaryOp, Expr};
use mizuchi_plan::{AggCall, JoinType, NamedExpr};

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
    if spec.join_type == JoinType::Inner {
        if !pool.has_limit() {
            return hash_join_stream_probe(left, right, &spec, policy, workers, pool, cancel);
        }
        // Under a limit, collect the build (right) side first. When it stays
        // in memory, stream the probe side through it rather than collecting
        // and concatenating the whole probe input.
        let right_chunks = collect_chunks(&mut right, spec.right_schema.clone(), pool, cancel)?;
        let split = match spec.on {
            Some(pred) => split_equi(pred, spec.left_schema.fields().len())?,
            None => EquiSplit {
                keys: Vec::new(),
                residual: None,
            },
        };
        if !split.keys.is_empty() && !chunks_on_disk(&right_chunks) {
            let build = take_batches(right_chunks, spec.right_schema.clone(), pool)?;
            return stream_probe(left, &build, &split, &spec, policy, workers, pool, cancel);
        }
        let left_chunks = collect_chunks(&mut left, spec.left_schema.clone(), pool, cancel)?;
        return inner_chunks(
            left_chunks,
            right_chunks,
            &spec,
            policy,
            workers,
            pool,
            cancel,
        );
    }
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
    // Unmatched rows for LEFT/RIGHT/FULL are relative to the whole opposite
    // side. Concatenate once, then emit unmatched rows a single time.
    let left = concat_chunk(
        &take_batches(left_chunks, spec.left_schema.clone(), pool)?,
        spec.left_schema.clone(),
        pool,
    )?;
    let right = concat_chunk(
        &take_batches(right_chunks, spec.right_schema.clone(), pool)?,
        spec.right_schema.clone(),
        pool,
    )?;
    join_pair(&left, &right, &spec, policy, workers, pool, cancel)
}

/// Inner join over both collected sides: hash partitions when either side
/// spilled, otherwise one in-memory build.
#[allow(clippy::too_many_arguments)]
fn inner_chunks(
    left_chunks: Vec<Chunk>,
    right_chunks: Vec<Chunk>,
    spec: &JoinSpec<'_>,
    policy: &BatchPolicy,
    workers: usize,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    if chunks_on_disk(&left_chunks) || chunks_on_disk(&right_chunks) {
        return partitioned_inner_join(
            left_chunks,
            right_chunks,
            spec,
            policy,
            workers,
            pool,
            cancel,
        );
    }
    let left = take_batches(left_chunks, spec.left_schema.clone(), pool)?;
    let right = take_batches(right_chunks, spec.right_schema.clone(), pool)?;
    if left.len() == 1 && right.len() == 1 {
        return join_pair(&left[0], &right[0], spec, policy, workers, pool, cancel);
    }
    hash_join_streamed(&left, &right, spec, policy, workers, pool, cancel)
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
    let left_chunks = collect_chunks(
        &mut left,
        spec.left_schema.clone(),
        pool,
        &session.exec_token(),
    )?;
    let right_chunks = collect_chunks(
        &mut right,
        spec.right_schema.clone(),
        pool,
        &session.exec_token(),
    )?;
    if pool.has_limit() {
        let joined = partitioned_inner_join(
            left_chunks,
            right_chunks,
            &spec,
            &session.policy,
            session.workers,
            pool,
            &session.exec_token(),
        )?;
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
            &session.exec_token(),
        );
    }
    let left = take_batches(left_chunks, spec.left_schema.clone(), pool)?;
    let right = take_batches(right_chunks, spec.right_schema.clone(), pool)?;
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
    // One build table, then probe morsels. A batch-pair loop here is O(L×R)
    // hash builds and is the #126 store_sales worker cliff.
    let joined = hash_join_streamed(
        &left,
        &right,
        &spec,
        &session.policy,
        session.workers,
        pool,
        &session.exec_token(),
    )?;
    if joined.is_empty() {
        return agg::merge_finish(Vec::new(), group, aggs, out_schema, &session.policy);
    }
    if group.is_empty() {
        let n = session.workers.max(1).min(joined.len());
        let mut chunks = vec![Vec::new(); n];
        for (i, batch) in joined.into_iter().enumerate() {
            chunks[i % n].push(batch);
        }
        let tables = Mutex::new(Vec::new());
        let token = session.exec_token();
        thread::scope(|scope| -> Result<()> {
            let mut handles = Vec::new();
            for chunk in chunks {
                let tables = &tables;
                let out_schema = out_schema.clone();
                let token = &token;
                handles.push(scope.spawn(move || -> Result<()> {
                    token.check()?;
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
        return agg::merge_finish(tables, group, aggs, out_schema, &session.policy);
    }
    agg::run_parallel(
        joined,
        group,
        aggs,
        out_schema,
        &session.policy,
        session.workers,
        &session.exec_token(),
    )
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
        // Each worker folds a contiguous run of probe morsels into its own
        // per-build-row sums; merging in worker order keeps the result the
        // same for a given worker count. Workers are capped so the partial
        // arrays stay under F64_SUM_PARTIALS_MAX_BYTES.
        let per_worker = n_build.saturating_mul(9).max(1);
        let n = n.min((F64_SUM_PARTIALS_MAX_BYTES / per_worker).max(1));
        let chunk = probe_batches.len().div_ceil(n).max(1);
        let token = session.exec_token();
        let fold = |batches: &[Batch]| -> Result<agg::BuildF64Sums> {
            let mut sums = agg::BuildF64Sums::new(out_schema.clone(), group.len(), n_build);
            for probe in batches {
                token.check()?;
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
            Ok(sums)
        };
        let mut partials: Vec<agg::BuildF64Sums> = if n <= 1 {
            vec![fold(probe_batches)?]
        } else {
            thread::scope(|scope| -> Result<Vec<agg::BuildF64Sums>> {
                let handles: Vec<_> = probe_batches
                    .chunks(chunk)
                    .map(|batches| scope.spawn(move || fold(batches)))
                    .collect();
                handles
                    .into_iter()
                    .map(|h| {
                        h.join()
                            .map_err(|_| Error::Eval("worker panicked".into()))?
                    })
                    .collect()
            })?
        };
        let mut sums = partials.remove(0);
        for part in &partials {
            sums.merge(part);
        }
        return Ok(Some(sums.finish(group_cols, &session.policy)?));
    }
    if unique_groups {
        let parts = Mutex::new(Vec::new());
        let parts_ref = &parts;
        let token = session.exec_token();
        thread::scope(|scope| -> Result<()> {
            let mut handles = Vec::new();
            for _ in 0..n {
                let out_schema = out_schema.clone();
                let queue = &queue;
                let token = &token;
                handles.push(scope.spawn(move || -> Result<()> {
                    token.check()?;
                    let mut slots_acc = agg::BuildSlots::new(group, aggs, out_schema, n_build)?;
                    drain_probe_queue(
                        queue,
                        probe_batches,
                        probe_key_exprs,
                        probe_aggs,
                        min,
                        slots,
                        token,
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
    let token = session.exec_token();
    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for _ in 0..n {
            let out_schema = out_schema.clone();
            let queue = &queue;
            let token = &token;
            handles.push(scope.spawn(move || -> Result<()> {
                token.check()?;
                let mut fold = agg::BuildFold::new(group, aggs, out_schema, n_build)?;
                drain_probe_queue(
                    queue,
                    probe_batches,
                    probe_key_exprs,
                    probe_aggs,
                    min,
                    slots,
                    token,
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

#[allow(clippy::too_many_arguments)]
fn drain_probe_queue(
    queue: &Mutex<Vec<usize>>,
    probe_batches: &[Batch],
    probe_key_exprs: &[&Expr],
    probe_aggs: &[AggCall],
    min: i128,
    slots: &[u32],
    cancel: &CancelToken,
    mut on_match: impl FnMut(usize, &[Option<ArrayRef>], usize) -> Result<()>,
) -> Result<()> {
    loop {
        cancel.check()?;
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
            let batches = pool.read_spill(p, schema.clone())?;
            concat_chunk(&batches, schema, pool)
        }
    }
}

fn take_batches(chunks: Vec<Chunk>, schema: SchemaRef, pool: &MemoryPool) -> Result<Vec<Batch>> {
    let mut out = Vec::new();
    for chunk in chunks {
        match chunk {
            Chunk::Mem(b) => {
                if b.num_rows() > 0 {
                    out.push(b);
                }
            }
            Chunk::File(p) => {
                for b in pool.read_spill(&p, schema.clone())? {
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
    let path = pool.write_spill_file(schema, std::slice::from_ref(&batch))?;
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

fn partition_count(n_left: usize, n_right: usize) -> usize {
    n_left.max(n_right).next_power_of_two().clamp(4, 64)
}

const GRACE_MAX_DEPTH: u32 = 4;

fn chunk_bytes(chunk: &Chunk) -> u64 {
    match chunk {
        Chunk::Mem(b) => batch_bytes(b),
        Chunk::File(p) => std::fs::metadata(p).map(|m| m.len()).unwrap_or(u64::MAX),
    }
}

fn chunks_fit(chunks: &[Chunk], pool: &MemoryPool) -> bool {
    let Some(limit) = pool.limit() else {
        return true;
    };
    let bytes: u64 = chunks.iter().map(chunk_bytes).sum();
    bytes <= limit / 2
}

/// Inner equi-join: hash-partition both sides, then join each partition pair
/// once. A batch-pair loop rebuilds a hash table per left×right chunk.
/// Oversized or skewed partitions recurse, then fall back to one chunk pair
/// at a time so the working set stays bounded by a pair of chunks.
fn partitioned_inner_join(
    left_chunks: Vec<Chunk>,
    right_chunks: Vec<Chunk>,
    spec: &JoinSpec<'_>,
    policy: &BatchPolicy,
    workers: usize,
    pool: &MemoryPool,
    cancel: &CancelToken,
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
        return nested_chunk_join(
            &left_chunks,
            &right_chunks,
            spec,
            policy,
            workers,
            pool,
            cancel,
        );
    }
    let left_keys: Vec<Expr> = split.keys.iter().map(|(l, _)| l.clone()).collect();
    let right_keys: Vec<Expr> = split.keys.iter().map(|(_, r)| r.clone()).collect();
    grace_join(
        left_chunks,
        right_chunks,
        &left_keys,
        &right_keys,
        spec,
        policy,
        workers,
        pool,
        cancel,
        0,
    )
}

#[allow(clippy::too_many_arguments)]
fn grace_join(
    left_chunks: Vec<Chunk>,
    right_chunks: Vec<Chunk>,
    left_keys: &[Expr],
    right_keys: &[Expr],
    spec: &JoinSpec<'_>,
    policy: &BatchPolicy,
    workers: usize,
    pool: &MemoryPool,
    cancel: &CancelToken,
    depth: u32,
) -> Result<Vec<Batch>> {
    cancel.check()?;
    if left_chunks.is_empty() || right_chunks.is_empty() {
        return Ok(Vec::new());
    }
    if left_chunks.len() <= 1 && right_chunks.len() <= 1 {
        return join_loaded_chunks(
            &left_chunks,
            &right_chunks,
            spec,
            policy,
            workers,
            pool,
            cancel,
        );
    }
    if chunks_fit(&left_chunks, pool) && chunks_fit(&right_chunks, pool) {
        let left = take_batches(left_chunks, spec.left_schema.clone(), pool)?;
        let right = take_batches(right_chunks, spec.right_schema.clone(), pool)?;
        if left.iter().all(|b| b.num_rows() == 0) || right.iter().all(|b| b.num_rows() == 0) {
            return Ok(Vec::new());
        }
        return hash_join_streamed(&left, &right, spec, policy, workers, pool, cancel);
    }
    if depth >= GRACE_MAX_DEPTH {
        return nested_chunk_join(
            &left_chunks,
            &right_chunks,
            spec,
            policy,
            workers,
            pool,
            cancel,
        );
    }
    let npart = partition_count(left_chunks.len(), right_chunks.len());
    let mut left_parts = partition_side(
        &left_chunks,
        spec.left_schema.clone(),
        left_keys,
        npart,
        depth,
        pool,
        cancel,
    )?;
    let mut right_parts = partition_side(
        &right_chunks,
        spec.right_schema.clone(),
        right_keys,
        npart,
        depth,
        pool,
        cancel,
    )?;
    let mut out = Vec::new();
    for i in 0..npart {
        out.extend(grace_join(
            std::mem::take(&mut left_parts[i]),
            std::mem::take(&mut right_parts[i]),
            left_keys,
            right_keys,
            spec,
            policy,
            workers,
            pool,
            cancel,
            depth + 1,
        )?);
    }
    Ok(out)
}

fn join_loaded_chunks(
    left_chunks: &[Chunk],
    right_chunks: &[Chunk],
    spec: &JoinSpec<'_>,
    policy: &BatchPolicy,
    workers: usize,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let left = match left_chunks.first() {
        Some(c) => load_chunk(c, spec.left_schema.clone(), pool)?,
        None => Batch::empty(spec.left_schema.clone())?,
    };
    let right = match right_chunks.first() {
        Some(c) => load_chunk(c, spec.right_schema.clone(), pool)?,
        None => Batch::empty(spec.right_schema.clone())?,
    };
    hash_join_streamed(
        std::slice::from_ref(&left),
        std::slice::from_ref(&right),
        spec,
        policy,
        workers,
        pool,
        cancel,
    )
}

#[allow(clippy::too_many_arguments)]
fn partition_side(
    chunks: &[Chunk],
    schema: SchemaRef,
    key_exprs: &[Expr],
    npart: usize,
    depth: u32,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Vec<Chunk>>> {
    let mut parts: Vec<Vec<Chunk>> = (0..npart).map(|_| Vec::new()).collect();
    for chunk in chunks {
        cancel.check()?;
        let batch = load_chunk(chunk, schema.clone(), pool)?;
        let scattered = scatter_batch(&batch, schema.clone(), key_exprs, npart, depth, pool)?;
        for (i, piece) in scattered.into_iter().enumerate() {
            if let Some(piece) = piece {
                if pool.can_spill() {
                    parts[i].push(spill_chunk(
                        std::slice::from_ref(&piece),
                        schema.clone(),
                        pool,
                    )?);
                } else {
                    parts[i].push(Chunk::Mem(piece));
                }
            }
        }
    }
    Ok(parts)
}

/// Power-of-two bucket. Depth takes the next 6 hash bits (max partition
/// count is 64) so a later pass can split rows that shared a first-pass
/// bucket. Adding a salt then masking cannot.
fn partition_bucket(h: u64, npart: usize, depth: u32) -> usize {
    let shift = (depth * 6) % 64;
    ((h >> shift) as usize) & (npart - 1)
}

fn scatter_batch(
    batch: &Batch,
    schema: SchemaRef,
    key_exprs: &[Expr],
    npart: usize,
    depth: u32,
    pool: &MemoryPool,
) -> Result<Vec<Option<Batch>>> {
    if batch.num_rows() == 0 {
        return Ok(vec![None; npart]);
    }
    let keys: Vec<ArrayRef> = key_exprs
        .iter()
        .map(|e| e.eval(batch))
        .collect::<Result<_>>()?;
    let mut buckets: Vec<Vec<u32>> = vec![Vec::new(); npart];
    for row in 0..batch.num_rows() {
        if row_has_null(&keys, row) {
            continue;
        }
        let h = hash_row(&keys, row)?;
        buckets[partition_bucket(h, npart, depth)].push(row as u32);
    }
    let mut out = Vec::with_capacity(npart);
    for idx in buckets {
        if idx.is_empty() {
            out.push(None);
        } else {
            out.push(Some(take_left(batch, &idx, schema.clone(), pool)?));
        }
    }
    Ok(out)
}

struct PartTable {
    batch: Batch,
    table: BuildTable,
}

#[cfg(test)]
impl Drop for PartTable {
    fn drop(&mut self) {
        JOIN_EVENTS.with(|e| e.borrow_mut().push("drop_build"));
    }
}

/// Upper bound on the per-worker partial sums of a parallel float-SUM fold.
const F64_SUM_PARTIALS_MAX_BYTES: usize = 256 * 1024 * 1024;

/// Build sides below this many rows stay one table: partitioning a small
/// build costs a scatter pass and gives up the dense unique-key index, and
/// one table is already fast to build and to probe.
const PARTITIONED_BUILD_MIN_ROWS: usize = 131_072;

/// Probe rows gathered per worker before a probe group is split across
/// workers, so a thread is spawned per group, never per morsel.
const PROBE_ROWS_PER_WORKER: usize = 16_384;

fn build_partition_count(workers: usize, build_rows: usize) -> usize {
    if build_rows < PARTITIONED_BUILD_MIN_ROWS {
        return 1;
    }
    workers.max(1).next_power_of_two().clamp(1, 64)
}

/// Scatter build morsels into hash partitions on worker threads, then
/// finalise each partition's table (one partition per task, no insert lock).
/// Small build sides stay one table (`build_partition_count`).
fn parallel_build(
    build_batches: &[Batch],
    key_exprs: &[Expr],
    schema: SchemaRef,
    probe_key_sample: &[ArrayRef],
    workers: usize,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Option<PartTable>>> {
    let n_rows: usize = build_batches.iter().map(Batch::num_rows).sum();
    let mut npart = build_partition_count(workers, n_rows);
    if npart > 1 && dense_int_build(build_batches, key_exprs, probe_key_sample)? {
        npart = 1;
    }
    partitioned_build(
        build_batches,
        key_exprs,
        schema,
        probe_key_sample,
        workers,
        npart,
        pool,
        cancel,
    )
}

/// A single integer build key whose span is dense enough for a direct
/// index (`dense_len`). One such table builds in one pass and probes by array
/// index, which beats hash partitions however large the build side is.
fn dense_int_build(
    build_batches: &[Batch],
    key_exprs: &[Expr],
    probe_key_sample: &[ArrayRef],
) -> Result<bool> {
    let ([key], [probe]) = (key_exprs, probe_key_sample) else {
        return Ok(false);
    };
    let mut span: Option<(i128, i128)> = None;
    let mut count = 0usize;
    for batch in build_batches {
        let col = key.eval(batch)?;
        if col.data_type() != probe.data_type() {
            return Ok(false);
        }
        let bounds = match col.data_type() {
            DataType::Int64 => int_bounds::<Int64Type>(col.as_primitive()),
            DataType::Int32 => int_bounds::<Int32Type>(col.as_primitive()),
            DataType::UInt64 => int_bounds::<UInt64Type>(col.as_primitive()),
            DataType::UInt32 => int_bounds::<UInt32Type>(col.as_primitive()),
            _ => return Ok(false),
        };
        count += col.len() - col.null_count();
        if let Some((lo, hi)) = bounds {
            span = Some(match span {
                Some((min, max)) => (min.min(lo), max.max(hi)),
                None => (lo, hi),
            });
        }
    }
    Ok(span.is_some_and(|(min, max)| dense_len(min, max, count, false).is_some()))
}

fn int_bounds<T: ArrowPrimitiveType>(arr: &PrimitiveArray<T>) -> Option<(i128, i128)>
where
    T::Native: IntKey,
{
    let min = arrow::compute::min(arr)?.as_i128();
    let max = arrow::compute::max(arr)?.as_i128();
    Some((min, max))
}

/// Build `npart` hash partitions of the build side on up to `workers` threads.
#[allow(clippy::too_many_arguments)]
fn partitioned_build(
    build_batches: &[Batch],
    key_exprs: &[Expr],
    schema: SchemaRef,
    probe_key_sample: &[ArrayRef],
    workers: usize,
    npart: usize,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Option<PartTable>>> {
    let n_rows: usize = build_batches.iter().map(Batch::num_rows).sum();
    if npart == 1 || n_rows == 0 || build_batches.len() <= 1 && workers <= 1 {
        let build = concat_chunk(build_batches, schema, pool)?;
        let keys: Vec<ArrayRef> = key_exprs
            .iter()
            .map(|e| e.eval(&build))
            .collect::<Result<_>>()?;
        let table = BuildTable::new(&keys, probe_key_sample, build.num_rows())?;
        return Ok(vec![Some(PartTable {
            batch: build,
            table,
        })]);
    }
    let queue = Mutex::new((0..build_batches.len()).collect::<Vec<usize>>());
    let locals = Mutex::new(Vec::<Vec<Vec<Batch>>>::new());
    let n = workers.max(1).min(build_batches.len().max(1));
    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for _ in 0..n {
            let queue = &queue;
            let locals = &locals;
            let schema = schema.clone();
            handles.push(scope.spawn(move || -> Result<()> {
                let mut local: Vec<Vec<Batch>> = (0..npart).map(|_| Vec::new()).collect();
                loop {
                    cancel.check()?;
                    let i = {
                        let mut q = queue
                            .lock()
                            .map_err(|_| Error::Eval("join build lock poisoned".into()))?;
                        q.pop()
                    };
                    let Some(i) = i else {
                        break;
                    };
                    let scattered = scatter_batch(
                        &build_batches[i],
                        schema.clone(),
                        key_exprs,
                        npart,
                        0,
                        pool,
                    )?;
                    for (p, piece) in scattered.into_iter().enumerate() {
                        if let Some(piece) = piece {
                            local[p].push(piece);
                        }
                    }
                }
                locals
                    .lock()
                    .map_err(|_| Error::Eval("join build lock poisoned".into()))?
                    .push(local);
                Ok(())
            }));
        }
        for h in handles {
            h.join()
                .map_err(|_| Error::Eval("worker panicked".into()))??;
        }
        Ok(())
    })?;
    let locals = locals
        .into_inner()
        .map_err(|_| Error::Eval("join build lock poisoned".into()))?;
    let mut merged: Vec<Vec<Batch>> = (0..npart).map(|_| Vec::new()).collect();
    for local in locals {
        for (p, pieces) in local.into_iter().enumerate() {
            merged[p].extend(pieces);
        }
    }
    let finished = Mutex::new(Vec::<(usize, PartTable)>::new());
    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for (p, pieces) in merged.into_iter().enumerate() {
            if pieces.is_empty() {
                continue;
            }
            let finished = &finished;
            let schema = schema.clone();
            handles.push(scope.spawn(move || -> Result<()> {
                cancel.check()?;
                let batch = concat_chunk(&pieces, schema.clone(), pool)?;
                let keys: Vec<ArrayRef> = key_exprs
                    .iter()
                    .map(|e| e.eval(&batch))
                    .collect::<Result<_>>()?;
                let table = BuildTable::new_partitioned(&keys, probe_key_sample, batch.num_rows())?;
                finished
                    .lock()
                    .map_err(|_| Error::Eval("join build lock poisoned".into()))?
                    .push((p, PartTable { batch, table }));
                Ok(())
            }));
        }
        for h in handles {
            h.join()
                .map_err(|_| Error::Eval("worker panicked".into()))??;
        }
        Ok(())
    })?;
    let mut finished = finished
        .into_inner()
        .map_err(|_| Error::Eval("join build lock poisoned".into()))?;
    finished.sort_by_key(|(p, _)| *p);
    let mut parts: Vec<Option<PartTable>> = (0..npart).map(|_| None).collect();
    for (p, part) in finished {
        parts[p] = Some(part);
    }
    #[cfg(test)]
    {
        // BuildTable::new ran on worker threads; thread_local counters live here.
        let tables = parts.iter().filter(|p| p.is_some()).count();
        let rows: usize = parts
            .iter()
            .filter_map(|p| p.as_ref().map(|p| p.batch.num_rows()))
            .sum();
        BUILD_TABLES.with(|c| c.set(c.get() + tables));
        BUILD_ROWS.with(|c| c.set(c.get() + rows));
        JOIN_EVENTS.with(|e| {
            for _ in 0..tables.max(1) {
                e.borrow_mut().push("build");
            }
        });
    }
    Ok(parts)
}

#[allow(clippy::too_many_arguments)]
fn probe_parts(
    parts: &[Option<PartTable>],
    npart: usize,
    probe: &Batch,
    probe_key_exprs: &[Expr],
    residual: Option<&Expr>,
    build_left: bool,
    schema: SchemaRef,
    policy: &BatchPolicy,
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    if probe.num_rows() == 0 {
        return Ok(Vec::new());
    }
    let key_refs: Vec<&Expr> = probe_key_exprs.iter().collect();
    if npart == 1 {
        let Some(part) = parts[0].as_ref() else {
            return Ok(Vec::new());
        };
        return probe_inner_batch(
            &part.table,
            &part.batch,
            probe,
            &key_refs,
            residual,
            build_left,
            schema,
            policy,
            pool,
        );
    }
    // Probe each row against its partition's table in place and keep the
    // matches as (probe row, build row) pairs per partition. The probe batch
    // is never scattered; only matched rows are gathered, once, for output.
    // Output stays grouped by partition, probe order within a partition.
    let probe_keys: Vec<ArrayRef> = probe_key_exprs
        .iter()
        .map(|e| e.eval(probe))
        .collect::<Result<_>>()?;
    let mut matches: Vec<(Vec<u32>, Vec<u32>)> = vec![(Vec::new(), Vec::new()); npart];
    for row in 0..probe.num_rows() {
        if row_has_null(&probe_keys, row) {
            continue;
        }
        let p = partition_bucket(hash_row(&probe_keys, row)?, npart, 0);
        let Some(part) = parts[p].as_ref() else {
            continue;
        };
        let (probe_rows, build_rows) = &mut matches[p];
        part.table.for_matches(&probe_keys, row, |brow| {
            let keep = match residual {
                None => true,
                Some(_) if build_left => {
                    residual_ok(&part.batch, probe, brow as usize, row, residual, pool)?
                }
                Some(_) => residual_ok(probe, &part.batch, row, brow as usize, residual, pool)?,
            };
            if keep {
                probe_rows.push(row as u32);
                build_rows.push(brow);
            }
            Ok(true)
        })?;
    }
    let mut out = Vec::new();
    for (p, (probe_rows, build_rows)) in matches.into_iter().enumerate() {
        if probe_rows.is_empty() {
            continue;
        }
        let Some(part) = parts[p].as_ref() else {
            continue;
        };
        let joined = if build_left {
            hconcat_take(
                &part.batch,
                probe,
                &build_rows,
                &probe_rows,
                schema.clone(),
                pool,
            )?
        } else {
            hconcat_take(
                probe,
                &part.batch,
                &probe_rows,
                &build_rows,
                schema.clone(),
                pool,
            )?
        };
        out.extend(
            joined
                .split_by_policy(policy)
                .into_iter()
                .filter(|b| b.num_rows() > 0),
        );
    }
    Ok(out)
}

/// Probe a group of morsels, spreading them across up to `workers` threads.
/// Morsels are probed zero-copy and the output keeps their order.
#[allow(clippy::too_many_arguments)]
fn probe_group_parallel(
    parts: &[Option<PartTable>],
    npart: usize,
    group: &[Batch],
    probe_key_exprs: &[Expr],
    residual: Option<&Expr>,
    build_left: bool,
    schema: SchemaRef,
    policy: &BatchPolicy,
    workers: usize,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let probe_one = |probe: &Batch| {
        probe_parts(
            parts,
            npart,
            probe,
            probe_key_exprs,
            residual,
            build_left,
            schema.clone(),
            policy,
            pool,
        )
    };
    // A morsel much larger than one worker's share is sliced (zero-copy), so
    // a few large batches still spread across workers.
    let mut sliced = Vec::new();
    for probe in group {
        let rows = probe.num_rows();
        if workers > 1 && rows > 2 * PROBE_ROWS_PER_WORKER {
            let mut offset = 0;
            while offset < rows {
                let len = PROBE_ROWS_PER_WORKER.min(rows - offset);
                sliced.push(probe.slice(offset, len)?);
                offset += len;
            }
        } else {
            sliced.push(probe.clone());
        }
    }
    let group = &sliced[..];
    let n = workers.max(1).min(group.len());
    if n <= 1 {
        let mut out = Vec::new();
        for probe in group {
            cancel.check()?;
            out.extend(probe_one(probe)?);
        }
        return Ok(out);
    }
    let next = AtomicUsize::new(0);
    let results = Mutex::new(Vec::<(usize, Vec<Batch>)>::with_capacity(group.len()));
    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::with_capacity(n);
        for _ in 0..n {
            let (next, results, probe_one) = (&next, &results, &probe_one);
            handles.push(scope.spawn(move || -> Result<()> {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(probe) = group.get(index) else {
                        return Ok(());
                    };
                    cancel.check()?;
                    let batches = probe_one(probe)?;
                    results
                        .lock()
                        .map_err(|_| Error::Eval("join probe lock poisoned".into()))?
                        .push((index, batches));
                }
            }));
        }
        for h in handles {
            h.join()
                .map_err(|_| Error::Eval("worker panicked".into()))??;
        }
        Ok(())
    })?;
    let mut results = results
        .into_inner()
        .map_err(|_| Error::Eval("join probe lock poisoned".into()))?;
    results.sort_by_key(|(index, _)| *index);
    Ok(results.into_iter().flat_map(|(_, b)| b).collect())
}

fn key_sample(schema: SchemaRef, keys: &[Expr]) -> Result<Vec<ArrayRef>> {
    let empty = Batch::empty(schema)?;
    keys.iter().map(|e| e.eval(&empty)).collect()
}

/// Unbounded inner join: build the right side (optimizer smaller), probe
/// left morsels as they arrive.
#[allow(clippy::too_many_arguments)]
fn hash_join_stream_probe(
    mut left: Box<dyn Operator>,
    mut right: Box<dyn Operator>,
    spec: &JoinSpec<'_>,
    policy: &BatchPolicy,
    workers: usize,
    pool: &MemoryPool,
    cancel: &CancelToken,
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
        let l = concat_chunk(
            &take_batches(
                collect_chunks(&mut left, spec.left_schema.clone(), pool, cancel)?,
                spec.left_schema.clone(),
                pool,
            )?,
            spec.left_schema.clone(),
            pool,
        )?;
        let r = concat_chunk(
            &take_batches(
                collect_chunks(&mut right, spec.right_schema.clone(), pool, cancel)?,
                spec.right_schema.clone(),
                pool,
            )?,
            spec.right_schema.clone(),
            pool,
        )?;
        return join_pair(&l, &r, spec, policy, workers, pool, cancel);
    }
    let right_chunks = collect_chunks(&mut right, spec.right_schema.clone(), pool, cancel)?;
    let build_batches = take_batches(right_chunks, spec.right_schema.clone(), pool)?;
    stream_probe(
        left,
        &build_batches,
        &split,
        spec,
        policy,
        workers,
        pool,
        cancel,
    )
}

/// Build one table set from the collected right side, then probe left
/// morsels in groups as the left operator yields them. The probe side is
/// never collected or concatenated; only matched rows are gathered.
///
/// Under a memory limit the build side and every retained probe batch hold
/// pool reservations. A group is probed early when the next batch does not
/// fit, which releases its reservations. When a batch still does not fit,
/// the rest of the probe side takes the collected path ([`collect_chunks`]),
/// which spills or refuses an oversized batch.
#[allow(clippy::too_many_arguments)]
fn stream_probe(
    mut left: Box<dyn Operator>,
    build_batches: &[Batch],
    split: &EquiSplit,
    spec: &JoinSpec<'_>,
    policy: &BatchPolicy,
    workers: usize,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let limited = pool.has_limit();
    let build_held = if limited {
        match pool.try_reserve(build_batches.iter().map(batch_bytes).sum()) {
            Ok(r) => Some(r),
            Err(_) => {
                return collected_probe(
                    left,
                    None,
                    build_batches,
                    spec,
                    policy,
                    workers,
                    pool,
                    cancel,
                );
            }
        }
    } else {
        None
    };
    let right_keys: Vec<Expr> = split.keys.iter().map(|(_, r)| r.clone()).collect();
    let left_keys: Vec<Expr> = split.keys.iter().map(|(l, _)| l.clone()).collect();
    let probe_sample = key_sample(spec.left_schema.clone(), &left_keys)?;
    let parts = parallel_build(
        build_batches,
        &right_keys,
        spec.right_schema.clone(),
        &probe_sample,
        workers,
        pool,
        cancel,
    )?;
    let npart = parts.len();
    let residual = split.residual.as_ref();
    let probe_group = |group: &[Batch]| {
        probe_group_parallel(
            &parts,
            npart,
            group,
            &left_keys,
            residual,
            false,
            spec.schema.clone(),
            policy,
            workers,
            pool,
            cancel,
        )
    };
    let mut out = Vec::new();
    let group_rows = workers.max(1) * PROBE_ROWS_PER_WORKER;
    let mut group = Vec::new();
    let mut held: Vec<super::memory::Reservation<'_>> = Vec::new();
    let mut gathered = 0;
    // A probe batch that does not fit beside the build ends the streamed
    // probe; it and the rest of `left` take the collected path below.
    let pending = loop {
        let probe = left.next_batch(cancel)?;
        let done = probe.is_none();
        if let Some(probe) = probe {
            if limited {
                let sz = batch_bytes(&probe);
                let r = match pool.try_reserve(sz) {
                    Ok(r) => r,
                    Err(_) => {
                        if !group.is_empty() {
                            out.extend(probe_group(&group)?);
                            group.clear();
                            held.clear();
                            gathered = 0;
                        }
                        match pool.try_reserve(sz) {
                            Ok(r) => r,
                            Err(_) => break probe,
                        }
                    }
                };
                held.push(r);
            }
            gathered += probe.num_rows();
            group.push(probe);
        }
        if !group.is_empty() && (done || gathered >= group_rows) {
            out.extend(probe_group(&group)?);
            group.clear();
            held.clear();
            gathered = 0;
        }
        if done {
            return Ok(out);
        }
    };
    // Release this table set before the collected path builds its own.
    drop(parts);
    drop(build_held);
    out.extend(collected_probe(
        left,
        Some(pending),
        build_batches,
        spec,
        policy,
        workers,
        pool,
        cancel,
    )?);
    Ok(out)
}

/// Finish a memory-limited inner join on the collected path: `first` and the
/// rest of `left` are collected (spilling or refusing as [`collect_chunks`]
/// does) and joined against the build side.
#[allow(clippy::too_many_arguments)]
fn collected_probe(
    left: Box<dyn Operator>,
    first: Option<Batch>,
    build_batches: &[Batch],
    spec: &JoinSpec<'_>,
    policy: &BatchPolicy,
    workers: usize,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    #[cfg(test)]
    JOIN_EVENTS.with(|e| e.borrow_mut().push("collected_probe"));
    let mut rest = Prepend { first, rest: left };
    let left_chunks = collect_chunks(&mut rest, spec.left_schema.clone(), pool, cancel)?;
    let right_chunks = build_batches.iter().cloned().map(Chunk::Mem).collect();
    inner_chunks(
        left_chunks,
        right_chunks,
        spec,
        policy,
        workers,
        pool,
        cancel,
    )
}

/// Yields `first`, then every batch of `rest`.
struct Prepend {
    first: Option<Batch>,
    rest: Box<dyn Operator>,
}

impl Operator for Prepend {
    fn next_batch(&mut self, cancel: &CancelToken) -> Result<Option<Batch>> {
        match self.first.take() {
            Some(batch) => Ok(Some(batch)),
            None => self.rest.next_batch(cancel),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn nested_chunk_join(
    left_chunks: &[Chunk],
    right_chunks: &[Chunk],
    spec: &JoinSpec<'_>,
    policy: &BatchPolicy,
    workers: usize,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let mut parts = Vec::new();
    for l in left_chunks {
        let l = load_chunk(l, spec.left_schema.clone(), pool)?;
        for r in right_chunks {
            let r = load_chunk(r, spec.right_schema.clone(), pool)?;
            parts.push(join_pair(&l, &r, spec, policy, workers, pool, cancel)?);
        }
    }
    Ok(parts
        .into_iter()
        .flatten()
        .filter(|b| b.num_rows() > 0)
        .collect())
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
    cancel: &CancelToken,
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
            cancel,
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
    cancel: &CancelToken,
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
        return join_pair(&l, &r, spec, policy, workers, pool, cancel);
    }
    let ln: usize = left.iter().map(Batch::num_rows).sum();
    let rn: usize = right.iter().map(Batch::num_rows).sum();
    let build_left = ln < rn;
    let (build_batches, probe_batches, build_schema, build_key_exprs, probe_key_exprs) =
        if build_left {
            (
                left,
                right,
                spec.left_schema.clone(),
                split
                    .keys
                    .iter()
                    .map(|(l, _)| l.clone())
                    .collect::<Vec<_>>(),
                split
                    .keys
                    .iter()
                    .map(|(_, r)| r.clone())
                    .collect::<Vec<_>>(),
            )
        } else {
            (
                right,
                left,
                spec.right_schema.clone(),
                split
                    .keys
                    .iter()
                    .map(|(_, r)| r.clone())
                    .collect::<Vec<_>>(),
                split
                    .keys
                    .iter()
                    .map(|(l, _)| l.clone())
                    .collect::<Vec<_>>(),
            )
        };
    let probe_schema = if build_left {
        spec.right_schema.clone()
    } else {
        spec.left_schema.clone()
    };
    let probe_sample = match probe_batches.iter().find(|b| b.num_rows() > 0) {
        Some(b) => probe_key_exprs
            .iter()
            .map(|e| e.eval(b))
            .collect::<Result<Vec<_>>>()?,
        None => key_sample(probe_schema, &probe_key_exprs)?,
    };
    let parts = parallel_build(
        build_batches,
        &build_key_exprs,
        build_schema,
        &probe_sample,
        workers,
        pool,
        cancel,
    )?;
    let npart = parts.len();
    let residual = split.residual.as_ref();
    let n = workers.max(1).min(probe_batches.len().max(1));
    if n <= 1 {
        let mut out = Vec::new();
        for probe in probe_batches {
            cancel.check()?;
            out.extend(probe_parts(
                &parts,
                npart,
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
    let out_parts = Mutex::new(Vec::new());
    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for _ in 0..n {
            let out_parts = &out_parts;
            let queue = &queue;
            let parts = &parts;
            let probe_key_exprs = &probe_key_exprs;
            handles.push(scope.spawn(move || -> Result<()> {
                loop {
                    cancel.check()?;
                    let i = {
                        let mut q = queue
                            .lock()
                            .map_err(|_| Error::Eval("join probe lock poisoned".into()))?;
                        q.pop()
                    };
                    let Some(i) = i else {
                        return Ok(());
                    };
                    let batches = probe_parts(
                        parts,
                        npart,
                        &probe_batches[i],
                        probe_key_exprs,
                        residual,
                        build_left,
                        spec.schema.clone(),
                        policy,
                        pool,
                    )?;
                    out_parts
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
    let mut out_parts = out_parts
        .into_inner()
        .map_err(|_| Error::Eval("join probe lock poisoned".into()))?;
    out_parts.sort_by_key(|(i, _)| *i);
    Ok(out_parts.into_iter().flat_map(|(_, b)| b).collect())
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
    cancel: &CancelToken,
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
            &table, probe_keys, left, right, build_left, schema, n, policy, pool, cancel,
        );
    }
    if n <= 1 {
        cancel.check()?;
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
                cancel.check()?;
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
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let n_probe = if build_left {
        right.num_rows()
    } else {
        left.num_rows()
    };
    let n = workers.max(1).min(n_probe.max(1));
    if n <= 1 {
        cancel.check()?;
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
                cancel.check()?;
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
        Self::from_keys(build_keys, probe_keys, n_build, true)
    }

    /// Hash partitions scatter consecutive keys, so skip the 1 MiB dense-index
    /// exemption that would otherwise allocate the full domain in every part.
    fn new_partitioned(
        build_keys: &[ArrayRef],
        probe_keys: &[ArrayRef],
        n_build: usize,
    ) -> Result<Self> {
        Self::from_keys(build_keys, probe_keys, n_build, false)
    }

    fn from_keys(
        build_keys: &[ArrayRef],
        probe_keys: &[ArrayRef],
        n_build: usize,
        range_index: bool,
    ) -> Result<Self> {
        #[cfg(test)]
        BUILD_TABLES.with(|c| c.set(c.get() + 1));
        #[cfg(test)]
        BUILD_ROWS.with(|c| c.set(c.get() + n_build));
        #[cfg(test)]
        JOIN_EVENTS.with(|e| e.borrow_mut().push("build"));
        if build_keys.len() == 1
            && probe_keys.len() == 1
            && build_keys[0].data_type() == probe_keys[0].data_type()
        {
            match build_keys[0].data_type() {
                DataType::Int64 => {
                    return Ok(Self::I64(int_relation::<Int64Type>(
                        build_keys[0].as_primitive(),
                        n_build,
                        range_index,
                    )));
                }
                DataType::Int32 => {
                    return Ok(Self::I32(int_relation::<Int32Type>(
                        build_keys[0].as_primitive(),
                        n_build,
                        range_index,
                    )));
                }
                DataType::UInt64 => {
                    return Ok(Self::U64(int_relation::<UInt64Type>(
                        build_keys[0].as_primitive(),
                        n_build,
                        range_index,
                    )));
                }
                DataType::UInt32 => {
                    return Ok(Self::U32(int_relation::<UInt32Type>(
                        build_keys[0].as_primitive(),
                        n_build,
                        range_index,
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

fn dense_len(min: i128, max: i128, n: usize, range_index: bool) -> Option<usize> {
    let range = max.checked_sub(min)?.checked_add(1)?;
    if range <= 0 || range > 64 * 1024 * 1024 {
        return None;
    }
    let range = range as usize;
    // Allow ~32 empty slots per live key so a filtered unique key
    // (200k hits in a 2M domain) still uses a direct index. Unpartitioned
    // tables also keep a 1 MiB-slot exemption. Hash partitions scatter
    // consecutive keys, so that exemption would allocate the full domain
    // in every partition.
    if range > n.saturating_mul(32) && (!range_index || range > 1_048_576) {
        return None;
    }
    Some(range)
}

#[allow(clippy::needless_range_loop)]
fn int_relation<T: ArrowPrimitiveType>(
    arr: &PrimitiveArray<T>,
    n: usize,
    range_index: bool,
) -> IntRelation<T::Native>
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
    if let Some(range) = dense_len(min, max, count, range_index) {
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
    mizuchi_plan::join_schema(&left.schema(), &right.schema())
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
        && let Some((offset, length)) = mizuchi_core::batch::contiguous_indices(indices)
    {
        return Ok(batch
            .as_arrow()
            .columns()
            .iter()
            .map(|c| c.slice(offset, length))
            .collect());
    }
    // The take index and the unmatched-row mask are built once and shared
    // by every column.
    let unmatched = indices.contains(&u32::MAX);
    let take_idx =
        UInt32Array::from_iter_values(indices.iter().map(|&i| if i == u32::MAX { 0 } else { i }));
    let mask =
        unmatched.then(|| BooleanArray::from_iter(indices.iter().map(|&i| Some(i != u32::MAX))));
    let mut out = Vec::with_capacity(batch.num_columns());
    for col in batch.as_arrow().columns() {
        if col.is_empty() {
            out.push(new_null_array(col.data_type(), indices.len()));
            continue;
        }
        let taken = take(col.as_ref(), &take_idx, None).map_err(|e| Error::Eval(e.to_string()))?;
        out.push(match &mask {
            None => taken,
            Some(mask) => {
                let nulls = new_null_array(col.data_type(), indices.len());
                zip(mask, &taken, &nulls).map_err(|e| Error::Eval(e.to_string()))?
            }
        });
    }
    let bytes: u64 = out.iter().map(|c| c.get_array_memory_size() as u64).sum();
    pool.record_copy(bytes);
    Ok(out)
}

fn take_left(left: &Batch, indices: &[u32], schema: SchemaRef, pool: &MemoryPool) -> Result<Batch> {
    let gathered = left.gather(indices)?;
    if !indices.is_empty() && mizuchi_core::batch::contiguous_indices(indices).is_none() {
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
thread_local! {
    static BUILD_TABLES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static BUILD_ROWS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static JOIN_EVENTS: std::cell::RefCell<Vec<&'static str>> = const { std::cell::RefCell::new(Vec::new()) };
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
        match int_relation::<Int64Type>(&arr, 200_000, true) {
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
        match int_relation::<Int64Type>(&arr, 3, true) {
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
        match int_relation::<Int64Type>(&arr, 3, true) {
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
        match int_relation::<Int64Type>(&arr, 2, true) {
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
            int_relation::<Int64Type>(&arr, 0, true),
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

    #[test]
    fn run_then_agg_builds_one_table_regardless_of_probe_batch_count() {
        use std::sync::Arc;

        use arrow::datatypes::{DataType, Field, Schema};

        BUILD_TABLES.with(|c| c.set(0));
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("x", DataType::Int64, true),
        ]));
        let join_schema = Arc::new(Schema::new(
            left_schema
                .fields()
                .iter()
                .cloned()
                .chain(right_schema.fields().iter().cloned())
                .collect::<Vec<_>>(),
        ));
        let out_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, true)]));
        let left: Vec<Batch> = (0..4)
            .map(|i| {
                Batch::try_new(
                    left_schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![1_i64, 1])),
                        Arc::new(Int64Array::from(vec![i, i + 10])),
                    ],
                )
                .unwrap()
            })
            .collect();
        let right: Vec<Batch> = (0..5)
            .map(|i| {
                Batch::try_new(
                    right_schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![1_i64])),
                        Arc::new(Int64Array::from(vec![i])),
                    ],
                )
                .unwrap()
            })
            .collect();
        let on = Expr::Binary(BinaryOp::Eq, Box::new(Expr::nth(0)), Box::new(Expr::nth(2)));
        let session = Session::new().with_workers(2).unwrap();
        let pool = MemoryPool::new(None, None).unwrap();
        let out = run_then_agg(
            Box::new(Exec::Scan {
                batches: left,
                index: 0,
            }),
            Box::new(Exec::Scan {
                batches: right,
                index: 0,
            }),
            JoinSpec {
                join_type: JoinType::Inner,
                on: Some(&on),
                left_schema,
                right_schema,
                schema: join_schema,
            },
            &[],
            &[("c".into(), AggCall::CountStar)],
            out_schema,
            &session,
            &pool,
        )
        .unwrap();
        let builds = BUILD_TABLES.with(|c| c.get());
        // fold_unique_i64_join may build once then return None; streamed
        // fallback builds one table per hash partition (workers=2 => 2).
        // L×R batch pairs would be 20.
        assert!(
            (1..=4).contains(&builds),
            "batch-pair fallback would build L×R tables, got {builds}"
        );
        let c = out[0].column(0).as_primitive::<Int64Type>();
        assert_eq!(c.value(0), 40);
    }

    #[test]
    fn recursive_partition_uses_fresh_hash_bits() {
        let npart = 64;
        let h1 = 0u64;
        let h2 = 1u64 << 6;
        assert_eq!(
            partition_bucket(h1, npart, 0),
            partition_bucket(h2, npart, 0)
        );
        assert_ne!(
            partition_bucket(h1, npart, 1),
            partition_bucket(h2, npart, 1)
        );
    }

    #[test]
    fn memory_limited_inner_join_builds_each_row_once() {
        use std::sync::Arc;
        use std::time::{SystemTime, UNIX_EPOCH};

        use arrow::datatypes::{DataType, Field, Schema};

        let left_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("x", DataType::Int64, true),
        ]));
        let join_schema = Arc::new(Schema::new(
            left_schema
                .fields()
                .iter()
                .cloned()
                .chain(right_schema.fields().iter().cloned())
                .collect::<Vec<_>>(),
        ));
        let left: Vec<Batch> = (0..4)
            .map(|i| {
                let a = i * 2;
                Batch::try_new(
                    left_schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![a, a + 1])),
                        Arc::new(Int64Array::from(vec![i, i + 10])),
                    ],
                )
                .unwrap()
            })
            .collect();
        let right: Vec<Batch> = (0..5)
            .map(|i| {
                Batch::try_new(
                    right_schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![i])),
                        Arc::new(Int64Array::from(vec![i])),
                    ],
                )
                .unwrap()
            })
            .collect();
        let cap = batch_bytes(&left[0]) + batch_bytes(&left[0]) / 2;
        let root = std::env::temp_dir().join(format!(
            "mizuchi-join-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let on = Expr::Binary(BinaryOp::Eq, Box::new(Expr::nth(0)), Box::new(Expr::nth(2)));
        let session = Session::new().with_workers(2).unwrap();
        let pool = MemoryPool::new(Some(cap), Some(root.clone())).unwrap();

        BUILD_ROWS.with(|c| c.set(0));
        let joined = run(
            Box::new(Exec::Scan {
                batches: left,
                index: 0,
            }),
            Box::new(Exec::Scan {
                batches: right,
                index: 0,
            }),
            JoinSpec {
                join_type: JoinType::Inner,
                on: Some(&on),
                left_schema: left_schema.clone(),
                right_schema: right_schema.clone(),
                schema: join_schema.clone(),
            },
            &session.policy,
            &pool,
            &session.exec_token(),
            session.workers,
        )
        .unwrap();
        let rows: usize = joined.iter().map(Batch::num_rows).sum();
        assert_eq!(rows, 5);
        let built = BUILD_ROWS.with(|c| c.get());
        // Nested left×right pairs would rebuild the 1-row right side 20 times.
        assert!((1..=5).contains(&built), "each build row once, got {built}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn unbounded_inner_join_builds_before_streaming_probe() {
        struct ProbeOp {
            batches: Vec<Batch>,
            i: usize,
        }
        impl Operator for ProbeOp {
            fn next_batch(&mut self, cancel: &CancelToken) -> Result<Option<Batch>> {
                cancel.check()?;
                JOIN_EVENTS.with(|e| e.borrow_mut().push("probe"));
                if self.i >= self.batches.len() {
                    return Ok(None);
                }
                let b = self.batches[self.i].clone();
                self.i += 1;
                Ok(Some(b))
            }
        }

        let left_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", DataType::Int64, true),
            arrow::datatypes::Field::new("v", DataType::Int64, true),
        ]));
        let right_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", DataType::Int64, true),
            arrow::datatypes::Field::new("x", DataType::Int64, true),
        ]));
        let join_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(
            left_schema
                .fields()
                .iter()
                .cloned()
                .chain(right_schema.fields().iter().cloned())
                .collect::<Vec<_>>(),
        ));
        let left: Vec<Batch> = (0..4)
            .map(|i| {
                let a = i * 2;
                Batch::try_new(
                    left_schema.clone(),
                    vec![
                        std::sync::Arc::new(Int64Array::from(vec![a, a + 1])),
                        std::sync::Arc::new(Int64Array::from(vec![i, i + 10])),
                    ],
                )
                .unwrap()
            })
            .collect();
        let right: Vec<Batch> = (0..5)
            .map(|i| {
                Batch::try_new(
                    right_schema.clone(),
                    vec![
                        std::sync::Arc::new(Int64Array::from(vec![i])),
                        std::sync::Arc::new(Int64Array::from(vec![i])),
                    ],
                )
                .unwrap()
            })
            .collect();
        JOIN_EVENTS.with(|e| e.borrow_mut().clear());
        let on = Expr::Binary(BinaryOp::Eq, Box::new(Expr::nth(0)), Box::new(Expr::nth(2)));
        let session = Session::new().with_workers(2).unwrap();
        let pool = MemoryPool::new(None, None).unwrap();
        let joined = run(
            Box::new(ProbeOp {
                batches: left,
                i: 0,
            }),
            Box::new(Exec::Scan {
                batches: right,
                index: 0,
            }),
            JoinSpec {
                join_type: JoinType::Inner,
                on: Some(&on),
                left_schema,
                right_schema,
                schema: join_schema,
            },
            &session.policy,
            &pool,
            &session.exec_token(),
            session.workers,
        )
        .unwrap();
        let rows: usize = joined.iter().map(Batch::num_rows).sum();
        assert_eq!(rows, 5);
        let events = JOIN_EVENTS.with(|e| e.borrow().clone());
        let first_build = events
            .iter()
            .position(|e| *e == "build")
            .expect("build table");
        let first_probe = events
            .iter()
            .position(|e| *e == "probe")
            .expect("probe pull");
        assert!(
            first_build < first_probe,
            "build must finish before the probe operator is pulled: {events:?}"
        );
    }

    #[test]
    fn mixed_integer_key_widths_do_not_panic() {
        use arrow::array::Int32Array;

        let left_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", DataType::Int32, true),
        ]));
        let right_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", DataType::Int64, true),
        ]));
        let join_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(
            left_schema
                .fields()
                .iter()
                .cloned()
                .chain(right_schema.fields().iter().cloned())
                .collect::<Vec<_>>(),
        ));
        let left = Batch::try_new(
            left_schema.clone(),
            vec![std::sync::Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let right = Batch::try_new(
            right_schema.clone(),
            vec![std::sync::Arc::new(Int64Array::from(vec![2_i64, 4]))],
        )
        .unwrap();
        let on = Expr::Binary(BinaryOp::Eq, Box::new(Expr::nth(0)), Box::new(Expr::nth(1)));
        let session = Session::new().with_workers(1).unwrap();
        let pool = MemoryPool::new(None, None).unwrap();
        let joined = run(
            Box::new(Exec::Scan {
                batches: vec![left],
                index: 0,
            }),
            Box::new(Exec::Scan {
                batches: vec![right],
                index: 0,
            }),
            JoinSpec {
                join_type: JoinType::Inner,
                on: Some(&on),
                left_schema,
                right_schema,
                schema: join_schema,
            },
            &session.policy,
            &pool,
            &session.exec_token(),
            session.workers,
        )
        .unwrap();
        let _ = joined;
    }

    #[test]
    fn partitioned_sparse_range_skips_million_slot_exemption() {
        let values: Vec<i64> = (0..64).map(|i| i * 15_625).collect();
        let arr = Int64Array::from(values);
        match int_relation::<Int64Type>(&arr, 64, true) {
            IntRelation::Unique { slots, .. } => {
                assert!(
                    slots.len() > 900_000,
                    "unpartitioned 1 MiB exemption still applies"
                );
            }
            other => panic!("expected unique range map, got {other:?}"),
        }
        match int_relation::<Int64Type>(&arr, 64, false) {
            IntRelation::Hash(map) => assert_eq!(map.len(), 64),
            other => panic!("partitioned sparse range must hash, got {other:?}"),
        }
        let compact = i64_array(&(0..64).collect::<Vec<_>>());
        match int_relation::<Int64Type>(&compact, 64, false) {
            IntRelation::Unique { slots, .. } => assert_eq!(slots.len(), 64),
            other => panic!("compact partition keys still use a range map, got {other:?}"),
        }
    }

    #[test]
    fn one_large_probe_batch_is_sliced_across_workers_in_order() {
        use std::sync::Arc;

        use arrow::datatypes::{Field, Schema};

        let build_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, true)]));
        let probe_schema = Arc::new(Schema::new(vec![Field::new("p", DataType::Int64, true)]));
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("p", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let build = Batch::try_new(
            build_schema.clone(),
            vec![Arc::new(Int64Array::from((0..100).collect::<Vec<i64>>()))],
        )
        .unwrap();
        let probe = Batch::try_new(
            probe_schema.clone(),
            vec![Arc::new(Int64Array::from(
                (0..70_000).map(|i| i % 200).collect::<Vec<i64>>(),
            ))],
        )
        .unwrap();
        let pool = MemoryPool::new(None, None).unwrap();
        let keys = [Expr::nth(0)];
        let sample = key_sample(probe_schema, &keys).unwrap();
        let parts = parallel_build(
            &[build],
            &keys,
            build_schema,
            &sample,
            1,
            &pool,
            &CancelToken::new(),
        )
        .unwrap();
        let probe_with = |workers| {
            probe_group_parallel(
                &parts,
                parts.len(),
                std::slice::from_ref(&probe),
                &keys,
                None,
                false,
                out_schema.clone(),
                &BatchPolicy::default(),
                workers,
                &pool,
                &CancelToken::new(),
            )
            .unwrap()
        };
        let probe_values = |batches: Vec<Batch>| -> Vec<i64> {
            batches
                .iter()
                .flat_map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect()
        };
        let serial = probe_values(probe_with(1));
        assert_eq!(serial.len(), 35_000);
        assert_eq!(probe_values(probe_with(4)), serial);
    }

    #[test]
    fn small_build_sides_stay_one_table() {
        assert_eq!(build_partition_count(8, PARTITIONED_BUILD_MIN_ROWS - 1), 1);
        assert_eq!(build_partition_count(8, PARTITIONED_BUILD_MIN_ROWS), 8);
        assert_eq!(build_partition_count(6, PARTITIONED_BUILD_MIN_ROWS), 8);
        assert_eq!(build_partition_count(1, 10 * PARTITIONED_BUILD_MIN_ROWS), 1);
    }

    #[test]
    fn parallel_partition_build_does_not_keep_sparse_unique_slots() {
        use std::sync::Arc;

        use arrow::datatypes::{Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let keys = [Expr::nth(0)];
        let batches: Vec<Batch> = (0..2)
            .map(|part| {
                let values: Vec<i64> = (0..32).map(|i| (part * 32 + i) * 15_625).collect();
                Batch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap()
            })
            .collect();
        let pool = MemoryPool::new(None, None).unwrap();
        let sample = key_sample(schema.clone(), &keys).unwrap();
        // Force two partitions: this build is small enough to stay one table.
        let parts = partitioned_build(
            &batches,
            &keys,
            schema,
            &sample,
            2,
            2,
            &pool,
            &CancelToken::new(),
        )
        .unwrap();
        let populated = parts.iter().filter(|p| p.is_some()).count();
        assert!(populated >= 1, "expected at least one populated partition");
        for part in parts.into_iter().flatten() {
            if let Some((_, slots)) = part.table.unique_i64_slots() {
                assert!(
                    slots.len() <= part.batch.num_rows().saturating_mul(32),
                    "partition allocated {} unique slots for {} rows",
                    slots.len(),
                    part.batch.num_rows()
                );
            }
        }
    }

    fn one_i64_batch(name: &str, values: impl IntoIterator<Item = i64>) -> Batch {
        use std::sync::Arc;

        use arrow::datatypes::{Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, true)]));
        Batch::try_new(schema, vec![Arc::new(Int64Array::from_iter_values(values))]).unwrap()
    }

    fn pair_rows(batches: &[Batch]) -> Vec<(i64, i64)> {
        let mut rows: Vec<(i64, i64)> = batches
            .iter()
            .flat_map(|b| {
                let l = b.column(0).as_primitive::<Int64Type>().clone();
                let r = b.column(1).as_primitive::<Int64Type>().clone();
                (0..b.num_rows()).map(move |i| (l.value(i), r.value(i)))
            })
            .collect();
        rows.sort_unstable();
        rows
    }

    #[test]
    fn partitioned_probe_gathers_matches_without_copying_the_probe() {
        use std::sync::Arc;

        use arrow::datatypes::{Field, Schema};

        let build = one_i64_batch("b", (0..64).map(|i| i * 1_000));
        let probe = one_i64_batch("p", 0..64_000);
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("p", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let keys = [Expr::nth(0)];
        let sample = key_sample(probe.schema(), &keys).unwrap();
        // Residual `b >= 32000` keeps the upper half of the matches.
        let residual = Expr::Binary(
            BinaryOp::GtEq,
            Box::new(Expr::nth(1)),
            Box::new(Expr::Literal(Arc::new(Int64Array::from(vec![32_000])))),
        );
        let probe_with = |npart: usize, residual: Option<&Expr>| {
            let pool = MemoryPool::new(None, None).unwrap();
            let parts = partitioned_build(
                std::slice::from_ref(&build),
                &keys,
                build.schema(),
                &sample,
                2,
                npart,
                &pool,
                &CancelToken::new(),
            )
            .unwrap();
            let out = probe_parts(
                &parts,
                npart,
                &probe,
                &keys,
                residual,
                false,
                out_schema.clone(),
                &BatchPolicy::default(),
                &pool,
            )
            .unwrap();
            (pair_rows(&out), pool.copied())
        };
        let expected: Vec<(i64, i64)> = (0..64).map(|i| (i * 1_000, i * 1_000)).collect();
        let (one, _) = probe_with(1, None);
        assert_eq!(one, expected);
        let (split, copied) = probe_with(4, None);
        assert_eq!(split, expected);
        let (filtered, _) = probe_with(4, Some(&residual));
        assert_eq!(filtered, expected[32..]);
        // Scattering the build and gathering 64 matched rows copy far less
        // than the 64,000-row probe batch would.
        assert!(copied < batch_bytes(&probe) / 4, "copied {copied} bytes");
    }

    #[test]
    fn dense_integer_builds_skip_hash_partitions() {
        let rows = PARTITIONED_BUILD_MIN_ROWS as i64;
        let keys = [Expr::nth(0)];
        let parts_for = |values: Vec<i64>| {
            let build = one_i64_batch("k", values);
            let sample = key_sample(build.schema(), &keys).unwrap();
            let pool = MemoryPool::new(None, None).unwrap();
            parallel_build(
                std::slice::from_ref(&build),
                &keys,
                build.schema(),
                &sample,
                8,
                &pool,
                &CancelToken::new(),
            )
            .unwrap()
        };
        // Every fourth key of a span: one direct-index table.
        let dense = parts_for((0..rows).map(|i| i * 4).collect());
        assert_eq!(dense.len(), 1);
        assert!(
            dense[0]
                .as_ref()
                .unwrap()
                .table
                .unique_i64_slots()
                .is_some()
        );
        // Keys 1,000 apart are too sparse for a direct index: hash partitions.
        let sparse = parts_for((0..rows).map(|i| i * 1_000).collect());
        assert_eq!(sparse.len(), 8);
    }

    /// Inner-join `probe` against keys 0..16 under `cap` bytes, with a spill
    /// directory when `spill`. Returns the result and the pool's peak and
    /// spilled bytes.
    fn limited_join(
        probe: Vec<Batch>,
        cap_extra: u64,
        spill: bool,
    ) -> (Result<Vec<Batch>>, u64, u64, u64) {
        use std::sync::Arc;
        use std::time::{SystemTime, UNIX_EPOCH};

        use arrow::datatypes::{Field, Schema};

        let build = one_i64_batch("b", 0..16);
        let join_schema = Arc::new(Schema::new(vec![
            Field::new("p", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let cap = batch_bytes(&build) + cap_extra;
        let root = std::env::temp_dir().join(format!(
            "mizuchi-join-stream-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let pool = MemoryPool::new(Some(cap), spill.then(|| root.clone())).unwrap();
        let on = Expr::Binary(BinaryOp::Eq, Box::new(Expr::nth(0)), Box::new(Expr::nth(1)));
        let left_schema = probe[0].schema();
        let joined = run(
            Box::new(Exec::Scan {
                batches: probe,
                index: 0,
            }),
            Box::new(Exec::Scan {
                batches: vec![build.clone()],
                index: 0,
            }),
            JoinSpec {
                join_type: JoinType::Inner,
                on: Some(&on),
                left_schema,
                right_schema: build.schema(),
                schema: join_schema,
            },
            &BatchPolicy::default(),
            &pool,
            &CancelToken::new(),
            2,
        );
        let (peak, spilled) = (pool.peak(), pool.spilled());
        drop(pool);
        let _ = std::fs::remove_dir_all(&root);
        (joined, cap, peak, spilled)
    }

    #[test]
    fn memory_limited_inner_join_streams_probe_groups_within_the_cap() {
        let probe: Vec<Batch> = (0..8)
            .map(|i| one_i64_batch("p", i * 1_000..(i + 1) * 1_000))
            .collect();
        // Room for the build side and two probe batches, not all eight.
        let (joined, cap, peak, spilled) =
            limited_join(probe.clone(), 2 * batch_bytes(&probe[0]), true);
        assert_eq!(
            pair_rows(&joined.unwrap()),
            (0..16).map(|k| (k, k)).collect::<Vec<_>>()
        );
        assert_eq!(spilled, 0, "the probe side was collected and spilled");
        assert!(peak <= cap, "peak {peak} over cap {cap}");
        assert!(
            peak >= batch_bytes(&probe[0]) * 2,
            "probe batches were not reserved: peak {peak}"
        );
    }

    #[test]
    fn memory_limited_inner_join_refuses_an_oversized_probe_batch() {
        let probe = vec![one_i64_batch("p", 0..10_000)];
        let small = batch_bytes(&probe[0]) / 4;
        for spill in [true, false] {
            let (joined, cap, peak, _) = limited_join(probe.clone(), small, spill);
            match joined {
                Err(Error::Memory(msg)) => {
                    if spill {
                        assert!(msg.contains("single join batch"), "{msg}");
                    }
                }
                other => panic!("expected a memory error, got {:?}", other.map(|b| b.len())),
            }
            assert!(peak <= cap, "peak {peak} over cap {cap}");
        }
    }

    #[test]
    fn collected_fallback_drops_the_streamed_build_first() {
        let probe = vec![one_i64_batch("p", 0..10_000)];
        let small = batch_bytes(&probe[0]) / 4;
        JOIN_EVENTS.with(|e| e.borrow_mut().clear());
        let (joined, _, _, _) = limited_join(probe, small, true);
        assert!(matches!(joined, Err(Error::Memory(_))));
        let events = JOIN_EVENTS.with(|e| e.borrow().clone());
        let pos = |name| events.iter().position(|e| *e == name);
        let fallback = pos("collected_probe").expect("collected path ran");
        let dropped = pos("drop_build").expect("streamed build dropped");
        assert!(dropped < fallback, "both table sets were alive: {events:?}");
    }
}
