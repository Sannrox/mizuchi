//! Hash aggregate over Arrow batches.
//!
//! Grouping expressions and aggregate arguments are evaluated column-at-a-time.
//! Small integer, boolean, and single-byte string key ranges use a perfect
//! hash: accumulators are indexed by the packed key with no per-group
//! `Scalar`. Other Utf8 keys of at most 16 bytes pack inline. Other grouped
//! input radix-partitions by hash bits, flushes
//! thread-local tables when they fill, and merges partitions in parallel.
//! COUNT(expr), SUM, AVG, MIN, and MAX skip nulls; COUNT(*) counts every
//! input row. COUNT(DISTINCT expr) counts unique non-null values per group.
//! Ungrouped aggregation of empty input still emits one row (COUNT is 0, the
//! rest null). Grouped aggregation of empty input emits no rows. Null grouping
//! keys form one group.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::thread;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeStringArray, StringArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow::compute;
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, SchemaRef,
    UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};

use super::CancelToken;
use super::Operator;
use super::key::{
    IdentityMap, Scalar, dict_utf8_at, eval_all, extract_key, hash_row, hash_scalars,
    identity_map_with_capacity, row_matches, scalar_at,
};
use super::memory::{self, MemoryPool, batch_bytes};
use mizuchi_core::batch::{Batch, BatchPolicy};
use mizuchi_core::error::{Error, Result};
use mizuchi_core::expr::Expr;
use mizuchi_core::udaf::CombineAgg;
use mizuchi_plan::{AggCall, NamedExpr};

/// Dense perfect-hash cap, including the null slot per dimension.
const PERFECT_MAX_SLOTS: usize = 1 << 20;
/// Flush a worker-local hash table into radix partitions at this many groups.
const LOCAL_FLUSH_GROUPS: usize = 4096;
const AGG_PART_MIN: usize = 4;
const AGG_PART_MAX: usize = 64;
const AGG_GRACE_DEPTH: u32 = 4;

#[cfg(test)]
pub(crate) mod agg_path_counters {
    use std::cell::Cell;
    use std::sync::atomic::AtomicUsize;

    pub static PERFECT_ROWS: AtomicUsize = AtomicUsize::new(0);
    pub static HASH_INSERTS: AtomicUsize = AtomicUsize::new(0);
    pub static PARALLEL_MERGES: AtomicUsize = AtomicUsize::new(0);

    thread_local! {
        /// Per-test, because `store` would race across default cargo threads.
        pub static RADIX_PARTS: Cell<usize> = const { Cell::new(0) };
        pub static RADIX_OCCUPIED: Cell<usize> = const { Cell::new(0) };
    }
}

fn bump_perfect_rows(n: usize) {
    let _ = n;
    #[cfg(test)]
    agg_path_counters::PERFECT_ROWS.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
}

fn bump_hash_inserts() {
    #[cfg(test)]
    agg_path_counters::HASH_INSERTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

fn set_radix_parts(n: usize) {
    let _ = n;
    #[cfg(test)]
    agg_path_counters::RADIX_PARTS.with(|c| c.set(n));
}

fn set_radix_occupied(n: usize) {
    let _ = n;
    #[cfg(test)]
    agg_path_counters::RADIX_OCCUPIED.with(|c| c.set(n));
}

#[cfg(test)]
fn perfect_index_bytes(slots: usize) -> u64 {
    (slots * std::mem::size_of::<u32>()) as u64
}

/// Avalanche before taking low bits. FastHasher's finish is the raw multiply
/// state; integer keys that live in the high bits otherwise collapse onto one
/// radix partition.
fn mix_hash(h: u64) -> u64 {
    let mut x = h;
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^= x >> 31;
    x
}

fn radix_slot(h: u64, mask: usize) -> usize {
    (mix_hash(h) as usize) & mask
}

fn bump_parallel_merges() {
    #[cfg(test)]
    agg_path_counters::PARALLEL_MERGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

pub(super) fn run(
    mut input: Box<dyn Operator>,
    group: &[NamedExpr],
    aggs: &[(String, AggCall)],
    schema: SchemaRef,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let mut table = GroupedTable::new(group, aggs, schema.clone())?;
    let mut held: Vec<super::memory::Reservation<'_>> = Vec::new();
    let npart = AGG_PART_MAX;
    let mut partials: Vec<Vec<std::path::PathBuf>> = vec![Vec::new(); npart];
    while let Some(batch) = input.next_batch(cancel)? {
        if batch.num_rows() == 0 {
            continue;
        }
        if pool.has_limit() {
            let sz = batch_bytes(&batch);
            match pool.try_reserve(sz) {
                Ok(r) => held.push(r),
                Err(e) => {
                    if !pool.can_spill() {
                        return Err(e);
                    }
                    if !spill_ok(aggs) {
                        return Err(Error::Memory("this aggregate cannot spill".into()));
                    }
                    let old = std::mem::replace(
                        &mut table,
                        GroupedTable::new(group, aggs, schema.clone())?,
                    );
                    spill_grouped(
                        old,
                        schema.clone(),
                        policy,
                        pool,
                        group.len(),
                        npart,
                        &mut partials,
                    )?;
                    held.clear();
                    let r = pool.try_reserve(sz).map_err(|_| {
                        Error::Memory("a single aggregate batch exceeds the memory limit".into())
                    })?;
                    held.push(r);
                }
            }
        }
        table.update(&batch, pool)?;
    }
    if partials.iter().all(|p| p.is_empty()) {
        return table.finish(policy);
    }
    spill_grouped(
        table,
        schema.clone(),
        policy,
        pool,
        group.len(),
        npart,
        &mut partials,
    )?;
    drop(held);
    merge_partial_files(&partials, group, aggs, schema, policy, pool)
}

fn spill_grouped(
    table: GroupedTable,
    schema: SchemaRef,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    group_n: usize,
    npart: usize,
    partials: &mut [Vec<std::path::PathBuf>],
) -> Result<()> {
    let finished = table.finish(policy)?;
    for batch in finished {
        if batch.num_rows() == 0 {
            continue;
        }
        let idx: Vec<u32> = (0..batch.num_rows() as u32).collect();
        let batch = take_rows(&batch, &idx)?;
        for (i, piece) in scatter_groups(&batch, group_n, npart)?
            .into_iter()
            .enumerate()
        {
            let Some(piece) = piece else {
                continue;
            };
            let path = pool.write_spill_file(schema.clone(), std::slice::from_ref(&piece))?;
            partials[i].push(path);
        }
    }
    Ok(())
}

fn scatter_groups(batch: &Batch, group_n: usize, npart: usize) -> Result<Vec<Option<Batch>>> {
    scatter_groups_at(batch, group_n, npart, 0)
}

fn scatter_groups_at(
    batch: &Batch,
    group_n: usize,
    npart: usize,
    depth: u32,
) -> Result<Vec<Option<Batch>>> {
    debug_assert!(npart.is_power_of_two() && npart > 0);
    if batch.num_rows() == 0 {
        return Ok(vec![None; npart]);
    }
    if group_n == 0 || npart == 1 {
        let mut out = vec![None; npart];
        out[0] = Some(batch.clone());
        return Ok(out);
    }
    let keys: Vec<ArrayRef> = (0..group_n)
        .map(|i| batch.as_arrow().column(i).clone())
        .collect();
    let mask = npart - 1;
    let shift = (depth * 6) % 64;
    let mut buckets: Vec<Vec<u32>> = vec![Vec::new(); npart];
    for row in 0..batch.num_rows() {
        let h = mix_hash(hash_row(&keys, row)?);
        buckets[((h >> shift) as usize) & mask].push(row as u32);
    }
    buckets
        .into_iter()
        .map(|idx| {
            if idx.is_empty() {
                Ok(None)
            } else {
                Ok(Some(take_rows(batch, &idx)?))
            }
        })
        .collect()
}

fn take_rows(batch: &Batch, idx: &[u32]) -> Result<Batch> {
    let arr = UInt32Array::from(idx.to_vec());
    let columns = batch
        .as_arrow()
        .columns()
        .iter()
        .map(|col| {
            compute::take(col.as_ref(), &arr, None).map_err(|e| Error::InvalidBatch(e.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    Batch::try_new(batch.schema(), columns)
}

/// Rebuild so Arrow reports the taken rows, not an IPC reader's parent buffer.
fn compact_batch(batch: Batch) -> Result<Batch> {
    if batch.num_rows() == 0 {
        return Ok(batch);
    }
    let idx: Vec<u32> = (0..batch.num_rows() as u32).collect();
    take_rows(&batch, &idx)
}

fn spill_halves(
    batch: &Batch,
    schema: SchemaRef,
    pool: &MemoryPool,
    more: &mut Vec<std::path::PathBuf>,
) -> Result<()> {
    let mid = batch.num_rows() / 2;
    let left: Vec<u32> = (0..mid as u32).collect();
    let right: Vec<u32> = (mid as u32..batch.num_rows() as u32).collect();
    more.push(pool.write_spill_file(
        schema.clone(),
        std::slice::from_ref(&take_rows(batch, &left)?),
    )?);
    more.push(pool.write_spill_file(schema, std::slice::from_ref(&take_rows(batch, &right)?))?);
    Ok(())
}

fn spill_finished(
    finished: Vec<Batch>,
    schema: SchemaRef,
    pool: &MemoryPool,
    more: &mut Vec<std::path::PathBuf>,
) -> Result<()> {
    for batch in finished {
        if batch.num_rows() == 0 {
            continue;
        }
        more.push(pool.write_spill_file(schema.clone(), std::slice::from_ref(&batch))?);
    }
    Ok(())
}

fn scalar_mem(s: &Scalar) -> u64 {
    24 + match s {
        Scalar::Utf8(t) => t.len() as u64,
        Scalar::Utf8Packed { .. } => 0,
        _ => 0,
    }
}

fn accum_retained(accums: &[Accum]) -> u64 {
    accums.iter().map(Accum::retained_bytes).sum()
}

/// Payload bytes in `batch`, not Arrow's 64-byte min-allocation per buffer.
fn piece_payload(batch: &Batch) -> u64 {
    let rows = batch.num_rows() as u64;
    if rows == 0 {
        return 0;
    }
    batch
        .as_arrow()
        .columns()
        .iter()
        .map(|c| match c.data_type() {
            DataType::Utf8 => c.as_string::<i32>().values().len() as u64 + rows.saturating_mul(4),
            DataType::LargeUtf8 => {
                c.as_string::<i64>().values().len() as u64 + rows.saturating_mul(8)
            }
            DataType::Dictionary(k, _) => {
                let key_w = match k.as_ref() {
                    DataType::Int8 | DataType::UInt8 => 1,
                    DataType::Int16 | DataType::UInt16 => 2,
                    DataType::Int32 | DataType::UInt32 => 4,
                    _ => 8,
                };
                rows.saturating_mul(key_w)
            }
            DataType::Boolean => rows.div_ceil(8),
            _ => rows.saturating_mul(8),
        })
        .fold(0u64, u64::saturating_add)
        .max(64)
}

pub(super) fn can_combine(aggs: &[(String, AggCall)]) -> bool {
    aggs.iter()
        .all(|(_, c)| !matches!(c, AggCall::CountDistinct(_)))
}

pub(super) fn can_spill(aggs: &[(String, AggCall)]) -> bool {
    spill_ok(aggs)
}

fn spill_ok(aggs: &[(String, AggCall)]) -> bool {
    aggs.iter()
        .all(|(_, c)| !matches!(c, AggCall::Avg(_) | AggCall::CountDistinct(_)))
}

pub(super) fn combine_partials(
    partials: Vec<Batch>,
    group: &[NamedExpr],
    aggs: &[(String, AggCall)],
    schema: SchemaRef,
    policy: &BatchPolicy,
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    if partials.is_empty() {
        return HashAgg::new(group, aggs, schema)?.finish(policy);
    }
    if !pool.has_limit() {
        return combine_partials_in_memory(&partials, group, aggs, schema, policy, pool);
    }
    let mut compact = Vec::with_capacity(partials.len());
    for batch in partials {
        if batch.num_rows() == 0 {
            continue;
        }
        compact.push(compact_batch(batch)?);
    }
    if compact.is_empty() {
        return HashAgg::new(group, aggs, schema)?.finish(policy);
    }
    match combine_partials_in_memory(&compact, group, aggs, schema.clone(), policy, pool) {
        Ok(out) => return Ok(out),
        Err(Error::Memory(_)) if pool.can_spill() => {}
        Err(e) => return Err(e),
    }
    let npart = AGG_PART_MAX;
    let mut parts: Vec<Vec<std::path::PathBuf>> = vec![Vec::new(); npart];
    for batch in compact {
        for (i, piece) in scatter_groups(&batch, group.len(), npart)?
            .into_iter()
            .enumerate()
        {
            let Some(piece) = piece else {
                continue;
            };
            parts[i].push(pool.write_spill_file(schema.clone(), std::slice::from_ref(&piece))?);
        }
    }
    merge_partial_files(&parts, group, aggs, schema, policy, pool)
}

fn combine_partials_in_memory(
    partials: &[Batch],
    group: &[NamedExpr],
    aggs: &[(String, AggCall)],
    schema: SchemaRef,
    policy: &BatchPolicy,
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    let group_n = group.len();
    let merge_group: Vec<NamedExpr> = (0..group_n)
        .map(|i| NamedExpr {
            expr: Expr::nth(i),
            name: schema.field(i).name().clone(),
        })
        .collect();
    let merge_aggs: Vec<(String, AggCall)> = aggs
        .iter()
        .enumerate()
        .map(|(i, (name, call))| {
            let col = Expr::nth(group_n + i);
            let call = match call {
                AggCall::CountStar | AggCall::Count(_) | AggCall::Sum(_) => AggCall::Sum(col),
                AggCall::Min(_) => AggCall::Min(col),
                AggCall::Max(_) => AggCall::Max(col),
                AggCall::User { name, agg, .. } => AggCall::User {
                    name: name.clone(),
                    arg: col,
                    agg: agg.clone(),
                },
                other => other.clone(),
            };
            (name.clone(), call)
        })
        .collect();
    let mut table = HashAgg::combining(&merge_group, &merge_aggs, schema)?;
    let mut held: Vec<super::memory::Reservation<'_>> = Vec::new();
    for batch in partials {
        if batch.num_rows() == 0 {
            continue;
        }
        if pool.has_limit() {
            let sz = piece_payload(batch);
            if table.n_groups() > 0
                && let Some(limit) = pool.limit()
                && table.retained_bytes().saturating_add(sz) > limit
            {
                return Err(Error::Memory(
                    "aggregate merge exceeds the memory limit".into(),
                ));
            }
            let incoming = pool.try_reserve(sz)?;
            table.update(batch)?;
            drop(incoming);
            held.clear();
            let proxy = table.retained_bytes();
            if proxy > 0 {
                held.push(pool.try_reserve(proxy)?);
            }
        } else {
            table.update(batch)?;
        }
    }
    table.finish(policy)
}

/// Fold `batches` into one hash table. Used by the morsel workers.
pub(super) fn fold_batches(
    batches: impl IntoIterator<Item = Batch>,
    group: &[NamedExpr],
    aggs: &[(String, AggCall)],
    schema: SchemaRef,
) -> Result<HashAgg> {
    let mut table = HashAgg::new(group, aggs, schema)?;
    for batch in batches {
        table.update(&batch)?;
    }
    Ok(table)
}

/// Per-build-row cache in front of [`HashAgg`]. The first probe that hits a
/// unique build row hashes the group key once; later matches only update
/// accumulators. Used when an inner unique-key join is aggregated immediately.
pub(super) struct BuildFold {
    table: HashAgg,
    by_build: Vec<u32>,
}

impl BuildFold {
    pub(super) fn new(
        group: &[NamedExpr],
        aggs: &[(String, AggCall)],
        schema: SchemaRef,
        n_build: usize,
    ) -> Result<Self> {
        Ok(Self {
            table: HashAgg::new(group, aggs, schema)?,
            by_build: vec![u32::MAX; n_build],
        })
    }

    pub(super) fn add(
        &mut self,
        group_cols: &[ArrayRef],
        brow: usize,
        arg_cols: &[Option<ArrayRef>],
        prow: usize,
    ) -> Result<()> {
        if !self.table.grouped {
            if self.table.keys.is_empty() {
                self.table.insert_group(Vec::new())?;
            }
            for (accum, arg) in self.table.accums[0].iter_mut().zip(arg_cols.iter()) {
                accum.update(arg.as_deref(), prow)?;
            }
            return Ok(());
        }
        let cached = self.by_build[brow];
        let slot = if cached == u32::MAX {
            let slot = self.table.ensure_group_row(group_cols, brow)?;
            self.by_build[brow] = slot as u32;
            slot
        } else {
            cached as usize
        };
        for (accum, arg) in self.table.accums[slot].iter_mut().zip(arg_cols.iter()) {
            accum.update(arg.as_deref(), prow)?;
        }
        Ok(())
    }

    pub(super) fn into_table(self) -> HashAgg {
        self.table
    }
}

/// Accumulators indexed by unique build row. Valid when grouping includes the
/// unique join key, so each build row is its own group and hashing is skipped.
pub(super) struct BuildSlots {
    schema: SchemaRef,
    group_n: usize,
    accums: Vec<Option<Vec<Accum>>>,
    empty: Vec<Accum>,
}

impl BuildSlots {
    pub(super) fn new(
        group: &[NamedExpr],
        aggs: &[(String, AggCall)],
        schema: SchemaRef,
        n_build: usize,
    ) -> Result<Self> {
        let group_n = group.len();
        let empty = aggs
            .iter()
            .enumerate()
            .map(|(i, (_, call))| {
                Accum::from_call(call, schema.field(group_n + i).data_type(), false)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            schema,
            group_n,
            accums: vec![None; n_build],
            empty,
        })
    }

    pub(super) fn add(
        &mut self,
        brow: usize,
        arg_cols: &[Option<ArrayRef>],
        prow: usize,
    ) -> Result<()> {
        let slot = self.accums[brow].get_or_insert_with(|| self.empty.clone());
        for (accum, arg) in slot.iter_mut().zip(arg_cols.iter()) {
            accum.update(arg.as_deref(), prow)?;
        }
        Ok(())
    }

    pub(super) fn merge_from(&mut self, mut other: Self) -> Result<()> {
        for (dst, src) in self.accums.iter_mut().zip(other.accums.iter_mut()) {
            match (dst.as_mut(), src.take()) {
                (Some(d), Some(s)) => {
                    for (a, b) in d.iter_mut().zip(s) {
                        a.merge(b)?;
                    }
                }
                (None, Some(s)) => *dst = Some(s),
                _ => {}
            }
        }
        Ok(())
    }

    pub(super) fn finish(
        self,
        group_cols: &[ArrayRef],
        policy: &BatchPolicy,
    ) -> Result<Vec<Batch>> {
        let hits: Vec<usize> = self
            .accums
            .iter()
            .enumerate()
            .filter_map(|(i, a)| a.is_some().then_some(i))
            .collect();
        if hits.is_empty() {
            return Ok(Vec::new());
        }
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for (g, field) in self.schema.fields().iter().enumerate().take(self.group_n) {
            let values: Vec<super::key::Scalar> = hits
                .iter()
                .map(|&i| super::key::scalar_at(group_cols[g].as_ref(), i))
                .collect::<Result<_>>()?;
            columns.push(build_array(&values, field.data_type())?);
        }
        for (a, field) in self.schema.fields().iter().enumerate().skip(self.group_n) {
            let ai = a - self.group_n;
            let values: Vec<super::key::Scalar> = hits
                .iter()
                .map(|&i| {
                    self.accums[i]
                        .as_ref()
                        .expect("hit")
                        .get(ai)
                        .expect("accum")
                        .finish()
                })
                .collect();
            columns.push(build_array(&values, field.data_type())?);
        }
        let full = Batch::try_new(self.schema, columns)?;
        Ok(full
            .split_by_policy(policy)
            .into_iter()
            .filter(|b| b.num_rows() > 0)
            .collect())
    }
}

/// One `SUM` of floats per unique build row. Avoids accumulator dispatch on
/// the probe loop for q3-shaped plans.
pub(super) struct BuildF64Sums {
    schema: SchemaRef,
    group_n: usize,
    sums: Vec<f64>,
    hits: Vec<u8>,
}

impl BuildF64Sums {
    pub(super) fn new(schema: SchemaRef, group_n: usize, n_build: usize) -> Self {
        Self {
            schema,
            group_n,
            sums: vec![0.0; n_build],
            hits: vec![0; n_build],
        }
    }

    pub(super) fn add(&mut self, brow: usize, value: f64) {
        self.sums[brow] += value;
        self.hits[brow] = 1;
    }

    pub(super) fn finish(
        self,
        group_cols: &[ArrayRef],
        policy: &BatchPolicy,
    ) -> Result<Vec<Batch>> {
        let hits: Vec<usize> = self
            .hits
            .iter()
            .enumerate()
            .filter_map(|(i, h)| (*h != 0).then_some(i))
            .collect();
        if hits.is_empty() {
            return Ok(Vec::new());
        }
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for (g, field) in self.schema.fields().iter().enumerate().take(self.group_n) {
            let values: Vec<Scalar> = hits
                .iter()
                .map(|&i| scalar_at(group_cols[g].as_ref(), i))
                .collect::<Result<_>>()?;
            columns.push(build_array(&values, field.data_type())?);
        }
        let sums: Vec<Scalar> = hits
            .iter()
            .map(|&i| Scalar::F64(self.sums[i].to_bits()))
            .collect();
        columns.push(build_array(
            &sums,
            self.schema.field(self.group_n).data_type(),
        )?);
        let full = Batch::try_new(self.schema, columns)?;
        Ok(full
            .split_by_policy(policy)
            .into_iter()
            .filter(|b| b.num_rows() > 0)
            .collect())
    }
}

/// Merge per-worker tables and emit the finished groups.
pub(super) fn merge_finish(
    tables: Vec<HashAgg>,
    group: &[NamedExpr],
    aggs: &[(String, AggCall)],
    schema: SchemaRef,
    policy: &BatchPolicy,
) -> Result<Vec<Batch>> {
    let mut tables = tables.into_iter();
    let Some(mut acc) = tables.next() else {
        return HashAgg::new(group, aggs, schema)?.finish(policy);
    };
    for table in tables {
        acc.merge_table(table)?;
    }
    acc.finish(policy)
}

fn merge_partial_files(
    parts: &[Vec<std::path::PathBuf>],
    group: &[NamedExpr],
    aggs: &[(String, AggCall)],
    schema: SchemaRef,
    policy: &BatchPolicy,
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    let mut out = Vec::new();
    for files in parts {
        if files.is_empty() {
            continue;
        }
        out.extend(merge_partition(
            files,
            group,
            aggs,
            schema.clone(),
            policy,
            pool,
            0,
        )?);
    }
    if out.is_empty() {
        HashAgg::new(group, aggs, schema)?.finish(policy)
    } else {
        Ok(out)
    }
}

fn merge_partition(
    paths: &[std::path::PathBuf],
    group: &[NamedExpr],
    aggs: &[(String, AggCall)],
    schema: SchemaRef,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    depth: u32,
) -> Result<Vec<Batch>> {
    if paths.len() == 1 && !pool.has_limit() {
        return memory::read_spill(&paths[0], schema)?
            .into_iter()
            .map(compact_batch)
            .collect();
    }
    let group_n = group.len();
    let merge_group: Vec<NamedExpr> = (0..group_n)
        .map(|i| NamedExpr {
            expr: Expr::nth(i),
            name: schema.field(i).name().clone(),
        })
        .collect();
    let merge_aggs: Vec<(String, AggCall)> = aggs
        .iter()
        .enumerate()
        .map(|(i, (name, call))| {
            let col = Expr::nth(group_n + i);
            let call = match call {
                AggCall::CountStar | AggCall::Count(_) | AggCall::Sum(_) => AggCall::Sum(col),
                AggCall::Min(_) => AggCall::Min(col),
                AggCall::Max(_) => AggCall::Max(col),
                AggCall::User { name, agg, .. } => AggCall::User {
                    name: name.clone(),
                    arg: col,
                    agg: agg.clone(),
                },
                other => other.clone(),
            };
            (name.clone(), call)
        })
        .collect();
    let mut table = HashAgg::combining(&merge_group, &merge_aggs, schema.clone())?;
    let mut held: Vec<super::memory::Reservation<'_>> = Vec::new();
    let mut more = Vec::new();
    for path in paths {
        for batch in memory::spill_batches(path)? {
            let batch = compact_batch(batch?)?;
            if batch.num_rows() == 0 {
                continue;
            }
            let sz = piece_payload(&batch);
            if table.n_groups() > 0
                && let Some(limit) = pool.limit()
                && table.retained_bytes().saturating_add(sz) > limit
            {
                let finished = table.finish(policy)?;
                spill_finished(finished, schema.clone(), pool, &mut more)?;
                held.clear();
                table = HashAgg::combining(&merge_group, &merge_aggs, schema.clone())?;
            }
            let incoming = match pool.try_reserve(sz) {
                Ok(r) => r,
                Err(e) if !pool.can_spill() => return Err(e),
                Err(_) => {
                    if table.n_groups() > 0 {
                        let finished = table.finish(policy)?;
                        spill_finished(finished, schema.clone(), pool, &mut more)?;
                        held.clear();
                        table = HashAgg::combining(&merge_group, &merge_aggs, schema.clone())?;
                    }
                    match pool.try_reserve(sz) {
                        Ok(r) => r,
                        Err(_) if batch.num_rows() > 1 => {
                            spill_halves(&batch, schema.clone(), pool, &mut more)?;
                            continue;
                        }
                        Err(_) => {
                            return Err(Error::Memory(
                                "a single aggregate batch exceeds the memory limit".into(),
                            ));
                        }
                    }
                }
            };
            table.update(&batch)?;
            drop(incoming);
            held.clear();
            let proxy = table.retained_bytes();
            if proxy == 0 {
                continue;
            }
            match pool.try_reserve(proxy) {
                Ok(r) => held.push(r),
                Err(e) if !pool.can_spill() => return Err(e),
                Err(_) => {
                    let finished = table.finish(policy)?;
                    spill_finished(finished, schema.clone(), pool, &mut more)?;
                    table = HashAgg::combining(&merge_group, &merge_aggs, schema.clone())?;
                }
            }
        }
    }
    if more.is_empty() {
        return table.finish(policy);
    }
    let finished = table.finish(policy)?;
    drop(held);
    spill_finished(finished, schema.clone(), pool, &mut more)?;
    if more.len() >= paths.len() {
        return repartition_merge(&more, group, aggs, schema, policy, pool, depth + 1);
    }
    merge_partition(&more, group, aggs, schema, policy, pool, depth)
}

fn repartition_merge(
    paths: &[std::path::PathBuf],
    group: &[NamedExpr],
    aggs: &[(String, AggCall)],
    schema: SchemaRef,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    depth: u32,
) -> Result<Vec<Batch>> {
    if depth > AGG_GRACE_DEPTH {
        return Err(Error::Memory(
            "aggregate partition could not fit under the memory limit".into(),
        ));
    }
    let nsub = 4;
    let mut parts: Vec<Vec<std::path::PathBuf>> = vec![Vec::new(); nsub];
    for path in paths {
        for batch in memory::spill_batches(path)? {
            let batch = compact_batch(batch?)?;
            if batch.num_rows() == 0 {
                continue;
            }
            for (i, piece) in scatter_groups_at(&batch, group.len(), nsub, depth)?
                .into_iter()
                .enumerate()
            {
                let Some(piece) = piece else {
                    continue;
                };
                let p = pool.write_spill_file(schema.clone(), std::slice::from_ref(&piece))?;
                parts[i].push(p);
            }
        }
    }
    let mut out = Vec::new();
    for files in parts {
        if files.is_empty() {
            continue;
        }
        out.extend(merge_partition(
            &files,
            group,
            aggs,
            schema.clone(),
            policy,
            pool,
            depth,
        )?);
    }
    if out.is_empty() {
        HashAgg::new(group, aggs, schema)?.finish(policy)
    } else {
        Ok(out)
    }
}

/// Grouped aggregation that starts as a hash table and switches to a perfect
/// hash when the first batch's key domain fits in [`PERFECT_MAX_SLOTS`].
enum GroupedTable {
    Hash(HashAgg),
    Perfect(PerfectAgg),
}

impl GroupedTable {
    fn new(group: &[NamedExpr], aggs: &[(String, AggCall)], schema: SchemaRef) -> Result<Self> {
        Ok(Self::Hash(HashAgg::new(group, aggs, schema)?))
    }

    fn update(&mut self, batch: &Batch, pool: &MemoryPool) -> Result<()> {
        match self {
            Self::Hash(table) => {
                // Memory-limited queries keep a hash table. A dense index of
                // [`PERFECT_MAX_SLOTS`] would steal budget from later batches.
                if table.grouped
                    && table.n_groups() == 0
                    && batch.num_rows() > 0
                    && !pool.has_limit()
                {
                    let key_cols = eval_all(&table.group_exprs, batch)?;
                    if let Some(spec) = infer_spec(&key_cols) {
                        let mut perfect = PerfectAgg::from_hash(table, spec)?;
                        perfect.update_encoded(batch, &key_cols)?;
                        *self = Self::Perfect(perfect);
                        return Ok(());
                    }
                }
                table.update(batch)
            }
            Self::Perfect(table) => {
                if table.try_update(batch)? {
                    return Ok(());
                }
                let mut hash = table.take_hash()?;
                hash.update(batch)?;
                *self = Self::Hash(hash);
                Ok(())
            }
        }
    }

    fn finish(self, policy: &BatchPolicy) -> Result<Vec<Batch>> {
        match self {
            Self::Hash(table) => table.finish(policy),
            Self::Perfect(table) => table.finish(policy),
        }
    }
}

/// Parallel grouped aggregation: perfect-hash when the domain fits, otherwise
/// radix-partitioned hash tables merged one partition per task.
#[allow(clippy::too_many_arguments)]
pub(super) fn run_parallel(
    morsels: Vec<Batch>,
    group: &[NamedExpr],
    aggs: &[(String, AggCall)],
    schema: SchemaRef,
    policy: &BatchPolicy,
    workers: usize,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    if morsels.is_empty() {
        return HashAgg::new(group, aggs, schema)?.finish(policy);
    }
    let exprs: Vec<Expr> = group.iter().map(|g| g.expr.clone()).collect();
    if let Some(spec) = infer_spec_morsels(&morsels, &exprs)? {
        return run_perfect_parallel(spec, morsels, group, aggs, schema, policy, workers, cancel);
    }
    if workers <= 1 {
        let mut table = HashAgg::new(group, aggs, schema)?;
        for batch in morsels {
            cancel.check()?;
            table.update(&batch)?;
        }
        return table.finish(policy);
    }
    run_radix(morsels, group, aggs, schema, policy, workers, cancel)
}

fn agg_partition_count(workers: usize) -> usize {
    workers
        .max(1)
        .next_power_of_two()
        .clamp(AGG_PART_MIN, AGG_PART_MAX)
}

#[allow(clippy::too_many_arguments)]
fn run_perfect_parallel(
    spec: PerfectSpec,
    morsels: Vec<Batch>,
    group: &[NamedExpr],
    aggs: &[(String, AggCall)],
    schema: SchemaRef,
    policy: &BatchPolicy,
    workers: usize,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let n = workers.max(1).min(morsels.len());
    if n == 1 {
        let mut table = PerfectAgg::new(group, aggs, schema, spec)?;
        for (i, batch) in morsels.iter().enumerate() {
            cancel.check()?;
            if !table.try_update(batch)? {
                let mut hash = table.take_hash()?;
                for rest in morsels.into_iter().skip(i) {
                    cancel.check()?;
                    hash.update(&rest)?;
                }
                return hash.finish(policy);
            }
        }
        return table.finish(policy);
    }
    let mut chunks: Vec<Vec<Batch>> = vec![Vec::new(); n];
    for (i, batch) in morsels.into_iter().enumerate() {
        chunks[i % n].push(batch);
    }
    let tables = Mutex::new(Vec::new());
    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for chunk in chunks {
            let tables = &tables;
            let spec = spec.clone();
            let schema = schema.clone();
            handles.push(scope.spawn(move || -> Result<()> {
                cancel.check()?;
                let mut table = PerfectAgg::new(group, aggs, schema, spec)?;
                for batch in chunk {
                    cancel.check()?;
                    if !table.try_update(&batch)? {
                        return Err(Error::Eval(
                            "perfect-hash domain missed a row after global infer".into(),
                        ));
                    }
                }
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
    let mut tables = tables
        .into_inner()
        .map_err(|_| Error::Eval("agg combine lock poisoned".into()))?;
    let Some(mut acc) = tables.pop() else {
        return HashAgg::new(group, aggs, schema)?.finish(policy);
    };
    for table in tables {
        acc.merge_table(table)?;
    }
    acc.finish(policy)
}

#[allow(clippy::too_many_arguments)]
fn run_radix(
    morsels: Vec<Batch>,
    group: &[NamedExpr],
    aggs: &[(String, AggCall)],
    schema: SchemaRef,
    policy: &BatchPolicy,
    workers: usize,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let npart = agg_partition_count(workers);
    set_radix_parts(npart);
    let n = workers.max(1).min(morsels.len());
    let mut chunks: Vec<Vec<Batch>> = vec![Vec::new(); n];
    for (i, batch) in morsels.into_iter().enumerate() {
        chunks[i % n].push(batch);
    }
    let worker_parts = Mutex::new(Vec::new());
    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for chunk in chunks {
            let worker_parts = &worker_parts;
            let schema = schema.clone();
            handles.push(scope.spawn(move || -> Result<()> {
                let mut local = HashAgg::new(group, aggs, schema.clone())?;
                let mut parts = (0..npart)
                    .map(|_| HashAgg::new(group, aggs, schema.clone()))
                    .collect::<Result<Vec<_>>>()?;
                for batch in chunk {
                    cancel.check()?;
                    local.update(&batch)?;
                    if local.n_groups() >= LOCAL_FLUSH_GROUPS {
                        local.flush_into(&mut parts)?;
                    }
                }
                local.flush_into(&mut parts)?;
                worker_parts
                    .lock()
                    .map_err(|_| Error::Eval("agg combine lock poisoned".into()))?
                    .push(parts);
                Ok(())
            }));
        }
        for h in handles {
            h.join()
                .map_err(|_| Error::Eval("worker panicked".into()))??;
        }
        Ok(())
    })?;
    let worker_parts = worker_parts
        .into_inner()
        .map_err(|_| Error::Eval("agg combine lock poisoned".into()))?;
    let mut by_part: Vec<Vec<HashAgg>> = (0..npart).map(|_| Vec::new()).collect();
    for parts in worker_parts {
        for (p, table) in parts.into_iter().enumerate() {
            by_part[p].push(table);
        }
    }
    set_radix_occupied(
        by_part
            .iter()
            .filter(|tables| tables.iter().any(|t| t.n_groups() > 0))
            .count(),
    );
    let finished = Mutex::new(vec![Vec::new(); npart]);
    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for (p, tables) in by_part.into_iter().enumerate() {
            let finished = &finished;
            let schema = schema.clone();
            handles.push(scope.spawn(move || -> Result<()> {
                cancel.check()?;
                bump_parallel_merges();
                let out = merge_finish(tables, group, aggs, schema, policy)?;
                finished
                    .lock()
                    .map_err(|_| Error::Eval("agg combine lock poisoned".into()))?[p] = out;
                Ok(())
            }));
        }
        for h in handles {
            h.join()
                .map_err(|_| Error::Eval("worker panicked".into()))??;
        }
        Ok(())
    })?;
    let finished = finished
        .into_inner()
        .map_err(|_| Error::Eval("agg combine lock poisoned".into()))?;
    Ok(finished.into_iter().flatten().collect())
}

#[derive(Clone)]
struct Dim {
    width: u32,
    kind: DimKind,
}

#[derive(Clone)]
enum DimKind {
    Int { min: i64 },
    UInt { min: u64 },
    Bool,
    Byte,
}

#[derive(Clone)]
struct PerfectSpec {
    dims: Vec<Dim>,
    slots: usize,
}

struct PerfectAgg {
    spec: PerfectSpec,
    group_exprs: Vec<Expr>,
    aggs: Vec<AggCall>,
    schema: SchemaRef,
    combining: bool,
    empty: Vec<Accum>,
    /// 0 = empty, else 1-based index into `accums`.
    index: Vec<u32>,
    accums: Vec<Vec<Accum>>,
    slots_hit: Vec<u32>,
}

impl PerfectAgg {
    fn new(
        group: &[NamedExpr],
        aggs: &[(String, AggCall)],
        schema: SchemaRef,
        spec: PerfectSpec,
    ) -> Result<Self> {
        let group_n = group.len();
        let empty = aggs
            .iter()
            .enumerate()
            .map(|(i, (_, call))| {
                Accum::from_call(call, schema.field(group_n + i).data_type(), false)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            index: vec![0; spec.slots],
            spec,
            group_exprs: group.iter().map(|g| g.expr.clone()).collect(),
            aggs: aggs.iter().map(|(_, c)| c.clone()).collect(),
            schema,
            combining: false,
            empty,
            accums: Vec::new(),
            slots_hit: Vec::new(),
        })
    }

    fn from_hash(table: &HashAgg, spec: PerfectSpec) -> Result<Self> {
        let group_n = table.group_exprs.len();
        let empty = table
            .aggs
            .iter()
            .enumerate()
            .map(|(i, call)| {
                Accum::from_call(
                    call,
                    table.schema.field(group_n + i).data_type(),
                    table.combining,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            index: vec![0; spec.slots],
            spec,
            group_exprs: table.group_exprs.clone(),
            aggs: table.aggs.clone(),
            schema: table.schema.clone(),
            combining: table.combining,
            empty,
            accums: Vec::new(),
            slots_hit: Vec::new(),
        })
    }

    fn try_update(&mut self, batch: &Batch) -> Result<bool> {
        if batch.num_rows() == 0 {
            return Ok(true);
        }
        let key_cols = eval_all(&self.group_exprs, batch)?;
        let n = batch.num_rows();
        let mut codes = vec![0usize; n];
        for (row, code) in codes.iter_mut().enumerate() {
            match encode_row(&self.spec, &key_cols, row) {
                Some(c) => *code = c,
                None => return Ok(false),
            }
        }
        self.apply_codes(batch, &key_cols, &codes)?;
        Ok(true)
    }

    fn update_encoded(&mut self, batch: &Batch, key_cols: &[ArrayRef]) -> Result<()> {
        let n = batch.num_rows();
        let mut codes = vec![0usize; n];
        for (row, code) in codes.iter_mut().enumerate() {
            *code = encode_row(&self.spec, key_cols, row).ok_or_else(|| {
                Error::Eval("perfect-hash encode failed on the inferred batch".into())
            })?;
        }
        self.apply_codes(batch, key_cols, &codes)
    }

    fn apply_codes(
        &mut self,
        batch: &Batch,
        _key_cols: &[ArrayRef],
        codes: &[usize],
    ) -> Result<()> {
        let mut arg_cols: Vec<Option<ArrayRef>> = Vec::with_capacity(self.aggs.len());
        for call in &self.aggs {
            arg_cols.push(match call {
                AggCall::CountStar => None,
                AggCall::Count(e)
                | AggCall::CountDistinct(e)
                | AggCall::Sum(e)
                | AggCall::Avg(e)
                | AggCall::Min(e)
                | AggCall::Max(e)
                | AggCall::User { arg: e, .. } => Some(e.eval(batch)?),
            });
        }
        bump_perfect_rows(codes.len());
        for (row, &code) in codes.iter().enumerate() {
            let dense = self.ensure_slot(code)?;
            for (accum, arg) in self.accums[dense].iter_mut().zip(arg_cols.iter()) {
                accum.update(arg.as_deref(), row)?;
            }
        }
        Ok(())
    }

    fn ensure_slot(&mut self, code: usize) -> Result<usize> {
        let cur = self.index[code];
        if cur != 0 {
            return Ok((cur - 1) as usize);
        }
        let dense = self.accums.len();
        self.accums.push(self.empty.clone());
        self.slots_hit.push(code as u32);
        self.index[code] = (dense as u32) + 1;
        Ok(dense)
    }

    fn merge_table(&mut self, mut other: PerfectAgg) -> Result<()> {
        for (i, &slot) in other.slots_hit.iter().enumerate() {
            let dense = self.ensure_slot(slot as usize)?;
            let src = std::mem::take(&mut other.accums[i]);
            for (dst, a) in self.accums[dense].iter_mut().zip(src) {
                dst.merge(a)?;
            }
        }
        Ok(())
    }

    fn take_hash(&mut self) -> Result<HashAgg> {
        let group: Vec<NamedExpr> = self
            .group_exprs
            .iter()
            .enumerate()
            .map(|(i, expr)| NamedExpr {
                expr: expr.clone(),
                name: self.schema.field(i).name().clone(),
            })
            .collect();
        let aggs: Vec<(String, AggCall)> = self
            .aggs
            .iter()
            .enumerate()
            .map(|(i, call)| {
                (
                    self.schema.field(self.group_exprs.len() + i).name().clone(),
                    call.clone(),
                )
            })
            .collect();
        let mut table = if self.combining {
            HashAgg::combining(&group, &aggs, self.schema.clone())?
        } else {
            HashAgg::new(&group, &aggs, self.schema.clone())?
        };
        let dts: Vec<DataType> = (0..self.group_exprs.len())
            .map(|i| self.schema.field(i).data_type().clone())
            .collect();
        for (i, &slot) in self.slots_hit.iter().enumerate() {
            let key = decode_slot(&self.spec, slot as usize, &dts)?;
            table.insert_ready(key, std::mem::take(&mut self.accums[i]))?;
        }
        self.accums.clear();
        self.slots_hit.clear();
        self.index.fill(0);
        Ok(table)
    }

    fn finish(self, policy: &BatchPolicy) -> Result<Vec<Batch>> {
        if self.accums.is_empty() {
            return Ok(Vec::new());
        }
        let n = self.accums.len();
        let group_n = self.group_exprs.len();
        let dts: Vec<DataType> = (0..group_n)
            .map(|i| self.schema.field(i).data_type().clone())
            .collect();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for (g, field) in self.schema.fields().iter().enumerate().take(group_n) {
            let values: Vec<Scalar> = self
                .slots_hit
                .iter()
                .map(|&slot| decode_slot(&self.spec, slot as usize, &dts).map(|k| k[g].clone()))
                .collect::<Result<_>>()?;
            columns.push(build_array(&values, field.data_type())?);
        }
        for (a, field) in self.schema.fields().iter().enumerate().skip(group_n) {
            let ai = a - group_n;
            let values: Vec<Scalar> = (0..n).map(|i| self.accums[i][ai].finish()).collect();
            columns.push(build_array(&values, field.data_type())?);
        }
        let full = Batch::try_new(self.schema, columns)?;
        Ok(full
            .split_by_policy(policy)
            .into_iter()
            .filter(|b| b.num_rows() > 0)
            .collect())
    }
}

fn infer_spec_morsels(morsels: &[Batch], exprs: &[Expr]) -> Result<Option<PerfectSpec>> {
    let mut spec: Option<PerfectSpec> = None;
    for batch in morsels {
        if batch.num_rows() == 0 {
            continue;
        }
        let cols = eval_all(exprs, batch)?;
        spec = Some(match spec {
            None => match infer_spec(&cols) {
                Some(s) => s,
                None => return Ok(None),
            },
            Some(cur) => match union_spec(&cur, &cols) {
                Some(s) => s,
                None => return Ok(None),
            },
        });
    }
    Ok(spec)
}

fn infer_spec(cols: &[ArrayRef]) -> Option<PerfectSpec> {
    if cols.is_empty() {
        return None;
    }
    let mut dims = Vec::with_capacity(cols.len());
    for col in cols {
        dims.push(infer_dim(col.as_ref())?);
    }
    let slots = product_slots(&dims)?;
    Some(PerfectSpec { dims, slots })
}

fn union_spec(cur: &PerfectSpec, cols: &[ArrayRef]) -> Option<PerfectSpec> {
    if cols.len() != cur.dims.len() {
        return None;
    }
    let mut dims = Vec::with_capacity(cur.dims.len());
    for (dim, col) in cur.dims.iter().zip(cols) {
        dims.push(union_dim(dim, col.as_ref())?);
    }
    let slots = product_slots(&dims)?;
    Some(PerfectSpec { dims, slots })
}

fn product_slots(dims: &[Dim]) -> Option<usize> {
    let mut n = 1u128;
    for dim in dims {
        n = n.checked_mul(u128::from(dim.width))?;
        if n > PERFECT_MAX_SLOTS as u128 {
            return None;
        }
    }
    if n == 0 {
        return None;
    }
    Some(n as usize)
}

fn infer_dim(array: &dyn Array) -> Option<Dim> {
    match array.data_type() {
        DataType::Boolean => Some(Dim {
            width: 3,
            kind: DimKind::Bool,
        }),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            int_dim(array, true)
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            int_dim(array, false)
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Dictionary(_, _) => {
            if utf8_is_byte(array) {
                Some(Dim {
                    width: 257,
                    kind: DimKind::Byte,
                })
            } else {
                None
            }
        }
        _ => None,
    }
}

fn union_dim(dim: &Dim, array: &dyn Array) -> Option<Dim> {
    match (&dim.kind, array.data_type()) {
        (DimKind::Bool, DataType::Boolean) => Some(dim.clone()),
        (
            DimKind::Int { min },
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
        ) => match int_bounds(array, true) {
            None => Some(dim.clone()),
            Some((bmin, bmax)) => {
                let old_max = int_dim_max(dim);
                let min = match old_max {
                    Some(_) => (*min).min(bmin),
                    None => bmin,
                };
                let max = old_max.map(|m| m.max(bmax)).unwrap_or(bmax);
                int_width(min, max).map(|width| Dim {
                    width,
                    kind: DimKind::Int { min },
                })
            }
        },
        (
            DimKind::UInt { min },
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64,
        ) => match uint_bounds(array) {
            None => Some(dim.clone()),
            Some((bmin, bmax)) => {
                let old_max = uint_dim_max(dim);
                let min = match old_max {
                    Some(_) => (*min).min(bmin),
                    None => bmin,
                };
                let max = old_max.map(|m| m.max(bmax)).unwrap_or(bmax);
                uint_width(min, max).map(|width| Dim {
                    width,
                    kind: DimKind::UInt { min },
                })
            }
        },
        (DimKind::Byte, DataType::Utf8 | DataType::LargeUtf8 | DataType::Dictionary(_, _)) => {
            if utf8_is_byte(array) {
                Some(dim.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

fn int_dim(array: &dyn Array, signed: bool) -> Option<Dim> {
    if signed {
        match int_bounds(array, true) {
            None => Some(Dim {
                width: 1,
                kind: DimKind::Int { min: 0 },
            }),
            Some((min, max)) => int_width(min, max).map(|width| Dim {
                width,
                kind: DimKind::Int { min },
            }),
        }
    } else {
        match uint_bounds(array) {
            None => Some(Dim {
                width: 1,
                kind: DimKind::UInt { min: 0 },
            }),
            Some((min, max)) => uint_width(min, max).map(|width| Dim {
                width,
                kind: DimKind::UInt { min },
            }),
        }
    }
}

fn int_dim_max(dim: &Dim) -> Option<i64> {
    match dim.kind {
        DimKind::Int { min } if dim.width >= 2 => Some(min.wrapping_add(i64::from(dim.width - 2))),
        _ => None,
    }
}

fn uint_dim_max(dim: &Dim) -> Option<u64> {
    match dim.kind {
        DimKind::UInt { min } if dim.width >= 2 => Some(min.wrapping_add(u64::from(dim.width - 2))),
        _ => None,
    }
}

fn int_width(min: i64, max: i64) -> Option<u32> {
    if max < min {
        return None;
    }
    let span = (max as i128) - (min as i128) + 1;
    let width = span + 1;
    if width <= 0 || width > PERFECT_MAX_SLOTS as i128 {
        None
    } else {
        Some(width as u32)
    }
}

fn uint_width(min: u64, max: u64) -> Option<u32> {
    if max < min {
        return None;
    }
    let span = (max as u128) - (min as u128) + 1;
    let width = span + 1;
    if width > PERFECT_MAX_SLOTS as u128 {
        None
    } else {
        Some(width as u32)
    }
}

fn int_bounds(array: &dyn Array, _signed: bool) -> Option<(i64, i64)> {
    if array.null_count() == array.len() {
        return None;
    }
    let min = min_scalar(array).ok()?;
    let max = max_scalar(array).ok()?;
    Some((scalar_as_i64(&min)?, scalar_as_i64(&max)?))
}

fn uint_bounds(array: &dyn Array) -> Option<(u64, u64)> {
    if array.null_count() == array.len() {
        return None;
    }
    let min = min_scalar(array).ok()?;
    let max = max_scalar(array).ok()?;
    Some((scalar_as_u64(&min)?, scalar_as_u64(&max)?))
}

fn scalar_as_i64(s: &Scalar) -> Option<i64> {
    match s {
        Scalar::I8(v) => Some(i64::from(*v)),
        Scalar::I16(v) => Some(i64::from(*v)),
        Scalar::I32(v) => Some(i64::from(*v)),
        Scalar::I64(v) => Some(*v),
        Scalar::U8(v) => Some(i64::from(*v)),
        Scalar::U16(v) => Some(i64::from(*v)),
        Scalar::U32(v) => Some(i64::from(*v)),
        _ => None,
    }
}

fn scalar_as_u64(s: &Scalar) -> Option<u64> {
    match s {
        Scalar::U8(v) => Some(u64::from(*v)),
        Scalar::U16(v) => Some(u64::from(*v)),
        Scalar::U32(v) => Some(u64::from(*v)),
        Scalar::U64(v) => Some(*v),
        _ => None,
    }
}

fn utf8_is_byte(array: &dyn Array) -> bool {
    for i in 0..array.len() {
        if array.is_null(i) {
            continue;
        }
        match utf8_cell(array, i) {
            Ok(s) if s.len() == 1 => {}
            _ => return false,
        }
    }
    true
}

fn utf8_cell(array: &dyn Array, row: usize) -> Result<&str> {
    match array.data_type() {
        DataType::Utf8 => Ok(array.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => Ok(array.as_string::<i64>().value(row)),
        DataType::Dictionary(_, _) => dict_utf8_at(array, row),
        dt => Err(Error::Eval(format!("not a utf8 grouping key: {dt}"))),
    }
}

fn encode_row(spec: &PerfectSpec, cols: &[ArrayRef], row: usize) -> Option<usize> {
    if cols.len() != spec.dims.len() {
        return None;
    }
    let mut idx = 0usize;
    let mut stride = 1usize;
    for (dim, col) in spec.dims.iter().zip(cols) {
        let code = encode_dim(dim, col.as_ref(), row)?;
        idx += code as usize * stride;
        stride = stride.checked_mul(dim.width as usize)?;
    }
    if idx >= spec.slots {
        return None;
    }
    Some(idx)
}

fn encode_dim(dim: &Dim, array: &dyn Array, row: usize) -> Option<u32> {
    match dim.kind {
        DimKind::Bool => {
            if array.is_null(row) {
                Some(0)
            } else {
                Some(if array.as_boolean().value(row) { 2 } else { 1 })
            }
        }
        DimKind::Int { min } => {
            if array.is_null(row) {
                Some(0)
            } else {
                let v = integer_value(array, row).ok()?;
                let code = (v as i128) - (min as i128) + 1;
                if code < 1 || code >= i128::from(dim.width) {
                    None
                } else {
                    Some(code as u32)
                }
            }
        }
        DimKind::UInt { min } => {
            if array.is_null(row) {
                Some(0)
            } else {
                let v = unsigned_value(array, row).ok()?;
                if v < min {
                    return None;
                }
                let code = (v as u128) - (min as u128) + 1;
                if code < 1 || code >= u128::from(dim.width) {
                    None
                } else {
                    Some(code as u32)
                }
            }
        }
        DimKind::Byte => {
            if array.is_null(row) {
                Some(0)
            } else {
                let s = utf8_cell(array, row).ok()?;
                if s.len() != 1 {
                    return None;
                }
                Some(u32::from(s.as_bytes()[0]) + 1)
            }
        }
    }
}

fn decode_slot(spec: &PerfectSpec, slot: usize, dts: &[DataType]) -> Result<Vec<Scalar>> {
    let mut rest = slot;
    let mut out = Vec::with_capacity(spec.dims.len());
    for (dim, dt) in spec.dims.iter().zip(dts) {
        let width = dim.width as usize;
        let code = rest % width;
        rest /= width;
        out.push(decode_dim(dim, code as u32, dt)?);
    }
    Ok(out)
}

fn decode_dim(dim: &Dim, code: u32, dt: &DataType) -> Result<Scalar> {
    if code == 0 {
        return Ok(Scalar::Null);
    }
    match dim.kind {
        DimKind::Bool => Ok(Scalar::Bool(code == 2)),
        DimKind::Int { min } => {
            let v = min.wrapping_add(i64::from(code) - 1);
            int_scalar(dt, v)
        }
        DimKind::UInt { min } => {
            let v = min.wrapping_add(u64::from(code) - 1);
            uint_scalar(dt, v)
        }
        DimKind::Byte => {
            let b = (code - 1) as u8;
            Ok(Scalar::from_utf8(
                std::str::from_utf8(&[b]).unwrap_or("\u{FFFD}"),
            ))
        }
    }
}

fn int_scalar(dt: &DataType, v: i64) -> Result<Scalar> {
    Ok(match dt {
        DataType::Int8 => Scalar::I8(v as i8),
        DataType::Int16 => Scalar::I16(v as i16),
        DataType::Int32 => Scalar::I32(v as i32),
        DataType::Int64 => Scalar::I64(v),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Dictionary(_, _) => {
            return Err(Error::Eval("integer perfect-hash decoded as utf8".into()));
        }
        _ => Scalar::I64(v),
    })
}

fn uint_scalar(dt: &DataType, v: u64) -> Result<Scalar> {
    Ok(match dt {
        DataType::UInt8 => Scalar::U8(v as u8),
        DataType::UInt16 => Scalar::U16(v as u16),
        DataType::UInt32 => Scalar::U32(v as u32),
        DataType::UInt64 => Scalar::U64(v),
        _ => Scalar::U64(v),
    })
}

#[derive(Clone)]
pub(super) struct HashAgg {
    group_exprs: Vec<Expr>,
    aggs: Vec<AggCall>,
    schema: SchemaRef,
    grouped: bool,
    combining: bool,
    /// Precomputed key hash → candidate slots. The map uses the hash as the
    /// bucket key. Lookups hash array values in place (no per-row `String`).
    /// Perfect-hash grouped input never stores these keys.
    slots: IdentityMap<Vec<usize>>,
    keys: Vec<Vec<Scalar>>,
    accums: Vec<Vec<Accum>>,
    retained: u64,
    /// MIN/MAX/COUNT DISTINCT/UDAF can grow retained bytes after insert.
    grow: bool,
}

impl HashAgg {
    pub(super) fn new(
        group: &[NamedExpr],
        aggs: &[(String, AggCall)],
        schema: SchemaRef,
    ) -> Result<Self> {
        Self::build(group, aggs, schema, false)
    }

    fn combining(
        group: &[NamedExpr],
        aggs: &[(String, AggCall)],
        schema: SchemaRef,
    ) -> Result<Self> {
        Self::build(group, aggs, schema, true)
    }

    fn build(
        group: &[NamedExpr],
        aggs: &[(String, AggCall)],
        schema: SchemaRef,
        combining: bool,
    ) -> Result<Self> {
        Ok(Self {
            group_exprs: group.iter().map(|g| g.expr.clone()).collect(),
            aggs: aggs.iter().map(|(_, c)| c.clone()).collect(),
            schema,
            grouped: !group.is_empty(),
            combining,
            slots: identity_map_with_capacity(8),
            keys: Vec::new(),
            accums: Vec::new(),
            retained: 0,
            grow: aggs.iter().any(|(_, c)| {
                matches!(
                    c,
                    AggCall::Min(_)
                        | AggCall::Max(_)
                        | AggCall::CountDistinct(_)
                        | AggCall::User { .. }
                )
            }),
        })
    }

    pub(super) fn update(&mut self, batch: &Batch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let key_cols = eval_all(&self.group_exprs, batch)?;
        let mut arg_cols: Vec<Option<ArrayRef>> = Vec::with_capacity(self.aggs.len());
        for call in &self.aggs {
            arg_cols.push(match call {
                AggCall::CountStar => None,
                AggCall::Count(e)
                | AggCall::CountDistinct(e)
                | AggCall::Sum(e)
                | AggCall::Avg(e)
                | AggCall::Min(e)
                | AggCall::Max(e)
                | AggCall::User { arg: e, .. } => Some(e.eval(batch)?),
            });
        }
        if !self.grouped {
            return self.update_ungrouped(&arg_cols, batch.num_rows());
        }
        self.update_grouped(&key_cols, &arg_cols, batch.num_rows())
    }

    /// Assign group slots, then update. Few groups (decision-support q1)
    /// gather rows and use whole-array kernels; high cardinality stays
    /// one pass.
    fn update_grouped(
        &mut self,
        key_cols: &[ArrayRef],
        arg_cols: &[Option<ArrayRef>],
        n: usize,
    ) -> Result<()> {
        let mut row_slot = vec![0usize; n];
        for (row, slot) in row_slot.iter_mut().enumerate() {
            *slot = self.ensure_group_row(key_cols, row)?;
        }
        let n_groups = self.accums.len();
        let grow = self.grow;
        if n_groups > 64 || n < 32 {
            for (row, &slot) in row_slot.iter().enumerate() {
                let old = grow.then(|| accum_retained(&self.accums[slot]));
                for (accum, arg) in self.accums[slot].iter_mut().zip(arg_cols.iter()) {
                    accum.update(arg.as_deref(), row)?;
                }
                if let Some(old) = old {
                    self.charge_accums(slot, old);
                }
            }
            return Ok(());
        }
        let mut rows_for = vec![Vec::new(); n_groups];
        for (row, &slot) in row_slot.iter().enumerate() {
            rows_for[slot].push(row as u32);
        }
        for (slot, rows) in rows_for.iter().enumerate() {
            if rows.is_empty() {
                continue;
            }
            let old = grow.then(|| accum_retained(&self.accums[slot]));
            for (accum, arg) in self.accums[slot].iter_mut().zip(arg_cols.iter()) {
                accum.update_rows(arg.as_deref(), rows)?;
            }
            if let Some(old) = old {
                self.charge_accums(slot, old);
            }
        }
        Ok(())
    }

    fn update_ungrouped(&mut self, arg_cols: &[Option<ArrayRef>], n: usize) -> Result<()> {
        if self.keys.is_empty() {
            self.insert_group(Vec::new())?;
        }
        let old = self.grow.then(|| accum_retained(&self.accums[0]));
        for (accum, arg) in self.accums[0].iter_mut().zip(arg_cols.iter()) {
            accum.update_array(arg.as_deref(), n)?;
        }
        if let Some(old) = old {
            self.charge_accums(0, old);
        }
        Ok(())
    }

    fn ensure_group_row(&mut self, cols: &[ArrayRef], row: usize) -> Result<usize> {
        let h = hash_row(cols, row)?;
        if let Some(cands) = self.slots.get(&h) {
            for &slot in cands {
                if row_matches(&self.keys[slot], cols, row)? {
                    return Ok(slot);
                }
            }
        }
        let key = extract_key(cols, row)?;
        self.insert_group_hashed(h, key)
    }

    fn ensure_group(&mut self, key: Vec<Scalar>) -> Result<usize> {
        let h = hash_scalars(&key);
        if let Some(cands) = self.slots.get(&h) {
            for &slot in cands {
                if self.keys[slot] == key {
                    return Ok(slot);
                }
            }
        }
        self.insert_group_hashed(h, key)
    }

    fn insert_group(&mut self, key: Vec<Scalar>) -> Result<usize> {
        let h = hash_scalars(&key);
        self.insert_group_hashed(h, key)
    }

    /// Fold `other`'s groups into this table. AVG keeps sum and count until
    /// [`Self::finish`], so partials combine with the right weights.
    pub(super) fn merge_table(&mut self, mut other: HashAgg) -> Result<()> {
        if other.keys.is_empty() {
            return Ok(());
        }
        if self.keys.is_empty() {
            *self = other;
            return Ok(());
        }
        for i in 0..other.keys.len() {
            let key = std::mem::take(&mut other.keys[i]);
            let accums = std::mem::take(&mut other.accums[i]);
            let slot = self.ensure_group(key)?;
            let old = self.grow.then(|| accum_retained(&self.accums[slot]));
            for (dst, src) in self.accums[slot].iter_mut().zip(accums) {
                dst.merge(src)?;
            }
            if let Some(old) = old {
                self.charge_accums(slot, old);
            }
        }
        Ok(())
    }

    fn n_groups(&self) -> usize {
        self.accums.len()
    }

    fn retained_bytes(&self) -> u64 {
        self.retained
    }

    fn charge_accums(&mut self, slot: usize, old: u64) {
        let new = accum_retained(&self.accums[slot]);
        self.retained = self.retained.saturating_sub(old).saturating_add(new);
    }

    fn insert_group_hashed(&mut self, h: u64, key: Vec<Scalar>) -> Result<usize> {
        bump_hash_inserts();
        let slot = self.keys.len();
        let n_group = self.group_exprs.len();
        let accums = self
            .aggs
            .iter()
            .enumerate()
            .map(|(i, call)| {
                Accum::from_call(
                    call,
                    self.schema.field(n_group + i).data_type(),
                    self.combining,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let extra = 32
            + key.iter().map(scalar_mem).sum::<u64>()
            + accums.iter().map(Accum::retained_bytes).sum::<u64>();
        self.slots.entry(h).or_default().push(slot);
        self.keys.push(key);
        self.accums.push(accums);
        self.retained = self.retained.saturating_add(extra);
        Ok(slot)
    }

    fn insert_ready(&mut self, key: Vec<Scalar>, accums: Vec<Accum>) -> Result<()> {
        bump_hash_inserts();
        let h = hash_scalars(&key);
        let slot = self.keys.len();
        let extra = 32
            + key.iter().map(scalar_mem).sum::<u64>()
            + accums.iter().map(Accum::retained_bytes).sum::<u64>();
        self.slots.entry(h).or_default().push(slot);
        self.keys.push(key);
        self.accums.push(accums);
        self.retained = self.retained.saturating_add(extra);
        Ok(())
    }

    /// Move groups into radix partitions by key hash. Equal keys stay in one
    /// partition so the later per-partition merge is disjoint.
    fn flush_into(&mut self, parts: &mut [HashAgg]) -> Result<()> {
        if self.keys.is_empty() {
            return Ok(());
        }
        let npart = parts.len();
        debug_assert!(npart.is_power_of_two() && npart > 0);
        let mask = npart - 1;
        for i in 0..self.keys.len() {
            let key = std::mem::take(&mut self.keys[i]);
            let accums = std::mem::take(&mut self.accums[i]);
            let h = hash_scalars(&key);
            let p = radix_slot(h, mask);
            let slot = parts[p].ensure_group(key)?;
            let old = parts[p]
                .grow
                .then(|| accum_retained(&parts[p].accums[slot]));
            for (dst, src) in parts[p].accums[slot].iter_mut().zip(accums) {
                dst.merge(src)?;
            }
            if let Some(old) = old {
                parts[p].charge_accums(slot, old);
            }
        }
        self.keys.clear();
        self.accums.clear();
        self.slots.clear();
        Ok(())
    }

    fn finish(mut self, policy: &BatchPolicy) -> Result<Vec<Batch>> {
        if !self.grouped && self.keys.is_empty() {
            self.ensure_group(Vec::new())?;
        }
        if self.keys.is_empty() {
            return Ok(Vec::new());
        }
        let n = self.keys.len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for (g, field) in self
            .schema
            .fields()
            .iter()
            .enumerate()
            .take(self.group_exprs.len())
        {
            let values: Vec<Scalar> = self.keys.iter().map(|k| k[g].clone()).collect();
            columns.push(build_array(&values, field.data_type())?);
        }
        for (a, field) in self
            .schema
            .fields()
            .iter()
            .enumerate()
            .skip(self.group_exprs.len())
        {
            let ai = a - self.group_exprs.len();
            let values: Vec<Scalar> = (0..n).map(|i| self.accums[i][ai].finish()).collect();
            columns.push(build_array(&values, field.data_type())?);
        }
        let full = Batch::try_new(self.schema, columns)?;
        Ok(full
            .split_by_policy(policy)
            .into_iter()
            .filter(|b| b.num_rows() > 0)
            .collect())
    }

    /// Complete output without dropping running state (micro-batch snapshot).
    pub(super) fn snapshot(&self, policy: &BatchPolicy) -> Result<Vec<Batch>> {
        self.clone().finish(policy)
    }
}

#[derive(Clone)]
enum Accum {
    Count(i64),
    CountDistinct {
        seen: HashSet<Scalar>,
        bytes: u64,
    },
    SumI64 {
        sum: i64,
        seen: bool,
    },
    SumU64 {
        sum: u64,
        seen: bool,
    },
    SumF64 {
        sum: f64,
        seen: bool,
    },
    Avg {
        sum: f64,
        count: i64,
    },
    Min {
        current: Option<Scalar>,
    },
    Max {
        current: Option<Scalar>,
    },
    User {
        agg: Arc<dyn CombineAgg>,
        state: Scalar,
        combining: bool,
        dt: DataType,
    },
}

impl Accum {
    fn retained_bytes(&self) -> u64 {
        32 + match self {
            Self::CountDistinct { bytes, .. } => *bytes,
            Self::User { state, .. } => scalar_mem(state),
            Self::Min { current } | Self::Max { current } => {
                current.as_ref().map(scalar_mem).unwrap_or(0)
            }
            _ => 0,
        }
    }

    fn from_call(call: &AggCall, result_type: &DataType, combining: bool) -> Result<Self> {
        Ok(match call {
            AggCall::CountStar | AggCall::Count(_) => Self::Count(0),
            AggCall::CountDistinct(_) => Self::CountDistinct {
                seen: HashSet::new(),
                bytes: 0,
            },
            AggCall::Sum(_) => match result_type {
                DataType::Float32 | DataType::Float64 => Self::SumF64 {
                    sum: 0.0,
                    seen: false,
                },
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    Self::SumU64 {
                        sum: 0,
                        seen: false,
                    }
                }
                _ => Self::SumI64 {
                    sum: 0,
                    seen: false,
                },
            },
            AggCall::Avg(_) => Self::Avg { sum: 0.0, count: 0 },
            AggCall::Min(_) => Self::Min { current: None },
            AggCall::Max(_) => Self::Max { current: None },
            AggCall::User { agg, .. } => {
                let id = agg.identity();
                Self::User {
                    agg: agg.clone(),
                    state: scalar_at(id.as_ref(), 0)?,
                    combining,
                    dt: agg.result_type(),
                }
            }
        })
    }

    fn update_rows(&mut self, arg: Option<&dyn Array>, rows: &[u32]) -> Result<()> {
        match self {
            Self::Count(n) => {
                if let Some(array) = arg {
                    for &row in rows {
                        if !array.is_null(row as usize) {
                            *n += 1;
                        }
                    }
                } else {
                    *n += rows.len() as i64;
                }
                Ok(())
            }
            Self::SumF64 { sum, seen } => {
                let array = arg.ok_or_else(|| Error::Eval("SUM needs an argument".into()))?;
                if array.data_type() == &DataType::Float64 {
                    let arr = array.as_primitive::<Float64Type>();
                    for &row in rows {
                        let i = row as usize;
                        if !arr.is_null(i) {
                            *sum += arr.value(i);
                            *seen = true;
                        }
                    }
                    return Ok(());
                }
                for &row in rows {
                    update_sum_f64(sum, seen, arg, row as usize)?;
                }
                Ok(())
            }
            Self::SumI64 { sum, seen } => {
                for &row in rows {
                    update_sum_i64(sum, seen, arg, row as usize)?;
                }
                Ok(())
            }
            Self::SumU64 { sum, seen } => {
                for &row in rows {
                    update_sum_u64(sum, seen, arg, row as usize)?;
                }
                Ok(())
            }
            Self::Avg { sum, count } => {
                for &row in rows {
                    update_avg(sum, count, arg, row as usize)?;
                }
                Ok(())
            }
            _ => {
                for &row in rows {
                    self.update(arg, row as usize)?;
                }
                Ok(())
            }
        }
    }

    fn update(&mut self, arg: Option<&dyn Array>, row: usize) -> Result<()> {
        match self {
            Self::Count(n) => {
                if let Some(array) = arg {
                    if !array.is_null(row) {
                        *n += 1;
                    }
                } else {
                    *n += 1;
                }
                Ok(())
            }
            Self::CountDistinct { seen, bytes } => {
                let array =
                    arg.ok_or_else(|| Error::Eval("COUNT DISTINCT needs an argument".into()))?;
                let value = scalar_at(array, row)?;
                if value != Scalar::Null {
                    let extra = scalar_mem(&value);
                    if seen.insert(value) {
                        *bytes = bytes.saturating_add(extra);
                    }
                }
                Ok(())
            }
            Self::SumI64 { sum, seen } => update_sum_i64(sum, seen, arg, row),
            Self::SumU64 { sum, seen } => update_sum_u64(sum, seen, arg, row),
            Self::SumF64 { sum, seen } => update_sum_f64(sum, seen, arg, row),
            Self::Avg { sum, count } => update_avg(sum, count, arg, row),
            Self::Min { current } => update_minmax(current, arg, row, true),
            Self::Max { current } => update_minmax(current, arg, row, false),
            Self::User {
                agg,
                state,
                combining,
                dt,
            } => {
                let array =
                    arg.ok_or_else(|| Error::Eval("user aggregate needs an argument".into()))?;
                let state_arr = build_array(std::slice::from_ref(state), dt)?;
                let out = if *combining {
                    let value = scalar_at(array, row)?;
                    let value_arr = build_array(std::slice::from_ref(&value), dt)?;
                    agg.merge(state_arr.as_ref(), value_arr.as_ref())?
                } else {
                    let value_arr = array.slice(row, 1);
                    agg.update(state_arr.as_ref(), value_arr.as_ref())?
                };
                *state = scalar_at(out.as_ref(), 0)?;
                Ok(())
            }
        }
    }

    /// Whole-array update for the ungrouped path. COUNT/SUM/AVG/MIN/MAX use
    /// Arrow compute kernels; COUNT DISTINCT and user aggs stay row-wise.
    fn update_array(&mut self, arg: Option<&dyn Array>, n: usize) -> Result<()> {
        if matches!(self, Self::CountDistinct { .. } | Self::User { .. }) {
            for row in 0..n {
                self.update(arg, row)?;
            }
            return Ok(());
        }
        match self {
            Self::Count(c) => {
                *c += match arg {
                    None => n as i64,
                    Some(array) => (array.len() - array.null_count()) as i64,
                };
                Ok(())
            }
            Self::SumF64 { sum, seen } => {
                let array = arg.ok_or_else(|| Error::Eval("SUM needs an argument".into()))?;
                if array.null_count() == array.len() {
                    return Ok(());
                }
                *sum += sum_f64_array(array)?;
                *seen = true;
                Ok(())
            }
            Self::SumI64 { sum, seen } => {
                let array = arg.ok_or_else(|| Error::Eval("SUM needs an argument".into()))?;
                if array.null_count() == array.len() {
                    return Ok(());
                }
                *sum = sum
                    .checked_add(sum_i64_array(array)?)
                    .ok_or_else(|| Error::Eval("SUM overflow".into()))?;
                *seen = true;
                Ok(())
            }
            Self::SumU64 { sum, seen } => {
                let array = arg.ok_or_else(|| Error::Eval("SUM needs an argument".into()))?;
                if array.null_count() == array.len() {
                    return Ok(());
                }
                *sum = sum
                    .checked_add(sum_u64_array(array)?)
                    .ok_or_else(|| Error::Eval("SUM overflow".into()))?;
                *seen = true;
                Ok(())
            }
            Self::Avg { sum, count } => {
                let array = arg.ok_or_else(|| Error::Eval("AVG needs an argument".into()))?;
                let valid = (array.len() - array.null_count()) as i64;
                if valid == 0 {
                    return Ok(());
                }
                *sum += sum_f64_array(array)?;
                *count += valid;
                Ok(())
            }
            Self::Min { current } => update_minmax_array(current, arg, true),
            Self::Max { current } => update_minmax_array(current, arg, false),
            Self::CountDistinct { .. } | Self::User { .. } => unreachable!("handled above"),
        }
    }

    fn finish(&self) -> Scalar {
        match self {
            Self::Count(n) => Scalar::I64(*n),
            Self::CountDistinct { seen, .. } => Scalar::I64(seen.len() as i64),
            Self::SumI64 { sum, seen } => {
                if *seen {
                    Scalar::I64(*sum)
                } else {
                    Scalar::Null
                }
            }
            Self::SumU64 { sum, seen } => {
                if *seen {
                    Scalar::U64(*sum)
                } else {
                    Scalar::Null
                }
            }
            Self::SumF64 { sum, seen } => {
                if *seen {
                    Scalar::F64(sum.to_bits())
                } else {
                    Scalar::Null
                }
            }
            Self::Avg { sum, count } => {
                if *count == 0 {
                    Scalar::Null
                } else {
                    Scalar::F64((*sum / *count as f64).to_bits())
                }
            }
            Self::Min { current } | Self::Max { current } => {
                current.clone().unwrap_or(Scalar::Null)
            }
            Self::User { state, .. } => state.clone(),
        }
    }

    fn merge(&mut self, other: Accum) -> Result<()> {
        match (self, other) {
            (Self::Count(a), Self::Count(b)) => {
                *a += b;
                Ok(())
            }
            (Self::CountDistinct { seen: a, bytes }, Self::CountDistinct { seen: b, .. }) => {
                for v in b {
                    let extra = scalar_mem(&v);
                    if a.insert(v) {
                        *bytes = bytes.saturating_add(extra);
                    }
                }
                Ok(())
            }
            (
                Self::SumI64 { sum, seen },
                Self::SumI64 {
                    sum: add,
                    seen: had,
                },
            ) => {
                if had {
                    *sum = sum
                        .checked_add(add)
                        .ok_or_else(|| Error::Eval("SUM overflow".into()))?;
                    *seen = true;
                }
                Ok(())
            }
            (
                Self::SumU64 { sum, seen },
                Self::SumU64 {
                    sum: add,
                    seen: had,
                },
            ) => {
                if had {
                    *sum = sum
                        .checked_add(add)
                        .ok_or_else(|| Error::Eval("SUM overflow".into()))?;
                    *seen = true;
                }
                Ok(())
            }
            (
                Self::SumF64 { sum, seen },
                Self::SumF64 {
                    sum: add,
                    seen: had,
                },
            ) => {
                if had {
                    *sum += add;
                    *seen = true;
                }
                Ok(())
            }
            (Self::Avg { sum, count }, Self::Avg { sum: add, count: n }) => {
                *sum += add;
                *count += n;
                Ok(())
            }
            (Self::Min { current }, Self::Min { current: other }) => {
                merge_minmax(current, other, true)
            }
            (Self::Max { current }, Self::Max { current: other }) => {
                merge_minmax(current, other, false)
            }
            (Self::User { agg, state, dt, .. }, Self::User { state: other, .. }) => {
                let left = build_array(std::slice::from_ref(state), dt)?;
                let right = build_array(std::slice::from_ref(&other), dt)?;
                let out = agg.merge(left.as_ref(), right.as_ref())?;
                *state = scalar_at(out.as_ref(), 0)?;
                Ok(())
            }
            _ => Err(Error::Eval("cannot merge accumulators".into())),
        }
    }
}

fn merge_minmax(current: &mut Option<Scalar>, other: Option<Scalar>, is_min: bool) -> Result<()> {
    let Some(value) = other else {
        return Ok(());
    };
    if value == Scalar::Null {
        return Ok(());
    }
    match current {
        None => {
            *current = Some(value);
            Ok(())
        }
        Some(cur) => {
            let ord = cmp_scalar(cur, &value)?;
            if (is_min && ord == Ordering::Greater) || (!is_min && ord == Ordering::Less) {
                *current = Some(value);
            }
            Ok(())
        }
    }
}

fn update_sum_i64(
    sum: &mut i64,
    seen: &mut bool,
    arg: Option<&dyn Array>,
    row: usize,
) -> Result<()> {
    let array = arg.ok_or_else(|| Error::Eval("SUM needs an argument".into()))?;
    if array.is_null(row) {
        return Ok(());
    }
    let v = integer_value(array, row)?;
    *sum = sum
        .checked_add(v)
        .ok_or_else(|| Error::Eval("SUM overflow".into()))?;
    *seen = true;
    Ok(())
}

fn update_sum_u64(
    sum: &mut u64,
    seen: &mut bool,
    arg: Option<&dyn Array>,
    row: usize,
) -> Result<()> {
    let array = arg.ok_or_else(|| Error::Eval("SUM needs an argument".into()))?;
    if array.is_null(row) {
        return Ok(());
    }
    let v = unsigned_value(array, row)?;
    *sum = sum
        .checked_add(v)
        .ok_or_else(|| Error::Eval("SUM overflow".into()))?;
    *seen = true;
    Ok(())
}

fn update_sum_f64(
    sum: &mut f64,
    seen: &mut bool,
    arg: Option<&dyn Array>,
    row: usize,
) -> Result<()> {
    let array = arg.ok_or_else(|| Error::Eval("SUM needs an argument".into()))?;
    if array.is_null(row) {
        return Ok(());
    }
    *sum += float_value(array, row)?;
    *seen = true;
    Ok(())
}

fn update_avg(sum: &mut f64, count: &mut i64, arg: Option<&dyn Array>, row: usize) -> Result<()> {
    let array = arg.ok_or_else(|| Error::Eval("AVG needs an argument".into()))?;
    if array.is_null(row) {
        return Ok(());
    }
    *sum += numeric_as_f64(array, row)?;
    *count += 1;
    Ok(())
}

fn update_minmax_array(
    current: &mut Option<Scalar>,
    arg: Option<&dyn Array>,
    is_min: bool,
) -> Result<()> {
    let array = arg.ok_or_else(|| Error::Eval("MIN/MAX needs an argument".into()))?;
    if array.null_count() == array.len() {
        return Ok(());
    }
    let part = if is_min {
        min_scalar(array)?
    } else {
        max_scalar(array)?
    };
    match current {
        None => *current = Some(part),
        Some(cur) => {
            let ord = cmp_scalar(cur, &part)?;
            if (is_min && ord == Ordering::Greater) || (!is_min && ord == Ordering::Less) {
                *current = Some(part);
            }
        }
    }
    Ok(())
}

fn update_minmax(
    current: &mut Option<Scalar>,
    arg: Option<&dyn Array>,
    row: usize,
    is_min: bool,
) -> Result<()> {
    let array = arg.ok_or_else(|| Error::Eval("MIN/MAX needs an argument".into()))?;
    let value = scalar_at(array, row)?;
    if value == Scalar::Null {
        return Ok(());
    }
    match current {
        None => *current = Some(value),
        Some(cur) => {
            let ord = cmp_scalar(cur, &value)?;
            if (is_min && ord == Ordering::Greater) || (!is_min && ord == Ordering::Less) {
                *current = Some(value);
            }
        }
    }
    Ok(())
}

fn sum_f64_array(array: &dyn Array) -> Result<f64> {
    Ok(match array.data_type() {
        DataType::Float64 => compute::sum(array.as_primitive::<Float64Type>()).unwrap_or(0.0),
        DataType::Float32 => compute::sum(array.as_primitive::<Float32Type>())
            .map(f64::from)
            .unwrap_or(0.0),
        DataType::Int64 => compute::sum(array.as_primitive::<Int64Type>())
            .map(|v| v as f64)
            .unwrap_or(0.0),
        DataType::Int32 => compute::sum(array.as_primitive::<Int32Type>())
            .map(f64::from)
            .unwrap_or(0.0),
        DataType::UInt64 => compute::sum(array.as_primitive::<UInt64Type>())
            .map(|v| v as f64)
            .unwrap_or(0.0),
        other => {
            let mut s = 0.0;
            for row in 0..array.len() {
                if !array.is_null(row) {
                    s += numeric_as_f64(array, row)?;
                }
            }
            let _ = other;
            s
        }
    })
}

fn sum_i64_array(array: &dyn Array) -> Result<i64> {
    // Integer SUM stays checked so overflow fails closed. Arrow's kernel wraps.
    if let DataType::Int64 = array.data_type() {
        let a = array.as_primitive::<Int64Type>();
        let mut s = 0i64;
        if a.null_count() == 0 {
            for v in a.values().iter() {
                s = s
                    .checked_add(*v)
                    .ok_or_else(|| Error::Eval("SUM overflow".into()))?;
            }
            return Ok(s);
        }
        for i in 0..a.len() {
            if a.is_valid(i) {
                s = s
                    .checked_add(a.value(i))
                    .ok_or_else(|| Error::Eval("SUM overflow".into()))?;
            }
        }
        return Ok(s);
    }
    let mut s = 0i64;
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        s = s
            .checked_add(integer_value(array, row)?)
            .ok_or_else(|| Error::Eval("SUM overflow".into()))?;
    }
    Ok(s)
}

fn sum_u64_array(array: &dyn Array) -> Result<u64> {
    if let DataType::UInt64 = array.data_type() {
        let a = array.as_primitive::<UInt64Type>();
        let mut s = 0u64;
        if a.null_count() == 0 {
            for v in a.values().iter() {
                s = s
                    .checked_add(*v)
                    .ok_or_else(|| Error::Eval("SUM overflow".into()))?;
            }
            return Ok(s);
        }
        for i in 0..a.len() {
            if a.is_valid(i) {
                s = s
                    .checked_add(a.value(i))
                    .ok_or_else(|| Error::Eval("SUM overflow".into()))?;
            }
        }
        return Ok(s);
    }
    let mut s = 0u64;
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        s = s
            .checked_add(unsigned_value(array, row)?)
            .ok_or_else(|| Error::Eval("SUM overflow".into()))?;
    }
    Ok(s)
}

fn min_scalar(array: &dyn Array) -> Result<Scalar> {
    if let Some(v) = kernel_extremum(array, true)? {
        return Ok(v);
    }
    extremum_scalar(array, true)
}

fn max_scalar(array: &dyn Array) -> Result<Scalar> {
    if let Some(v) = kernel_extremum(array, false)? {
        return Ok(v);
    }
    extremum_scalar(array, false)
}

fn kernel_extremum(array: &dyn Array, is_min: bool) -> Result<Option<Scalar>> {
    let scalar = match array.data_type() {
        DataType::Int8 => {
            let a = array.as_primitive::<Int8Type>();
            let v = if is_min {
                compute::min(a)
            } else {
                compute::max(a)
            };
            v.map(Scalar::I8)
        }
        DataType::Int16 => {
            let a = array.as_primitive::<Int16Type>();
            let v = if is_min {
                compute::min(a)
            } else {
                compute::max(a)
            };
            v.map(Scalar::I16)
        }
        DataType::Int32 => {
            let a = array.as_primitive::<Int32Type>();
            let v = if is_min {
                compute::min(a)
            } else {
                compute::max(a)
            };
            v.map(Scalar::I32)
        }
        DataType::Int64 => {
            let a = array.as_primitive::<Int64Type>();
            let v = if is_min {
                compute::min(a)
            } else {
                compute::max(a)
            };
            v.map(Scalar::I64)
        }
        DataType::UInt8 => {
            let a = array.as_primitive::<UInt8Type>();
            let v = if is_min {
                compute::min(a)
            } else {
                compute::max(a)
            };
            v.map(Scalar::U8)
        }
        DataType::UInt16 => {
            let a = array.as_primitive::<UInt16Type>();
            let v = if is_min {
                compute::min(a)
            } else {
                compute::max(a)
            };
            v.map(Scalar::U16)
        }
        DataType::UInt32 => {
            let a = array.as_primitive::<UInt32Type>();
            let v = if is_min {
                compute::min(a)
            } else {
                compute::max(a)
            };
            v.map(Scalar::U32)
        }
        DataType::UInt64 => {
            let a = array.as_primitive::<UInt64Type>();
            let v = if is_min {
                compute::min(a)
            } else {
                compute::max(a)
            };
            v.map(Scalar::U64)
        }
        DataType::Float32 => {
            let a = array.as_primitive::<Float32Type>();
            let v = if is_min {
                compute::min(a)
            } else {
                compute::max(a)
            };
            v.map(|x| Scalar::F32(x.to_bits()))
        }
        DataType::Float64 => {
            let a = array.as_primitive::<Float64Type>();
            let v = if is_min {
                compute::min(a)
            } else {
                compute::max(a)
            };
            v.map(|x| Scalar::F64(x.to_bits()))
        }
        DataType::Boolean => {
            let a = array.as_boolean();
            let mut cur: Option<bool> = None;
            for i in 0..a.len() {
                if a.is_valid(i) {
                    let v = a.value(i);
                    cur = Some(match cur {
                        None => v,
                        Some(c) => {
                            if is_min {
                                c && v
                            } else {
                                c || v
                            }
                        }
                    });
                }
            }
            cur.map(Scalar::Bool)
        }
        DataType::Utf8 => {
            let a = array.as_string::<i32>();
            let mut cur: Option<&str> = None;
            for i in 0..a.len() {
                if a.is_valid(i) {
                    let v = a.value(i);
                    cur = Some(match cur {
                        None => v,
                        Some(c) => {
                            if is_min {
                                if v < c { v } else { c }
                            } else if v > c {
                                v
                            } else {
                                c
                            }
                        }
                    });
                }
            }
            cur.map(Scalar::from_utf8)
        }
        _ => return Ok(None),
    };
    scalar
        .ok_or_else(|| Error::Eval("MIN/MAX of empty array".into()))
        .map(Some)
}

fn extremum_scalar(array: &dyn Array, is_min: bool) -> Result<Scalar> {
    let mut cur: Option<Scalar> = None;
    for row in 0..array.len() {
        update_minmax(&mut cur, Some(array), row, is_min)?;
    }
    cur.ok_or_else(|| Error::Eval("MIN/MAX of empty array".into()))
}

fn integer_value(array: &dyn Array, row: usize) -> Result<i64> {
    Ok(match array.data_type() {
        DataType::Int8 => i64::from(array.as_primitive::<Int8Type>().value(row)),
        DataType::Int16 => i64::from(array.as_primitive::<Int16Type>().value(row)),
        DataType::Int32 => i64::from(array.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => array.as_primitive::<Int64Type>().value(row),
        DataType::UInt8 => i64::from(array.as_primitive::<UInt8Type>().value(row)),
        DataType::UInt16 => i64::from(array.as_primitive::<UInt16Type>().value(row)),
        DataType::UInt32 => i64::from(array.as_primitive::<UInt32Type>().value(row)),
        dt => {
            return Err(Error::Eval(format!(
                "SUM does not support {dt} in the integer accumulator"
            )));
        }
    })
}

fn unsigned_value(array: &dyn Array, row: usize) -> Result<u64> {
    Ok(match array.data_type() {
        DataType::UInt8 => u64::from(array.as_primitive::<UInt8Type>().value(row)),
        DataType::UInt16 => u64::from(array.as_primitive::<UInt16Type>().value(row)),
        DataType::UInt32 => u64::from(array.as_primitive::<UInt32Type>().value(row)),
        DataType::UInt64 => array.as_primitive::<UInt64Type>().value(row),
        dt => {
            return Err(Error::Eval(format!(
                "SUM does not support {dt} in the unsigned accumulator"
            )));
        }
    })
}

fn float_value(array: &dyn Array, row: usize) -> Result<f64> {
    Ok(match array.data_type() {
        DataType::Float32 => f64::from(array.as_primitive::<Float32Type>().value(row)),
        DataType::Float64 => array.as_primitive::<Float64Type>().value(row),
        dt => return Err(Error::Eval(format!("float SUM does not support {dt}"))),
    })
}

fn numeric_as_f64(array: &dyn Array, row: usize) -> Result<f64> {
    match array.data_type() {
        DataType::Float32 | DataType::Float64 => float_value(array, row),
        DataType::UInt64 => Ok(array.as_primitive::<UInt64Type>().value(row) as f64),
        _ => Ok(integer_value(array, row)? as f64),
    }
}

fn cmp_scalar(a: &Scalar, b: &Scalar) -> Result<Ordering> {
    Ok(match (a, b) {
        (Scalar::Bool(x), Scalar::Bool(y)) => x.cmp(y),
        (Scalar::I8(x), Scalar::I8(y)) => x.cmp(y),
        (Scalar::I16(x), Scalar::I16(y)) => x.cmp(y),
        (Scalar::I32(x), Scalar::I32(y)) => x.cmp(y),
        (Scalar::I64(x), Scalar::I64(y)) => x.cmp(y),
        (Scalar::U8(x), Scalar::U8(y)) => x.cmp(y),
        (Scalar::U16(x), Scalar::U16(y)) => x.cmp(y),
        (Scalar::U32(x), Scalar::U32(y)) => x.cmp(y),
        (Scalar::U64(x), Scalar::U64(y)) => x.cmp(y),
        (Scalar::F32(x), Scalar::F32(y)) => f32::from_bits(*x).total_cmp(&f32::from_bits(*y)),
        (Scalar::F64(x), Scalar::F64(y)) => f64::from_bits(*x).total_cmp(&f64::from_bits(*y)),
        (x, y) => match (x.as_utf8(), y.as_utf8()) {
            (Some(a), Some(b)) => a.cmp(b),
            _ => {
                return Err(Error::Eval(
                    "MIN/MAX compared values of different types".into(),
                ));
            }
        },
    })
}

fn build_array(values: &[Scalar], dt: &DataType) -> Result<ArrayRef> {
    Ok(match dt {
        DataType::Boolean => Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|v| match v {
                    Scalar::Null => Ok(None),
                    Scalar::Bool(b) => Ok(Some(*b)),
                    _ => Err(Error::Eval("expected boolean aggregate value".into())),
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        DataType::Int8 => Arc::new(Int8Array::from(map_i8(values)?)),
        DataType::Int16 => Arc::new(Int16Array::from(map_i16(values)?)),
        DataType::Int32 => Arc::new(Int32Array::from(map_i32(values)?)),
        DataType::Int64 => Arc::new(Int64Array::from(map_i64(values)?)),
        DataType::UInt8 => Arc::new(UInt8Array::from(map_u8(values)?)),
        DataType::UInt16 => Arc::new(UInt16Array::from(map_u16(values)?)),
        DataType::UInt32 => Arc::new(UInt32Array::from(map_u32(values)?)),
        DataType::UInt64 => Arc::new(UInt64Array::from(map_u64(values)?)),
        DataType::Float32 => Arc::new(Float32Array::from(map_f32(values)?)),
        DataType::Float64 => Arc::new(Float64Array::from(map_f64(values)?)),
        DataType::Utf8 => Arc::new(StringArray::from(map_utf8(values)?)),
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(map_utf8(values)?)),
        DataType::Dictionary(_, value)
            if matches!(value.as_ref(), DataType::Utf8 | DataType::LargeUtf8) =>
        {
            let plain = match value.as_ref() {
                DataType::LargeUtf8 => {
                    Arc::new(LargeStringArray::from(map_utf8(values)?)) as ArrayRef
                }
                _ => Arc::new(StringArray::from(map_utf8(values)?)) as ArrayRef,
            };
            compute::cast(plain.as_ref(), dt).map_err(|e| Error::Eval(e.to_string()))?
        }
        dt => {
            return Err(Error::Eval(format!(
                "unsupported aggregate output type: {dt}"
            )));
        }
    })
}

fn map_i8(values: &[Scalar]) -> Result<Vec<Option<i8>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::I8(x) => Ok(Some(*x)),
            other => downcast_int(other, |x| i8::try_from(x).ok()),
        })
        .collect()
}

fn map_i16(values: &[Scalar]) -> Result<Vec<Option<i16>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::I16(x) => Ok(Some(*x)),
            other => downcast_int(other, |x| i16::try_from(x).ok()),
        })
        .collect()
}

fn map_i32(values: &[Scalar]) -> Result<Vec<Option<i32>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::I32(x) => Ok(Some(*x)),
            other => downcast_int(other, |x| i32::try_from(x).ok()),
        })
        .collect()
}

fn map_i64(values: &[Scalar]) -> Result<Vec<Option<i64>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::I64(x) => Ok(Some(*x)),
            Scalar::I8(x) => Ok(Some(i64::from(*x))),
            Scalar::I16(x) => Ok(Some(i64::from(*x))),
            Scalar::I32(x) => Ok(Some(i64::from(*x))),
            _ => Err(Error::Eval("expected integer aggregate value".into())),
        })
        .collect()
}

fn map_u8(values: &[Scalar]) -> Result<Vec<Option<u8>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::U8(x) => Ok(Some(*x)),
            other => downcast_uint(other, |x| u8::try_from(x).ok()),
        })
        .collect()
}

fn map_u16(values: &[Scalar]) -> Result<Vec<Option<u16>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::U16(x) => Ok(Some(*x)),
            other => downcast_uint(other, |x| u16::try_from(x).ok()),
        })
        .collect()
}

fn map_u32(values: &[Scalar]) -> Result<Vec<Option<u32>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::U32(x) => Ok(Some(*x)),
            other => downcast_uint(other, |x| u32::try_from(x).ok()),
        })
        .collect()
}

fn map_u64(values: &[Scalar]) -> Result<Vec<Option<u64>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::U64(x) => Ok(Some(*x)),
            _ => Err(Error::Eval("expected u64 aggregate value".into())),
        })
        .collect()
}

fn map_f32(values: &[Scalar]) -> Result<Vec<Option<f32>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::F32(x) => Ok(Some(f32::from_bits(*x))),
            Scalar::F64(x) => Ok(Some(f64::from_bits(*x) as f32)),
            _ => Err(Error::Eval("expected float aggregate value".into())),
        })
        .collect()
}

fn map_f64(values: &[Scalar]) -> Result<Vec<Option<f64>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::F64(x) => Ok(Some(f64::from_bits(*x))),
            Scalar::F32(x) => Ok(Some(f64::from(f32::from_bits(*x)))),
            _ => Err(Error::Eval("expected float64 aggregate value".into())),
        })
        .collect()
}

fn map_utf8(values: &[Scalar]) -> Result<Vec<Option<&str>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            other => other
                .as_utf8()
                .map(Some)
                .ok_or_else(|| Error::Eval("expected utf8 aggregate value".into())),
        })
        .collect()
}

fn downcast_int<T>(v: &Scalar, f: impl Fn(i64) -> Option<T>) -> Result<Option<T>> {
    let n = match v {
        Scalar::I64(x) => *x,
        Scalar::I32(x) => i64::from(*x),
        Scalar::I16(x) => i64::from(*x),
        Scalar::I8(x) => i64::from(*x),
        _ => return Err(Error::Eval("expected integer aggregate value".into())),
    };
    f(n).map(Some)
        .ok_or_else(|| Error::Eval("integer SUM does not fit the result type".into()))
}

fn downcast_uint<T>(v: &Scalar, f: impl Fn(u64) -> Option<T>) -> Result<Option<T>> {
    let n = match v {
        Scalar::U64(x) => *x,
        Scalar::U32(x) => u64::from(*x),
        Scalar::U16(x) => u64::from(*x),
        Scalar::U8(x) => u64::from(*x),
        _ => return Err(Error::Eval("expected unsigned aggregate value".into())),
    };
    f(n).map(Some)
        .ok_or_else(|| Error::Eval("unsigned SUM does not fit the result type".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{DictionaryArray, Float64Array, Int32Array, Int64Array};
    use arrow::datatypes::{Field, Schema};
    use mizuchi_plan::NamedExpr;

    fn i64_batch(values: Vec<Option<i64>>) -> Batch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        Batch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    fn finish_sum(batch: &Batch) -> Scalar {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Int64, true)]));
        let mut table =
            HashAgg::new(&[], &[("s".into(), AggCall::Sum(Expr::nth(0)))], schema).unwrap();
        table.update(batch).unwrap();
        table.accums[0][0].finish()
    }

    #[test]
    fn hash_row_matches_extracted_scalar_key() {
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
        ];
        for row in 0..3 {
            let key = extract_key(&cols, row).unwrap();
            assert_eq!(hash_row(&cols, row).unwrap(), hash_scalars(&key));
        }
        assert_eq!(hash_row(&[], 0).unwrap(), hash_scalars(&[]));
    }

    #[test]
    fn hash_row_matches_dictionary_utf8_key() {
        let keys = Int32Array::from(vec![Some(0), Some(1), None, Some(0)]);
        let values: ArrayRef = Arc::new(StringArray::from(vec!["red", "blue"]));
        let dict = DictionaryArray::try_new(keys, values).unwrap();
        let cols: Vec<ArrayRef> = vec![Arc::new(dict)];
        for row in 0..4 {
            let key = extract_key(&cols, row).unwrap();
            assert_eq!(hash_row(&cols, row).unwrap(), hash_scalars(&key));
            assert!(row_matches(&key, &cols, row).unwrap());
        }
    }

    #[test]
    fn ungrouped_sum_uses_whole_array_and_skips_nulls() {
        let batch = i64_batch(vec![Some(1), None, Some(2), Some(3)]);
        assert_eq!(finish_sum(&batch), Scalar::I64(6));
        let empty = i64_batch(vec![]);
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Int64, true)]));
        let mut table =
            HashAgg::new(&[], &[("s".into(), AggCall::Sum(Expr::nth(0)))], schema).unwrap();
        table.update(&empty).unwrap();
        let out = table.finish(&BatchPolicy::unlimited()).unwrap();
        assert_eq!(out[0].num_rows(), 1);
        assert!(
            out[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .is_null(0)
        );
    }

    #[test]
    fn ungrouped_sum_of_one_million_int64() {
        const N: i64 = 1_000_000;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = Batch::try_new(
            schema,
            vec![Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(
                1i64, N as usize,
            )))],
        )
        .unwrap();
        assert_eq!(finish_sum(&batch), Scalar::I64(N));
    }

    #[test]
    fn grouped_utf8_lookup_does_not_split_equal_keys() {
        let n = 8_192usize;
        let flags: Vec<Option<&str>> = (0..n).map(|i| Some(["A", "N", "R"][i % 3])).collect();
        let qty: Vec<Option<i64>> = (0..n).map(|i| Some(i as i64)).collect();
        let batch = Batch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("flag", DataType::Utf8, true),
                Field::new("qty", DataType::Int64, true),
            ])),
            vec![
                Arc::new(StringArray::from(flags)),
                Arc::new(Int64Array::from(qty)),
            ],
        )
        .unwrap();
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("flag", DataType::Utf8, true),
            Field::new("s", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]));
        let mut table = HashAgg::new(
            &[NamedExpr {
                expr: Expr::nth(0),
                name: "flag".into(),
            }],
            &[
                ("s".into(), AggCall::Sum(Expr::nth(1))),
                ("c".into(), AggCall::CountStar),
            ],
            out_schema,
        )
        .unwrap();
        table.update(&batch).unwrap();
        let out = table.finish(&BatchPolicy::unlimited()).unwrap();
        assert_eq!(out[0].num_rows(), 3);
        let counts = out[0]
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut total = 0i64;
        for i in 0..counts.len() {
            total += counts.value(i);
        }
        assert_eq!(total, n as i64);
    }

    #[test]
    fn retained_bytes_counts_utf8_group_keys() {
        let key = "k".repeat(8_192);
        let batch = Batch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("g", DataType::Utf8, true),
                Field::new("v", DataType::Int64, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec![Some(key.as_str())])),
                Arc::new(Int64Array::from(vec![Some(1)])),
            ],
        )
        .unwrap();
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, true),
            Field::new("c", DataType::Int64, true),
        ]));
        let mut table = HashAgg::new(
            &[NamedExpr {
                expr: Expr::nth(0),
                name: "g".into(),
            }],
            &[("c".into(), AggCall::CountStar)],
            out_schema,
        )
        .unwrap();
        table.update(&batch).unwrap();
        assert!(
            table.retained_bytes() >= 8_192,
            "retained={}",
            table.retained_bytes()
        );
        assert!(piece_payload(&batch) >= 8_192, "{}", piece_payload(&batch));
    }

    #[test]
    fn overlapping_count_merge_does_not_charge_value_payload() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, true),
            Field::new("c", DataType::Int64, true),
        ]));
        let key = "k".repeat(8_192);
        let partial = |n: i64| {
            Batch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![Some(key.as_str())])),
                    Arc::new(Int64Array::from(vec![Some(n)])),
                ],
            )
            .unwrap()
        };
        let mut table = HashAgg::combining(
            &[NamedExpr {
                expr: Expr::nth(0),
                name: "g".into(),
            }],
            &[("c".into(), AggCall::Sum(Expr::nth(1)))],
            schema.clone(),
        )
        .unwrap();
        table.update(&partial(1)).unwrap();
        let after_first = table.retained_bytes();
        table.update(&partial(2)).unwrap();
        assert_eq!(table.retained_bytes(), after_first);
        assert!(after_first >= 8_192, "retained={after_first}");
    }

    #[test]
    fn grouped_dictionary_utf8_matches_plain_utf8() {
        let keys = Int32Array::from(vec![0, 1, 0, 1, 0]);
        let values: ArrayRef = Arc::new(StringArray::from(vec!["A", "N"]));
        let dict = DictionaryArray::try_new(keys, values).unwrap();
        let qty = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let batch = Batch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(
                    "flag",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    true,
                ),
                Field::new("qty", DataType::Int64, true),
            ])),
            vec![Arc::new(dict), Arc::new(qty)],
        )
        .unwrap();
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("flag", DataType::Utf8, true),
            Field::new("c", DataType::Int64, true),
        ]));
        let mut table = HashAgg::new(
            &[NamedExpr {
                expr: Expr::nth(0),
                name: "flag".into(),
            }],
            &[("c".into(), AggCall::CountStar)],
            out_schema,
        )
        .unwrap();
        table.update(&batch).unwrap();
        let out = table.finish(&BatchPolicy::unlimited()).unwrap();
        let mut rows: Vec<(String, i64)> = (0..out[0].num_rows())
            .map(|i| {
                let k = out[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(i)
                    .to_owned();
                let c = out[0]
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(i);
                (k, c)
            })
            .collect();
        rows.sort();
        assert_eq!(rows, vec![("A".into(), 3), ("N".into(), 2)]);
    }

    #[test]
    fn grouped_float_signed_zero_is_one_group() {
        let batch = Batch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, true)])),
            vec![Arc::new(Float64Array::from(vec![0.0f64, -0.0f64, 0.0]))],
        )
        .unwrap();
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("c", DataType::Int64, true),
        ]));
        let mut table = HashAgg::new(
            &[NamedExpr {
                expr: Expr::nth(0),
                name: "x".into(),
            }],
            &[("c".into(), AggCall::CountStar)],
            out_schema,
        )
        .unwrap();
        table.update(&batch).unwrap();
        let out = table.finish(&BatchPolicy::unlimited()).unwrap();
        assert_eq!(out[0].num_rows(), 1);
        assert_eq!(
            out[0]
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            3
        );
    }

    #[test]
    fn grouped_high_cardinality_keeps_one_row_per_key() {
        let n = 64usize;
        let keys: Vec<Option<i64>> = (0..n).map(|i| Some(i as i64)).collect();
        let qty: Vec<Option<i64>> = (0..n).map(|i| Some((i * 2) as i64)).collect();
        let batch = Batch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("qty", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Int64Array::from(qty)),
            ],
        )
        .unwrap();
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Int64, true),
        ]));
        let mut table = HashAgg::new(
            &[NamedExpr {
                expr: Expr::nth(0),
                name: "k".into(),
            }],
            &[("s".into(), AggCall::Sum(Expr::nth(1)))],
            out_schema,
        )
        .unwrap();
        table.update(&batch).unwrap();
        let out = table.finish(&BatchPolicy::unlimited()).unwrap();
        assert_eq!(out[0].num_rows(), n);
        let sums = out[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut total = 0i64;
        for i in 0..sums.len() {
            total += sums.value(i);
        }
        assert_eq!(total, (0..n as i64).map(|i| i * 2).sum::<i64>());
    }

    fn grouped_counts(session: &mut crate::Session, sql: &str) -> Vec<(i64, i64)> {
        let out = session.execute_sql(sql).unwrap();
        let mut rows = Vec::new();
        for b in &out {
            let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let c = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                rows.push((k.value(i), c.value(i)));
            }
        }
        rows.sort_unstable();
        rows
    }

    #[test]
    fn perfect_hash_small_int_range_skips_scalar_insert() {
        let before = agg_path_counters::PERFECT_ROWS.load(std::sync::atomic::Ordering::Relaxed);
        let n = 50_000i64;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..n)),
                Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(
                    1, n as usize,
                ))),
            ],
        )
        .unwrap();
        let mut session = crate::Session::new().with_workers(2).unwrap();
        session.register("t", schema, vec![batch]).unwrap();
        let rows = grouped_counts(&mut session, "SELECT k, COUNT(*) AS c FROM t GROUP BY k");
        assert_eq!(rows.len(), n as usize);
        assert_eq!(rows[0], (0, 1));
        assert_eq!(rows[n as usize - 1], (n - 1, 1));
        let perfect = agg_path_counters::PERFECT_ROWS.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            perfect.saturating_sub(before) >= n as usize,
            "perfect rows delta {perfect} - {before}"
        );
    }

    #[test]
    fn perfect_hash_two_utf8_bytes_skips_scalar_insert() {
        let before = agg_path_counters::PERFECT_ROWS.load(std::sync::atomic::Ordering::Relaxed);
        let flags = ["A", "N", "R"];
        let status = ["O", "F"];
        let n = 6_000usize;
        let f: Vec<Option<&str>> = (0..n).map(|i| Some(flags[i % 3])).collect();
        let s: Vec<Option<&str>> = (0..n).map(|i| Some(status[i % 2])).collect();
        let qty: Vec<i64> = (0..n as i64).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("flag", DataType::Utf8, true),
            Field::new("st", DataType::Utf8, true),
            Field::new("qty", DataType::Int64, true),
        ]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(f)),
                Arc::new(StringArray::from(s)),
                Arc::new(Int64Array::from(qty)),
            ],
        )
        .unwrap();
        let mut session = crate::Session::new().with_workers(2).unwrap();
        session.register("t", schema, vec![batch]).unwrap();
        let out = session
            .execute_sql("SELECT flag, st, COUNT(*) AS c, SUM(qty) AS s FROM t GROUP BY flag, st")
            .unwrap();
        let mut total = 0i64;
        for b in &out {
            let c = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..c.len() {
                total += c.value(i);
            }
        }
        assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 6);
        assert_eq!(total, n as i64);
        let perfect = agg_path_counters::PERFECT_ROWS.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            perfect.saturating_sub(before) >= n,
            "perfect rows delta {perfect} - {before}"
        );
    }

    #[test]
    fn grouped_range_too_large_falls_back_to_hash() {
        let before = agg_path_counters::HASH_INSERTS.load(std::sync::atomic::Ordering::Relaxed);
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![
                0i64,
                i64::MAX,
                1,
                i64::MAX,
            ]))],
        )
        .unwrap();
        let mut session = crate::Session::new().with_workers(2).unwrap();
        session.register("t", schema, vec![batch]).unwrap();
        let rows = grouped_counts(&mut session, "SELECT k, COUNT(*) AS c FROM t GROUP BY k");
        assert_eq!(rows, vec![(0, 1), (1, 1), (i64::MAX, 2)]);
        let inserts = agg_path_counters::HASH_INSERTS.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            inserts.saturating_sub(before) >= 3,
            "hash inserts delta {inserts} - {before}"
        );
    }

    #[test]
    fn radix_partition_high_cardinality_merges_in_parallel() {
        let merges_before =
            agg_path_counters::PARALLEL_MERGES.load(std::sync::atomic::Ordering::Relaxed);
        let n = 16_384i64;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let keys: Vec<i64> = (0..n).map(|i| i.wrapping_mul(1i64 << 40)).collect();
        let batch = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(
                    2, n as usize,
                ))),
            ],
        )
        .unwrap();
        let mut session = crate::Session::new().with_workers(4).unwrap();
        session.register("t", schema, vec![batch]).unwrap();
        let rows = grouped_counts(&mut session, "SELECT k, SUM(v) AS c FROM t GROUP BY k");
        assert_eq!(rows.len(), n as usize);
        assert!(rows.iter().all(|(_, c)| *c == 2));
        let parts = agg_path_counters::RADIX_PARTS.with(|c| c.get());
        let occupied = agg_path_counters::RADIX_OCCUPIED.with(|c| c.get());
        assert!(parts >= 4, "radix parts {parts}");
        assert!(occupied >= 4, "occupied radix partitions {occupied}");
        let merges = agg_path_counters::PARALLEL_MERGES.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            merges.saturating_sub(merges_before) >= 4,
            "parallel merges delta {merges} - {merges_before}"
        );
    }

    #[test]
    fn radix_slot_spreads_shifted_int_keys() {
        let mask = 7;
        let mut seen = [false; 8];
        for i in 0..256i64 {
            let h = hash_scalars(&[Scalar::I64(i.wrapping_mul(1i64 << 40))]);
            seen[radix_slot(h, mask)] = true;
        }
        let occupied = seen.iter().filter(|s| **s).count();
        assert!(occupied >= 4, "occupied {occupied} of 8");
    }

    #[test]
    fn perfect_and_hash_paths_agree_on_null_group_key() {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let small = Batch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![
                Some(1),
                None,
                Some(1),
                None,
                Some(2),
            ]))],
        )
        .unwrap();
        let mut session = crate::Session::new().with_workers(2).unwrap();
        session.register("t", schema.clone(), vec![small]).unwrap();
        let out = session
            .execute_sql("SELECT k, COUNT(*) AS c FROM t GROUP BY k")
            .unwrap();
        let mut got = Vec::new();
        for b in &out {
            let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let c = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                got.push((
                    k.is_null(i),
                    if k.is_null(i) { 0 } else { k.value(i) },
                    c.value(i),
                ));
            }
        }
        got.sort_unstable();
        assert_eq!(got, vec![(false, 1, 2), (false, 2, 1), (true, 0, 2)]);
    }

    #[test]
    fn streaming_uint_below_first_min_falls_back_without_panic() {
        let in_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::UInt64, true)]));
        let first = Batch::try_new(
            in_schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![10u64, 11]))],
        )
        .unwrap();
        let second =
            Batch::try_new(in_schema, vec![Arc::new(UInt64Array::from(vec![5u64, 10]))]).unwrap();
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::UInt64, true),
            Field::new("c", DataType::Int64, true),
        ]));
        let pool = MemoryPool::new(None, None).unwrap();
        let out = run(
            Box::new(crate::Exec::Scan {
                batches: vec![first, second],
                index: 0,
            }),
            &[NamedExpr {
                expr: Expr::nth(0),
                name: "k".into(),
            }],
            &[("c".into(), AggCall::CountStar)],
            out_schema,
            &BatchPolicy::unlimited(),
            &pool,
            &crate::CancelToken::new(),
        )
        .unwrap();
        let mut rows = Vec::new();
        for b in &out {
            let k = b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
            let c = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                rows.push((k.value(i), c.value(i)));
            }
        }
        rows.sort_unstable();
        assert_eq!(rows, vec![(5, 1), (10, 2), (11, 1)]);
    }

    fn sparse_wide_int_scan() -> (crate::Exec, SchemaRef) {
        let in_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let batch = Batch::try_new(
            in_schema,
            vec![Arc::new(Int64Array::from(vec![0i64, 1_048_574]))],
        )
        .unwrap();
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]));
        (
            crate::Exec::Scan {
                batches: vec![batch],
                index: 0,
            },
            out_schema,
        )
    }

    fn run_sparse_wide_int(pool: &MemoryPool) -> Vec<(i64, i64)> {
        let (scan, out_schema) = sparse_wide_int_scan();
        let out = run(
            Box::new(scan),
            &[NamedExpr {
                expr: Expr::nth(0),
                name: "k".into(),
            }],
            &[("c".into(), AggCall::CountStar)],
            out_schema,
            &BatchPolicy::unlimited(),
            pool,
            &crate::CancelToken::new(),
        )
        .unwrap();
        let mut rows = Vec::new();
        for b in &out {
            let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let c = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                rows.push((k.value(i), c.value(i)));
            }
        }
        rows.sort_unstable();
        rows
    }

    #[test]
    fn sparse_perfect_domain_falls_back_when_index_exceeds_budget() {
        let pool = MemoryPool::new(Some(8 * 1024), None).unwrap();
        let rows = run_sparse_wide_int(&pool);
        assert_eq!(rows, vec![(0, 1), (1_048_574, 1)]);
        assert!(
            pool.peak() < perfect_index_bytes(PERFECT_MAX_SLOTS),
            "peak {} reserved the dense index",
            pool.peak()
        );
    }

    #[test]
    fn memory_limited_sparse_domain_keeps_budget_for_later_batches() {
        let in_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let first = Batch::try_new(
            in_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![0i64, 1_048_574]))],
        )
        .unwrap();
        let n = 300_000i64;
        let keys: Vec<i64> = (0..n)
            .map(|i| if i % 2 == 0 { 0 } else { 1_048_574 })
            .collect();
        let second = Batch::try_new(in_schema, vec![Arc::new(Int64Array::from(keys))]).unwrap();
        let limit = 5 * 1024 * 1024;
        assert!(
            batch_bytes(&second) + perfect_index_bytes(PERFECT_MAX_SLOTS) > limit,
            "second batch {} plus index must exceed the limit",
            batch_bytes(&second)
        );
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]));
        let pool = MemoryPool::new(Some(limit), None).unwrap();
        let out = run(
            Box::new(crate::Exec::Scan {
                batches: vec![first, second],
                index: 0,
            }),
            &[NamedExpr {
                expr: Expr::nth(0),
                name: "k".into(),
            }],
            &[("c".into(), AggCall::CountStar)],
            out_schema,
            &BatchPolicy::unlimited(),
            &pool,
            &crate::CancelToken::new(),
        )
        .unwrap();
        let mut rows = Vec::new();
        for b in &out {
            let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let c = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                rows.push((k.value(i), c.value(i)));
            }
        }
        rows.sort_unstable();
        assert_eq!(rows, vec![(0, 150_001), (1_048_574, 150_001)]);
        assert!(
            pool.peak() < perfect_index_bytes(PERFECT_MAX_SLOTS),
            "peak {} reserved the dense index",
            pool.peak()
        );
    }
}
