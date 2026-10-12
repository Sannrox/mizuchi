//! Hash aggregate over Arrow batches.
//!
//! Grouping expressions and aggregate arguments are evaluated column-at-a-time.
//! Groups live in a hash table keyed by those grouping values. COUNT(expr),
//! SUM, AVG, MIN, and MAX skip nulls; COUNT(*) counts every input row.
//! COUNT(DISTINCT expr) counts unique non-null values per group. Ungrouped
//! aggregation of empty input still emits one row (COUNT is 0, the rest null).
//! Grouped aggregation of empty input emits no rows. Null grouping keys form
//! one group.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

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
    IdentityMap, Scalar, eval_all, extract_key, hash_row, hash_scalars, identity_map_with_capacity,
    row_matches, scalar_at,
};
use super::memory::{self, MemoryPool, batch_bytes};
use mizuchi_core::batch::{Batch, BatchPolicy};
use mizuchi_core::error::{Error, Result};
use mizuchi_core::expr::Expr;
use mizuchi_core::udaf::CombineAgg;
use mizuchi_plan::{AggCall, NamedExpr};

pub(super) fn run(
    mut input: Box<dyn Operator>,
    group: &[NamedExpr],
    aggs: &[(String, AggCall)],
    schema: SchemaRef,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let mut table = HashAgg::new(group, aggs, schema.clone())?;
    let mut held: Vec<super::memory::Reservation<'_>> = Vec::new();
    let mut partials: Vec<std::path::PathBuf> = Vec::new();
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
                    let old =
                        std::mem::replace(&mut table, HashAgg::new(group, aggs, schema.clone())?);
                    spill_hash(old, schema.clone(), policy, pool, &mut partials)?;
                    held.clear();
                    let r = pool.try_reserve(sz).map_err(|_| {
                        Error::Memory("a single aggregate batch exceeds the memory limit".into())
                    })?;
                    held.push(r);
                }
            }
        }
        table.update(&batch)?;
    }
    if partials.is_empty() {
        return table.finish(policy);
    }
    spill_hash(table, schema.clone(), policy, pool, &mut partials)?;
    merge_partial_files(&partials, group, aggs, schema, policy)
}

fn spill_hash(
    table: HashAgg,
    schema: SchemaRef,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    partials: &mut Vec<std::path::PathBuf>,
) -> Result<()> {
    let finished = table.finish(policy)?;
    if finished.is_empty() {
        return Ok(());
    }
    let path = pool.write_spill_file(schema, &finished)?;
    partials.push(path);
    Ok(())
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
) -> Result<Vec<Batch>> {
    if partials.is_empty() {
        return HashAgg::new(group, aggs, schema)?.finish(policy);
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
    let mut table = HashAgg::combining(&merge_group, &merge_aggs, schema)?;
    for batch in partials {
        table.update(&batch)?;
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
    paths: &[std::path::PathBuf],
    group: &[NamedExpr],
    aggs: &[(String, AggCall)],
    schema: SchemaRef,
    policy: &BatchPolicy,
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
    let mut table = HashAgg::combining(&merge_group, &merge_aggs, schema.clone())?;
    for path in paths {
        for batch in memory::read_spill(path, schema.clone())? {
            table.update(&batch)?;
        }
    }
    table.finish(policy)
}

#[derive(Clone)]
pub(super) struct HashAgg {
    group_exprs: Vec<Expr>,
    aggs: Vec<AggCall>,
    schema: SchemaRef,
    grouped: bool,
    combining: bool,
    /// Precomputed key hash → candidate slots. The map uses the hash as the
    /// bucket key. New groups allocate `Scalar` keys once; lookups hash array
    /// values in place (no per-row `String`).
    slots: IdentityMap<Vec<usize>>,
    keys: Vec<Vec<Scalar>>,
    accums: Vec<Vec<Accum>>,
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
        if n_groups > 64 || n < 32 {
            for (row, &slot) in row_slot.iter().enumerate() {
                for (accum, arg) in self.accums[slot].iter_mut().zip(arg_cols.iter()) {
                    accum.update(arg.as_deref(), row)?;
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
            for (accum, arg) in self.accums[slot].iter_mut().zip(arg_cols.iter()) {
                accum.update_rows(arg.as_deref(), rows)?;
            }
        }
        Ok(())
    }

    fn update_ungrouped(&mut self, arg_cols: &[Option<ArrayRef>], n: usize) -> Result<()> {
        if self.keys.is_empty() {
            self.insert_group(Vec::new())?;
        }
        for (accum, arg) in self.accums[0].iter_mut().zip(arg_cols.iter()) {
            accum.update_array(arg.as_deref(), n)?;
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
            for (dst, src) in self.accums[slot].iter_mut().zip(accums) {
                dst.merge(src)?;
            }
        }
        Ok(())
    }

    fn insert_group_hashed(&mut self, h: u64, key: Vec<Scalar>) -> Result<usize> {
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
        self.slots.entry(h).or_default().push(slot);
        self.keys.push(key);
        self.accums.push(accums);
        Ok(slot)
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
    CountDistinct(HashSet<Scalar>),
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
    fn from_call(call: &AggCall, result_type: &DataType, combining: bool) -> Result<Self> {
        Ok(match call {
            AggCall::CountStar | AggCall::Count(_) => Self::Count(0),
            AggCall::CountDistinct(_) => Self::CountDistinct(HashSet::new()),
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
            Self::CountDistinct(seen) => {
                let array =
                    arg.ok_or_else(|| Error::Eval("COUNT DISTINCT needs an argument".into()))?;
                let value = scalar_at(array, row)?;
                if value != Scalar::Null {
                    seen.insert(value);
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
        if matches!(self, Self::CountDistinct(_) | Self::User { .. }) {
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
            Self::CountDistinct(_) | Self::User { .. } => unreachable!("handled above"),
        }
    }

    fn finish(&self) -> Scalar {
        match self {
            Self::Count(n) => Scalar::I64(*n),
            Self::CountDistinct(seen) => Scalar::I64(seen.len() as i64),
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
            (Self::CountDistinct(a), Self::CountDistinct(b)) => {
                a.extend(b);
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
            cur.map(|s| Scalar::Utf8(s.to_owned()))
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
        (Scalar::Utf8(x), Scalar::Utf8(y)) => x.cmp(y),
        _ => {
            return Err(Error::Eval(
                "MIN/MAX compared values of different types".into(),
            ));
        }
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
            Scalar::Utf8(s) => Ok(Some(s.as_str())),
            _ => Err(Error::Eval("expected utf8 aggregate value".into())),
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
}
