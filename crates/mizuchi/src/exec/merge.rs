//! MERGE INTO: match source rows to a target table and apply WHEN clauses.

use std::collections::HashMap;

use arrow::array::{Array, ArrayRef, BooleanArray, UInt32Array, new_null_array};
use arrow::compute::kernels::boolean::{and, is_not_null, not};
use arrow::compute::{cast, concat_batches, filter_record_batch, take};
use arrow::datatypes::SchemaRef;

use super::key::{Scalar, extract_key};
use super::memory::{self, MemoryPool};
use super::{Session, collect, filter_batch, physical};
use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::expr::Expr;
use crate::plan::{
    LogicalPlan, MergeActionPlan, MergeClausePlan, MergeMatch, MergePlan, join_schema,
};

/// Counts of rows changed by a MERGE.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MergeStats {
    /// Target table that was rewritten.
    pub table: String,
    /// Rows inserted from unmatched source rows.
    pub inserted: u64,
    /// Rows updated from matched pairs.
    pub updated: u64,
    /// Rows deleted from the target.
    pub deleted: u64,
}

pub(super) fn run(session: &Session, plan: &MergePlan) -> Result<(Vec<Batch>, MergeStats)> {
    let pool = session.memory_pool()?;
    let (target, _t_hold) = concat_or_empty(
        exec_plan(
            session,
            &scan_table(plan.target.clone(), plan.target_schema.clone()),
            &pool,
        )?,
        plan.target_schema.clone(),
        &pool,
    )?;
    let (source, _s_hold) = concat_or_empty(
        exec_plan(session, &plan.source, &pool)?,
        plan.source_schema.clone(),
        &pool,
    )?;
    let work = memory::batch_bytes(&target).saturating_add(memory::batch_bytes(&source));
    let _work_hold = pool.try_reserve(work)?;
    let matched_schema = join_schema(&plan.target_schema, &plan.source_schema);
    let rows = match_rows(&target, &source, &plan.on_ts)?;

    let mut stats = MergeStats {
        table: plan.target.clone(),
        ..MergeStats::default()
    };
    let mut kept: Vec<Batch> = Vec::new();

    if let Some(matched) = take_join(&target, &source, &rows.t_idx, &rows.s_idx, matched_schema)? {
        let (keep, u, d) = apply_kind(
            &matched,
            MergeMatch::Matched,
            plan,
            plan.target_schema.clone(),
        )?;
        stats.updated += u;
        stats.deleted += d;
        if let Some(b) = keep {
            kept.push(b);
        }
    }

    if let Some(target_only) = take_only(&target, &rows.t_un)? {
        let (keep, u, d) = apply_kind(
            &target_only,
            MergeMatch::NotMatchedBySource,
            plan,
            plan.target_schema.clone(),
        )?;
        stats.updated += u;
        stats.deleted += d;
        if let Some(b) = keep {
            kept.push(b);
        }
    }

    if let Some(source_only) = take_only(&source, &rows.s_un)? {
        let (keep, ins, d) = apply_kind(
            &source_only,
            MergeMatch::NotMatched,
            plan,
            plan.target_schema.clone(),
        )?;
        stats.inserted += ins;
        stats.deleted += d;
        if let Some(b) = keep {
            kept.push(b);
        }
    }

    if kept.is_empty() {
        kept.push(Batch::empty(plan.target_schema.clone())?);
    }
    Ok((kept, stats))
}

fn scan_table(table: String, schema: SchemaRef) -> LogicalPlan {
    LogicalPlan::Scan {
        table,
        schema,
        alias: None,
        projection: None,
        filters: Vec::new(),
        stats: None,
    }
}

fn exec_plan(session: &Session, plan: &LogicalPlan, pool: &MemoryPool) -> Result<Vec<Batch>> {
    session.exec_token().check()?;
    collect(physical(plan, session, pool)?, &session.exec_token())
}

fn concat_or_empty<'a>(
    batches: Vec<Batch>,
    schema: SchemaRef,
    pool: &'a MemoryPool,
) -> Result<(Batch, memory::Reservation<'a>)> {
    let bytes = batches
        .iter()
        .map(memory::batch_bytes)
        .fold(0u64, u64::saturating_add);
    let hold = pool.try_reserve(bytes)?;
    let nonempty: Vec<Batch> = batches.into_iter().filter(|b| b.num_rows() > 0).collect();
    if nonempty.is_empty() {
        return Ok((Batch::empty(schema)?, hold));
    }
    if nonempty.len() == 1 {
        return Ok((nonempty.into_iter().next().expect("len 1"), hold));
    }
    let arrows: Vec<_> = nonempty.iter().map(|b| b.as_arrow().clone()).collect();
    let inner = concat_batches(&schema, &arrows).map_err(|e| Error::Eval(e.to_string()))?;
    Ok((Batch::from_arrow(inner)?, hold))
}

struct RowMatch {
    t_idx: Vec<u32>,
    s_idx: Vec<u32>,
    t_un: Vec<u32>,
    s_un: Vec<u32>,
}

fn match_rows(target: &Batch, source: &Batch, on: &Expr) -> Result<RowMatch> {
    let n_t = target.num_columns();
    let pairs = super::join::equi_key_pairs(on, n_t)?;
    let mut t_hit = vec![false; target.num_rows()];
    let mut s_hit = vec![false; source.num_rows()];
    let mut t_idx = Vec::new();
    let mut s_idx = Vec::new();

    if pairs.is_empty() {
        for trow in 0..target.num_rows() {
            let mut hits = Vec::new();
            for srow in 0..source.num_rows() {
                if pair_matches(target, source, trow, srow, on)? {
                    hits.push(srow as u32);
                }
            }
            push_match(
                trow as u32,
                &hits,
                &mut t_hit,
                &mut s_hit,
                &mut t_idx,
                &mut s_idx,
            )?;
        }
    } else {
        let skeys: Vec<ArrayRef> = pairs
            .iter()
            .map(|(_, r)| r.eval(source))
            .collect::<Result<_>>()?;
        let tkeys: Vec<ArrayRef> = pairs
            .iter()
            .map(|(l, _)| l.eval(target))
            .collect::<Result<_>>()?;
        let mut table: HashMap<Vec<Scalar>, Vec<u32>> = HashMap::new();
        for srow in 0..source.num_rows() {
            let key = extract_key(&skeys, srow)?;
            if key.iter().any(|s| matches!(s, Scalar::Null)) {
                continue;
            }
            table.entry(key).or_default().push(srow as u32);
        }
        for trow in 0..target.num_rows() {
            let key = extract_key(&tkeys, trow)?;
            let mut hits = Vec::new();
            if !key.iter().any(|s| matches!(s, Scalar::Null))
                && let Some(cands) = table.get(&key)
            {
                for &srow in cands {
                    if pair_matches(target, source, trow, srow as usize, on)? {
                        hits.push(srow);
                    }
                }
            }
            push_match(
                trow as u32,
                &hits,
                &mut t_hit,
                &mut s_hit,
                &mut t_idx,
                &mut s_idx,
            )?;
        }
    }

    let t_un: Vec<u32> = (0..target.num_rows() as u32)
        .filter(|i| !t_hit[*i as usize])
        .collect();
    let s_un: Vec<u32> = (0..source.num_rows() as u32)
        .filter(|i| !s_hit[*i as usize])
        .collect();
    Ok(RowMatch {
        t_idx,
        s_idx,
        t_un,
        s_un,
    })
}

fn push_match(
    trow: u32,
    hits: &[u32],
    t_hit: &mut [bool],
    s_hit: &mut [bool],
    t_idx: &mut Vec<u32>,
    s_idx: &mut Vec<u32>,
) -> Result<()> {
    if hits.len() > 1 {
        return Err(Error::Plan(
            "MERGE matched more than one source row for a target row".into(),
        ));
    }
    if let Some(&srow) = hits.first() {
        t_hit[trow as usize] = true;
        s_hit[srow as usize] = true;
        t_idx.push(trow);
        s_idx.push(srow);
    }
    Ok(())
}

fn pair_matches(
    target: &Batch,
    source: &Batch,
    trow: usize,
    srow: usize,
    on: &Expr,
) -> Result<bool> {
    let schema = join_schema(&target.schema(), &source.schema());
    let joined = match take_join(target, source, &[trow as u32], &[srow as u32], schema)? {
        Some(b) => b,
        None => return Ok(false),
    };
    let mask = on.eval(&joined)?;
    let bools = mask
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| Error::Eval("MERGE ON is not boolean".into()))?;
    Ok(!bools.is_null(0) && bools.value(0))
}

fn take_join(
    left: &Batch,
    right: &Batch,
    li: &[u32],
    ri: &[u32],
    schema: SchemaRef,
) -> Result<Option<Batch>> {
    if li.is_empty() {
        return Ok(None);
    }
    let lidx = UInt32Array::from(li.to_vec());
    let ridx = UInt32Array::from(ri.to_vec());
    let mut cols = Vec::with_capacity(left.num_columns() + right.num_columns());
    for c in left.as_arrow().columns() {
        cols.push(take(c.as_ref(), &lidx, None).map_err(|e| Error::Eval(e.to_string()))?);
    }
    for c in right.as_arrow().columns() {
        cols.push(take(c.as_ref(), &ridx, None).map_err(|e| Error::Eval(e.to_string()))?);
    }
    Ok(Some(Batch::try_new(schema, cols)?))
}

fn take_only(batch: &Batch, rows: &[u32]) -> Result<Option<Batch>> {
    if rows.is_empty() {
        return Ok(None);
    }
    let idx = UInt32Array::from(rows.to_vec());
    let cols: Result<Vec<_>> = batch
        .as_arrow()
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &idx, None).map_err(|e| Error::Eval(e.to_string())))
        .collect();
    Ok(Some(Batch::try_new(batch.schema(), cols?)?))
}

fn apply_kind(
    batch: &Batch,
    kind: MergeMatch,
    plan: &MergePlan,
    target_schema: SchemaRef,
) -> Result<(Option<Batch>, u64, u64)> {
    if batch.num_rows() == 0 {
        return Ok((None, 0, 0));
    }
    let clauses: Vec<&MergeClausePlan> = plan.clauses.iter().filter(|c| c.kind == kind).collect();
    let mut remaining = Some(batch.clone());
    let mut outs: Vec<Batch> = Vec::new();
    let mut updated = 0u64;
    let mut inserted = 0u64;
    let mut deleted = 0u64;
    for clause in clauses {
        let Some(cur) = remaining.take() else {
            break;
        };
        let (hit, rest) = partition(cur, clause.predicate.as_ref())?;
        remaining = rest;
        if let Some(hit) = hit {
            match &clause.action {
                MergeActionPlan::Delete => {
                    deleted += hit.num_rows() as u64;
                }
                MergeActionPlan::Update { assignments } => {
                    updated += hit.num_rows() as u64;
                    outs.push(apply_update(&hit, assignments, target_schema.clone())?);
                }
                MergeActionPlan::Insert { values } => {
                    inserted += hit.num_rows() as u64;
                    outs.push(apply_insert(&hit, values, target_schema.clone())?);
                }
            }
        }
    }
    if let Some(rest) = remaining {
        match kind {
            MergeMatch::NotMatched => {
                // Unmatched source rows with no INSERT clause are dropped.
            }
            MergeMatch::Matched | MergeMatch::NotMatchedBySource => {
                outs.push(project_target(&rest, target_schema.clone())?);
            }
        }
    }
    let changed = if kind == MergeMatch::NotMatched {
        inserted
    } else {
        updated
    };
    Ok((concat_kept(&outs, target_schema)?, changed, deleted))
}

fn partition(batch: Batch, predicate: Option<&Expr>) -> Result<(Option<Batch>, Option<Batch>)> {
    match predicate {
        None => Ok((Some(batch), None)),
        Some(pred) => {
            let hit = filter_batch(&batch, pred)?;
            // Rows where pred is null stay in rest (null is not true).
            let rest = filter_null_or_false(&batch, pred)?;
            Ok((hit, rest))
        }
    }
}

fn filter_null_or_false(batch: &Batch, predicate: &Expr) -> Result<Option<Batch>> {
    let mask = predicate.eval(batch)?;
    let bools = mask
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| Error::Eval("MERGE predicate is not boolean".into()))?;
    let is_true = and(
        bools,
        &is_not_null(bools).map_err(|e| Error::Eval(e.to_string()))?,
    )
    .map_err(|e| Error::Eval(e.to_string()))?;
    let keep = not(&is_true).map_err(|e| Error::Eval(e.to_string()))?;
    let filtered =
        filter_record_batch(batch.as_arrow(), &keep).map_err(|e| Error::Eval(e.to_string()))?;
    if filtered.num_rows() == 0 {
        Ok(None)
    } else {
        Ok(Some(Batch::from_arrow(filtered)?))
    }
}

fn project_target(batch: &Batch, schema: SchemaRef) -> Result<Batch> {
    let n = schema.fields().len();
    if batch.num_columns() < n {
        return Err(Error::Eval("MERGE batch is missing target columns".into()));
    }
    let cols: Vec<ArrayRef> = (0..n)
        .map(|i| cast_to(batch.column(i).clone(), schema.field(i).data_type()))
        .collect::<Result<_>>()?;
    Batch::try_new(schema, cols)
}

fn apply_update(batch: &Batch, assignments: &[(usize, Expr)], schema: SchemaRef) -> Result<Batch> {
    let n = schema.fields().len();
    let mut cols: Vec<ArrayRef> = (0..n).map(|i| batch.column(i).clone()).collect();
    for (idx, expr) in assignments {
        if *idx >= n {
            return Err(Error::Plan(format!(
                "UPDATE column index {idx} out of range"
            )));
        }
        cols[*idx] = cast_to(expr.eval(batch)?, schema.field(*idx).data_type())?;
    }
    Batch::try_new(schema, cols)
}

fn apply_insert(batch: &Batch, values: &[Option<Expr>], schema: SchemaRef) -> Result<Batch> {
    let n = schema.fields().len();
    if values.len() != n {
        return Err(Error::Plan(
            "INSERT values do not match the target schema".into(),
        ));
    }
    let mut cols = Vec::with_capacity(n);
    for (i, expr) in values.iter().enumerate() {
        let dt = schema.field(i).data_type();
        let arr = match expr {
            Some(e) => cast_to(e.eval(batch)?, dt)?,
            None => new_null_array(dt, batch.num_rows()),
        };
        cols.push(arr);
    }
    Batch::try_new(schema, cols)
}

fn cast_to(arr: ArrayRef, dt: &arrow::datatypes::DataType) -> Result<ArrayRef> {
    if arr.data_type() == dt {
        Ok(arr)
    } else {
        cast(arr.as_ref(), dt).map_err(|e| Error::Eval(e.to_string()))
    }
}

fn concat_kept(batches: &[Batch], schema: SchemaRef) -> Result<Option<Batch>> {
    let nonempty: Vec<_> = batches.iter().filter(|b| b.num_rows() > 0).collect();
    if nonempty.is_empty() {
        return Ok(None);
    }
    if nonempty.len() == 1 {
        return Ok(Some(nonempty[0].clone()));
    }
    let arrows: Vec<_> = nonempty.iter().map(|b| b.as_arrow().clone()).collect();
    let inner = concat_batches(&schema, &arrows).map_err(|e| Error::Eval(e.to_string()))?;
    Ok(Some(Batch::from_arrow(inner)?))
}
