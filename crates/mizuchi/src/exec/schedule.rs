//! Morsel work queue across a bounded thread pool.
//!
//! Scan/filter/project morsels, combinable aggregates (including AVG via
//! per-thread sum/count merge), inner hash-join probe, and the Sort/Limit
//! wrappers around those plans run on `Session::with_workers` threads.
//! Operators check cancellation between morsels. Plans that still need a
//! single-threaded pipeline fall back to pull execution.

use std::sync::Mutex;
use std::thread;

use arrow::datatypes::SchemaRef;

use super::join::{self, JoinSpec};
use super::memory::MemoryPool;
use super::{Exec, Session, collect, filter_batch, physical, project_batch};
use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::expr::Expr;
use crate::plan::{AggCall, LogicalPlan, NamedExpr};

pub(super) fn try_parallel(
    plan: &LogicalPlan,
    session: &Session,
    pool: &MemoryPool,
) -> Result<Option<Vec<Batch>>> {
    session.cancel.check()?;
    match plan {
        LogicalPlan::Sort { input, order } => {
            let Some(child) = try_parallel(input, session, pool)? else {
                return Ok(None);
            };
            Ok(Some(super::sort::ordered(
                Box::new(Exec::Scan {
                    batches: child,
                    index: 0,
                }),
                input.schema().clone(),
                order,
                0,
                None,
                &session.policy,
                pool,
                &session.cancel,
            )?))
        }
        LogicalPlan::Limit { input, skip, fetch } => match input.as_ref() {
            LogicalPlan::Sort {
                input: sorted,
                order,
            } => {
                let Some(child) = try_parallel(sorted, session, pool)? else {
                    return Ok(None);
                };
                Ok(Some(super::sort::ordered(
                    Box::new(Exec::Scan {
                        batches: child,
                        index: 0,
                    }),
                    sorted.schema().clone(),
                    order,
                    *skip,
                    *fetch,
                    &session.policy,
                    pool,
                    &session.cancel,
                )?))
            }
            _ => {
                let Some(child) = try_parallel(input, session, pool)? else {
                    return Ok(None);
                };
                Ok(Some(collect(
                    Box::new(Exec::Limit {
                        input: Box::new(Exec::Scan {
                            batches: child,
                            index: 0,
                        }),
                        skip: *skip,
                        fetch: *fetch,
                    }),
                    &session.cancel,
                )?))
            }
        },
        LogicalPlan::Aggregate {
            input,
            group,
            aggs,
            schema,
        } => {
            if !agg_parallel_ok(aggs, pool) {
                return Ok(None);
            }
            if let Some(pipe) = as_pipeline(input) {
                return Ok(Some(parallel_agg(
                    &pipe,
                    group,
                    aggs,
                    schema.clone(),
                    session,
                    pool,
                )?));
            }
            let child = plan_batches(input, session, pool)?;
            Ok(Some(agg_from_batches(
                child,
                group,
                aggs,
                schema.clone(),
                session,
                pool,
            )?))
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            on,
            schema,
        } => {
            let l = plan_batches(left, session, pool)?;
            let r = plan_batches(right, session, pool)?;
            Ok(Some(join::run(
                Box::new(Exec::Scan {
                    batches: l,
                    index: 0,
                }),
                Box::new(Exec::Scan {
                    batches: r,
                    index: 0,
                }),
                JoinSpec {
                    join_type: *join_type,
                    on: on.as_ref(),
                    left_schema: left.schema().clone(),
                    right_schema: right.schema().clone(),
                    schema: schema.clone(),
                },
                &session.policy,
                pool,
                &session.cancel,
                session.workers,
            )?))
        }
        other => {
            if let Some(pipe) = as_pipeline(other) {
                return Ok(Some(parallel_pipeline(&pipe, session)?));
            }
            Ok(None)
        }
    }
}

fn agg_parallel_ok(aggs: &[(String, AggCall)], pool: &MemoryPool) -> bool {
    if !super::agg::can_combine(aggs) {
        return false;
    }
    if pool.has_limit() && !super::agg::can_spill(aggs) {
        return false;
    }
    true
}

fn plan_batches(plan: &LogicalPlan, session: &Session, pool: &MemoryPool) -> Result<Vec<Batch>> {
    if let Some(out) = try_parallel(plan, session, pool)? {
        return Ok(out);
    }
    collect(physical(plan, session, pool)?, &session.cancel)
}

struct Pipeline {
    table: String,
    projection: Option<Vec<String>>,
    scan_filters: Vec<Expr>,
    extra_filters: Vec<Expr>,
    project: Option<(Vec<NamedExpr>, SchemaRef)>,
}

fn as_pipeline(plan: &LogicalPlan) -> Option<Pipeline> {
    match plan {
        LogicalPlan::Scan {
            table,
            projection,
            filters,
            ..
        } => Some(Pipeline {
            table: table.clone(),
            projection: projection.clone(),
            scan_filters: filters.clone(),
            extra_filters: Vec::new(),
            project: None,
        }),
        LogicalPlan::Filter { input, predicate } => {
            let mut pipe = as_pipeline(input)?;
            if pipe.project.is_some() {
                return None;
            }
            pipe.extra_filters.push(predicate.clone());
            Some(pipe)
        }
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            let mut pipe = as_pipeline(input)?;
            if pipe.project.is_some() {
                return None;
            }
            pipe.project = Some((exprs.clone(), schema.clone()));
            Some(pipe)
        }
        _ => None,
    }
}

fn parallel_pipeline(pipe: &Pipeline, session: &Session) -> Result<Vec<Batch>> {
    let morsels = session.table_morsels(&pipe.table)?;
    map_morsels(session, morsels, |batch| apply_pipe(pipe, batch))
}

fn parallel_agg(
    pipe: &Pipeline,
    group: &[NamedExpr],
    aggs: &[(String, AggCall)],
    schema: SchemaRef,
    session: &Session,
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    let morsels = parallel_pipeline(pipe, session)?;
    agg_from_batches(morsels, group, aggs, schema, session, pool)
}

fn agg_from_batches(
    morsels: Vec<Batch>,
    group: &[NamedExpr],
    aggs: &[(String, AggCall)],
    schema: SchemaRef,
    session: &Session,
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    if morsels.is_empty() {
        return super::agg::run(
            Box::new(Exec::Scan {
                batches: Vec::new(),
                index: 0,
            }),
            group,
            aggs,
            schema,
            &session.policy,
            pool,
            &session.cancel,
        );
    }
    let n = session.workers.max(1).min(morsels.len());
    let mut chunks = vec![Vec::new(); n];
    for (i, batch) in morsels.into_iter().enumerate() {
        chunks[i % n].push(batch);
    }
    if pool.has_limit() {
        let partials = Mutex::new(Vec::new());
        thread::scope(|scope| -> Result<()> {
            let mut handles = Vec::new();
            for chunk in chunks {
                let partials = &partials;
                let schema = schema.clone();
                handles.push(scope.spawn(move || -> Result<()> {
                    session.cancel.check()?;
                    let out = super::agg::run(
                        Box::new(Exec::Scan {
                            batches: chunk,
                            index: 0,
                        }),
                        group,
                        aggs,
                        schema,
                        &session.policy,
                        pool,
                        &session.cancel,
                    )?;
                    partials
                        .lock()
                        .map_err(|_| Error::Eval("agg combine lock poisoned".into()))?
                        .extend(out);
                    Ok(())
                }));
            }
            for h in handles {
                h.join()
                    .map_err(|_| Error::Eval("worker panicked".into()))??;
            }
            Ok(())
        })?;
        let partials = partials
            .into_inner()
            .map_err(|_| Error::Eval("agg combine lock poisoned".into()))?;
        return super::agg::combine_partials(partials, group, aggs, schema, &session.policy);
    }
    let tables = Mutex::new(Vec::new());
    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for chunk in chunks {
            let tables = &tables;
            let schema = schema.clone();
            handles.push(scope.spawn(move || -> Result<()> {
                session.cancel.check()?;
                let table = super::agg::fold_batches(chunk, group, aggs, schema)?;
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
    super::agg::merge_finish(tables, group, aggs, schema, &session.policy)
}

fn map_morsels(
    session: &Session,
    morsels: Vec<Batch>,
    f: impl Fn(Batch) -> Result<Option<Batch>> + Sync,
) -> Result<Vec<Batch>> {
    if morsels.is_empty() {
        return Ok(Vec::new());
    }
    let n = session.workers.max(1).min(morsels.len());
    if n == 1 {
        let mut out = Vec::new();
        for batch in morsels {
            session.cancel.check()?;
            if let Some(batch) = f(batch)? {
                out.push(batch);
            }
        }
        return Ok(out);
    }
    let queue = Mutex::new(
        morsels
            .into_iter()
            .enumerate()
            .collect::<Vec<(usize, Batch)>>(),
    );
    let collected = Mutex::new(Vec::new());
    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for _ in 0..n {
            let queue = &queue;
            let collected = &collected;
            let f = &f;
            handles.push(scope.spawn(move || -> Result<()> {
                loop {
                    session.cancel.check()?;
                    let item = {
                        let mut g = queue
                            .lock()
                            .map_err(|_| Error::Eval("morsel queue lock poisoned".into()))?;
                        g.pop()
                    };
                    let Some((i, batch)) = item else {
                        return Ok(());
                    };
                    if let Some(out) = f(batch)? {
                        collected
                            .lock()
                            .map_err(|_| Error::Eval("morsel output lock poisoned".into()))?
                            .push((i, out));
                    }
                }
            }));
        }
        for h in handles {
            h.join()
                .map_err(|_| Error::Eval("worker panicked".into()))??;
        }
        Ok(())
    })?;
    let mut parts = collected
        .into_inner()
        .map_err(|_| Error::Eval("morsel output lock poisoned".into()))?;
    parts.sort_by_key(|(i, _)| *i);
    Ok(parts
        .into_iter()
        .map(|(_, b)| b)
        .filter(|b| b.num_rows() > 0)
        .collect())
}

fn apply_filters(mut batch: Batch, filters: &[Expr]) -> Result<Option<Batch>> {
    for pred in filters {
        match filter_batch(&batch, pred)? {
            Some(kept) => batch = kept,
            None => return Ok(None),
        }
    }
    Ok(Some(batch))
}

fn apply_pipe(pipe: &Pipeline, batch: Batch) -> Result<Option<Batch>> {
    let Some(mut batch) = apply_filters(batch, &pipe.scan_filters)? else {
        return Ok(None);
    };
    if let Some(names) = &pipe.projection {
        let mut indices = Vec::with_capacity(names.len());
        for name in names {
            let idx = batch
                .schema()
                .index_of(name)
                .map_err(|e| Error::Plan(format!("scan projection: {e}")))?;
            indices.push(idx);
        }
        batch = batch.project(&indices)?;
    }
    let Some(mut batch) = apply_filters(batch, &pipe.extra_filters)? else {
        return Ok(None);
    };
    if let Some((exprs, schema)) = &pipe.project {
        batch = project_batch(&batch, exprs, schema.clone())?;
    }
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    Ok(Some(batch))
}
