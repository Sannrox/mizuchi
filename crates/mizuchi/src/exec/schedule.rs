//! Morsel work queue across a bounded thread pool.
//!
//! Independent scan/filter/project morsels and combinable aggregates run on
//! `Session::with_workers` threads. Operators check cancellation between
//! morsels. Plans that need a single-threaded pipeline fall back to pull
//! execution.

use std::sync::Mutex;
use std::thread;

use arrow::datatypes::SchemaRef;

use super::memory::MemoryPool;
use super::{Exec, Session, filter_batch, project_batch};
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
    if let LogicalPlan::Aggregate {
        input,
        group,
        aggs,
        schema,
    } = plan
    {
        if super::agg::can_combine(aggs)
            && let Some(pipe) = as_pipeline(input)
        {
            return Ok(Some(parallel_agg(
                &pipe,
                group,
                aggs,
                schema.clone(),
                session,
                pool,
            )?));
        }
        return Ok(None);
    }
    if let Some(pipe) = as_pipeline(plan) {
        return Ok(Some(parallel_pipeline(&pipe, session)?));
    }
    Ok(None)
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
    super::agg::combine_partials(partials, group, aggs, schema, &session.policy)
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
