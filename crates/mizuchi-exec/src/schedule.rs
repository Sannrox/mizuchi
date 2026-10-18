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
use super::runtime_filter;
use super::{Exec, Session, collect, physical, project_batch};
use mizuchi_core::batch::Batch;
use mizuchi_core::error::{Error, Result};
use mizuchi_core::expr::Expr;
use mizuchi_plan::{AggCall, JoinType, LogicalPlan, NamedExpr};

pub(super) fn try_parallel(
    plan: &LogicalPlan,
    session: &Session,
    pool: &MemoryPool,
) -> Result<Option<Vec<Batch>>> {
    session.exec_token().check()?;
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
                &session.exec_token(),
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
                    &session.exec_token(),
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
                    &session.exec_token(),
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
            if let Some(fused) = inner_join_under_project(input, group, aggs) {
                let (l, r) = join_sides(
                    fused.left,
                    fused.right,
                    fused.on,
                    JoinType::Inner,
                    session,
                    pool,
                )?;
                return Ok(Some(join::run_then_agg(
                    Box::new(Exec::Scan {
                        batches: l,
                        index: 0,
                    }),
                    Box::new(Exec::Scan {
                        batches: r,
                        index: 0,
                    }),
                    JoinSpec {
                        join_type: JoinType::Inner,
                        on: fused.on,
                        left_schema: fused.left.schema().clone(),
                        right_schema: fused.right.schema().clone(),
                        schema: fused.join_schema,
                    },
                    &fused.group,
                    &fused.aggs,
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
            let (l, r) = join_sides(left, right, on.as_ref(), *join_type, session, pool)?;
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
                &session.exec_token(),
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

struct FusedJoinAgg<'a> {
    left: &'a LogicalPlan,
    right: &'a LogicalPlan,
    on: Option<&'a Expr>,
    join_schema: SchemaRef,
    group: Vec<NamedExpr>,
    aggs: Vec<(String, AggCall)>,
}

/// Binder wraps join outputs in a Project. Remap grouping and aggregate
/// expressions onto the inner join so the unique-key fold can run.
fn inner_join_under_project<'a>(
    input: &'a LogicalPlan,
    group: &[NamedExpr],
    aggs: &[(String, AggCall)],
) -> Option<FusedJoinAgg<'a>> {
    let (join, group, aggs) = match input {
        LogicalPlan::Join {
            join_type: JoinType::Inner,
            ..
        } => (input, group.to_vec(), aggs.to_vec()),
        LogicalPlan::Project {
            input: child,
            exprs,
            ..
        } => {
            let LogicalPlan::Join {
                join_type: JoinType::Inner,
                schema,
                ..
            } = child.as_ref()
            else {
                return None;
            };
            let group = remap_named(group, exprs, schema)?;
            let aggs = remap_aggs(aggs, exprs, schema)?;
            (child.as_ref(), group, aggs)
        }
        _ => return None,
    };
    let LogicalPlan::Join {
        left,
        right,
        on,
        schema,
        ..
    } = join
    else {
        return None;
    };
    Some(FusedJoinAgg {
        left,
        right,
        on: on.as_ref(),
        join_schema: schema.clone(),
        group,
        aggs,
    })
}

fn remap_named(
    group: &[NamedExpr],
    project: &[NamedExpr],
    join_schema: &arrow::datatypes::Schema,
) -> Option<Vec<NamedExpr>> {
    group
        .iter()
        .map(|g| {
            let expr = through_project(&g.expr, project)?;
            let expr = resolve_nth(&expr, join_schema)?;
            Some(NamedExpr {
                expr,
                name: g.name.clone(),
            })
        })
        .collect()
}

fn remap_aggs(
    aggs: &[(String, AggCall)],
    project: &[NamedExpr],
    join_schema: &arrow::datatypes::Schema,
) -> Option<Vec<(String, AggCall)>> {
    aggs.iter()
        .map(|(name, call)| {
            let call = map_agg(call, |e| {
                let e = through_project(e, project)?;
                resolve_nth(&e, join_schema)
            })?;
            Some((name.clone(), call))
        })
        .collect()
}

fn map_agg(call: &AggCall, mut f: impl FnMut(&Expr) -> Option<Expr>) -> Option<AggCall> {
    Some(match call {
        AggCall::CountStar => AggCall::CountStar,
        AggCall::Count(e) => AggCall::Count(f(e)?),
        AggCall::CountDistinct(e) => AggCall::CountDistinct(f(e)?),
        AggCall::Sum(e) => AggCall::Sum(f(e)?),
        AggCall::Avg(e) => AggCall::Avg(f(e)?),
        AggCall::Min(e) => AggCall::Min(f(e)?),
        AggCall::Max(e) => AggCall::Max(f(e)?),
        AggCall::User { name, arg, agg } => AggCall::User {
            name: name.clone(),
            arg: f(arg)?,
            agg: agg.clone(),
        },
    })
}

fn through_project(expr: &Expr, project: &[NamedExpr]) -> Option<Expr> {
    map_expr(expr, &|e| match e {
        Expr::Nth(i) => project.get(*i).map(|n| n.expr.clone()),
        Expr::Column(name) => project
            .iter()
            .find(|n| n.name == *name)
            .map(|n| n.expr.clone()),
        other => Some(other.clone()),
    })
}

fn resolve_nth(expr: &Expr, schema: &arrow::datatypes::Schema) -> Option<Expr> {
    map_expr(expr, &|e| match e {
        Expr::Column(name) => schema.index_of(name).ok().map(Expr::nth),
        Expr::Nth(i) if *i < schema.fields().len() => Some(e.clone()),
        Expr::Nth(_) => None,
        other => Some(other.clone()),
    })
}

fn map_expr(expr: &Expr, leaf: &impl Fn(&Expr) -> Option<Expr>) -> Option<Expr> {
    match expr {
        Expr::Nth(_) | Expr::Column(_) | Expr::Literal(_) => leaf(expr),
        Expr::Binary(op, l, r) => Some(Expr::Binary(
            *op,
            Box::new(map_expr(l, leaf)?),
            Box::new(map_expr(r, leaf)?),
        )),
        Expr::Unary(op, e) => Some(Expr::Unary(*op, Box::new(map_expr(e, leaf)?))),
        Expr::Cast { expr, data_type } => Some(Expr::Cast {
            expr: Box::new(map_expr(expr, leaf)?),
            data_type: data_type.clone(),
        }),
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => Some(Expr::Case {
            operand: match operand.as_deref() {
                Some(e) => Some(Box::new(map_expr(e, leaf)?)),
                None => None,
            },
            when_then: when_then
                .iter()
                .map(|(w, t)| Some((map_expr(w, leaf)?, map_expr(t, leaf)?)))
                .collect::<Option<_>>()?,
            else_expr: match else_expr.as_deref() {
                Some(e) => Some(Box::new(map_expr(e, leaf)?)),
                None => None,
            },
        }),
        Expr::Coalesce(args) => Some(Expr::Coalesce(
            args.iter()
                .map(|e| map_expr(e, leaf))
                .collect::<Option<_>>()?,
        )),
        Expr::Concat(args) => Some(Expr::Concat(
            args.iter()
                .map(|e| map_expr(e, leaf))
                .collect::<Option<_>>()?,
        )),
        Expr::Substring {
            expr,
            start,
            length,
        } => Some(Expr::Substring {
            expr: Box::new(map_expr(expr, leaf)?),
            start: *start,
            length: *length,
        }),
        Expr::Array(args) => Some(Expr::Array(
            args.iter()
                .map(|e| map_expr(e, leaf))
                .collect::<Option<_>>()?,
        )),
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
    plan_batches_filtered(plan, &[], session, pool)
}

fn plan_batches_filtered(
    plan: &LogicalPlan,
    extra: &[Expr],
    session: &Session,
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    if extra.is_empty() {
        if let Some(out) = try_parallel(plan, session, pool)? {
            return Ok(out);
        }
        return collect(physical(plan, session, pool)?, &session.exec_token());
    }
    match plan {
        LogicalPlan::Join {
            left,
            right,
            join_type,
            on,
            schema,
        } => {
            let (l, r) = join_sides(left, right, on.as_ref(), *join_type, session, pool)?;
            let batches = join::run(
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
                &session.exec_token(),
                session.workers,
            )?;
            runtime_filter::filter_batches(batches, extra)
        }
        _ => {
            if let Some(mut pipe) = as_pipeline(plan) {
                push_runtime(&mut pipe, extra);
                return parallel_pipeline(&pipe, session);
            }
            let batches = plan_batches(plan, session, pool)?;
            runtime_filter::filter_batches(batches, extra)
        }
    }
}

/// Build the right (build) side first so inner-join keys can filter the probe.
/// Parent runtime preds stay above this join; a fallible parent key may only
/// be safe after this join removes rows that would fail.
fn join_sides(
    left: &LogicalPlan,
    right: &LogicalPlan,
    on: Option<&Expr>,
    join_type: JoinType,
    session: &Session,
    pool: &MemoryPool,
) -> Result<(Vec<Batch>, Vec<Batch>)> {
    let left_ncols = left.schema().fields().len();
    let inner = join_type == JoinType::Inner;
    let r = plan_batches_filtered(right, &[], session, pool)?;
    let mut left_extra = Vec::new();
    if session.runtime_join_filters
        && inner
        && let Some(rf) = runtime_filter::from_build(on, left_ncols, left.schema().as_ref(), &r)
    {
        left_extra.extend(rf.preds);
    }
    let l = plan_batches_filtered(left, &left_extra, session, pool)?;
    Ok((l, r))
}

fn push_runtime(pipe: &mut Pipeline, runtime: &[Expr]) {
    if runtime.is_empty() {
        return;
    }
    if let Some((exprs, _)) = &pipe.project {
        if let Some(rewritten) = runtime
            .iter()
            .map(|e| runtime_filter::rewrite_through_project(e, exprs))
            .collect::<Option<Vec<_>>>()
        {
            pipe.extra_filters.extend(rewritten);
            return;
        }
        pipe.runtime_filters.extend(runtime.iter().cloned());
        return;
    }
    pipe.extra_filters.extend(runtime.iter().cloned());
}

struct Pipeline {
    table: String,
    projection: Option<Vec<String>>,
    scan_filters: Vec<Expr>,
    extra_filters: Vec<Expr>,
    runtime_filters: Vec<Expr>,
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
            runtime_filters: Vec::new(),
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
            &session.exec_token(),
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
            let token = session.exec_token();
            for chunk in chunks {
                let partials = &partials;
                let schema = schema.clone();
                let token = token.clone();
                handles.push(scope.spawn(move || -> Result<()> {
                    let prev = Session::push_cancel(token);
                    let result = (|| {
                        session.exec_token().check()?;
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
                            &session.exec_token(),
                        )?;
                        partials
                            .lock()
                            .map_err(|_| Error::Eval("agg combine lock poisoned".into()))?
                            .extend(out);
                        Ok(())
                    })();
                    Session::restore_cancel(prev);
                    result
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
        return super::agg::combine_partials(partials, group, aggs, schema, &session.policy, pool);
    }
    if group.is_empty() {
        let tables = Mutex::new(Vec::new());
        thread::scope(|scope| -> Result<()> {
            let mut handles = Vec::new();
            let token = session.exec_token();
            for chunk in chunks {
                let tables = &tables;
                let schema = schema.clone();
                let token = token.clone();
                handles.push(scope.spawn(move || -> Result<()> {
                    let prev = Session::push_cancel(token);
                    let result = (|| {
                        session.exec_token().check()?;
                        let table = super::agg::fold_batches(chunk, group, aggs, schema)?;
                        tables
                            .lock()
                            .map_err(|_| Error::Eval("agg combine lock poisoned".into()))?
                            .push(table);
                        Ok(())
                    })();
                    Session::restore_cancel(prev);
                    result
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
        return super::agg::merge_finish(tables, group, aggs, schema, &session.policy);
    }
    super::agg::run_parallel(
        chunks.into_iter().flatten().collect(),
        group,
        aggs,
        schema,
        &session.policy,
        session.workers,
        &session.exec_token(),
    )
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
            session.exec_token().check()?;
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
        let token = session.exec_token();
        for _ in 0..n {
            let queue = &queue;
            let collected = &collected;
            let f = &f;
            let token = token.clone();
            handles.push(scope.spawn(move || -> Result<()> {
                let prev = Session::push_cancel(token);
                let result = (|| {
                    loop {
                        session.exec_token().check()?;
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
                })();
                Session::restore_cancel(prev);
                result
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

fn apply_filters(batch: Batch, filters: &[Expr]) -> Result<Option<Batch>> {
    runtime_filter::apply_preds(batch, filters)
}

fn apply_pipe(pipe: &Pipeline, batch: Batch) -> Result<Option<Batch>> {
    let project_first = pipe
        .projection
        .as_ref()
        .is_some_and(|names| super::filters_on_projected(&pipe.scan_filters, names));
    let batch = match &pipe.projection {
        Some(names) if project_first => super::project_named(batch, names)?,
        _ => batch,
    };
    let Some(mut batch) = apply_filters(batch, &pipe.scan_filters)? else {
        return Ok(None);
    };
    if !project_first && let Some(names) = &pipe.projection {
        batch = super::project_named(batch, names)?;
    }
    let Some(mut batch) = apply_filters(batch, &pipe.extra_filters)? else {
        return Ok(None);
    };
    if let Some((exprs, schema)) = &pipe.project {
        batch = project_batch(&batch, exprs, schema.clone())?;
    }
    let Some(batch) = apply_filters(batch, &pipe.runtime_filters)? else {
        return Ok(None);
    };
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    Ok(Some(batch))
}
