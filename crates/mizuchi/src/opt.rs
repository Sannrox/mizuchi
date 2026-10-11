//! Rule-based logical rewrites.
//!
//! Batches of rules run to a fixed point. Each rule walks the plan bottom-up.
//! Core rules: fold literals, simplify boolean algebra, merge adjacent
//! filters, push predicates and projections, reorder inner joins by cost,
//! push limit through projection, and drop identity / nested projections.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, RecordBatch, RecordBatchOptions};
use arrow::datatypes::Schema;

use crate::batch::Batch;
use crate::error::Result;
use crate::expr::{BinaryOp, Expr, UnaryOp};
use crate::plan::{JoinType, LogicalPlan, NamedExpr, TableStats, inner_equi_rows, join_schema};

/// Apply the standard rule batches to `plan`.
pub fn optimize(plan: LogicalPlan) -> Result<LogicalPlan> {
    Optimizer::standard().run(plan)
}

/// Estimated output rows from table stats, or `None` when stats are missing.
pub fn estimate_rows(plan: &LogicalPlan) -> Option<f64> {
    match plan {
        LogicalPlan::Empty { .. } => Some(1.0),
        LogicalPlan::Scan {
            stats: Some(stats),
            filters,
            ..
        } => {
            let mut rows = stats.row_count as f64;
            for pred in filters {
                rows *= selectivity(stats, pred);
            }
            Some(rows.max(1.0))
        }
        LogicalPlan::Scan { stats: None, .. } => None,
        LogicalPlan::Filter { input, predicate } => {
            let rows = estimate_rows(input)?;
            let sel = scan_stats(input)
                .map(|s| selectivity(s, predicate))
                .unwrap_or(1.0 / 3.0);
            Some((rows * sel).max(1.0))
        }
        LogicalPlan::Project { input, .. } | LogicalPlan::Sort { input, .. } => {
            estimate_rows(input)
        }
        LogicalPlan::Limit { input, fetch, .. } => {
            let rows = estimate_rows(input)?;
            Some(match fetch {
                Some(n) => rows.min(*n as f64),
                None => rows,
            })
        }
        LogicalPlan::Join {
            left,
            right,
            join_type: JoinType::Inner,
            on: Some(on),
            ..
        } => {
            let lrows = estimate_rows(left)?;
            let rrows = estimate_rows(right)?;
            let ln = left.schema().fields().len();
            let mut rows = f64::MAX;
            let mut any = false;
            for (a, b) in eq_nth_pairs(on) {
                let (l_idx, r_idx) = if a < ln && b >= ln {
                    (a, b - ln)
                } else if b < ln && a >= ln {
                    (b, a - ln)
                } else {
                    continue;
                };
                any = true;
                let lndv = col_ndv(left, l_idx).unwrap_or(lrows);
                let rndv = col_ndv(right, r_idx).unwrap_or(rrows);
                rows = rows.min(inner_equi_rows(lrows, rrows, lndv, rndv));
            }
            if any && rows.is_finite() {
                Some(rows.max(1.0))
            } else {
                Some(lrows.min(rrows).max(1.0))
            }
        }
        LogicalPlan::Join { left, right, .. } => {
            Some((estimate_rows(left)? * estimate_rows(right)?).max(1.0))
        }
        LogicalPlan::Aggregate { input, group, .. } => {
            let rows = estimate_rows(input)?;
            if group.is_empty() {
                Some(1.0)
            } else {
                Some(rows)
            }
        }
        LogicalPlan::Union { inputs, .. } => {
            let mut n = 0.0;
            for input in inputs {
                n += estimate_rows(input)?;
            }
            Some(n.max(1.0))
        }
    }
}

struct Optimizer {
    batches: Vec<RuleBatch>,
}

struct RuleBatch {
    max_iterations: usize,
    rules: Vec<Rule>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Rule {
    ConstantFold,
    SimplifyBoolean,
    CombineFilters,
    PushJoinFilters,
    PushScanFilters,
    PushProjection,
    ReorderJoins,
    PushLimit,
    CollapseProject,
}

impl Optimizer {
    fn standard() -> Self {
        Self {
            batches: vec![
                RuleBatch {
                    max_iterations: 16,
                    rules: vec![Rule::ConstantFold, Rule::SimplifyBoolean],
                },
                RuleBatch {
                    max_iterations: 16,
                    rules: vec![Rule::CombineFilters],
                },
                RuleBatch {
                    max_iterations: 16,
                    rules: vec![
                        Rule::PushJoinFilters,
                        Rule::PushScanFilters,
                        Rule::PushProjection,
                    ],
                },
                RuleBatch {
                    max_iterations: 16,
                    rules: vec![Rule::CollapseProject],
                },
                RuleBatch {
                    max_iterations: 1,
                    rules: vec![Rule::ReorderJoins],
                },
                RuleBatch {
                    max_iterations: 16,
                    rules: vec![Rule::PushJoinFilters, Rule::PushScanFilters],
                },
                RuleBatch {
                    max_iterations: 16,
                    rules: vec![Rule::PushLimit],
                },
            ],
        }
    }

    fn run(&self, mut plan: LogicalPlan) -> Result<LogicalPlan> {
        for batch in &self.batches {
            for _ in 0..batch.max_iterations {
                let mut next = plan.clone();
                for rule in &batch.rules {
                    next = if *rule == Rule::ReorderJoins {
                        transform_down(next, *rule)?
                    } else {
                        transform_up(next, *rule)?
                    };
                }
                if next.explain() == plan.explain() {
                    plan = next;
                    break;
                }
                plan = next;
            }
        }
        Ok(plan)
    }
}

fn transform_up(plan: LogicalPlan, rule: Rule) -> Result<LogicalPlan> {
    let plan = map_children(plan, |child| transform_up(child, rule))?;
    rewrite(plan, rule)
}

fn transform_down(plan: LogicalPlan, rule: Rule) -> Result<LogicalPlan> {
    let snapshot = plan.explain();
    let plan = rewrite(plan, rule)?;
    if rule == Rule::ReorderJoins && plan.explain() != snapshot {
        return Ok(plan);
    }
    map_children(plan, |child| transform_down(child, rule))
}

fn map_children(
    plan: LogicalPlan,
    mut f: impl FnMut(LogicalPlan) -> Result<LogicalPlan>,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Filter { input, predicate } => Ok(LogicalPlan::Filter {
            input: Box::new(f(*input)?),
            predicate,
        }),
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => Ok(LogicalPlan::Project {
            input: Box::new(f(*input)?),
            exprs,
            schema,
        }),
        LogicalPlan::Aggregate {
            input,
            group,
            aggs,
            schema,
        } => Ok(LogicalPlan::Aggregate {
            input: Box::new(f(*input)?),
            group,
            aggs,
            schema,
        }),
        LogicalPlan::Join {
            left,
            right,
            join_type,
            on,
            schema,
        } => Ok(LogicalPlan::Join {
            left: Box::new(f(*left)?),
            right: Box::new(f(*right)?),
            join_type,
            on,
            schema,
        }),
        LogicalPlan::Sort { input, order } => Ok(LogicalPlan::Sort {
            input: Box::new(f(*input)?),
            order,
        }),
        LogicalPlan::Limit { input, skip, fetch } => Ok(LogicalPlan::Limit {
            input: Box::new(f(*input)?),
            skip,
            fetch,
        }),
        LogicalPlan::Union {
            inputs,
            schema,
            all,
        } => {
            let inputs = inputs.into_iter().map(f).collect::<Result<Vec<_>>>()?;
            Ok(LogicalPlan::Union {
                inputs,
                schema,
                all,
            })
        }
        leaf @ (LogicalPlan::Empty { .. } | LogicalPlan::Scan { .. }) => Ok(leaf),
    }
}

fn rewrite(plan: LogicalPlan, rule: Rule) -> Result<LogicalPlan> {
    match rule {
        Rule::ConstantFold => fold_plan(plan),
        Rule::SimplifyBoolean => simplify_plan(plan),
        Rule::CombineFilters => Ok(combine_filters(plan)),
        Rule::PushJoinFilters => Ok(push_join_filters(plan)),
        Rule::PushScanFilters => Ok(push_scan_filters(plan)),
        Rule::PushProjection => Ok(push_projection(plan)),
        Rule::ReorderJoins => Ok(reorder_joins(plan)),
        Rule::PushLimit => Ok(push_limit(plan)),
        Rule::CollapseProject => Ok(collapse_project(plan)),
    }
}

fn fold_plan(plan: LogicalPlan) -> Result<LogicalPlan> {
    Ok(match plan {
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input,
            predicate: fold_expr(predicate)?,
        },
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => LogicalPlan::Project {
            input,
            exprs: fold_named(exprs)?,
            schema,
        },
        LogicalPlan::Join {
            left,
            right,
            join_type,
            on,
            schema,
        } => LogicalPlan::Join {
            left,
            right,
            join_type,
            on: on.map(fold_expr).transpose()?,
            schema,
        },
        LogicalPlan::Sort { input, order } => LogicalPlan::Sort {
            input,
            order: order
                .into_iter()
                .map(|mut s| {
                    s.expr = fold_expr(s.expr)?;
                    Ok(s)
                })
                .collect::<Result<Vec<_>>>()?,
        },
        other => other,
    })
}

fn fold_named(exprs: Vec<NamedExpr>) -> Result<Vec<NamedExpr>> {
    exprs
        .into_iter()
        .map(|mut e| {
            e.expr = fold_expr(e.expr)?;
            Ok(e)
        })
        .collect()
}

fn fold_expr(expr: Expr) -> Result<Expr> {
    let expr = match expr {
        Expr::Binary(op, left, right) => Expr::Binary(
            op,
            Box::new(fold_expr(*left)?),
            Box::new(fold_expr(*right)?),
        ),
        Expr::Unary(op, child) => Expr::Unary(op, Box::new(fold_expr(*child)?)),
        Expr::Cast { expr, data_type } => Expr::Cast {
            expr: Box::new(fold_expr(*expr)?),
            data_type,
        },
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => Expr::Case {
            operand: operand.map(|e| fold_expr(*e).map(Box::new)).transpose()?,
            when_then: when_then
                .into_iter()
                .map(|(w, t)| Ok((fold_expr(w)?, fold_expr(t)?)))
                .collect::<Result<Vec<_>>>()?,
            else_expr: else_expr.map(|e| fold_expr(*e).map(Box::new)).transpose()?,
        },
        Expr::Coalesce(args) => Expr::Coalesce(
            args.into_iter()
                .map(fold_expr)
                .collect::<Result<Vec<_>>>()?,
        ),
        Expr::Concat(args) => Expr::Concat(
            args.into_iter()
                .map(fold_expr)
                .collect::<Result<Vec<_>>>()?,
        ),
        Expr::Substring {
            expr,
            start,
            length,
        } => Expr::Substring {
            expr: Box::new(fold_expr(*expr)?),
            start,
            length,
        },
        Expr::Array(args) => Expr::Array(
            args.into_iter()
                .map(fold_expr)
                .collect::<Result<Vec<_>>>()?,
        ),
        other => other,
    };
    if matches!(expr, Expr::Literal(_)) || !is_foldable(&expr) {
        return Ok(expr);
    }
    match expr.eval(&unit_batch()?) {
        Ok(value) => Ok(Expr::Literal(value)),
        Err(_) => Ok(expr),
    }
}

fn is_foldable(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) => true,
        Expr::Column(_) | Expr::Nth(_) => false,
        Expr::Binary(_, l, r) => is_foldable(l) && is_foldable(r),
        Expr::Unary(_, c) => is_foldable(c),
        Expr::Cast { expr, .. } | Expr::Substring { expr, .. } => is_foldable(expr),
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand.as_deref().is_none_or(is_foldable)
                && when_then
                    .iter()
                    .all(|(w, t)| is_foldable(w) && is_foldable(t))
                && else_expr.as_deref().is_none_or(is_foldable)
        }
        Expr::Coalesce(args) | Expr::Concat(args) | Expr::Array(args) => {
            args.iter().all(is_foldable)
        }
    }
}

fn unit_batch() -> Result<Batch> {
    let inner = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        Vec::new(),
        &RecordBatchOptions::default().with_row_count(Some(1)),
    )
    .map_err(|e| crate::error::Error::Eval(e.to_string()))?;
    Batch::from_arrow(inner)
}

fn simplify_plan(plan: LogicalPlan) -> Result<LogicalPlan> {
    Ok(match plan {
        LogicalPlan::Filter { input, predicate } => {
            let predicate = simplify_expr(predicate);
            if bool_lit(&predicate) == Some(true) {
                *input
            } else {
                LogicalPlan::Filter { input, predicate }
            }
        }
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => LogicalPlan::Project {
            input,
            exprs: exprs
                .into_iter()
                .map(|mut e| {
                    e.expr = simplify_expr(e.expr);
                    e
                })
                .collect(),
            schema,
        },
        LogicalPlan::Join {
            left,
            right,
            join_type,
            on,
            schema,
        } => LogicalPlan::Join {
            left,
            right,
            join_type,
            on: on.map(simplify_expr),
            schema,
        },
        other => other,
    })
}

fn simplify_expr(expr: Expr) -> Expr {
    match expr {
        Expr::Binary(op, left, right) => {
            let left = simplify_expr(*left);
            let right = simplify_expr(*right);
            match op {
                BinaryOp::And => match (bool_lit(&left), bool_lit(&right)) {
                    (Some(true), _) => right,
                    (_, Some(true)) => left,
                    (Some(false), _) | (_, Some(false)) => Expr::lit_bool(false),
                    _ => Expr::Binary(op, Box::new(left), Box::new(right)),
                },
                BinaryOp::Or => match (bool_lit(&left), bool_lit(&right)) {
                    (Some(false), _) => right,
                    (_, Some(false)) => left,
                    (Some(true), _) | (_, Some(true)) => Expr::lit_bool(true),
                    _ => Expr::Binary(op, Box::new(left), Box::new(right)),
                },
                _ => Expr::Binary(op, Box::new(left), Box::new(right)),
            }
        }
        Expr::Unary(UnaryOp::Not, child) => {
            let child = simplify_expr(*child);
            match child {
                Expr::Unary(UnaryOp::Not, inner) => *inner,
                other => Expr::Unary(UnaryOp::Not, Box::new(other)),
            }
        }
        Expr::Unary(op, child) => Expr::Unary(op, Box::new(simplify_expr(*child))),
        Expr::Cast { expr, data_type } => Expr::Cast {
            expr: Box::new(simplify_expr(*expr)),
            data_type,
        },
        other => other,
    }
}

fn bool_lit(expr: &Expr) -> Option<bool> {
    let Expr::Literal(arr) = expr else {
        return None;
    };
    let b = arr.as_any().downcast_ref::<BooleanArray>()?;
    if b.len() != 1 || b.is_null(0) {
        return None;
    }
    Some(b.value(0))
}

fn combine_filters(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter {
            input,
            predicate: outer,
        } => match *input {
            LogicalPlan::Filter {
                input,
                predicate: inner,
            } => LogicalPlan::Filter {
                input,
                predicate: Expr::Binary(BinaryOp::And, Box::new(inner), Box::new(outer)),
            },
            other => LogicalPlan::Filter {
                input: Box::new(other),
                predicate: outer,
            },
        },
        other => other,
    }
}

fn push_scan_filters(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { input, predicate } => match *input {
            LogicalPlan::Scan {
                table,
                schema,
                alias,
                projection,
                mut filters,
                stats,
            } => {
                for pred in conjuncts(predicate) {
                    filters.push(nth_to_column(pred, schema.as_ref()));
                }
                LogicalPlan::Scan {
                    table,
                    schema,
                    alias,
                    projection,
                    filters,
                    stats,
                }
            }
            other => LogicalPlan::Filter {
                input: Box::new(other),
                predicate,
            },
        },
        other => other,
    }
}

fn push_join_filters(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { input, predicate } => match *input {
            LogicalPlan::Join {
                left,
                right,
                join_type: JoinType::Inner,
                on,
                schema,
            } => {
                let left_ncols = left.schema().fields().len();
                let mut left_preds = Vec::new();
                let mut right_preds = Vec::new();
                let mut rest = Vec::new();
                for pred in conjuncts(predicate) {
                    match expr_side(&pred, left_ncols) {
                        Side::Left => left_preds.push(pred),
                        Side::Right => right_preds.push(shift_nth(pred, left_ncols)),
                        Side::Both => rest.push(pred),
                    }
                }
                let left = Box::new(wrap_filters(*left, left_preds));
                let right = Box::new(wrap_filters(*right, right_preds));
                let join = LogicalPlan::Join {
                    left,
                    right,
                    join_type: JoinType::Inner,
                    on,
                    schema,
                };
                wrap_filters(join, rest)
            }
            other => LogicalPlan::Filter {
                input: Box::new(other),
                predicate,
            },
        },
        other => other,
    }
}

fn wrap_filters(plan: LogicalPlan, preds: Vec<Expr>) -> LogicalPlan {
    match and_all(preds) {
        Some(predicate) => LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        },
        None => plan,
    }
}

fn push_projection(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Project {
            input,
            exprs,
            schema: out_schema,
        } => match *input {
            LogicalPlan::Scan {
                table,
                schema,
                alias,
                projection,
                filters,
                stats,
            } => {
                let mut names = Vec::with_capacity(exprs.len());
                for e in &exprs {
                    match &e.expr {
                        Expr::Nth(i) if *i < schema.fields().len() => {
                            names.push(schema.field(*i).name().to_string());
                        }
                        Expr::Column(n) => names.push(n.clone()),
                        _ => {
                            return LogicalPlan::Project {
                                input: Box::new(LogicalPlan::Scan {
                                    table,
                                    schema,
                                    alias,
                                    projection,
                                    filters,
                                    stats,
                                }),
                                exprs,
                                schema: out_schema,
                            };
                        }
                    }
                }
                let mut fields = Vec::with_capacity(names.len());
                for name in &names {
                    match schema.index_of(name) {
                        Ok(i) => fields.push(schema.field(i).clone()),
                        Err(_) => {
                            return LogicalPlan::Project {
                                input: Box::new(LogicalPlan::Scan {
                                    table,
                                    schema,
                                    alias,
                                    projection,
                                    filters,
                                    stats,
                                }),
                                exprs,
                                schema: out_schema,
                            };
                        }
                    }
                }
                let new_schema = Arc::new(Schema::new(fields));
                let scan = LogicalPlan::Scan {
                    table,
                    schema: new_schema.clone(),
                    alias,
                    projection: Some(names),
                    filters,
                    stats,
                };
                if is_identity(&exprs, &new_schema) {
                    scan
                } else {
                    let exprs = exprs
                        .into_iter()
                        .enumerate()
                        .map(|(i, mut e)| {
                            e.expr = Expr::nth(i);
                            e
                        })
                        .collect();
                    LogicalPlan::Project {
                        input: Box::new(scan),
                        exprs,
                        schema: out_schema,
                    }
                }
            }
            other => LogicalPlan::Project {
                input: Box::new(other),
                exprs,
                schema: out_schema,
            },
        },
        other => other,
    }
}

fn conjuncts(expr: Expr) -> Vec<Expr> {
    match expr {
        Expr::Binary(BinaryOp::And, l, r) => {
            let mut out = conjuncts(*l);
            out.extend(conjuncts(*r));
            out
        }
        other => vec![other],
    }
}

fn and_all(preds: Vec<Expr>) -> Option<Expr> {
    preds
        .into_iter()
        .reduce(|a, b| Expr::Binary(BinaryOp::And, Box::new(a), Box::new(b)))
}

fn nth_to_column(expr: Expr, schema: &Schema) -> Expr {
    match expr {
        Expr::Nth(i) if i < schema.fields().len() => Expr::column(schema.field(i).name()),
        Expr::Binary(op, l, r) => Expr::Binary(
            op,
            Box::new(nth_to_column(*l, schema)),
            Box::new(nth_to_column(*r, schema)),
        ),
        Expr::Unary(op, c) => Expr::Unary(op, Box::new(nth_to_column(*c, schema))),
        Expr::Cast { expr, data_type } => Expr::Cast {
            expr: Box::new(nth_to_column(*expr, schema)),
            data_type,
        },
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => Expr::Case {
            operand: operand.map(|e| Box::new(nth_to_column(*e, schema))),
            when_then: when_then
                .into_iter()
                .map(|(w, t)| (nth_to_column(w, schema), nth_to_column(t, schema)))
                .collect(),
            else_expr: else_expr.map(|e| Box::new(nth_to_column(*e, schema))),
        },
        Expr::Coalesce(args) => {
            Expr::Coalesce(args.into_iter().map(|e| nth_to_column(e, schema)).collect())
        }
        Expr::Concat(args) => {
            Expr::Concat(args.into_iter().map(|e| nth_to_column(e, schema)).collect())
        }
        Expr::Substring {
            expr,
            start,
            length,
        } => Expr::Substring {
            expr: Box::new(nth_to_column(*expr, schema)),
            start,
            length,
        },
        Expr::Array(args) => {
            Expr::Array(args.into_iter().map(|e| nth_to_column(e, schema)).collect())
        }
        other => other,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Left,
    Right,
    Both,
}

fn expr_side(expr: &Expr, left_ncols: usize) -> Side {
    match expr {
        Expr::Nth(i) => {
            if *i < left_ncols {
                Side::Left
            } else {
                Side::Right
            }
        }
        Expr::Literal(_) => Side::Both,
        Expr::Column(_) => Side::Both,
        Expr::Binary(_, l, r) => merge_side(expr_side(l, left_ncols), expr_side(r, left_ncols)),
        Expr::Unary(_, c) | Expr::Cast { expr: c, .. } | Expr::Substring { expr: c, .. } => {
            expr_side(c, left_ncols)
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let mut s = operand
                .as_deref()
                .map_or(Side::Both, |e| expr_side(e, left_ncols));
            for (w, t) in when_then {
                s = merge_side(s, expr_side(w, left_ncols));
                s = merge_side(s, expr_side(t, left_ncols));
            }
            if let Some(e) = else_expr {
                s = merge_side(s, expr_side(e, left_ncols));
            }
            s
        }
        Expr::Coalesce(args) | Expr::Concat(args) | Expr::Array(args) => args
            .iter()
            .map(|e| expr_side(e, left_ncols))
            .fold(Side::Both, merge_side),
    }
}

fn merge_side(a: Side, b: Side) -> Side {
    match (a, b) {
        (Side::Both, x) | (x, Side::Both) => x,
        (Side::Left, Side::Left) => Side::Left,
        (Side::Right, Side::Right) => Side::Right,
        _ => Side::Both,
    }
}

fn shift_nth(expr: Expr, left_ncols: usize) -> Expr {
    match expr {
        Expr::Nth(i) if i >= left_ncols => Expr::Nth(i - left_ncols),
        Expr::Nth(i) => Expr::Nth(i),
        Expr::Binary(op, l, r) => Expr::Binary(
            op,
            Box::new(shift_nth(*l, left_ncols)),
            Box::new(shift_nth(*r, left_ncols)),
        ),
        Expr::Unary(op, c) => Expr::Unary(op, Box::new(shift_nth(*c, left_ncols))),
        Expr::Cast { expr, data_type } => Expr::Cast {
            expr: Box::new(shift_nth(*expr, left_ncols)),
            data_type,
        },
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => Expr::Case {
            operand: operand.map(|e| Box::new(shift_nth(*e, left_ncols))),
            when_then: when_then
                .into_iter()
                .map(|(w, t)| (shift_nth(w, left_ncols), shift_nth(t, left_ncols)))
                .collect(),
            else_expr: else_expr.map(|e| Box::new(shift_nth(*e, left_ncols))),
        },
        Expr::Coalesce(args) => {
            Expr::Coalesce(args.into_iter().map(|e| shift_nth(e, left_ncols)).collect())
        }
        Expr::Concat(args) => {
            Expr::Concat(args.into_iter().map(|e| shift_nth(e, left_ncols)).collect())
        }
        Expr::Substring {
            expr,
            start,
            length,
        } => Expr::Substring {
            expr: Box::new(shift_nth(*expr, left_ncols)),
            start,
            length,
        },
        Expr::Array(args) => {
            Expr::Array(args.into_iter().map(|e| shift_nth(e, left_ncols)).collect())
        }
        other => other,
    }
}

/// Inner joins of this many tables use dynamic programming; more use greedy.
const JOIN_REORDER_DP_THRESHOLD: usize = 12;

fn reorder_joins(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Join {
            join_type: JoinType::Inner,
            on: Some(_),
            ..
        } => reorder_inner_join_tree(plan),
        other => other,
    }
}

fn reorder_inner_join_tree(plan: LogicalPlan) -> LogicalPlan {
    let original_schema = plan.schema().clone();
    let mut items = Vec::new();
    collect_join_items(&plan, &mut items);
    if items.len() < 3 {
        return plan;
    }
    let mut global_eq = Vec::new();
    let mut residuals = Vec::new();
    collect_join_preds(&plan, 0, &mut global_eq, &mut residuals);
    let mut offset = 0usize;
    let mut global_cols = Vec::new();
    let mut leaves = Vec::new();
    for (idx, item) in items.iter().enumerate() {
        let Some(rows) = estimate_rows(item) else {
            return plan;
        };
        let n = item.schema().fields().len();
        let mut ndv = BTreeMap::new();
        let mut cols = Vec::with_capacity(n);
        for local in 0..n {
            global_cols.push((idx, local));
            cols.push((idx, local));
            ndv.insert((idx, local), col_ndv(item, local).unwrap_or(rows));
        }
        leaves.push(JoinPlan {
            plan: item.clone(),
            items: BTreeSet::from([idx]),
            cols,
            ndv,
            rows,
            cost: rows,
        });
        offset += n;
    }
    let _ = offset;
    let mut pairs = Vec::new();
    for (a, b) in global_eq {
        if a >= global_cols.len() || b >= global_cols.len() {
            return plan;
        }
        let (ia, ca) = global_cols[a];
        let (ib, cb) = global_cols[b];
        if ia == ib {
            // Same-leaf equalities (a.x = a.y, a.x = a.x, or columns of a
            // non-flattened child) are not join-graph edges.
            residuals.push(Expr::Binary(
                BinaryOp::Eq,
                Box::new(Expr::Nth(a)),
                Box::new(Expr::Nth(b)),
            ));
        } else {
            pairs.push((ia, ca, ib, cb));
        }
    }
    if pairs.is_empty() {
        return plan;
    }
    let orig_cols = global_cols.clone();
    let reordered = if items.len() <= JOIN_REORDER_DP_THRESHOLD {
        dp_join_order(&leaves, &pairs)
    } else {
        greedy_join_order(&leaves, &pairs)
    };
    let Some(best) = reordered else {
        return plan;
    };
    let rebuilt = match and_all(residuals) {
        Some(predicate) => LogicalPlan::Filter {
            input: Box::new(best.plan.clone()),
            predicate: remap_nth(predicate, &orig_cols, &best.cols),
        },
        None => best.plan.clone(),
    };
    restore_join_output(rebuilt, original_schema, &best.cols, &orig_cols)
}

#[derive(Clone)]
struct JoinPlan {
    plan: LogicalPlan,
    items: BTreeSet<usize>,
    cols: Vec<(usize, usize)>,
    ndv: BTreeMap<(usize, usize), f64>,
    rows: f64,
    cost: f64,
}

fn collect_join_items(plan: &LogicalPlan, items: &mut Vec<LogicalPlan>) {
    match plan {
        LogicalPlan::Join {
            left,
            right,
            join_type: JoinType::Inner,
            on: Some(_),
            ..
        } => {
            collect_join_items(left, items);
            collect_join_items(right, items);
        }
        other => items.push(other.clone()),
    }
}

fn collect_join_preds(
    plan: &LogicalPlan,
    offset: usize,
    eqs: &mut Vec<(usize, usize)>,
    residuals: &mut Vec<Expr>,
) {
    if let LogicalPlan::Join {
        left,
        right,
        join_type: JoinType::Inner,
        on: Some(on),
        ..
    } = plan
    {
        collect_join_preds(left, offset, eqs, residuals);
        let ln = left.schema().fields().len();
        collect_join_preds(right, offset + ln, eqs, residuals);
        for pred in conjuncts(on.clone()) {
            match eq_nth_pair(&pred) {
                Some((a, b)) => eqs.push((a + offset, b + offset)),
                None => residuals.push(add_nth(pred, offset)),
            }
        }
    }
}

fn eq_nth_pair(expr: &Expr) -> Option<(usize, usize)> {
    match expr {
        Expr::Binary(BinaryOp::Eq, l, r) => match (l.as_ref(), r.as_ref()) {
            (Expr::Nth(a), Expr::Nth(b)) => Some((*a, *b)),
            _ => None,
        },
        _ => None,
    }
}

fn add_nth(expr: Expr, n: usize) -> Expr {
    match expr {
        Expr::Nth(i) => Expr::Nth(i + n),
        Expr::Binary(op, l, r) => {
            Expr::Binary(op, Box::new(add_nth(*l, n)), Box::new(add_nth(*r, n)))
        }
        Expr::Unary(op, c) => Expr::Unary(op, Box::new(add_nth(*c, n))),
        Expr::Cast { expr, data_type } => Expr::Cast {
            expr: Box::new(add_nth(*expr, n)),
            data_type,
        },
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => Expr::Case {
            operand: operand.map(|e| Box::new(add_nth(*e, n))),
            when_then: when_then
                .into_iter()
                .map(|(w, t)| (add_nth(w, n), add_nth(t, n)))
                .collect(),
            else_expr: else_expr.map(|e| Box::new(add_nth(*e, n))),
        },
        Expr::Coalesce(args) => Expr::Coalesce(args.into_iter().map(|e| add_nth(e, n)).collect()),
        Expr::Concat(args) => Expr::Concat(args.into_iter().map(|e| add_nth(e, n)).collect()),
        Expr::Substring {
            expr,
            start,
            length,
        } => Expr::Substring {
            expr: Box::new(add_nth(*expr, n)),
            start,
            length,
        },
        Expr::Array(args) => Expr::Array(args.into_iter().map(|e| add_nth(e, n)).collect()),
        other => other,
    }
}

fn eq_nth_pairs(expr: &Expr) -> Vec<(usize, usize)> {
    match expr {
        Expr::Binary(BinaryOp::And, l, r) => {
            let mut out = eq_nth_pairs(l);
            out.extend(eq_nth_pairs(r));
            out
        }
        Expr::Binary(BinaryOp::Eq, l, r) => match (l.as_ref(), r.as_ref()) {
            (Expr::Nth(a), Expr::Nth(b)) => vec![(*a, *b)],
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

fn dp_join_order(leaves: &[JoinPlan], pairs: &[(usize, usize, usize, usize)]) -> Option<JoinPlan> {
    let n = leaves.len();
    let full = (1u64 << n) - 1;
    let mut best: HashMap<u64, JoinPlan> = HashMap::new();
    for (i, leaf) in leaves.iter().enumerate() {
        best.insert(1u64 << i, leaf.clone());
    }
    for size in 2..=n {
        for mask in 1..=full {
            if mask.count_ones() as usize != size {
                continue;
            }
            let mut winner: Option<JoinPlan> = None;
            let mut sub = (mask - 1) & mask;
            while sub > 0 {
                let other = mask ^ sub;
                if let (Some(l), Some(r)) = (best.get(&sub), best.get(&other))
                    && let Some(joined) = connect_joins(l, r, pairs)
                {
                    winner = better_plan(winner, joined);
                }
                sub = (sub - 1) & mask;
            }
            if let Some(plan) = winner {
                best.insert(mask, plan);
            }
        }
    }
    best.remove(&full)
}

fn greedy_join_order(
    leaves: &[JoinPlan],
    pairs: &[(usize, usize, usize, usize)],
) -> Option<JoinPlan> {
    let n = leaves.len();
    let start = (0..n).min_by(|&a, &b| leaves[a].rows.total_cmp(&leaves[b].rows))?;
    let mut used = BTreeSet::from([start]);
    let mut current = leaves[start].clone();
    while used.len() < n {
        let mut winner: Option<(usize, JoinPlan)> = None;
        for (j, leaf) in leaves.iter().enumerate() {
            if used.contains(&j) {
                continue;
            }
            let cand = connect_joins(&current, leaf, pairs)
                .or_else(|| connect_joins(leaf, &current, pairs));
            if let Some(joined) = cand {
                let better = winner.as_ref().is_none_or(|(_, w)| joined.cost < w.cost);
                if better {
                    winner = Some((j, joined));
                }
            }
        }
        let (j, joined) = winner?;
        used.insert(j);
        current = joined;
    }
    Some(current)
}

fn better_plan(current: Option<JoinPlan>, cand: JoinPlan) -> Option<JoinPlan> {
    match current {
        None => Some(cand),
        Some(old) => {
            if cand.cost < old.cost - 1e-9 {
                Some(cand)
            } else if old.cost < cand.cost - 1e-9 {
                Some(old)
            } else if item_order_key(&cand) < item_order_key(&old) {
                Some(cand)
            } else {
                Some(old)
            }
        }
    }
}

fn item_order_key(plan: &JoinPlan) -> Vec<usize> {
    let mut seen = Vec::new();
    for (item, _) in &plan.cols {
        if seen.last() != Some(item) {
            seen.push(*item);
        }
    }
    seen
}

fn connect_joins(
    left: &JoinPlan,
    right: &JoinPlan,
    pairs: &[(usize, usize, usize, usize)],
) -> Option<JoinPlan> {
    if !left.items.is_disjoint(&right.items) {
        return None;
    }
    let mut eqs = Vec::new();
    for &(ia, ca, ib, cb) in pairs {
        if left.items.contains(&ia) && right.items.contains(&ib) {
            eqs.push((ia, ca, ib, cb));
        } else if left.items.contains(&ib) && right.items.contains(&ia) {
            eqs.push((ib, cb, ia, ca));
        }
    }
    if eqs.is_empty() {
        return None;
    }
    let mut rows = f64::MAX;
    for &(ia, ca, ib, cb) in &eqs {
        let lndv = *left.ndv.get(&(ia, ca)).unwrap_or(&left.rows);
        let rndv = *right.ndv.get(&(ib, cb)).unwrap_or(&right.rows);
        rows = rows.min(inner_equi_rows(left.rows, right.rows, lndv, rndv));
    }
    let rows = rows.max(1.0);
    let cost = left.cost + right.cost + rows;
    let mut on_parts = Vec::new();
    for &(ia, ca, ib, cb) in &eqs {
        let li = left.cols.iter().position(|&k| k == (ia, ca))?;
        let ri = right.cols.iter().position(|&k| k == (ib, cb))?;
        on_parts.push(Expr::Binary(
            BinaryOp::Eq,
            Box::new(Expr::nth(li)),
            Box::new(Expr::nth(left.cols.len() + ri)),
        ));
    }
    let on = and_all(on_parts)?;
    let schema = join_schema(left.plan.schema(), right.plan.schema());
    let plan = LogicalPlan::Join {
        left: Box::new(left.plan.clone()),
        right: Box::new(right.plan.clone()),
        join_type: JoinType::Inner,
        on: Some(on),
        schema,
    };
    let mut items = left.items.clone();
    items.extend(&right.items);
    let mut cols = left.cols.clone();
    cols.extend(right.cols.iter().copied());
    let mut ndv = left.ndv.clone();
    ndv.extend(right.ndv.iter().map(|(k, v)| (*k, *v)));
    for &(ia, ca, ib, cb) in &eqs {
        let joined = *ndv.get(&(ia, ca)).unwrap_or(&rows);
        let joined = joined.min(*ndv.get(&(ib, cb)).unwrap_or(&rows)).min(rows);
        ndv.insert((ia, ca), joined);
        ndv.insert((ib, cb), joined);
    }
    Some(JoinPlan {
        plan,
        items,
        cols,
        ndv,
        rows,
        cost,
    })
}

fn restore_join_output(
    plan: LogicalPlan,
    original: arrow::datatypes::SchemaRef,
    new_cols: &[(usize, usize)],
    orig_cols: &[(usize, usize)],
) -> LogicalPlan {
    if new_cols == orig_cols {
        return plan;
    }
    let exprs = orig_cols
        .iter()
        .enumerate()
        .filter_map(|(i, key)| {
            let j = new_cols.iter().position(|k| k == key)?;
            Some(NamedExpr {
                expr: Expr::nth(j),
                name: original.field(i).name().to_string(),
            })
        })
        .collect::<Vec<_>>();
    if exprs.len() != original.fields().len() {
        return plan;
    }
    LogicalPlan::Project {
        input: Box::new(plan),
        exprs,
        schema: original,
    }
}

fn scan_stats(plan: &LogicalPlan) -> Option<&TableStats> {
    match plan {
        LogicalPlan::Scan { stats, .. } => stats.as_ref(),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. } => scan_stats(input),
        _ => None,
    }
}

fn col_ndv(plan: &LogicalPlan, local: usize) -> Option<f64> {
    match plan {
        LogicalPlan::Scan {
            stats: Some(stats),
            schema,
            ..
        } => stats
            .columns
            .get(schema.field(local).name())
            .and_then(|c| c.ndv)
            .map(|n| n as f64),
        LogicalPlan::Join { left, right, .. } => {
            let ln = left.schema().fields().len();
            if local < ln {
                col_ndv(left, local)
            } else {
                col_ndv(right, local - ln)
            }
        }
        LogicalPlan::Project { input, exprs, .. } => match exprs.get(local)?.expr {
            Expr::Nth(i) => col_ndv(input, i),
            Expr::Column(ref name) => {
                let i = input.schema().index_of(name).ok()?;
                col_ndv(input, i)
            }
            _ => None,
        },
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => col_ndv(input, local),
        _ => None,
    }
}

fn remap_nth(expr: Expr, from_cols: &[(usize, usize)], to_cols: &[(usize, usize)]) -> Expr {
    match expr {
        Expr::Nth(i) if i < from_cols.len() => {
            let key = from_cols[i];
            match to_cols.iter().position(|k| *k == key) {
                Some(j) => Expr::Nth(j),
                None => Expr::Nth(i),
            }
        }
        Expr::Binary(op, l, r) => Expr::Binary(
            op,
            Box::new(remap_nth(*l, from_cols, to_cols)),
            Box::new(remap_nth(*r, from_cols, to_cols)),
        ),
        Expr::Unary(op, c) => Expr::Unary(op, Box::new(remap_nth(*c, from_cols, to_cols))),
        Expr::Cast { expr, data_type } => Expr::Cast {
            expr: Box::new(remap_nth(*expr, from_cols, to_cols)),
            data_type,
        },
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => Expr::Case {
            operand: operand.map(|e| Box::new(remap_nth(*e, from_cols, to_cols))),
            when_then: when_then
                .into_iter()
                .map(|(w, t)| {
                    (
                        remap_nth(w, from_cols, to_cols),
                        remap_nth(t, from_cols, to_cols),
                    )
                })
                .collect(),
            else_expr: else_expr.map(|e| Box::new(remap_nth(*e, from_cols, to_cols))),
        },
        Expr::Coalesce(args) => Expr::Coalesce(
            args.into_iter()
                .map(|e| remap_nth(e, from_cols, to_cols))
                .collect(),
        ),
        Expr::Concat(args) => Expr::Concat(
            args.into_iter()
                .map(|e| remap_nth(e, from_cols, to_cols))
                .collect(),
        ),
        Expr::Substring {
            expr,
            start,
            length,
        } => Expr::Substring {
            expr: Box::new(remap_nth(*expr, from_cols, to_cols)),
            start,
            length,
        },
        Expr::Array(args) => Expr::Array(
            args.into_iter()
                .map(|e| remap_nth(e, from_cols, to_cols))
                .collect(),
        ),
        other => other,
    }
}

fn selectivity(stats: &TableStats, pred: &Expr) -> f64 {
    match pred {
        Expr::Binary(BinaryOp::And, l, r) => selectivity(stats, l) * selectivity(stats, r),
        Expr::Binary(BinaryOp::Or, l, r) => {
            let a = selectivity(stats, l);
            let b = selectivity(stats, r);
            (a + b - a * b).clamp(0.0, 1.0)
        }
        Expr::Binary(op, l, r) => cmp_selectivity(stats, *op, l, r),
        Expr::Unary(UnaryOp::IsNull, e) => col_name_of(e)
            .and_then(|n| stats.columns.get(&n))
            .map(|c| {
                if stats.row_count == 0 {
                    0.0
                } else {
                    c.null_count as f64 / stats.row_count as f64
                }
            })
            .unwrap_or(1.0 / 3.0),
        Expr::Unary(UnaryOp::IsNotNull, e) => {
            1.0 - selectivity(stats, &Expr::Unary(UnaryOp::IsNull, e.clone()))
        }
        _ => 1.0 / 3.0,
    }
}

fn cmp_selectivity(stats: &TableStats, op: BinaryOp, l: &Expr, r: &Expr) -> f64 {
    let (name, lit, swapped) = match (
        col_name_of(l),
        i64_literal(r),
        col_name_of(r),
        i64_literal(l),
    ) {
        (Some(n), Some(v), _, _) => (n, v, false),
        (_, _, Some(n), Some(v)) => (n, v, true),
        _ => return 1.0 / 3.0,
    };
    let Some(col) = stats.columns.get(&name) else {
        return 1.0 / 3.0;
    };
    let op = if swapped { swap_cmp_op(op) } else { op };
    match op {
        BinaryOp::Eq => col
            .ndv
            .map(|n| 1.0 / (n as f64).max(1.0))
            .unwrap_or(1.0 / 3.0),
        BinaryOp::NotEq => {
            1.0 - col
                .ndv
                .map(|n| 1.0 / (n as f64).max(1.0))
                .unwrap_or(1.0 / 3.0)
        }
        BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::Lt | BinaryOp::LtEq => {
            range_selectivity(col, op, lit)
        }
        _ => 1.0 / 3.0,
    }
}

fn range_selectivity(col: &crate::plan::ColumnStats, op: BinaryOp, lit: i64) -> f64 {
    let (Some(min), Some(max)) = (col.min_i64, col.max_i64) else {
        return 1.0 / 3.0;
    };
    let min = min as f64;
    let max = max as f64;
    let lit = lit as f64;
    if max <= min {
        return 1.0 / 3.0;
    }
    let span = max - min;
    let frac = match op {
        BinaryOp::Gt => (max - lit) / span,
        BinaryOp::GtEq => (max - lit + 1.0) / span,
        BinaryOp::Lt => (lit - min) / span,
        BinaryOp::LtEq => (lit - min + 1.0) / span,
        _ => 1.0 / 3.0,
    };
    frac.clamp(0.0, 1.0)
}

fn swap_cmp_op(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        other => other,
    }
}

fn col_name_of(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(n) => Some(n.clone()),
        _ => None,
    }
}

fn i64_literal(expr: &Expr) -> Option<i64> {
    let Expr::Literal(arr) = expr else {
        return None;
    };
    let a = arr.as_any().downcast_ref::<arrow::array::Int64Array>()?;
    if a.len() != 1 || a.is_null(0) {
        return None;
    }
    Some(a.value(0))
}

fn push_limit(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Limit { input, skip, fetch } => match *input {
            LogicalPlan::Project {
                input,
                exprs,
                schema,
            } => LogicalPlan::Project {
                input: Box::new(LogicalPlan::Limit { input, skip, fetch }),
                exprs,
                schema,
            },
            LogicalPlan::Limit {
                input,
                skip: inner_skip,
                fetch: inner_fetch,
            } => merge_limits(*input, inner_skip, inner_fetch, skip, fetch),
            other => LogicalPlan::Limit {
                input: Box::new(other),
                skip,
                fetch,
            },
        },
        other => other,
    }
}

fn merge_limits(
    input: LogicalPlan,
    inner_skip: u64,
    inner_fetch: Option<u64>,
    outer_skip: u64,
    outer_fetch: Option<u64>,
) -> LogicalPlan {
    let skip = inner_skip.saturating_add(outer_skip);
    let fetch = match inner_fetch {
        None => outer_fetch,
        Some(inner) => {
            let remain = inner.saturating_sub(outer_skip);
            Some(match outer_fetch {
                None => remain,
                Some(outer) => remain.min(outer),
            })
        }
    };
    LogicalPlan::Limit {
        input: Box::new(input),
        skip,
        fetch,
    }
}

fn collapse_project(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            if is_identity(&exprs, input.schema()) {
                return *input;
            }
            match *input {
                LogicalPlan::Project {
                    input: inner,
                    exprs: inner_exprs,
                    schema: _,
                } => {
                    let exprs = exprs
                        .into_iter()
                        .map(|mut e| {
                            e.expr = subst_nth(e.expr, &inner_exprs);
                            e
                        })
                        .collect();
                    LogicalPlan::Project {
                        input: inner,
                        exprs,
                        schema,
                    }
                }
                other => LogicalPlan::Project {
                    input: Box::new(other),
                    exprs,
                    schema,
                },
            }
        }
        other => other,
    }
}

fn is_identity(exprs: &[NamedExpr], schema: &arrow::datatypes::SchemaRef) -> bool {
    if exprs.len() != schema.fields().len() {
        return false;
    }
    exprs.iter().enumerate().all(|(i, e)| {
        matches!(&e.expr, Expr::Nth(n) if *n == i) && e.name == schema.field(i).name().as_str()
    })
}

fn subst_nth(expr: Expr, inner: &[NamedExpr]) -> Expr {
    match expr {
        Expr::Nth(i) if i < inner.len() => inner[i].expr.clone(),
        Expr::Binary(op, l, r) => Expr::Binary(
            op,
            Box::new(subst_nth(*l, inner)),
            Box::new(subst_nth(*r, inner)),
        ),
        Expr::Unary(op, c) => Expr::Unary(op, Box::new(subst_nth(*c, inner))),
        Expr::Cast { expr, data_type } => Expr::Cast {
            expr: Box::new(subst_nth(*expr, inner)),
            data_type,
        },
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => Expr::Case {
            operand: operand.map(|e| Box::new(subst_nth(*e, inner))),
            when_then: when_then
                .into_iter()
                .map(|(w, t)| (subst_nth(w, inner), subst_nth(t, inner)))
                .collect(),
            else_expr: else_expr.map(|e| Box::new(subst_nth(*e, inner))),
        },
        Expr::Coalesce(args) => {
            Expr::Coalesce(args.into_iter().map(|e| subst_nth(e, inner)).collect())
        }
        Expr::Concat(args) => Expr::Concat(args.into_iter().map(|e| subst_nth(e, inner)).collect()),
        Expr::Substring {
            expr,
            start,
            length,
        } => Expr::Substring {
            expr: Box::new(subst_nth(*expr, inner)),
            start,
            length,
        },
        Expr::Array(args) => Expr::Array(args.into_iter().map(|e| subst_nth(e, inner)).collect()),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{Catalog, ColumnStats, TableStats, plan_sql};
    use arrow::datatypes::{DataType, Field, Schema};

    fn catalog() -> Catalog {
        let mut c = Catalog::new();
        c.register(
            "t",
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
            ])),
        )
        .unwrap();
        c.register(
            "u",
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("x", DataType::Int64, true),
            ])),
        )
        .unwrap();
        c
    }

    fn explain_sql(sql: &str) -> String {
        let plan = plan_sql(sql, &catalog()).unwrap();
        optimize(plan).unwrap().explain()
    }

    fn apply(sql: &str, rule: Rule) -> String {
        let plan = plan_sql(sql, &catalog()).unwrap();
        transform_up(plan, rule).unwrap().explain()
    }

    fn assert_idempotent(plan: LogicalPlan) {
        let once = optimize(plan.clone()).unwrap();
        let twice = optimize(once.clone()).unwrap();
        assert_eq!(once.explain(), twice.explain(), "rules must be idempotent");
    }

    #[test]
    fn constant_fold_snapshot() {
        let text = apply("SELECT 1 + 2", Rule::ConstantFold);
        assert!(text.contains("Project ["), "{text}");
        assert!(!text.contains("Add"), "{text}");
        assert_idempotent(plan_sql("SELECT 1 + 2", &Catalog::new()).unwrap());
    }

    #[test]
    fn simplify_true_filter_snapshot() {
        let folded = transform_up(
            plan_sql("SELECT a FROM t WHERE 1 = 1 AND a > 1", &catalog()).unwrap(),
            Rule::ConstantFold,
        )
        .unwrap();
        let text = transform_up(folded, Rule::SimplifyBoolean)
            .unwrap()
            .explain();
        assert!(text.contains("Filter"), "{text}");
        assert!(!text.contains("And"), "{text}");
        assert_idempotent(plan_sql("SELECT a FROM t WHERE 1 = 1 AND a > 1", &catalog()).unwrap());
    }

    #[test]
    fn combine_filters_snapshot() {
        let scan = plan_sql("SELECT a FROM t", &catalog()).unwrap();
        let nested = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan),
                predicate: Expr::Binary(
                    BinaryOp::Gt,
                    Box::new(Expr::nth(0)),
                    Box::new(Expr::lit_i64(1)),
                ),
            }),
            predicate: Expr::Binary(
                BinaryOp::Lt,
                Box::new(Expr::nth(0)),
                Box::new(Expr::lit_i64(3)),
            ),
        };
        let text = transform_up(nested.clone(), Rule::CombineFilters)
            .unwrap()
            .explain();
        let filters = text.matches("Filter").count();
        assert_eq!(filters, 1, "{text}");
        assert!(text.contains("And"), "{text}");
        assert_idempotent(nested);
    }

    #[test]
    fn push_limit_through_project_snapshot() {
        let text = apply("SELECT a FROM t LIMIT 10", Rule::PushLimit);
        let limit = text.find("Limit").expect("Limit");
        let project = text.find("Project").expect("Project");
        assert!(limit > project, "limit should sit under project:\n{text}");
        assert_idempotent(plan_sql("SELECT a FROM t LIMIT 10", &catalog()).unwrap());
    }

    #[test]
    fn collapse_identity_project_snapshot() {
        let scan = match plan_sql("SELECT a, b FROM t", &catalog()).unwrap() {
            LogicalPlan::Project { input, .. } => *input,
            other => panic!("{other:?}"),
        };
        let schema = scan.schema().clone();
        let identity = LogicalPlan::Project {
            input: Box::new(scan),
            exprs: vec![
                NamedExpr {
                    expr: Expr::nth(0),
                    name: "a".into(),
                },
                NamedExpr {
                    expr: Expr::nth(1),
                    name: "b".into(),
                },
            ],
            schema,
        };
        let text = transform_up(identity.clone(), Rule::CollapseProject)
            .unwrap()
            .explain();
        assert!(!text.contains("Project"), "{text}");
        assert!(text.contains("Scan t"), "{text}");
        assert_idempotent(identity);
    }

    #[test]
    fn standard_optimize_folds_and_pushes() {
        let text = explain_sql("SELECT a FROM t WHERE 1 + 1 = 2 LIMIT 5");
        assert!(text.contains("Limit"), "{text}");
        assert!(!text.contains("Add"), "{text}");
    }

    #[test]
    fn push_filter_and_projection_into_scan() {
        let text = explain_sql("SELECT a FROM t WHERE a > 1");
        assert!(text.contains("projection=[a]"), "{text}");
        assert!(text.contains("filters=["), "{text}");
        assert!(!text.contains("Filter"), "{text}");
        assert_idempotent(plan_sql("SELECT a FROM t WHERE a > 1", &catalog()).unwrap());
    }

    #[test]
    fn push_join_filter_to_left() {
        let text = explain_sql("SELECT t.a, u.id FROM t JOIN u ON t.a = u.id WHERE t.a > 1");
        assert!(text.contains("Join Inner"), "{text}");
        assert!(
            text.contains("filters=[") || text.contains("Filter"),
            "{text}"
        );
        assert_idempotent(
            plan_sql(
                "SELECT t.a, u.id FROM t JOIN u ON t.a = u.id WHERE t.a > 1",
                &catalog(),
            )
            .unwrap(),
        );
    }

    #[test]
    fn push_coalesce_filter_after_reordering_projection() {
        let text = explain_sql("SELECT a FROM (SELECT b, a FROM t) s WHERE COALESCE(a, 0) > 1");
        assert!(text.contains("filters=["), "{text}");
        assert!(text.contains("a"), "{text}");
        assert!(!text.contains("#1"), "{text}");
        assert_idempotent(
            plan_sql(
                "SELECT a FROM (SELECT b, a FROM t) s WHERE COALESCE(a, 0) > 1",
                &catalog(),
            )
            .unwrap(),
        );
    }

    #[test]
    fn push_join_coalesce_filter_to_right() {
        let text =
            explain_sql("SELECT t.a, u.id FROM t JOIN u ON t.a = u.id WHERE COALESCE(u.x, 0) > 1");
        assert!(text.contains("Join Inner"), "{text}");
        assert!(
            text.contains("filters=[") || text.contains("Filter"),
            "{text}"
        );
        assert!(!text.contains("#2"), "{text}");
        assert_idempotent(
            plan_sql(
                "SELECT t.a, u.id FROM t JOIN u ON t.a = u.id WHERE COALESCE(u.x, 0) > 1",
                &catalog(),
            )
            .unwrap(),
        );
    }

    fn id_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]))
    }

    fn col_stats(ndv: u64, min: i64, max: i64) -> ColumnStats {
        ColumnStats {
            ndv: Some(ndv),
            min_i64: Some(min),
            max_i64: Some(max),
            null_count: 0,
        }
    }

    fn table_stats(rows: u64, ndv: u64, min: i64, max: i64) -> TableStats {
        let mut columns = BTreeMap::new();
        columns.insert("id".into(), col_stats(ndv, min, max));
        TableStats {
            row_count: rows,
            columns,
            ..TableStats::default()
        }
    }

    fn stats_catalog() -> Catalog {
        let mut c = Catalog::new();
        let id = id_schema();
        c.register("fact", id.clone()).unwrap();
        c.register("d0", id.clone()).unwrap();
        c.register("d1", id).unwrap();
        c.set_stats("fact", table_stats(1000, 10, 0, 9)).unwrap();
        c.set_stats("d0", table_stats(10, 10, 0, 9)).unwrap();
        c.set_stats("d1", table_stats(10, 10, 0, 9)).unwrap();
        c
    }

    #[test]
    fn cost_based_join_reorder_puts_small_dims_first() {
        let sql = "SELECT fact.id FROM fact JOIN d1 ON fact.id = d1.id JOIN d0 ON d1.id = d0.id";
        let plan = optimize(plan_sql(sql, &stats_catalog()).unwrap()).unwrap();
        let text = plan.explain();
        let fact = text.find("Scan fact").unwrap_or_else(|| panic!("{text}"));
        let inner_join = text.rfind("Join Inner").unwrap_or_else(|| panic!("{text}"));
        assert!(
            inner_join > fact,
            "small tables should join with each other, not left-deep through fact:\n{text}"
        );
        assert!(text.contains("Scan d0"), "{text}");
        assert!(text.contains("Scan d1"), "{text}");
        assert!(text.contains("rowcount=10"), "{text}");
        assert!(text.contains("rowcount=1000"), "{text}");
        let est = estimate_rows(&plan).expect("stats");
        assert!(
            (est - 1000.0).abs() < 1.0,
            "estimate {est} should track fact cardinality"
        );
        assert_idempotent(plan_sql(sql, &stats_catalog()).unwrap());
    }

    #[test]
    fn same_leaf_on_equality_survives_reorder() {
        let sql = "SELECT fact.id FROM fact JOIN d1 ON fact.id = d1.id AND fact.id = fact.id JOIN d0 ON d1.id = d0.id";
        let plan = optimize(plan_sql(sql, &stats_catalog()).unwrap()).unwrap();
        let text = plan.explain();
        assert!(
            text.contains("id Eq id") || text.contains("#0 Eq #0"),
            "same-leaf ON equality must remain after reorder:\n{text}"
        );
        assert_idempotent(plan_sql(sql, &stats_catalog()).unwrap());
    }

    #[test]
    fn greedy_join_order_matches_dp_on_three_tables() {
        let sql = "SELECT fact.id FROM fact JOIN d1 ON fact.id = d1.id JOIN d0 ON d1.id = d0.id";
        let plan = plan_sql(sql, &stats_catalog()).unwrap();
        let plan = transform_up(plan, Rule::PushProjection).unwrap();
        let mut items = Vec::new();
        collect_join_items(&plan, &mut items);
        if items.len() < 3 {
            let mut items = Vec::new();
            if let LogicalPlan::Project { input, .. } = &plan {
                collect_join_items(input, &mut items);
            }
            assert!(items.len() >= 3, "{}", plan.explain());
        }
        let root = match &plan {
            LogicalPlan::Project { input, .. } => input.as_ref(),
            other => other,
        };
        let mut items = Vec::new();
        collect_join_items(root, &mut items);
        assert_eq!(items.len(), 3, "{}", plan.explain());
        let mut global_eq = Vec::new();
        let mut residuals = Vec::new();
        collect_join_preds(root, 0, &mut global_eq, &mut residuals);
        let _ = residuals;
        let mut global_cols = Vec::new();
        let mut leaves = Vec::new();
        for (idx, item) in items.iter().enumerate() {
            let rows = estimate_rows(item).unwrap();
            let n = item.schema().fields().len();
            let mut ndv = BTreeMap::new();
            let mut cols = Vec::new();
            for local in 0..n {
                global_cols.push((idx, local));
                cols.push((idx, local));
                ndv.insert((idx, local), col_ndv(item, local).unwrap_or(rows));
            }
            leaves.push(JoinPlan {
                plan: item.clone(),
                items: BTreeSet::from([idx]),
                cols,
                ndv,
                rows,
                cost: rows,
            });
        }
        let mut pairs = Vec::new();
        for (a, b) in global_eq {
            let (ia, ca) = global_cols[a];
            let (ib, cb) = global_cols[b];
            pairs.push((ia, ca, ib, cb));
        }
        let dp = dp_join_order(&leaves, &pairs).unwrap();
        let greedy = greedy_join_order(&leaves, &pairs).unwrap();
        assert_eq!(dp.items, greedy.items);
        assert!(
            (dp.rows - greedy.rows).abs() < 1.0,
            "{} vs {}",
            dp.rows,
            greedy.rows
        );
    }

    #[test]
    fn range_selectivity_does_not_overflow_on_extreme_int64() {
        let mut columns = BTreeMap::new();
        columns.insert(
            "id".into(),
            ColumnStats {
                ndv: Some(2),
                min_i64: Some(i64::MIN),
                max_i64: Some(i64::MAX),
                null_count: 0,
            },
        );
        let stats = TableStats {
            row_count: 2,
            columns,
            ..TableStats::default()
        };
        let scan = LogicalPlan::Scan {
            table: "t".into(),
            schema: id_schema(),
            alias: None,
            projection: None,
            filters: vec![Expr::Binary(
                BinaryOp::Gt,
                Box::new(Expr::column("id")),
                Box::new(Expr::lit_i64(0)),
            )],
            stats: Some(stats),
        };
        let rows = estimate_rows(&scan).unwrap();
        assert!(rows.is_finite() && rows >= 1.0, "{rows}");
    }
}
