//! Rule-based logical rewrites.
//!
//! Batches of rules run to a fixed point. Each rule walks the plan bottom-up.
//! Core rules: fold literals, simplify boolean algebra, merge adjacent
//! filters, push limit through projection, and drop identity / nested
//! projections.

use std::sync::Arc;

use arrow::array::{Array, BooleanArray, RecordBatch, RecordBatchOptions};
use arrow::datatypes::Schema;

use crate::batch::Batch;
use crate::error::Result;
use crate::expr::{BinaryOp, Expr, UnaryOp};
use crate::plan::{JoinType, LogicalPlan, NamedExpr};

/// Apply the standard rule batches to `plan`.
pub fn optimize(plan: LogicalPlan) -> Result<LogicalPlan> {
    Optimizer::standard().run(plan)
}

struct Optimizer {
    batches: Vec<RuleBatch>,
}

struct RuleBatch {
    max_iterations: usize,
    rules: Vec<Rule>,
}

#[derive(Clone, Copy)]
enum Rule {
    ConstantFold,
    SimplifyBoolean,
    CombineFilters,
    PushJoinFilters,
    PushScanFilters,
    PushProjection,
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
                    next = transform_up(next, *rule)?;
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
        Expr::Coalesce(args) | Expr::Concat(args) => args.iter().all(is_foldable),
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
        Expr::Coalesce(args) | Expr::Concat(args) => args
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
        other => other,
    }
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
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{Catalog, plan_sql};
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
}
