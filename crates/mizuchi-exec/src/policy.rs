//! Query policy context applied in planning.
//!
//! Off until the embedder sets [`PolicyContext`]. Governed mode refuses a
//! query that has no context. The caller owns the policy document.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};

use mizuchi_core::error::{Error, Result};
use mizuchi_core::expr::{BinaryOp, Expr};
use mizuchi_plan::{AggCall, Catalog, LogicalPlan, NamedExpr, normalize_name, plan_sql_at};

use super::Session;

/// Embedder- or caller-supplied governance for one query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicyContext {
    /// Tenant the token or embedder named.
    pub tenant: String,
    /// Authenticated principal, recorded on the query report.
    pub principal: String,
    /// Acting identity (delegation / actor chain), recorded on the report.
    pub actor: String,
    /// Tables this principal may scan. Empty allows none.
    pub allowed_tables: BTreeSet<String>,
    /// Per-table SQL boolean predicates, bound against the full table schema.
    pub row_filters: BTreeMap<String, String>,
    /// Per-table columns stripped from the planning catalog so `SELECT *`,
    /// joins, and aggregates cannot name them.
    pub column_masks: BTreeMap<String, BTreeSet<String>>,
}

impl PolicyContext {
    /// Identity with no tables, filters, or masks.
    pub fn new(
        tenant: impl Into<String>,
        principal: impl Into<String>,
        actor: impl Into<String>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            principal: principal.into(),
            actor: actor.into(),
            allowed_tables: BTreeSet::new(),
            row_filters: BTreeMap::new(),
            column_masks: BTreeMap::new(),
        }
    }

    /// Permit `table` (name is normalized like the catalog).
    pub fn allow_table(mut self, table: impl Into<String>) -> Self {
        self.allowed_tables.insert(normalize_name(&table.into()));
        self
    }

    /// Attach a SQL boolean that every scan of `table` must satisfy.
    pub fn row_filter(mut self, table: impl Into<String>, predicate: impl Into<String>) -> Self {
        self.row_filters
            .insert(normalize_name(&table.into()), predicate.into());
        self
    }

    /// Hide `column` of `table` from planning.
    pub fn mask_column(mut self, table: impl Into<String>, column: impl Into<String>) -> Self {
        self.column_masks
            .entry(normalize_name(&table.into()))
            .or_default()
            .insert(column.into().to_ascii_lowercase());
        self
    }

    pub(super) fn allows_table(&self, name: &str, table_tenant: Option<&str>) -> bool {
        if !self.allowed_tables.contains(&normalize_name(name)) {
            return false;
        }
        !matches!(table_tenant, Some(t) if t != self.tenant)
    }

    fn masks_for(&self, name: &str) -> BTreeSet<String> {
        self.column_masks
            .get(&normalize_name(name))
            .cloned()
            .unwrap_or_default()
    }
}

impl Session {
    /// Session-level policy applied at plan time. Off until set.
    pub fn with_policy(mut self, policy: PolicyContext) -> Self {
        self.query_policy = Some(policy);
        self
    }

    /// When true, every query needs a [`PolicyContext`].
    pub fn with_governed(mut self, governed: bool) -> Self {
        self.governed = governed;
        self
    }

    /// Bind a registered table to `tenant`. Cross-tenant scans fail.
    pub fn bind_table_tenant(
        &mut self,
        name: impl Into<String>,
        tenant: impl Into<String>,
    ) -> Result<()> {
        let name = normalize_name(&name.into());
        let table = self
            .tables
            .get_mut(&name)
            .ok_or_else(|| Error::Plan(format!("table not found: {name}")))?;
        table.tenant = Some(tenant.into());
        Ok(())
    }

    pub(super) fn require_policy(&self) -> Result<()> {
        if self.governed && self.query_policy.is_none() {
            return Err(Error::Plan(
                "governed mode requires a policy context".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn planning_catalog(&self) -> Result<Catalog> {
        let mut catalog = self.catalog.planning_copy();
        if let Some(policy) = &self.query_policy {
            let mut drop = Vec::new();
            for ns in catalog.list_namespaces() {
                let raw = if ns.catalog == mizuchi_plan::DEFAULT_CATALOG {
                    ns.namespace.clone()
                } else {
                    format!("{}.{}", ns.catalog, ns.namespace)
                };
                let names = catalog.list_tables(Some(&raw))?;
                for table in names {
                    let ident =
                        mizuchi_plan::TableIdent::from_parts(&ns.catalog, &ns.namespace, &table)?;
                    let name = ident.sql_name();
                    let tenant = self.tables.get(&name).and_then(|t| t.tenant.as_deref());
                    if !policy.allows_table(&name, tenant) {
                        drop.push(name);
                    }
                }
            }
            for name in drop {
                catalog.drop_table(name, true)?;
            }
        }
        for (name, table) in &self.tables {
            if let Some(policy) = &self.query_policy
                && !policy.allows_table(name, table.tenant.as_deref())
            {
                continue;
            }
            let schema = match &self.query_policy {
                Some(policy) => visible_schema(&table.schema, &policy.masks_for(name)),
                None => table.schema.clone(),
            };
            if catalog.table(name).is_err() {
                catalog.register(name, schema)?;
            } else {
                catalog.alter_schema(name, schema)?;
            }
            let mut stats = table.stats.clone();
            if stats.partitions.is_empty()
                && let Ok(parts) = catalog.partitions(name)
            {
                stats.partitions = parts;
            }
            catalog.set_stats(name, stats)?;
        }
        Ok(catalog)
    }

    pub(super) fn apply_row_filters(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let Some(policy) = &self.query_policy else {
            return Ok(plan);
        };
        map_scans(plan, |scan| inject_scan_filter(scan, self, policy))
    }

    /// `Scan.projection = None` means every MemTable column. Pin it to the
    /// planning schema so masked fields cannot ride through `SELECT *`.
    pub(super) fn pin_scan_projections(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        if self.query_policy.is_none() {
            return Ok(plan);
        }
        map_scans(plan, |scan| {
            let LogicalPlan::Scan {
                table,
                schema,
                alias,
                projection,
                filters,
                stats,
            } = scan
            else {
                return Ok(scan);
            };
            let projection = Some(match projection {
                Some(names) => names,
                None => schema.fields().iter().map(|f| f.name().clone()).collect(),
            });
            Ok(LogicalPlan::Scan {
                table,
                schema,
                alias,
                projection,
                filters,
                stats,
            })
        })
    }

    pub(super) fn enforce_policy(&self, plan: &LogicalPlan, check_filters: bool) -> Result<()> {
        self.require_policy()?;
        let Some(policy) = &self.query_policy else {
            return Ok(());
        };
        walk_scans(plan, &mut |table, schema, projection, filters| {
            let tenant = self
                .tables
                .get(&normalize_name(table))
                .and_then(|t| t.tenant.as_deref());
            if !policy.allows_table(table, tenant) {
                return Err(Error::Plan(format!(
                    "table {table} is not allowed for tenant {}",
                    policy.tenant
                )));
            }
            let masks = policy.masks_for(table);
            if masks.is_empty() {
                return Ok(());
            }
            let names: Box<dyn Iterator<Item = &str>> = match projection {
                Some(cols) => Box::new(cols.iter().map(String::as_str)),
                None => Box::new(schema.fields().iter().map(|f| f.name().as_str())),
            };
            for col in names {
                if masks.contains(&col.to_ascii_lowercase()) {
                    return Err(Error::Plan(format!("column {col} of {table} is masked")));
                }
            }
            if !check_filters {
                return Ok(());
            }
            let full_schema = self
                .tables
                .get(&normalize_name(table))
                .map(|t| t.schema.as_ref())
                .unwrap_or(schema.as_ref());
            let trusted = trusted_row_filters(self, policy, table);
            for pred in filters {
                if filter_uses_untrusted_mask(
                    pred,
                    &masks,
                    Some(full_schema),
                    &trusted,
                    full_schema,
                ) {
                    return Err(Error::Plan(format!("column of {table} is masked")));
                }
            }
            Ok(())
        })?;
        if check_filters {
            check_parent_preds(plan, self, policy)?;
        }
        Ok(())
    }
}

fn visible_schema(schema: &Schema, masks: &BTreeSet<String>) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .filter(|f| !masks.contains(&f.name().to_ascii_lowercase()))
        .map(|f| f.as_ref().clone())
        .collect();
    Arc::new(Schema::new(fields))
}

fn inject_scan_filter(
    mut scan: LogicalPlan,
    session: &Session,
    policy: &PolicyContext,
) -> Result<LogicalPlan> {
    let LogicalPlan::Scan { table, filters, .. } = &mut scan else {
        return Ok(scan);
    };
    let Some(pred_sql) = policy.row_filters.get(&normalize_name(table)) else {
        return Ok(scan);
    };
    let schema = session
        .tables
        .get(&normalize_name(table))
        .map(|t| t.schema.clone())
        .ok_or_else(|| Error::Plan(format!("table not found: {table}")))?;
    let extra = bind_row_filter(table, schema.clone(), pred_sql, session.query_now_micros())?;
    if extra.is_empty() {
        return Err(Error::Plan(format!(
            "row filter for {table} did not produce a predicate"
        )));
    }
    for pred in extra {
        if !filters
            .iter()
            .any(|existing| expr_equiv(existing, &pred, schema.as_ref()))
        {
            filters.push(pred);
        }
    }
    Ok(scan)
}

fn trusted_row_filters(session: &Session, policy: &PolicyContext, table: &str) -> Vec<Expr> {
    let Some(pred_sql) = policy.row_filters.get(&normalize_name(table)) else {
        return Vec::new();
    };
    let Some(schema) = session
        .tables
        .get(&normalize_name(table))
        .map(|t| t.schema.clone())
    else {
        return Vec::new();
    };
    // NOW()/CURRENT_DATE fold through query_now_micros. Pin them with
    // Session::with_now_micros so logical_plan and execute share a clock.
    bind_row_filter(table, schema, pred_sql, session.query_now_micros()).unwrap_or_default()
}

/// Caller predicates that mention a mask, excluding the embedder's own row filter.
fn filter_uses_untrusted_mask(
    pred: &Expr,
    masks: &BTreeSet<String>,
    nth_schema: Option<&Schema>,
    trusted: &[Expr],
    equiv_schema: &Schema,
) -> bool {
    if trusted.iter().any(|t| expr_equiv(pred, t, equiv_schema)) {
        return false;
    }
    match pred {
        Expr::Binary(BinaryOp::And, left, right) => {
            filter_uses_untrusted_mask(left, masks, nth_schema, trusted, equiv_schema)
                || filter_uses_untrusted_mask(right, masks, nth_schema, trusted, equiv_schema)
        }
        _ => expr_uses_masked(pred, masks, nth_schema),
    }
}

/// Rewrite `Nth` to a named column using `schema` so parent predicates can be
/// checked against table-scoped masks without using that table's ordinals.
fn bind_nth(expr: &Expr, schema: &Schema) -> Expr {
    match expr {
        Expr::Nth(i) => schema
            .fields()
            .get(*i)
            .map(|f| Expr::Column(f.name().clone()))
            .unwrap_or_else(|| expr.clone()),
        Expr::Binary(op, left, right) => Expr::Binary(
            *op,
            Box::new(bind_nth(left, schema)),
            Box::new(bind_nth(right, schema)),
        ),
        Expr::Unary(op, child) => Expr::Unary(*op, Box::new(bind_nth(child, schema))),
        Expr::Cast { expr, data_type } => Expr::Cast {
            expr: Box::new(bind_nth(expr, schema)),
            data_type: data_type.clone(),
        },
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => Expr::Case {
            operand: operand.as_ref().map(|e| Box::new(bind_nth(e, schema))),
            when_then: when_then
                .iter()
                .map(|(w, t)| (bind_nth(w, schema), bind_nth(t, schema)))
                .collect(),
            else_expr: else_expr.as_ref().map(|e| Box::new(bind_nth(e, schema))),
        },
        Expr::Coalesce(items) => {
            Expr::Coalesce(items.iter().map(|e| bind_nth(e, schema)).collect())
        }
        Expr::Concat(items) => Expr::Concat(items.iter().map(|e| bind_nth(e, schema)).collect()),
        Expr::Array(items) => Expr::Array(items.iter().map(|e| bind_nth(e, schema)).collect()),
        Expr::Substring {
            expr,
            start,
            length,
        } => Expr::Substring {
            expr: Box::new(bind_nth(expr, schema)),
            start: *start,
            length: *length,
        },
        Expr::Column(_) | Expr::Literal(_) => expr.clone(),
    }
}

fn expr_equiv(left: &Expr, right: &Expr, schema: &Schema) -> bool {
    match (left, right) {
        (Expr::Literal(a), Expr::Literal(b)) => a.as_ref() == b.as_ref(),
        (Expr::Column(a), Expr::Column(b)) => a.eq_ignore_ascii_case(b),
        (Expr::Nth(i), Expr::Nth(j)) => i == j,
        (Expr::Column(name), Expr::Nth(i)) | (Expr::Nth(i), Expr::Column(name)) => schema
            .fields()
            .get(*i)
            .is_some_and(|f| f.name().eq_ignore_ascii_case(name)),
        (Expr::Binary(op_a, l_a, r_a), Expr::Binary(op_b, l_b, r_b)) if op_a == op_b => {
            let same = expr_equiv(l_a, l_b, schema) && expr_equiv(r_a, r_b, schema);
            same || (matches!(
                op_a,
                BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::And | BinaryOp::Or
            ) && expr_equiv(l_a, r_b, schema)
                && expr_equiv(r_a, l_b, schema))
        }
        (Expr::Unary(op_a, c_a), Expr::Unary(op_b, c_b)) if op_a == op_b => {
            expr_equiv(c_a, c_b, schema)
        }
        (
            Expr::Cast {
                expr: e_a,
                data_type: t_a,
            },
            Expr::Cast {
                expr: e_b,
                data_type: t_b,
            },
        ) if t_a == t_b => expr_equiv(e_a, e_b, schema),
        (Expr::Coalesce(a), Expr::Coalesce(b))
        | (Expr::Concat(a), Expr::Concat(b))
        | (Expr::Array(a), Expr::Array(b))
            if a.len() == b.len() =>
        {
            a.iter().zip(b).all(|(l, r)| expr_equiv(l, r, schema))
        }
        (
            Expr::Case {
                operand: oa,
                when_then: wa,
                else_expr: ea,
            },
            Expr::Case {
                operand: ob,
                when_then: wb,
                else_expr: eb,
            },
        ) if wa.len() == wb.len() => {
            let operand_eq = match (oa, ob) {
                (None, None) => true,
                (Some(a), Some(b)) => expr_equiv(a, b, schema),
                _ => false,
            };
            let when_eq = wa.iter().zip(wb).all(|((la, ra), (lb, rb))| {
                expr_equiv(la, lb, schema) && expr_equiv(ra, rb, schema)
            });
            let else_eq = match (ea, eb) {
                (None, None) => true,
                (Some(a), Some(b)) => expr_equiv(a, b, schema),
                _ => false,
            };
            operand_eq && when_eq && else_eq
        }
        (
            Expr::Substring {
                expr: ea,
                start: sa,
                length: la,
            },
            Expr::Substring {
                expr: eb,
                start: sb,
                length: lb,
            },
        ) if sa == sb && la == lb => expr_equiv(ea, eb, schema),
        _ => false,
    }
}

fn bind_row_filter(
    table: &str,
    schema: SchemaRef,
    pred: &str,
    now_micros: i64,
) -> Result<Vec<Expr>> {
    let mut catalog = Catalog::new();
    catalog.register(table, schema)?;
    let sql = format!("SELECT 1 FROM {} WHERE {pred}", quote_sql_name(table));
    let plan = plan_sql_at(&sql, &catalog, &BTreeMap::new(), now_micros)?;
    collect_predicates(&plan)
}

fn collect_predicates(plan: &LogicalPlan) -> Result<Vec<Expr>> {
    match plan {
        LogicalPlan::Scan { filters, .. } => Ok(filters.clone()),
        LogicalPlan::Filter { input, predicate } => {
            let mut out = collect_predicates(input)?;
            out.push(predicate.clone());
            Ok(out)
        }
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Scalar { input, .. }
        | LogicalPlan::Aggregate { input, .. } => collect_predicates(input),
        _ => Err(Error::Plan("row filter must be a scalar predicate".into())),
    }
}

fn expr_uses_masked(expr: &Expr, masks: &BTreeSet<String>, schema: Option<&Schema>) -> bool {
    if masks.is_empty() {
        return false;
    }
    match expr {
        Expr::Column(name) => masks.contains(&name.to_ascii_lowercase()),
        Expr::Nth(i) => schema
            .and_then(|s| s.fields().get(*i))
            .is_some_and(|f| masks.contains(&f.name().to_ascii_lowercase())),
        Expr::Literal(_) => false,
        Expr::Binary(_, l, r) => {
            expr_uses_masked(l, masks, schema) || expr_uses_masked(r, masks, schema)
        }
        Expr::Unary(_, c) | Expr::Cast { expr: c, .. } | Expr::Substring { expr: c, .. } => {
            expr_uses_masked(c, masks, schema)
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand
                .as_deref()
                .is_some_and(|e| expr_uses_masked(e, masks, schema))
                || when_then.iter().any(|(w, t)| {
                    expr_uses_masked(w, masks, schema) || expr_uses_masked(t, masks, schema)
                })
                || else_expr
                    .as_deref()
                    .is_some_and(|e| expr_uses_masked(e, masks, schema))
        }
        Expr::Coalesce(items) | Expr::Concat(items) | Expr::Array(items) => {
            items.iter().any(|e| expr_uses_masked(e, masks, schema))
        }
    }
}

fn quote_sql_name(name: &str) -> String {
    name.split('.')
        .map(|p| format!("\"{}\"", p.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(".")
}

fn map_scans(
    plan: LogicalPlan,
    f: impl Fn(LogicalPlan) -> Result<LogicalPlan> + Copy,
) -> Result<LogicalPlan> {
    match plan {
        scan @ LogicalPlan::Scan { .. } => f(scan),
        LogicalPlan::Filter { input, predicate } => Ok(LogicalPlan::Filter {
            input: Box::new(map_scans(*input, f)?),
            predicate,
        }),
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => Ok(LogicalPlan::Project {
            input: Box::new(map_scans(*input, f)?),
            exprs,
            schema,
        }),
        LogicalPlan::Aggregate {
            input,
            group,
            aggs,
            schema,
        } => Ok(LogicalPlan::Aggregate {
            input: Box::new(map_scans(*input, f)?),
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
            left: Box::new(map_scans(*left, f)?),
            right: Box::new(map_scans(*right, f)?),
            join_type,
            on,
            schema,
        }),
        LogicalPlan::Sort { input, order } => Ok(LogicalPlan::Sort {
            input: Box::new(map_scans(*input, f)?),
            order,
        }),
        LogicalPlan::Limit { input, skip, fetch } => Ok(LogicalPlan::Limit {
            input: Box::new(map_scans(*input, f)?),
            skip,
            fetch,
        }),
        LogicalPlan::Union {
            inputs,
            schema,
            all,
        } => Ok(LogicalPlan::Union {
            inputs: inputs
                .into_iter()
                .map(|p| map_scans(p, f))
                .collect::<Result<Vec<_>>>()?,
            schema,
            all,
        }),
        LogicalPlan::Intersect {
            left,
            right,
            schema,
            all,
        } => Ok(LogicalPlan::Intersect {
            left: Box::new(map_scans(*left, f)?),
            right: Box::new(map_scans(*right, f)?),
            schema,
            all,
        }),
        LogicalPlan::Except {
            left,
            right,
            schema,
            all,
        } => Ok(LogicalPlan::Except {
            left: Box::new(map_scans(*left, f)?),
            right: Box::new(map_scans(*right, f)?),
            schema,
            all,
        }),
        LogicalPlan::Window {
            input,
            partition,
            order,
            frame,
            fns,
            schema,
        } => Ok(LogicalPlan::Window {
            input: Box::new(map_scans(*input, f)?),
            partition,
            order,
            frame,
            fns,
            schema,
        }),
        LogicalPlan::Recursive {
            name,
            anchors,
            step,
            schema,
        } => Ok(LogicalPlan::Recursive {
            name,
            anchors: anchors
                .into_iter()
                .map(|p| map_scans(p, f))
                .collect::<Result<Vec<_>>>()?,
            step: Box::new(map_scans(*step, f)?),
            schema,
        }),
        LogicalPlan::Scalar { input, schema } => Ok(LogicalPlan::Scalar {
            input: Box::new(map_scans(*input, f)?),
            schema,
        }),
        other => Ok(other),
    }
}

fn walk_scans(
    plan: &LogicalPlan,
    f: &mut impl FnMut(&str, &SchemaRef, Option<&Vec<String>>, &[Expr]) -> Result<()>,
) -> Result<()> {
    match plan {
        LogicalPlan::Scan {
            table,
            schema,
            projection,
            filters,
            ..
        } => f(table, schema, projection.as_ref(), filters),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Scalar { input, .. } => walk_scans(input, f),
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::Intersect { left, right, .. }
        | LogicalPlan::Except { left, right, .. } => {
            walk_scans(left, f)?;
            walk_scans(right, f)
        }
        LogicalPlan::Union { inputs, .. } => {
            for p in inputs {
                walk_scans(p, f)?;
            }
            Ok(())
        }
        LogicalPlan::Recursive { anchors, step, .. } => {
            for p in anchors {
                walk_scans(p, f)?;
            }
            walk_scans(step, f)
        }
        LogicalPlan::Empty { .. } | LogicalPlan::WorkScan { .. } => Ok(()),
    }
}

fn check_parent_preds(plan: &LogicalPlan, session: &Session, policy: &PolicyContext) -> Result<()> {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            if pred_uses_mask_on(predicate, input, input.schema(), session, policy) {
                return Err(Error::Plan("column is masked".into()));
            }
            check_parent_preds(input, session, policy)
        }
        LogicalPlan::Join {
            left, right, on, ..
        } => {
            if let Some(on) = on
                && join_pred_uses_mask(
                    on,
                    left.schema().fields().len(),
                    left,
                    right,
                    session,
                    policy,
                )
            {
                return Err(Error::Plan("column is masked".into()));
            }
            check_parent_preds(left, session, policy)?;
            check_parent_preds(right, session, policy)
        }
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Scalar { input, .. } => check_parent_preds(input, session, policy),
        LogicalPlan::Intersect { left, right, .. } | LogicalPlan::Except { left, right, .. } => {
            check_parent_preds(left, session, policy)?;
            check_parent_preds(right, session, policy)
        }
        LogicalPlan::Union { inputs, .. } => {
            for p in inputs {
                check_parent_preds(p, session, policy)?;
            }
            Ok(())
        }
        LogicalPlan::Recursive { anchors, step, .. } => {
            for p in anchors {
                check_parent_preds(p, session, policy)?;
            }
            check_parent_preds(step, session, policy)
        }
        LogicalPlan::Scan { .. } | LogicalPlan::Empty { .. } | LogicalPlan::WorkScan { .. } => {
            Ok(())
        }
    }
}

fn pred_uses_mask_on(
    pred: &Expr,
    input: &LogicalPlan,
    schema: &Schema,
    session: &Session,
    policy: &PolicyContext,
) -> bool {
    match input {
        LogicalPlan::Scan { table, .. } => {
            let masks = policy.masks_for(table);
            let trusted = trusted_row_filters(session, policy, table);
            let full = session
                .tables
                .get(&normalize_name(table))
                .map(|t| t.schema.as_ref())
                .unwrap_or(schema);
            let bound = bind_nth(pred, schema);
            filter_uses_untrusted_mask(&bound, &masks, None, &trusted, full)
        }
        LogicalPlan::Join { left, right, .. } => join_pred_uses_mask(
            pred,
            left.schema().fields().len(),
            left,
            right,
            session,
            policy,
        ),
        LogicalPlan::Project { input, exprs, .. } => {
            let rewritten = subst_outputs(pred, exprs);
            pred_uses_mask_on(&rewritten, input, input.schema(), session, policy)
        }
        LogicalPlan::Aggregate {
            input, group, aggs, ..
        } => {
            let outputs: Vec<NamedExpr> = group
                .iter()
                .cloned()
                .chain(aggs.iter().map(|(name, call)| NamedExpr {
                    expr: agg_arg(call).cloned().unwrap_or_else(|| Expr::lit_i64(0)),
                    name: name.clone(),
                }))
                .collect();
            let rewritten = subst_outputs(pred, &outputs);
            pred_uses_mask_on(&rewritten, input, input.schema(), session, policy)
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Scalar { input, .. } => {
            pred_uses_mask_on(pred, input, schema, session, policy)
        }
        _ => false,
    }
}

fn agg_arg(call: &AggCall) -> Option<&Expr> {
    match call {
        AggCall::CountStar => None,
        AggCall::Count(e)
        | AggCall::CountDistinct(e)
        | AggCall::Sum(e)
        | AggCall::Avg(e)
        | AggCall::Min(e)
        | AggCall::Max(e) => Some(e),
        AggCall::User { arg, .. } => Some(arg),
    }
}

/// Replace output Column/Nth with the expression that produced that field.
fn subst_outputs(pred: &Expr, outputs: &[NamedExpr]) -> Expr {
    match pred {
        Expr::Column(name) => outputs
            .iter()
            .find(|o| o.name.eq_ignore_ascii_case(name))
            .map(|o| o.expr.clone())
            .unwrap_or_else(|| pred.clone()),
        Expr::Nth(i) => outputs
            .get(*i)
            .map(|o| o.expr.clone())
            .unwrap_or_else(|| pred.clone()),
        Expr::Binary(op, left, right) => Expr::Binary(
            *op,
            Box::new(subst_outputs(left, outputs)),
            Box::new(subst_outputs(right, outputs)),
        ),
        Expr::Unary(op, child) => Expr::Unary(*op, Box::new(subst_outputs(child, outputs))),
        Expr::Cast { expr, data_type } => Expr::Cast {
            expr: Box::new(subst_outputs(expr, outputs)),
            data_type: data_type.clone(),
        },
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => Expr::Case {
            operand: operand
                .as_ref()
                .map(|e| Box::new(subst_outputs(e, outputs))),
            when_then: when_then
                .iter()
                .map(|(w, t)| (subst_outputs(w, outputs), subst_outputs(t, outputs)))
                .collect(),
            else_expr: else_expr
                .as_ref()
                .map(|e| Box::new(subst_outputs(e, outputs))),
        },
        Expr::Coalesce(items) => {
            Expr::Coalesce(items.iter().map(|e| subst_outputs(e, outputs)).collect())
        }
        Expr::Concat(items) => {
            Expr::Concat(items.iter().map(|e| subst_outputs(e, outputs)).collect())
        }
        Expr::Array(items) => {
            Expr::Array(items.iter().map(|e| subst_outputs(e, outputs)).collect())
        }
        Expr::Substring {
            expr,
            start,
            length,
        } => Expr::Substring {
            expr: Box::new(subst_outputs(expr, outputs)),
            start: *start,
            length: *length,
        },
        Expr::Literal(_) => pred.clone(),
    }
}

fn join_pred_uses_mask(
    pred: &Expr,
    left_width: usize,
    left: &LogicalPlan,
    right: &LogicalPlan,
    session: &Session,
    policy: &PolicyContext,
) -> bool {
    match pred {
        Expr::Nth(i) => {
            let src = if *i < left_width {
                source_field(left, *i)
            } else {
                source_field(right, *i - left_width)
            };
            match src {
                Some((table, col)) => policy.masks_for(&table).contains(&col.to_ascii_lowercase()),
                None => false,
            }
        }
        Expr::Column(name) => {
            let name = name.to_ascii_lowercase();
            let left_has = descendant_has_column(left, session, &name);
            let right_has = descendant_has_column(right, session, &name);
            match (left_has, right_has) {
                (true, false) => descendant_masks(left, policy).contains(&name),
                (false, true) => descendant_masks(right, policy).contains(&name),
                _ => false,
            }
        }
        Expr::Binary(_, l, r) => {
            join_pred_uses_mask(l, left_width, left, right, session, policy)
                || join_pred_uses_mask(r, left_width, left, right, session, policy)
        }
        Expr::Unary(_, c) | Expr::Cast { expr: c, .. } | Expr::Substring { expr: c, .. } => {
            join_pred_uses_mask(c, left_width, left, right, session, policy)
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand
                .as_deref()
                .is_some_and(|e| join_pred_uses_mask(e, left_width, left, right, session, policy))
                || when_then.iter().any(|(w, t)| {
                    join_pred_uses_mask(w, left_width, left, right, session, policy)
                        || join_pred_uses_mask(t, left_width, left, right, session, policy)
                })
                || else_expr.as_deref().is_some_and(|e| {
                    join_pred_uses_mask(e, left_width, left, right, session, policy)
                })
        }
        Expr::Coalesce(items) | Expr::Concat(items) | Expr::Array(items) => items
            .iter()
            .any(|e| join_pred_uses_mask(e, left_width, left, right, session, policy)),
        Expr::Literal(_) => false,
    }
}

fn source_field(plan: &LogicalPlan, ordinal: usize) -> Option<(String, String)> {
    match plan {
        LogicalPlan::Scan { table, schema, .. } => schema
            .fields()
            .get(ordinal)
            .map(|f| (table.clone(), f.name().clone())),
        LogicalPlan::Join { left, right, .. } => {
            let left_width = left.schema().fields().len();
            if ordinal < left_width {
                source_field(left, ordinal)
            } else {
                source_field(right, ordinal - left_width)
            }
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Scalar { input, .. } => source_field(input, ordinal),
        LogicalPlan::Project { input, exprs, .. } => {
            let expr = &exprs.get(ordinal)?.expr;
            match expr {
                Expr::Column(name) => {
                    let idx = input
                        .schema()
                        .fields()
                        .iter()
                        .position(|f| f.name().eq_ignore_ascii_case(name))?;
                    source_field(input, idx)
                }
                Expr::Nth(i) => source_field(input, *i),
                _ => None,
            }
        }
        LogicalPlan::Aggregate { input, group, .. } => {
            let expr = &group.get(ordinal)?.expr;
            match expr {
                Expr::Column(name) => {
                    let idx = input
                        .schema()
                        .fields()
                        .iter()
                        .position(|f| f.name().eq_ignore_ascii_case(name))?;
                    source_field(input, idx)
                }
                Expr::Nth(i) => source_field(input, *i),
                _ => None,
            }
        }
        LogicalPlan::Union { inputs, .. } => source_field(inputs.first()?, ordinal),
        LogicalPlan::Intersect { left, .. } | LogicalPlan::Except { left, .. } => {
            source_field(left, ordinal)
        }
        LogicalPlan::Empty { .. }
        | LogicalPlan::WorkScan { .. }
        | LogicalPlan::Recursive { .. } => None,
    }
}

fn descendant_masks(plan: &LogicalPlan, policy: &PolicyContext) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let _ = walk_scans(plan, &mut |table, _, _, _| {
        out.extend(policy.masks_for(table));
        Ok(())
    });
    out
}

fn descendant_has_column(plan: &LogicalPlan, session: &Session, name: &str) -> bool {
    let mut found = false;
    let _ = walk_scans(plan, &mut |table, schema, _, _| {
        let schema = session
            .tables
            .get(&normalize_name(table))
            .map(|t| t.schema.as_ref())
            .unwrap_or(schema.as_ref());
        if schema
            .fields()
            .iter()
            .any(|f| f.name().eq_ignore_ascii_case(name))
        {
            found = true;
        }
        Ok(())
    });
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::QueryReport;
    use arrow::array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, TimeUnit};
    use mizuchi_core::batch::Batch;
    use mizuchi_core::expr::{BinaryOp, Expr};

    fn employees() -> (SchemaRef, Vec<Batch>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("tenant", DataType::Utf8, true),
            Field::new("dept", DataType::Int64, true),
            Field::new("salary", DataType::Int64, true),
        ]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("a"), Some("b")])),
                Arc::new(Int64Array::from(vec![Some(10), Some(10), Some(20)])),
                Arc::new(Int64Array::from(vec![Some(100), Some(200), Some(999)])),
            ],
        )
        .unwrap();
        (schema, vec![batch])
    }

    fn depts() -> (SchemaRef, Vec<Batch>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(10), Some(20)])),
                Arc::new(StringArray::from(vec![Some("eng"), Some("hr")])),
            ],
        )
        .unwrap();
        (schema, vec![batch])
    }

    fn policy_a() -> PolicyContext {
        PolicyContext::new("a", "alice", "job")
            .allow_table("employees")
            .allow_table("depts")
            .row_filter("employees", "tenant = 'a'")
            .mask_column("employees", "salary")
    }

    fn session_ab() -> Session {
        let (es, eb) = employees();
        let (ds, db) = depts();
        let mut s = Session::new();
        s.register("employees", es, eb).unwrap();
        s.bind_table_tenant("employees", "a").unwrap();
        s.register("depts", ds, db).unwrap();
        s
    }

    fn col_names(batches: &[Batch]) -> Vec<String> {
        batches
            .first()
            .map(|b| {
                b.schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn col_i64(batches: &[Batch], name: &str) -> Vec<Option<i64>> {
        let mut out = Vec::new();
        for batch in batches {
            let col = batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..col.len() {
                out.push(if col.is_null(i) {
                    None
                } else {
                    Some(col.value(i))
                });
            }
        }
        out
    }

    #[test]
    fn tenant_a_cannot_read_tenant_b_table() {
        let (schema, batches) = employees();
        let mut s = Session::new()
            .with_policy(PolicyContext::new("a", "alice", "alice").allow_table("secrets"));
        s.register("secrets", schema, batches).unwrap();
        s.bind_table_tenant("secrets", "b").unwrap();
        let err = s.execute_sql("SELECT id FROM secrets").unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
        assert!(
            err.to_string().contains("secrets") || err.to_string().contains("not found"),
            "{err}"
        );
    }

    #[test]
    fn masked_column_never_appears_star_join_or_aggregate() {
        let s = session_ab().with_policy(policy_a());
        let star = s.execute_sql("SELECT * FROM employees").unwrap();
        assert!(
            !col_names(&star).iter().any(|c| c == "salary"),
            "{:?}",
            col_names(&star)
        );
        assert_eq!(col_i64(&star, "id"), vec![Some(1), Some(2)]);

        let join = s
            .execute_sql(
                "SELECT employees.id, depts.name FROM employees JOIN depts ON employees.dept = depts.id ORDER BY id",
            )
            .unwrap();
        assert!(!col_names(&join).iter().any(|c| c == "salary"));
        assert_eq!(col_i64(&join, "id"), vec![Some(1), Some(2)]);

        let s = session_ab().with_policy(
            PolicyContext::new("a", "alice", "job")
                .allow_table("employees")
                .allow_table("depts")
                .row_filter("employees", "tenant = 'a'")
                .mask_column("employees", "id")
                .mask_column("employees", "salary"),
        );
        let join = s
            .execute_sql(
                "SELECT employees.dept, depts.name FROM employees JOIN depts ON employees.dept = depts.id ORDER BY dept",
            )
            .unwrap();
        assert!(!col_names(&join).iter().any(|c| c == "id" || c == "salary"));
        assert_eq!(col_i64(&join, "dept"), vec![Some(10), Some(10)]);

        let nested = s
            .execute_sql(
                "SELECT d.name FROM employees e JOIN depts d ON e.dept = d.id JOIN depts d2 ON d.id = d2.id",
            )
            .unwrap();
        assert_eq!(nested.first().map(|b| b.num_rows()).unwrap_or(0), 2);

        let err = s
            .execute_sql("SELECT SUM(salary) FROM employees")
            .unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
        let err = s.execute_sql("SELECT salary FROM employees").unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
    }

    #[test]
    fn governed_mode_without_policy_is_refused() {
        let s = session_ab().with_governed(true);
        let err = s.execute_sql("SELECT id FROM employees").unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
        assert!(err.to_string().contains("policy"), "{err}");
    }

    #[test]
    fn report_names_principal_and_actor() {
        let s = session_ab().with_policy(policy_a());
        let QueryReport {
            principal, actor, ..
        } = s
            .execute_sql_reported("SELECT id FROM employees")
            .unwrap()
            .report;
        assert_eq!(principal.as_deref(), Some("alice"));
        assert_eq!(actor.as_deref(), Some("job"));
    }

    #[test]
    fn row_filter_may_use_a_masked_column() {
        let s = session_ab().with_policy(
            PolicyContext::new("a", "alice", "alice")
                .allow_table("employees")
                .row_filter("employees", "tenant = 'a'")
                .mask_column("employees", "tenant")
                .mask_column("employees", "salary"),
        );
        let out = s.execute_sql("SELECT id FROM employees").unwrap();
        assert_eq!(col_i64(&out, "id"), vec![Some(1), Some(2)]);
        assert!(!col_names(&out).iter().any(|c| c == "tenant"));

        let plan = s.logical_plan("SELECT id FROM employees").unwrap();
        let out = s.execute(&plan).unwrap();
        assert_eq!(col_i64(&out, "id"), vec![Some(1), Some(2)]);
    }

    #[test]
    fn supplied_plan_cannot_filter_on_masked_column() {
        let s = session_ab().with_policy(policy_a());
        let schema = s.table_data("employees").unwrap().0;
        let plan = LogicalPlan::Scan {
            table: "employees".into(),
            schema: schema.clone(),
            alias: None,
            projection: Some(vec!["id".into()]),
            filters: vec![Expr::Binary(
                BinaryOp::Gt,
                Box::new(Expr::column("salary")),
                Box::new(Expr::lit_i64(0)),
            )],
            stats: None,
        };
        let err = s.execute(&plan).unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
        assert!(err.to_string().contains("mask"), "{err}");

        let scan = LogicalPlan::Scan {
            table: "employees".into(),
            schema: schema.clone(),
            alias: None,
            projection: Some(vec!["id".into()]),
            filters: vec![],
            stats: None,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: Expr::Binary(
                BinaryOp::Gt,
                Box::new(Expr::column("salary")),
                Box::new(Expr::lit_i64(100)),
            ),
        };
        let err = s.execute(&plan).unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
        assert!(err.to_string().contains("mask"), "{err}");

        let scan = LogicalPlan::Scan {
            table: "employees".into(),
            schema: schema.clone(),
            alias: None,
            projection: Some(vec!["id".into()]),
            filters: vec![],
            stats: None,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: Expr::Binary(
                BinaryOp::Gt,
                Box::new(Expr::Nth(3)),
                Box::new(Expr::lit_i64(100)),
            ),
        };
        let err = s.execute(&plan).unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
        assert!(err.to_string().contains("mask"), "{err}");
    }

    #[test]
    fn projected_alias_matching_a_mask_is_not_the_masked_column() {
        let s = session_ab().with_policy(policy_a());
        let out = s
            .execute_sql(
                "SELECT salary FROM (SELECT id AS salary FROM employees) e WHERE salary > 1",
            )
            .unwrap();
        assert_eq!(col_i64(&out, "salary"), vec![Some(2)]);

        let out = s
            .execute_sql("SELECT SUM(id) AS salary FROM employees HAVING salary > 0")
            .unwrap();
        assert_eq!(col_i64(&out, "salary"), vec![Some(3)]);

        let schema = s.table_data("employees").unwrap().0;
        let scan = LogicalPlan::Scan {
            table: "employees".into(),
            schema: schema.clone(),
            alias: None,
            projection: Some(vec!["id".into()]),
            filters: vec![],
            stats: None,
        };
        let id_field = schema.field_with_name("id").unwrap().clone();
        let project = LogicalPlan::Project {
            input: Box::new(scan),
            exprs: vec![NamedExpr {
                expr: Expr::column("salary"),
                name: "id".into(),
            }],
            schema: Arc::new(Schema::new(vec![id_field])),
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(project),
            predicate: Expr::Binary(
                BinaryOp::Gt,
                Box::new(Expr::column("id")),
                Box::new(Expr::lit_i64(0)),
            ),
        };
        let err = s.execute(&plan).unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
        assert!(err.to_string().contains("mask"), "{err}");
    }

    #[test]
    fn subquery_row_filter_is_refused() {
        let s = session_ab().with_policy(
            PolicyContext::new("a", "alice", "alice")
                .allow_table("employees")
                .row_filter(
                    "employees",
                    "id IN (SELECT id FROM employees WHERE tenant = 'a')",
                ),
        );
        let err = s.execute_sql("SELECT id FROM employees").unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
        assert!(
            err.to_string().contains("scalar predicate") || err.to_string().contains("row filter"),
            "{err}"
        );
    }

    #[test]
    fn row_filter_now_uses_session_clock() {
        const NOW: i64 = 1_577_836_800_000_000; // 2020-01-01 00:00:00 UTC
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new(
                "as_of",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(NOW - 1),
                    Some(NOW + 1),
                ])),
            ],
        )
        .unwrap();
        let mut s = Session::new().with_now_micros(NOW).with_policy(
            PolicyContext::new("a", "alice", "alice")
                .allow_table("events")
                .row_filter("events", "as_of <= NOW()"),
        );
        s.register("events", schema, vec![batch]).unwrap();
        let out = s.execute_sql("SELECT id FROM events").unwrap();
        assert_eq!(col_i64(&out, "id"), vec![Some(1)]);
        let plan = s.logical_plan("SELECT id FROM events").unwrap();
        let out = s.execute(&plan).unwrap();
        assert_eq!(col_i64(&out, "id"), vec![Some(1)]);
    }

    #[test]
    fn execute_logical_plan_accepts_coalesce_and_now_row_filters() {
        let s = session_ab().with_policy(
            PolicyContext::new("a", "alice", "alice")
                .allow_table("employees")
                .row_filter("employees", "COALESCE(tenant, '') = 'a'")
                .mask_column("employees", "tenant"),
        );
        let plan = s.logical_plan("SELECT id FROM employees").unwrap();
        let out = s.execute(&plan).unwrap();
        assert_eq!(col_i64(&out, "id"), vec![Some(1), Some(2)]);

        const NOW: i64 = 1_577_836_800_000_000;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new(
                "as_of",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1)])),
                Arc::new(TimestampMicrosecondArray::from(vec![Some(0)])),
            ],
        )
        .unwrap();
        let mut s = Session::new().with_now_micros(NOW).with_policy(
            PolicyContext::new("a", "alice", "alice")
                .allow_table("events")
                .row_filter("events", "as_of <= NOW()")
                .mask_column("events", "as_of"),
        );
        s.register("events", schema.clone(), vec![batch]).unwrap();
        let plan = s.logical_plan("SELECT id FROM events").unwrap();
        let out = s.execute(&plan).unwrap();
        assert_eq!(col_i64(&out, "id"), vec![Some(1)]);

        let scan = LogicalPlan::Scan {
            table: "events".into(),
            schema,
            alias: None,
            projection: Some(vec!["id".into()]),
            filters: vec![],
            stats: None,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: Expr::Binary(
                BinaryOp::LtEq,
                Box::new(Expr::column("as_of")),
                Box::new(Expr::Literal(Arc::new(TimestampMicrosecondArray::from(
                    vec![Some(0)],
                )))),
            ),
        };
        let err = s.execute(&plan).unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
        assert!(err.to_string().contains("mask"), "{err}");
    }

    #[test]
    fn merge_is_refused_when_policy_is_set() {
        let mut s = session_ab().with_policy(policy_a());
        let err = s
            .merge_sql(
                "MERGE INTO employees t USING depts s ON t.dept = s.id WHEN MATCHED THEN DELETE",
            )
            .unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
        assert!(err.to_string().contains("MERGE"), "{err}");
    }

    #[test]
    fn off_by_default_keeps_current_behavior() {
        let s = session_ab();
        let out = s.execute_sql("SELECT salary FROM employees").unwrap();
        assert_eq!(
            col_i64(&out, "salary"),
            vec![Some(100), Some(200), Some(999)]
        );
    }
}
