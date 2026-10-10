//! Bind parsed SQL to a resolved [`LogicalPlan`].

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use sqlparser::ast::{
    BinaryOperator, CastKind, Distinct, DuplicateTreatment, Expr as SqlExpr, Function, FunctionArg,
    FunctionArgExpr, FunctionArguments, GroupByExpr, Ident, JoinConstraint, JoinOperator,
    LimitClause, ObjectName, ObjectNamePart, Offset, OrderByKind, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, SetOperator, SetQuantifier, Statement, TableFactor,
    TableWithJoins, UnaryOperator, Value, WildcardAdditionalOptions,
};

use super::{
    AggCall, Catalog, JoinType, LogicalPlan, NamedExpr, SortExpr, expr_type, join_schema,
    parse_statements, require_boolean, schema_from_fields,
};
use crate::error::{Error, Result};
use crate::expr::{BinaryOp, Expr, UnaryOp};

#[derive(Clone)]
struct ScopeCol {
    qualifier: Option<String>,
    name: String,
    index: usize,
}

#[derive(Clone)]
struct Scope {
    columns: Vec<ScopeCol>,
    schema: SchemaRef,
}

impl Scope {
    fn from_schema(qualifier: Option<String>, schema: SchemaRef) -> Self {
        let columns = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| ScopeCol {
                qualifier: qualifier.clone(),
                name: field.name().clone(),
                index,
            })
            .collect();
        Self { columns, schema }
    }

    fn join(left: Scope, right: Scope) -> Self {
        let schema = join_schema(&left.schema, &right.schema);
        let mut columns = left.columns;
        let offset = columns.len();
        for mut col in right.columns {
            col.index += offset;
            columns.push(col);
        }
        Self { columns, schema }
    }
}

/// Parse and bind one SQL statement against `catalog`.
pub fn plan_sql(sql: &str, catalog: &Catalog) -> Result<LogicalPlan> {
    let stmt = parse_statements(sql)?;
    match stmt {
        Statement::Query(query) => bind_query(&query, catalog),
        other => Err(Error::NotImplemented(statement_kind(&other))),
    }
}

fn statement_kind(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Insert(_) => "INSERT",
        Statement::Query(_) => "query",
        _ => "statement",
    }
}

fn bind_query(query: &Query, catalog: &Catalog) -> Result<LogicalPlan> {
    reject_query_extras(query)?;
    let mut plan = bind_set_expr(&query.body, catalog)?;
    if let Some(order_by) = &query.order_by {
        plan = bind_order_by(plan, order_by)?;
    }
    if let Some(limit) = &query.limit_clause {
        plan = bind_limit(plan, limit)?;
    }
    Ok(plan)
}

fn reject_query_extras(query: &Query) -> Result<()> {
    if query.with.is_some() {
        return Err(Error::NotImplemented("WITH"));
    }
    if query.fetch.is_some() {
        return Err(Error::NotImplemented("FETCH"));
    }
    if !query.locks.is_empty() {
        return Err(Error::NotImplemented("FOR UPDATE/SHARE"));
    }
    if query.for_clause.is_some() {
        return Err(Error::NotImplemented("FOR XML/JSON"));
    }
    if query.settings.is_some() {
        return Err(Error::NotImplemented("SETTINGS"));
    }
    if query.format_clause.is_some() {
        return Err(Error::NotImplemented("FORMAT"));
    }
    if !query.pipe_operators.is_empty() {
        return Err(Error::NotImplemented("pipe operators"));
    }
    Ok(())
}

fn bind_set_expr(expr: &SetExpr, catalog: &Catalog) -> Result<LogicalPlan> {
    match expr {
        SetExpr::Select(select) => bind_select(select, catalog),
        SetExpr::Query(query) => bind_query(query, catalog),
        SetExpr::SetOperation {
            left,
            op,
            set_quantifier,
            right,
        } => {
            if *op != SetOperator::Union {
                return Err(Error::NotImplemented("EXCEPT/INTERSECT"));
            }
            let all = match set_quantifier {
                SetQuantifier::All => true,
                SetQuantifier::None | SetQuantifier::Distinct => false,
                _ => return Err(Error::NotImplemented("UNION BY NAME")),
            };
            let left = bind_set_expr(left, catalog)?;
            let right = bind_set_expr(right, catalog)?;
            bind_union(left, right, all)
        }
        SetExpr::Values(_) => Err(Error::NotImplemented("VALUES")),
        _ => Err(Error::NotImplemented("set expression")),
    }
}

fn bind_union(left: LogicalPlan, right: LogicalPlan, all: bool) -> Result<LogicalPlan> {
    if left.schema().fields().len() != right.schema().fields().len() {
        return Err(Error::Plan(
            "UNION inputs must have the same number of columns".into(),
        ));
    }
    for (l, r) in left.schema().fields().iter().zip(right.schema().fields()) {
        if l.data_type() != r.data_type() {
            return Err(Error::Plan(format!(
                "UNION type mismatch: {} vs {}",
                l.data_type(),
                r.data_type()
            )));
        }
    }
    let schema = left.schema().clone();
    Ok(LogicalPlan::Union {
        inputs: vec![left, right],
        schema,
        all,
    })
}

fn bind_select(select: &Select, catalog: &Catalog) -> Result<LogicalPlan> {
    reject_select_extras(select)?;
    let from = bind_from(&select.from, catalog)?;
    let mut plan = from.plan;
    let mut scope = from.scope;

    if let Some(pred) = &select.selection {
        let predicate = bind_scalar(pred, &scope)?;
        require_boolean(expr_type(&predicate, &scope.schema)?, "WHERE")?;
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        };
        scope.schema = plan.schema().clone();
    }

    let grouped = is_grouped(select);
    if grouped {
        plan = bind_aggregate(plan, &scope, select)?;
        scope = Scope::from_schema(None, plan.schema().clone());
        if let Some(having) = &select.having {
            let predicate = bind_having(having, &scope)?;
            require_boolean(expr_type(&predicate, &scope.schema)?, "HAVING")?;
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }
    } else {
        if select.having.is_some() {
            return Err(Error::Plan(
                "HAVING requires GROUP BY or an aggregate".into(),
            ));
        }
        plan = bind_project(plan, &scope, &select.projection)?;
    }
    Ok(plan)
}

fn reject_select_extras(select: &Select) -> Result<()> {
    if select.distinct.is_some() {
        if matches!(select.distinct, Some(Distinct::Distinct)) {
            return Err(Error::NotImplemented("SELECT DISTINCT"));
        }
        return Err(Error::NotImplemented("SELECT DISTINCT ON"));
    }
    if select.qualify.is_some() {
        return Err(Error::NotImplemented("QUALIFY"));
    }
    if select.into.is_some() {
        return Err(Error::NotImplemented("SELECT INTO"));
    }
    if !select.lateral_views.is_empty() {
        return Err(Error::NotImplemented("LATERAL VIEW"));
    }
    if !select.named_window.is_empty() {
        return Err(Error::NotImplemented("WINDOW"));
    }
    if !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
    {
        return Err(Error::NotImplemented("CLUSTER/DISTRIBUTE/SORT BY"));
    }
    if !select.connect_by.is_empty() {
        return Err(Error::NotImplemented("CONNECT BY"));
    }
    if select.prewhere.is_some() {
        return Err(Error::NotImplemented("PREWHERE"));
    }
    if select.top.is_some() {
        return Err(Error::NotImplemented("TOP"));
    }
    if !select.optimizer_hints.is_empty() {
        return Err(Error::NotImplemented("optimizer hints"));
    }
    if select.select_modifiers.is_some() {
        return Err(Error::NotImplemented("SELECT modifiers"));
    }
    if select.value_table_mode.is_some() {
        return Err(Error::NotImplemented("SELECT AS VALUE/STRUCT"));
    }
    if select.exclude.is_some() {
        return Err(Error::NotImplemented("EXCLUDE"));
    }
    Ok(())
}

struct BoundFrom {
    plan: LogicalPlan,
    scope: Scope,
}

fn bind_from(from: &[TableWithJoins], catalog: &Catalog) -> Result<BoundFrom> {
    if from.is_empty() {
        let schema = Arc::new(Schema::empty());
        return Ok(BoundFrom {
            plan: LogicalPlan::Empty {
                schema: schema.clone(),
            },
            scope: Scope::from_schema(None, schema),
        });
    }
    let mut bound = bind_table_with_joins(&from[0], catalog)?;
    for extra in &from[1..] {
        let right = bind_table_with_joins(extra, catalog)?;
        bound = cross_join(bound, right)?;
    }
    Ok(bound)
}

fn bind_table_with_joins(twj: &TableWithJoins, catalog: &Catalog) -> Result<BoundFrom> {
    let mut left = bind_table_factor(&twj.relation, catalog)?;
    for join in &twj.joins {
        if join.global {
            return Err(Error::NotImplemented("GLOBAL JOIN"));
        }
        let right = bind_table_factor(&join.relation, catalog)?;
        left = bind_join(left, right, &join.join_operator)?;
    }
    Ok(left)
}

fn bind_table_factor(factor: &TableFactor, catalog: &Catalog) -> Result<BoundFrom> {
    match factor {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
            ..
        } => {
            if args.is_some()
                || !with_hints.is_empty()
                || version.is_some()
                || *with_ordinality
                || !partitions.is_empty()
                || json_path.is_some()
                || sample.is_some()
                || !index_hints.is_empty()
            {
                return Err(Error::NotImplemented("table function or scan hints"));
            }
            let table = object_name(name)?;
            let schema = catalog.table(&table)?.clone();
            let alias_name = alias.as_ref().map(|a| ident_key(&a.name));
            if let Some(alias) = alias
                && !alias.columns.is_empty()
            {
                return Err(Error::NotImplemented("column aliases on table"));
            }
            let qualifier = alias_name.clone().unwrap_or_else(|| table.clone());
            Ok(BoundFrom {
                plan: LogicalPlan::Scan {
                    table: table.clone(),
                    schema: schema.clone(),
                    alias: alias_name,
                },
                scope: Scope::from_schema(Some(qualifier), schema),
            })
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
            sample,
        } => {
            if *lateral {
                return Err(Error::NotImplemented("LATERAL"));
            }
            if sample.is_some() {
                return Err(Error::NotImplemented("TABLESAMPLE"));
            }
            let plan = bind_query(subquery, catalog)?;
            let qualifier = alias.as_ref().map(|a| ident_key(&a.name));
            let schema = plan.schema().clone();
            Ok(BoundFrom {
                plan,
                scope: Scope::from_schema(qualifier, schema),
            })
        }
        _ => Err(Error::NotImplemented("table factor")),
    }
}

fn bind_join(left: BoundFrom, right: BoundFrom, op: &JoinOperator) -> Result<BoundFrom> {
    let (join_type, constraint) = match op {
        JoinOperator::Join(c) | JoinOperator::Inner(c) => (JoinType::Inner, c),
        JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (JoinType::Left, c),
        JoinOperator::Right(c) | JoinOperator::RightOuter(c) => (JoinType::Right, c),
        JoinOperator::FullOuter(c) => (JoinType::Full, c),
        JoinOperator::CrossJoin(c) => (JoinType::Cross, c),
        JoinOperator::Semi(c) | JoinOperator::LeftSemi(c) => (JoinType::Semi, c),
        JoinOperator::Anti(c) | JoinOperator::LeftAnti(c) => (JoinType::Anti, c),
        _ => return Err(Error::NotImplemented("join type")),
    };
    let concat = Scope::join(left.scope.clone(), right.scope.clone());
    let on = match constraint {
        JoinConstraint::On(expr) => {
            let pred = bind_scalar(expr, &concat)?;
            require_boolean(expr_type(&pred, &concat.schema)?, "JOIN ON")?;
            Some(pred)
        }
        JoinConstraint::None => None,
        JoinConstraint::Using(_) => return Err(Error::NotImplemented("JOIN USING")),
        JoinConstraint::Natural => return Err(Error::NotImplemented("NATURAL JOIN")),
    };
    if join_type == JoinType::Cross && on.is_some() {
        return Err(Error::Plan("CROSS JOIN does not take ON".into()));
    }
    if join_type != JoinType::Cross && on.is_none() {
        return Err(Error::Plan("join requires ON".into()));
    }
    let (schema, scope) = match join_type {
        JoinType::Semi | JoinType::Anti => (left.plan.schema().clone(), left.scope),
        _ => (concat.schema.clone(), concat),
    };
    Ok(BoundFrom {
        plan: LogicalPlan::Join {
            left: Box::new(left.plan),
            right: Box::new(right.plan),
            join_type,
            on,
            schema,
        },
        scope,
    })
}

fn cross_join(left: BoundFrom, right: BoundFrom) -> Result<BoundFrom> {
    let scope = Scope::join(left.scope, right.scope);
    let schema = scope.schema.clone();
    Ok(BoundFrom {
        plan: LogicalPlan::Join {
            left: Box::new(left.plan),
            right: Box::new(right.plan),
            join_type: JoinType::Cross,
            on: None,
            schema,
        },
        scope,
    })
}

fn is_grouped(select: &Select) -> bool {
    match &select.group_by {
        GroupByExpr::Expressions(exprs, _) if !exprs.is_empty() => true,
        GroupByExpr::All(_) => true,
        _ => projection_has_agg(&select.projection),
    }
}

fn projection_has_agg(items: &[SelectItem]) -> bool {
    items.iter().any(|item| match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => is_agg_expr(e),
        _ => false,
    })
}

fn is_agg_expr(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::Function(func) => classify_agg(func).is_some(),
        SqlExpr::Nested(inner) => is_agg_expr(inner),
        _ => false,
    }
}

#[derive(Clone, Copy)]
enum AggKind {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

fn classify_agg(func: &Function) -> Option<AggKind> {
    if func.over.is_some() {
        return None;
    }
    let name = func_name(func)?;
    Some(match name.as_str() {
        "count" => AggKind::Count,
        "sum" => AggKind::Sum,
        "avg" => AggKind::Avg,
        "min" => AggKind::Min,
        "max" => AggKind::Max,
        _ => return None,
    })
}

fn make_agg(kind: AggKind, arg: Option<Expr>, distinct: bool) -> Result<AggCall> {
    if distinct {
        return match (kind, arg) {
            (AggKind::Count, Some(e)) => Ok(AggCall::CountDistinct(e)),
            (AggKind::Count, None) => Err(Error::Plan("COUNT(DISTINCT *) is not valid".into())),
            _ => Err(Error::NotImplemented("DISTINCT aggregate")),
        };
    }
    match (kind, arg) {
        (AggKind::Count, None) => Ok(AggCall::CountStar),
        (AggKind::Count, Some(e)) => Ok(AggCall::Count(e)),
        (AggKind::Sum, Some(e)) => Ok(AggCall::Sum(e)),
        (AggKind::Avg, Some(e)) => Ok(AggCall::Avg(e)),
        (AggKind::Min, Some(e)) => Ok(AggCall::Min(e)),
        (AggKind::Max, Some(e)) => Ok(AggCall::Max(e)),
        _ => Err(Error::Plan("aggregate is missing its argument".into())),
    }
}

fn func_name(func: &Function) -> Option<String> {
    match func.name.0.last()? {
        ObjectNamePart::Identifier(id) => Some(id.value.to_ascii_lowercase()),
        ObjectNamePart::Function(_) => None,
    }
}

fn bind_aggregate(input: LogicalPlan, scope: &Scope, select: &Select) -> Result<LogicalPlan> {
    let group_sql = match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) => {
            if !modifiers.is_empty() {
                return Err(Error::NotImplemented("GROUP BY modifiers"));
            }
            exprs.clone()
        }
        GroupByExpr::All(_) => return Err(Error::NotImplemented("GROUP BY ALL")),
    };

    let mut group = Vec::new();
    for expr in &group_sql {
        let bound = bind_scalar(expr, scope)?;
        let name = infer_sql_name(expr);
        group.push(NamedExpr { expr: bound, name });
    }

    let mut aggs = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                if let SqlExpr::Function(func) = expr
                    && let Some(kind) = classify_agg(func)
                {
                    if func.over.is_some() {
                        return Err(Error::NotImplemented("window function"));
                    }
                    let arg = agg_arg(func, scope)?;
                    let distinct = agg_distinct(func)?;
                    let call = make_agg(kind, arg, distinct)?;
                    let name = match item {
                        SelectItem::ExprWithAlias { alias, .. } => ident_key(alias),
                        _ => infer_sql_name(expr),
                    };
                    aggs.push((name, call));
                    continue;
                }
                if is_agg_expr(expr) {
                    return Err(Error::NotImplemented("nested aggregate"));
                }
                // non-agg SELECT item must be a grouping expr
                let bound = bind_scalar(expr, scope)?;
                if !group.iter().any(|g| expr_eq(&g.expr, &bound)) {
                    return Err(Error::Plan(
                        "SELECT expression is neither an aggregate nor a GROUP BY key".into(),
                    ));
                }
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                return Err(Error::Plan(
                    "* not supported with GROUP BY in this binder".into(),
                ));
            }
            _ => return Err(Error::NotImplemented("select item")),
        }
    }

    if aggs.is_empty() && group.is_empty() {
        return Err(Error::Plan(
            "aggregate with no grouping or aggregates".into(),
        ));
    }

    let mut fields = Vec::new();
    for g in &group {
        let dt = expr_type(&g.expr, input.schema())?;
        fields.push(Field::new(&g.name, dt, true));
    }
    for (name, call) in &aggs {
        let dt = call.result_type(input.schema())?;
        fields.push(Field::new(name, dt, true));
    }
    Ok(LogicalPlan::Aggregate {
        input: Box::new(input),
        group,
        aggs,
        schema: schema_from_fields(fields)?,
    })
}

fn agg_arg(func: &Function, scope: &Scope) -> Result<Option<Expr>> {
    if func.filter.is_some() {
        return Err(Error::NotImplemented("FILTER on aggregate"));
    }
    if !func.within_group.is_empty() {
        return Err(Error::NotImplemented("WITHIN GROUP"));
    }
    if func.null_treatment.is_some() {
        return Err(Error::NotImplemented("null treatment on aggregate"));
    }
    if !matches!(func.parameters, FunctionArguments::None) {
        return Err(Error::NotImplemented("function parameters"));
    }
    match &func.args {
        FunctionArguments::None => Ok(None),
        FunctionArguments::List(list) => {
            if !list.clauses.is_empty() {
                return Err(Error::NotImplemented("aggregate argument clause"));
            }
            if list.args.len() == 1 {
                match &list.args[0] {
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(None),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                        Ok(Some(bind_scalar(expr, scope)?))
                    }
                    _ => Err(Error::NotImplemented("named aggregate argument")),
                }
            } else if list.args.is_empty() {
                Ok(None)
            } else {
                Err(Error::Plan("aggregate expects 0 or 1 argument".into()))
            }
        }
        FunctionArguments::Subquery(_) => Err(Error::NotImplemented("subquery aggregate argument")),
    }
}

fn agg_distinct(func: &Function) -> Result<bool> {
    match &func.args {
        FunctionArguments::List(list) => Ok(matches!(
            list.duplicate_treatment,
            Some(DuplicateTreatment::Distinct)
        )),
        FunctionArguments::None => Ok(false),
        FunctionArguments::Subquery(_) => Err(Error::NotImplemented("subquery aggregate argument")),
    }
}

fn bind_project(input: LogicalPlan, scope: &Scope, items: &[SelectItem]) -> Result<LogicalPlan> {
    let mut exprs = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard(opts) => {
                reject_wildcard_opts(opts)?;
                for col in &scope.columns {
                    exprs.push(NamedExpr {
                        expr: Expr::nth(col.index),
                        name: col.name.clone(),
                    });
                }
            }
            SelectItem::QualifiedWildcard(kind, opts) => {
                reject_wildcard_opts(opts)?;
                let qualifier = match kind {
                    SelectItemQualifiedWildcardKind::ObjectName(name) => object_name(name)?,
                    SelectItemQualifiedWildcardKind::Expr(_) => {
                        return Err(Error::NotImplemented("expression.*"));
                    }
                };
                let mut found = false;
                for col in &scope.columns {
                    if col.qualifier.as_deref() == Some(qualifier.as_str()) {
                        found = true;
                        exprs.push(NamedExpr {
                            expr: Expr::nth(col.index),
                            name: col.name.clone(),
                        });
                    }
                }
                if !found {
                    return Err(Error::Plan(format!("relation not found for {qualifier}.*")));
                }
            }
            SelectItem::UnnamedExpr(expr) => {
                let bound = bind_scalar(expr, scope)?;
                exprs.push(NamedExpr {
                    name: infer_sql_name(expr),
                    expr: bound,
                });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let bound = bind_scalar(expr, scope)?;
                exprs.push(NamedExpr {
                    name: ident_key(alias),
                    expr: bound,
                });
            }
            _ => return Err(Error::NotImplemented("select item")),
        }
    }
    if exprs.is_empty() {
        return Err(Error::Plan("empty projection".into()));
    }
    let mut fields = Vec::new();
    for e in &exprs {
        let dt = expr_type(&e.expr, input.schema())?;
        fields.push(Field::new(&e.name, dt, true));
    }
    Ok(LogicalPlan::Project {
        input: Box::new(input),
        exprs,
        schema: schema_from_fields(fields)?,
    })
}

fn reject_wildcard_opts(opts: &WildcardAdditionalOptions) -> Result<()> {
    if opts.opt_ilike.is_some()
        || opts.opt_exclude.is_some()
        || opts.opt_except.is_some()
        || opts.opt_replace.is_some()
        || opts.opt_rename.is_some()
        || opts.opt_alias.is_some()
    {
        return Err(Error::NotImplemented("wildcard options"));
    }
    Ok(())
}

fn bind_order_by(input: LogicalPlan, order_by: &sqlparser::ast::OrderBy) -> Result<LogicalPlan> {
    if order_by.interpolate.is_some() {
        return Err(Error::NotImplemented("ORDER BY INTERPOLATE"));
    }
    let exprs = match &order_by.kind {
        OrderByKind::Expressions(exprs) => exprs,
        OrderByKind::All(_) => return Err(Error::NotImplemented("ORDER BY ALL")),
    };
    let scope = Scope::from_schema(None, input.schema().clone());
    let mut order = Vec::new();
    for e in exprs {
        if e.with_fill.is_some() {
            return Err(Error::NotImplemented("WITH FILL"));
        }
        let expr = bind_scalar(&e.expr, &scope)?;
        let asc = match &e.options.sort {
            None | Some(sqlparser::ast::OrderBySort::Asc) => true,
            Some(sqlparser::ast::OrderBySort::Desc) => false,
            Some(sqlparser::ast::OrderBySort::Using(_)) => {
                return Err(Error::NotImplemented("ORDER BY USING"));
            }
        };
        let nulls_first = e.options.nulls_first.unwrap_or(!asc);
        order.push(SortExpr {
            expr,
            asc,
            nulls_first,
        });
    }
    Ok(LogicalPlan::Sort {
        input: Box::new(input),
        order,
    })
}

fn bind_limit(input: LogicalPlan, limit: &LimitClause) -> Result<LogicalPlan> {
    match limit {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if !limit_by.is_empty() {
                return Err(Error::NotImplemented("LIMIT BY"));
            }
            let fetch = match limit {
                Some(expr) => Some(literal_u64(expr, "LIMIT")?),
                None => None,
            };
            let skip = match offset {
                Some(Offset { value, .. }) => literal_u64(value, "OFFSET")?,
                None => 0,
            };
            Ok(LogicalPlan::Limit {
                input: Box::new(input),
                skip,
                fetch,
            })
        }
        LimitClause::OffsetCommaLimit { .. } => Err(Error::NotImplemented("LIMIT offset, count")),
    }
}

fn literal_u64(expr: &SqlExpr, what: &str) -> Result<u64> {
    match expr {
        SqlExpr::Value(v) => match &v.value {
            Value::Number(n, _) => n
                .parse::<u64>()
                .map_err(|_| Error::Plan(format!("{what} must be a non-negative integer"))),
            _ => Err(Error::Plan(format!(
                "{what} must be a non-negative integer"
            ))),
        },
        SqlExpr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => literal_u64(expr, what),
        _ => Err(Error::Plan(format!("{what} must be a literal integer"))),
    }
}

fn bind_scalar(expr: &SqlExpr, scope: &Scope) -> Result<Expr> {
    match expr {
        SqlExpr::Identifier(id) => resolve_name(None, &ident_key(id), scope),
        SqlExpr::CompoundIdentifier(parts) => {
            if parts.len() == 2 {
                resolve_name(Some(&ident_key(&parts[0])), &ident_key(&parts[1]), scope)
            } else {
                Err(Error::NotImplemented(
                    "qualified name with more than two parts",
                ))
            }
        }
        SqlExpr::Nested(inner) => bind_scalar(inner, scope),
        SqlExpr::Value(v) => bind_value(&v.value),
        SqlExpr::BinaryOp { left, op, right } => {
            let l = bind_scalar(left, scope)?;
            let r = bind_scalar(right, scope)?;
            let bop = map_binary(op.clone())?;
            let out = Expr::Binary(bop, Box::new(l), Box::new(r));
            let _ = expr_type(&out, &scope.schema)?;
            Ok(out)
        }
        SqlExpr::UnaryOp { op, expr } => {
            let inner = bind_scalar(expr, scope)?;
            let uop = map_unary(*op)?;
            let out = Expr::Unary(uop, Box::new(inner));
            let _ = expr_type(&out, &scope.schema)?;
            Ok(out)
        }
        SqlExpr::IsNull(inner) => Ok(Expr::Unary(
            UnaryOp::IsNull,
            Box::new(bind_scalar(inner, scope)?),
        )),
        SqlExpr::IsNotNull(inner) => Ok(Expr::Unary(
            UnaryOp::IsNotNull,
            Box::new(bind_scalar(inner, scope)?),
        )),
        SqlExpr::Cast {
            kind,
            expr,
            data_type,
            format,
        } => {
            if !matches!(kind, CastKind::Cast | CastKind::DoubleColon) {
                return Err(Error::NotImplemented("TRY_CAST"));
            }
            if format.is_some() {
                return Err(Error::NotImplemented("CAST FORMAT"));
            }
            Ok(Expr::Cast {
                expr: Box::new(bind_scalar(expr, scope)?),
                data_type: map_type(data_type)?,
            })
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let operand = match operand {
                Some(e) => Some(Box::new(bind_scalar(e, scope)?)),
                None => None,
            };
            let mut when_then = Vec::new();
            for c in conditions {
                when_then.push((
                    bind_scalar(&c.condition, scope)?,
                    bind_scalar(&c.result, scope)?,
                ));
            }
            let else_expr = match else_result {
                Some(e) => Some(Box::new(bind_scalar(e, scope)?)),
                None => None,
            };
            Ok(Expr::Case {
                operand,
                when_then,
                else_expr,
            })
        }
        SqlExpr::Function(func) => bind_function(func, scope),
        SqlExpr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let inner = bind_scalar(expr, scope)?;
            let start = match substring_from {
                Some(s) => literal_i64(s, "SUBSTRING start")?,
                None => 1,
            };
            let length = match substring_for {
                Some(l) => Some(literal_u64(l, "SUBSTRING length")?),
                None => None,
            };
            Ok(Expr::Substring {
                expr: Box::new(inner),
                start,
                length,
            })
        }
        SqlExpr::Subquery(_) => Err(Error::NotImplemented("scalar subquery")),
        SqlExpr::Exists { .. } => Err(Error::NotImplemented("EXISTS")),
        SqlExpr::Like { .. } | SqlExpr::ILike { .. } => Err(Error::NotImplemented("LIKE")),
        _ => Err(Error::NotImplemented("expression")),
    }
}

fn bind_function(func: &Function, scope: &Scope) -> Result<Expr> {
    if func.over.is_some() {
        return Err(Error::NotImplemented("window function"));
    }
    if classify_agg(func).is_some() {
        return Err(Error::Plan(
            "aggregate function not allowed in this expression".into(),
        ));
    }
    let name = func_name(func).ok_or(Error::NotImplemented("function name"))?;
    let args = function_args(func, scope)?;
    match name.as_str() {
        "upper" if args.len() == 1 => Ok(Expr::Unary(
            UnaryOp::Upper,
            Box::new(args.into_iter().next().unwrap()),
        )),
        "lower" if args.len() == 1 => Ok(Expr::Unary(
            UnaryOp::Lower,
            Box::new(args.into_iter().next().unwrap()),
        )),
        "coalesce" => Ok(Expr::Coalesce(args)),
        "concat" => Ok(Expr::Concat(args)),
        _ => Err(Error::NotImplemented("function")),
    }
}

fn function_args(func: &Function, scope: &Scope) -> Result<Vec<Expr>> {
    match &func.args {
        FunctionArguments::None => Ok(Vec::new()),
        FunctionArguments::List(list) => list
            .args
            .iter()
            .map(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => bind_scalar(e, scope),
                _ => Err(Error::NotImplemented("function argument")),
            })
            .collect(),
        FunctionArguments::Subquery(_) => Err(Error::NotImplemented("subquery function argument")),
    }
}

fn bind_value(value: &Value) -> Result<Expr> {
    match value {
        Value::Number(n, _) => {
            if n.contains('.') {
                let v: f64 = n
                    .parse()
                    .map_err(|_| Error::Plan(format!("invalid number {n}")))?;
                Ok(Expr::Literal(Arc::new(arrow::array::Float64Array::from(
                    vec![v],
                ))))
            } else {
                let v: i64 = n
                    .parse()
                    .map_err(|_| Error::Plan(format!("invalid integer {n}")))?;
                Ok(Expr::lit_i64(v))
            }
        }
        Value::Boolean(b) => Ok(Expr::lit_bool(*b)),
        Value::SingleQuotedString(s)
        | Value::DollarQuotedString(sqlparser::ast::DollarQuotedString { value: s, .. })
        | Value::EscapedStringLiteral(s)
        | Value::UnicodeStringLiteral(s) => Ok(Expr::lit_utf8(s.clone())),
        Value::Null => Ok(Expr::lit_null(DataType::Null)),
        _ => Err(Error::NotImplemented("literal")),
    }
}

fn bind_having(expr: &SqlExpr, scope: &Scope) -> Result<Expr> {
    match expr {
        SqlExpr::Function(func) if classify_agg(func).is_some() => {
            resolve_name(None, &infer_sql_name(expr), scope)
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let l = bind_having(left, scope)?;
            let r = bind_having(right, scope)?;
            let out = Expr::Binary(map_binary(op.clone())?, Box::new(l), Box::new(r));
            let _ = expr_type(&out, &scope.schema)?;
            Ok(out)
        }
        SqlExpr::Nested(inner) => bind_having(inner, scope),
        _ => bind_scalar(expr, scope),
    }
}

fn literal_i64(expr: &SqlExpr, what: &str) -> Result<i64> {
    match expr {
        SqlExpr::Value(v) => match &v.value {
            Value::Number(n, _) => n
                .parse::<i64>()
                .map_err(|_| Error::Plan(format!("{what} must be an integer literal"))),
            _ => Err(Error::Plan(format!("{what} must be an integer literal"))),
        },
        SqlExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => Ok(-literal_i64(expr, what)?),
        SqlExpr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => literal_i64(expr, what),
        _ => Err(Error::Plan(format!("{what} must be an integer literal"))),
    }
}

fn resolve_name(qualifier: Option<&str>, name: &str, scope: &Scope) -> Result<Expr> {
    let mut matches = scope.columns.iter().filter(|c| {
        let name_ok = c.name.eq_ignore_ascii_case(name);
        match qualifier {
            Some(q) => {
                name_ok
                    && c.qualifier
                        .as_deref()
                        .is_some_and(|qq| qq.eq_ignore_ascii_case(q))
            }
            None => name_ok,
        }
    });
    match (matches.next(), matches.next()) {
        (Some(col), None) => Ok(Expr::nth(col.index)),
        (Some(_), Some(_)) => Err(Error::Plan(format!("ambiguous column: {name}"))),
        (None, _) => {
            let q = qualifier.map(|q| format!("{q}.")).unwrap_or_default();
            Err(Error::Plan(format!("column not found: {q}{name}")))
        }
    }
}

fn map_binary(op: BinaryOperator) -> Result<BinaryOp> {
    Ok(match op {
        BinaryOperator::Plus => BinaryOp::Add,
        BinaryOperator::Minus => BinaryOp::Sub,
        BinaryOperator::Multiply => BinaryOp::Mul,
        BinaryOperator::Divide => BinaryOp::Div,
        BinaryOperator::Eq => BinaryOp::Eq,
        BinaryOperator::NotEq => BinaryOp::NotEq,
        BinaryOperator::Lt => BinaryOp::Lt,
        BinaryOperator::LtEq => BinaryOp::LtEq,
        BinaryOperator::Gt => BinaryOp::Gt,
        BinaryOperator::GtEq => BinaryOp::GtEq,
        BinaryOperator::And => BinaryOp::And,
        BinaryOperator::Or => BinaryOp::Or,
        _ => return Err(Error::NotImplemented("binary operator")),
    })
}

fn map_unary(op: UnaryOperator) -> Result<UnaryOp> {
    Ok(match op {
        UnaryOperator::Not => UnaryOp::Not,
        UnaryOperator::Minus => UnaryOp::Neg,
        _ => return Err(Error::NotImplemented("unary operator")),
    })
}

fn map_type(dt: &sqlparser::ast::DataType) -> Result<DataType> {
    use sqlparser::ast::DataType as Sql;
    Ok(match dt {
        Sql::Boolean => DataType::Boolean,
        Sql::TinyInt(_) => DataType::Int8,
        Sql::SmallInt(_) | Sql::Int2(_) | Sql::Int16 => DataType::Int16,
        Sql::Int(_) | Sql::Int4(_) | Sql::Integer(_) | Sql::Int32 => DataType::Int32,
        Sql::BigInt(_) | Sql::Int8(_) | Sql::Int64 => DataType::Int64,
        Sql::Float32 | Sql::Real => DataType::Float32,
        Sql::Float64 | Sql::Double(_) | Sql::DoublePrecision => DataType::Float64,
        Sql::Text
        | Sql::String(_)
        | Sql::Varchar(_)
        | Sql::Char(_)
        | Sql::Character(_)
        | Sql::CharacterVarying(_)
        | Sql::CharVarying(_)
        | Sql::Nvarchar(_) => DataType::Utf8,
        Sql::Bytea => DataType::Binary,
        _ => return Err(Error::NotImplemented("SQL type")),
    })
}

fn object_name(name: &ObjectName) -> Result<String> {
    if name.0.len() != 1 {
        return Err(Error::NotImplemented("catalog.schema.table name"));
    }
    match &name.0[0] {
        ObjectNamePart::Identifier(id) => Ok(ident_key(id)),
        ObjectNamePart::Function(_) => Err(Error::NotImplemented("function object name")),
    }
}

fn ident_key(id: &Ident) -> String {
    if id.quote_style.is_some() {
        id.value.clone()
    } else {
        id.value.to_ascii_lowercase()
    }
}

fn infer_sql_name(expr: &SqlExpr) -> String {
    match expr {
        SqlExpr::Identifier(id) => ident_key(id),
        SqlExpr::CompoundIdentifier(parts) => ident_key(parts.last().expect("compound")),
        SqlExpr::Function(func) => func_name(func).unwrap_or_else(|| expr.to_string()),
        _ => expr.to_string(),
    }
}

fn expr_eq(a: &Expr, b: &Expr) -> bool {
    format!("{a:?}") == format!("{b:?}")
}
