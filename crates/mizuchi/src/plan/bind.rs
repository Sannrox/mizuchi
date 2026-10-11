//! Bind parsed SQL to a resolved [`LogicalPlan`].

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use sqlparser::ast::{
    Array as SqlArray, Assignment, AssignmentTarget, BinaryOperator, CastKind, CeilFloorKind, Cte,
    DateTimeField, Distinct, DuplicateTreatment, Expr as SqlExpr, Function, FunctionArg,
    FunctionArgExpr, FunctionArguments, GroupByExpr, Ident, JoinConstraint, JoinOperator,
    LimitClause, Merge as SqlMerge, MergeAction, MergeClauseKind, MergeInsertKind, MergeUpdateKind,
    NamedWindowDefinition, NamedWindowExpr, ObjectName, ObjectNamePart, Offset, OrderByExpr,
    OrderByKind, Query, Select, SelectItem, SelectItemQualifiedWildcardKind, SetExpr, SetOperator,
    SetQuantifier, Statement, TableFactor, TableWithJoins, TrimWhereField, UnaryOperator, Value,
    WildcardAdditionalOptions, WindowFrame as SqlWindowFrame, WindowFrameBound, WindowFrameUnits,
    WindowType,
};

use super::{
    AggCall, AlterTable, Catalog, Ddl, FrameBound, FrameUnits, JoinType, LogicalPlan,
    MergeActionPlan, MergeClausePlan, MergeMatch, MergePlan, NamedExpr, SortExpr, TableIdent,
    WindowCall, WindowFn, WindowFrame, expr_type, join_schema, parse_statements, require_boolean,
    schema_from_fields,
};
use crate::error::{Error, Result};
use crate::expr::{BinaryOp, Expr, UnaryOp};
use crate::udaf::CombineAgg;

#[derive(Clone)]
struct BindCtx<'a> {
    catalog: &'a Catalog,
    udafs: &'a BTreeMap<String, Arc<dyn CombineAgg>>,
    /// Non-recursive CTE names inlined as plans later CTEs and the body can scan.
    ctes: BTreeMap<String, LogicalPlan>,
    /// Recursive working-table names visible only in the recursive step.
    work: BTreeMap<String, SchemaRef>,
}

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
    udafs: BTreeMap<String, Arc<dyn CombineAgg>>,
}

impl Scope {
    fn from_schema(qualifier: Option<String>, schema: SchemaRef) -> Self {
        Self::from_schema_udafs(qualifier, schema, BTreeMap::new())
    }

    fn from_schema_udafs(
        qualifier: Option<String>,
        schema: SchemaRef,
        udafs: BTreeMap<String, Arc<dyn CombineAgg>>,
    ) -> Self {
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
        Self {
            columns,
            schema,
            udafs,
        }
    }

    fn join(left: Scope, right: Scope) -> Self {
        let schema = join_schema(&left.schema, &right.schema);
        let udafs = left.udafs;
        let mut columns = left.columns;
        let offset = columns.len();
        for mut col in right.columns {
            col.index += offset;
            columns.push(col);
        }
        Self {
            columns,
            schema,
            udafs,
        }
    }
}

/// Parse and bind one SQL statement against `catalog`.
pub fn plan_sql(sql: &str, catalog: &Catalog) -> Result<LogicalPlan> {
    plan_sql_with_udafs(sql, catalog, &BTreeMap::new())
}

/// [`plan_sql`] plus registered combinable aggregates.
pub fn plan_sql_with_udafs(
    sql: &str,
    catalog: &Catalog,
    udafs: &BTreeMap<String, Arc<dyn CombineAgg>>,
) -> Result<LogicalPlan> {
    let stmt = parse_statements(sql)?;
    let ctx = BindCtx {
        catalog,
        udafs,
        ctes: BTreeMap::new(),
        work: BTreeMap::new(),
    };
    match stmt {
        Statement::Query(query) => bind_query(&query, &ctx),
        other => Err(Error::NotImplemented(statement_kind(&other))),
    }
}

fn statement_kind(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Insert(_) => "INSERT",
        Statement::Merge(_) => "MERGE",
        Statement::Query(_) => "query",
        Statement::CreateTable(_) => "CREATE TABLE",
        Statement::Drop { .. } => "DROP",
        Statement::AlterTable(_) => "ALTER TABLE",
        Statement::ShowTables { .. } => "SHOW TABLES",
        _ => "statement",
    }
}

/// Bind `CREATE` / `DROP` / `ALTER TABLE` / `SHOW TABLES`.
pub fn bind_ddl(sql: &str) -> Result<Ddl> {
    let stmt = parse_statements(sql)?;
    bind_ddl_stmt(&stmt)
}

fn bind_ddl_stmt(stmt: &Statement) -> Result<Ddl> {
    match stmt {
        Statement::CreateTable(ct) => {
            let ident = TableIdent::parse(&object_name(&ct.name)?)?;
            if !ct.columns.is_empty() {
                let mut fields = Vec::new();
                for col in &ct.columns {
                    let name = ident_key(&col.name);
                    let dt = map_type(&col.data_type)?;
                    fields.push(Field::new(name, dt, true));
                }
                let schema = schema_from_fields(fields)?;
                return Ok(Ddl::CreateTable {
                    ident,
                    schema,
                    if_not_exists: ct.if_not_exists,
                });
            }
            Err(Error::Plan("CREATE TABLE requires a column list".into()))
        }
        Statement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => {
            if *object_type != sqlparser::ast::ObjectType::Table {
                return Err(Error::NotImplemented("DROP"));
            }
            if names.len() != 1 {
                return Err(Error::Plan("DROP TABLE supports one name".into()));
            }
            let name = &names[0];
            Ok(Ddl::DropTable {
                ident: TableIdent::parse(&object_name(name)?)?,
                if_exists: *if_exists,
            })
        }
        Statement::AlterTable(at) => {
            let ident = TableIdent::parse(&object_name(&at.name)?)?;
            let action = bind_alter_op(&at.operations)?;
            Ok(Ddl::AlterTable { ident, action })
        }
        Statement::ShowTables { .. } => {
            let namespace = show_tables_namespace(stmt)?;
            Ok(Ddl::ShowTables { namespace })
        }
        other => Err(Error::NotImplemented(statement_kind(other))),
    }
}

fn bind_alter_op(ops: &[sqlparser::ast::AlterTableOperation]) -> Result<AlterTable> {
    if ops.len() != 1 {
        return Err(Error::Plan("ALTER TABLE supports one operation".into()));
    }
    match &ops[0] {
        sqlparser::ast::AlterTableOperation::AddColumn { column_def, .. } => {
            Ok(AlterTable::AddColumn {
                name: ident_key(&column_def.name),
                data_type: map_type(&column_def.data_type)?,
            })
        }
        sqlparser::ast::AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => Ok(AlterTable::RenameColumn {
            old: ident_key(old_column_name),
            new: ident_key(new_column_name),
        }),
        _ => Err(Error::NotImplemented("ALTER TABLE operation")),
    }
}

fn show_tables_namespace(stmt: &Statement) -> Result<Option<String>> {
    match stmt {
        Statement::ShowTables { show_options, .. } => match &show_options.show_in {
            Some(show_in) => match &show_in.parent_name {
                Some(name) => Ok(Some(object_name(name)?)),
                None => Ok(None),
            },
            None => Ok(None),
        },
        _ => Ok(None),
    }
}

/// Target table name of a `MERGE` statement, before catalog binding.
pub fn merge_target_table(sql: &str) -> Result<String> {
    let stmt = parse_statements(sql)?;
    match stmt {
        Statement::Merge(merge) => match &merge.table {
            TableFactor::Table { name, .. } => object_name(name),
            _ => Err(Error::NotImplemented("MERGE target subquery")),
        },
        other => Err(Error::Plan(format!(
            "expected MERGE, got {}",
            statement_kind(&other)
        ))),
    }
}

/// Bind a `MERGE` statement against `catalog`.
pub fn bind_merge(sql: &str, catalog: &Catalog) -> Result<MergePlan> {
    let stmt = parse_statements(sql)?;
    match stmt {
        Statement::Merge(merge) => bind_merge_stmt(&merge, catalog),
        other => Err(Error::Plan(format!(
            "expected MERGE, got {}",
            statement_kind(&other)
        ))),
    }
}

fn bind_merge_stmt(merge: &SqlMerge, catalog: &Catalog) -> Result<MergePlan> {
    if !merge.optimizer_hints.is_empty() {
        return Err(Error::NotImplemented("MERGE hints"));
    }
    if merge.output.is_some() {
        return Err(Error::NotImplemented("MERGE OUTPUT"));
    }
    let (target, target_alias) = merge_target(&merge.table, catalog)?;
    let target_schema = catalog.table(&target)?.clone();
    let empty = BTreeMap::new();
    let ctx = BindCtx {
        catalog,
        udafs: &empty,
        ctes: BTreeMap::new(),
        work: BTreeMap::new(),
    };
    let source = bind_table_factor(&merge.source, &ctx)?;
    let source_schema = source.plan.schema().clone();
    let target_qual = target_alias.unwrap_or_else(|| target.clone());
    let target_scope = Scope::from_schema(Some(target_qual), target_schema.clone());
    let ts_scope = Scope::join(target_scope.clone(), source.scope.clone());
    let st_scope = Scope::join(source.scope.clone(), target_scope.clone());
    let on_ts = bind_scalar(&merge.on, &ts_scope)?;
    require_boolean(expr_type(&on_ts, &ts_scope.schema)?, "MERGE ON")?;
    let on_st = bind_scalar(&merge.on, &st_scope)?;
    require_boolean(expr_type(&on_st, &st_scope.schema)?, "MERGE ON")?;
    if merge.clauses.is_empty() {
        return Err(Error::Plan("MERGE needs at least one WHEN clause".into()));
    }
    let mut clauses = Vec::with_capacity(merge.clauses.len());
    for clause in &merge.clauses {
        clauses.push(bind_merge_clause(
            clause,
            &target_scope,
            &source.scope,
            &ts_scope,
        )?);
    }
    Ok(MergePlan {
        target,
        source: source.plan,
        on_ts,
        on_st,
        clauses,
        target_schema,
        source_schema,
    })
}

fn merge_target(factor: &TableFactor, catalog: &Catalog) -> Result<(String, Option<String>)> {
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
                return Err(Error::NotImplemented("MERGE target hints"));
            }
            let table = object_name(name)?;
            let _ = catalog.table(&table)?;
            if let Some(alias) = alias
                && !alias.columns.is_empty()
            {
                return Err(Error::NotImplemented("column aliases on MERGE target"));
            }
            let alias_name = alias.as_ref().map(|a| ident_key(&a.name));
            Ok((table, alias_name))
        }
        _ => Err(Error::NotImplemented("MERGE target subquery")),
    }
}

fn bind_merge_clause(
    clause: &sqlparser::ast::MergeClause,
    target: &Scope,
    source: &Scope,
    joined: &Scope,
) -> Result<MergeClausePlan> {
    let kind = match clause.clause_kind {
        MergeClauseKind::Matched => MergeMatch::Matched,
        MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget => MergeMatch::NotMatched,
        MergeClauseKind::NotMatchedBySource => MergeMatch::NotMatchedBySource,
    };
    let pred_scope = match kind {
        MergeMatch::Matched => joined,
        MergeMatch::NotMatched => source,
        MergeMatch::NotMatchedBySource => target,
    };
    let predicate = match &clause.predicate {
        Some(expr) => {
            let bound = bind_scalar(expr, pred_scope)?;
            require_boolean(expr_type(&bound, &pred_scope.schema)?, "MERGE WHEN")?;
            Some(bound)
        }
        None => None,
    };
    let action = match (&clause.action, kind) {
        (MergeAction::Delete { .. }, MergeMatch::NotMatched) => {
            return Err(Error::Plan("WHEN NOT MATCHED cannot THEN DELETE".into()));
        }
        (MergeAction::Delete { .. }, _) => MergeActionPlan::Delete,
        (MergeAction::DoNothing { .. }, _) => {
            return Err(Error::NotImplemented("MERGE DO NOTHING"));
        }
        (MergeAction::Update(_), MergeMatch::NotMatched) => {
            return Err(Error::Plan("WHEN NOT MATCHED cannot THEN UPDATE".into()));
        }
        (MergeAction::Update(update), _) => {
            if update.update_predicate.is_some() || update.delete_predicate.is_some() {
                return Err(Error::NotImplemented("MERGE UPDATE WHERE"));
            }
            bind_merge_update(update, target, source, joined, kind)?
        }
        (MergeAction::Insert(_), MergeMatch::Matched | MergeMatch::NotMatchedBySource) => {
            return Err(Error::Plan(
                "INSERT is only valid on WHEN NOT MATCHED".into(),
            ));
        }
        (MergeAction::Insert(insert), _) => {
            if insert.insert_predicate.is_some() {
                return Err(Error::NotImplemented("MERGE INSERT WHERE"));
            }
            bind_merge_insert(insert, target, source)?
        }
    };
    Ok(MergeClausePlan {
        kind,
        predicate,
        action,
    })
}

fn bind_merge_update(
    update: &sqlparser::ast::MergeUpdateExpr,
    target: &Scope,
    source: &Scope,
    joined: &Scope,
    kind: MergeMatch,
) -> Result<MergeActionPlan> {
    let value_scope = match kind {
        MergeMatch::Matched => joined,
        MergeMatch::NotMatchedBySource => target,
        MergeMatch::NotMatched => joined,
    };
    match &update.kind {
        MergeUpdateKind::Wildcard => {
            let mut assignments = Vec::new();
            for (idx, field) in target.schema.fields().iter().enumerate() {
                let expr = match kind {
                    MergeMatch::Matched => {
                        let qual = source.columns.iter().find_map(|c| c.qualifier.clone());
                        match resolve_name(qual.as_deref(), field.name(), source)? {
                            Expr::Nth(i) => Expr::nth(i + target.schema.fields().len()),
                            _ => {
                                return Err(Error::Plan(
                                    "UPDATE * expected a source column".into(),
                                ));
                            }
                        }
                    }
                    MergeMatch::NotMatchedBySource => {
                        let qual = target.columns.iter().find_map(|c| c.qualifier.clone());
                        resolve_name(qual.as_deref(), field.name(), target)?
                    }
                    MergeMatch::NotMatched => {
                        let qual = source.columns.iter().find_map(|c| c.qualifier.clone());
                        resolve_name(qual.as_deref(), field.name(), source)?
                    }
                };
                assignments.push((idx, expr));
            }
            Ok(MergeActionPlan::Update { assignments })
        }
        MergeUpdateKind::Set(assigns) => {
            let mut assignments = Vec::new();
            for assign in assigns {
                assignments.push(bind_assignment(assign, target, value_scope)?);
            }
            Ok(MergeActionPlan::Update { assignments })
        }
    }
}

fn bind_assignment(assign: &Assignment, target: &Scope, values: &Scope) -> Result<(usize, Expr)> {
    let name = match &assign.target {
        AssignmentTarget::ColumnName(name) => assignment_column(name)?,
        AssignmentTarget::Tuple(_) => return Err(Error::NotImplemented("tuple UPDATE SET")),
    };
    let idx = target
        .schema
        .fields()
        .iter()
        .position(|f| f.name().eq_ignore_ascii_case(&name))
        .ok_or_else(|| Error::Plan(format!("UPDATE column not found: {name}")))?;
    let expr = bind_scalar(&assign.value, values)?;
    Ok((idx, expr))
}

fn assignment_column(name: &ObjectName) -> Result<String> {
    match name.0.last() {
        Some(ObjectNamePart::Identifier(id)) => Ok(ident_key(id)),
        _ => Err(Error::Plan("UPDATE SET target must be a column".into())),
    }
}

fn bind_merge_insert(
    insert: &sqlparser::ast::MergeInsertExpr,
    target: &Scope,
    source: &Scope,
) -> Result<MergeActionPlan> {
    let n = target.schema.fields().len();
    let mut values: Vec<Option<Expr>> = vec![None; n];
    match &insert.kind {
        MergeInsertKind::Wildcard | MergeInsertKind::Row => {
            if !insert.columns.is_empty() {
                return Err(Error::Plan("INSERT * does not take a column list".into()));
            }
            for (idx, field) in target.schema.fields().iter().enumerate() {
                if let Ok(expr) = resolve_name(None, field.name(), source) {
                    values[idx] = Some(expr);
                }
            }
        }
        MergeInsertKind::Values(vals) => {
            if vals.rows.len() != 1 {
                return Err(Error::Plan(
                    "MERGE INSERT VALUES must be a single row".into(),
                ));
            }
            let row = &vals.rows[0];
            let cols: Vec<usize> = if insert.columns.is_empty() {
                (0..n).collect()
            } else {
                insert
                    .columns
                    .iter()
                    .map(|c| {
                        let name = assignment_column(c)?;
                        target
                            .schema
                            .fields()
                            .iter()
                            .position(|f| f.name().eq_ignore_ascii_case(&name))
                            .ok_or_else(|| Error::Plan(format!("INSERT column not found: {name}")))
                    })
                    .collect::<Result<Vec<_>>>()?
            };
            if cols.len() != row.len() {
                return Err(Error::Plan(format!(
                    "INSERT column count {} does not match VALUES {}",
                    cols.len(),
                    row.len()
                )));
            }
            for (idx, expr) in cols.into_iter().zip(row.iter()) {
                values[idx] = Some(bind_scalar(expr, source)?);
            }
        }
    }
    Ok(MergeActionPlan::Insert { values })
}

fn bind_query(query: &Query, ctx: &BindCtx<'_>) -> Result<LogicalPlan> {
    if let Some(with) = &query.with {
        let mut inner = ctx.clone();
        let mut seen = BTreeSet::new();
        for cte in &with.cte_tables {
            let name = ident_key(&cte.alias.name);
            if !seen.insert(name.clone()) {
                return Err(Error::Plan(format!("duplicate CTE name {name}")));
            }
            bind_cte(&mut inner, cte, with.recursive)?;
        }
        return bind_query_body(query, &inner);
    }
    bind_query_body(query, ctx)
}

fn bind_query_body(query: &Query, ctx: &BindCtx<'_>) -> Result<LogicalPlan> {
    reject_query_extras(query)?;
    let mut plan = bind_set_expr(&query.body, ctx)?;
    if let Some(order_by) = &query.order_by {
        plan = bind_order_by(plan, order_by)?;
    }
    if let Some(limit) = &query.limit_clause {
        plan = bind_limit(plan, limit)?;
    }
    Ok(plan)
}

fn bind_cte(ctx: &mut BindCtx<'_>, cte: &Cte, recursive: bool) -> Result<()> {
    if !cte.alias.columns.is_empty() {
        return Err(Error::NotImplemented("column aliases on CTE"));
    }
    if cte.from.is_some() {
        return Err(Error::NotImplemented("CTE FROM"));
    }
    let name = ident_key(&cte.alias.name);
    let plan = if recursive {
        match split_recursive_union(&cte.query, &name)? {
            Some((seeds, step)) => bind_recursive_cte(ctx, &name, &cte.query, seeds, step)?,
            None => bind_query(&cte.query, ctx)?,
        }
    } else {
        bind_query(&cte.query, ctx)?
    };
    ctx.ctes.insert(name, plan);
    Ok(())
}

fn split_recursive_union<'a>(
    query: &'a Query,
    name: &str,
) -> Result<Option<(Vec<&'a SetExpr>, &'a SetExpr)>> {
    match unwrap_set(&query.body) {
        SetExpr::SetOperation {
            left,
            op,
            set_quantifier,
            right,
        } if *op == SetOperator::Union => match set_quantifier {
            SetQuantifier::All => {
                let step = unwrap_set(right);
                if !set_expr_refs_name(step, name) {
                    return Ok(None);
                }
                let seeds = flatten_union_all(left)?;
                for seed in seeds.iter().skip(1) {
                    if set_expr_refs_name(seed, name) {
                        return Err(Error::Plan(
                            "recursive reference in non-recursive term".into(),
                        ));
                    }
                }
                Ok(Some((seeds, step)))
            }
            _ if query_refs_name(query, name) => {
                Err(Error::NotImplemented("recursive UNION DISTINCT"))
            }
            _ => Ok(None),
        },
        _ if query_refs_name(query, name) => {
            Err(Error::NotImplemented("recursive CTE without UNION ALL"))
        }
        _ => Ok(None),
    }
}

fn flatten_union_all(expr: &SetExpr) -> Result<Vec<&SetExpr>> {
    match unwrap_set(expr) {
        SetExpr::SetOperation {
            left,
            op,
            set_quantifier,
            right,
        } if *op == SetOperator::Union => match set_quantifier {
            SetQuantifier::All => {
                let mut terms = flatten_union_all(left)?;
                terms.extend(flatten_union_all(right)?);
                Ok(terms)
            }
            _ => Err(Error::NotImplemented("recursive UNION DISTINCT")),
        },
        other => Ok(vec![other]),
    }
}

fn bind_recursive_cte(
    ctx: &BindCtx<'_>,
    name: &str,
    query: &Query,
    seeds: Vec<&SetExpr>,
    step: &SetExpr,
) -> Result<LogicalPlan> {
    if query.with.is_some() {
        return Err(Error::NotImplemented("WITH inside recursive CTE"));
    }
    if !ctx.work.is_empty() {
        return Err(Error::NotImplemented("nested recursive CTE"));
    }
    reject_query_extras(query)?;
    let mut anchors = Vec::with_capacity(seeds.len());
    for seed in seeds {
        anchors.push(bind_set_expr(seed, ctx)?);
    }
    let schema = anchors[0].schema().clone();
    let mut aligned = Vec::with_capacity(anchors.len());
    for plan in anchors {
        aligned.push(align_schema(plan, &schema)?);
    }
    let mut step_ctx = ctx.clone();
    step_ctx.ctes.remove(name);
    step_ctx.work.insert(name.to_string(), schema.clone());
    let step = align_schema(bind_set_expr(step, &step_ctx)?, &schema)?;
    let mut plan = LogicalPlan::Recursive {
        name: name.to_string(),
        anchors: aligned,
        step: Box::new(step),
        schema,
    };
    if let Some(order_by) = &query.order_by {
        plan = bind_order_by(plan, order_by)?;
    }
    if let Some(limit) = &query.limit_clause {
        plan = bind_limit(plan, limit)?;
    }
    Ok(plan)
}

fn align_schema(plan: LogicalPlan, target: &SchemaRef) -> Result<LogicalPlan> {
    let src = plan.schema();
    if src.fields().len() != target.fields().len() {
        return Err(Error::Plan("recursive CTE column count mismatch".into()));
    }
    for (a, b) in src.fields().iter().zip(target.fields()) {
        if a.data_type() != b.data_type() {
            return Err(Error::Plan(format!(
                "recursive CTE type mismatch: {} vs {}",
                a.data_type(),
                b.data_type()
            )));
        }
    }
    let names_match = src
        .fields()
        .iter()
        .zip(target.fields())
        .all(|(a, b)| a.name() == b.name());
    if names_match {
        return Ok(plan);
    }
    let exprs = (0..target.fields().len())
        .map(|i| NamedExpr {
            expr: Expr::nth(i),
            name: target.field(i).name().clone(),
        })
        .collect();
    Ok(LogicalPlan::Project {
        input: Box::new(plan),
        exprs,
        schema: target.clone(),
    })
}

fn unwrap_set(expr: &SetExpr) -> &SetExpr {
    match expr {
        SetExpr::Query(q)
            if q.with.is_none()
                && q.order_by.is_none()
                && q.limit_clause.is_none()
                && q.fetch.is_none() =>
        {
            unwrap_set(&q.body)
        }
        other => other,
    }
}

fn query_refs_name(query: &Query, name: &str) -> bool {
    if let Some(with) = &query.with
        && with
            .cte_tables
            .iter()
            .any(|cte| query_refs_name(&cte.query, name))
    {
        return true;
    }
    set_expr_refs_name(&query.body, name)
}

fn set_expr_refs_name(expr: &SetExpr, name: &str) -> bool {
    match expr {
        SetExpr::Select(select) => select
            .from
            .iter()
            .any(|twj| table_with_joins_refs(twj, name)),
        SetExpr::Query(q) => query_refs_name(q, name),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_refs_name(left, name) || set_expr_refs_name(right, name)
        }
        _ => false,
    }
}

fn table_with_joins_refs(twj: &TableWithJoins, name: &str) -> bool {
    factor_refs(&twj.relation, name) || twj.joins.iter().any(|j| factor_refs(&j.relation, name))
}

fn factor_refs(factor: &TableFactor, name: &str) -> bool {
    match factor {
        TableFactor::Table { name: n, .. } => object_name(n)
            .ok()
            .is_some_and(|t| table_name_matches(&t, name)),
        TableFactor::Derived { subquery, .. } => query_refs_name(subquery, name),
        _ => false,
    }
}

fn table_name_matches(resolved: &str, cte: &str) -> bool {
    !resolved.contains('.') && resolved == cte
}

fn cte_from_factor_name(name: &ObjectName) -> Option<String> {
    if name.0.len() != 1 {
        return None;
    }
    match &name.0[0] {
        ObjectNamePart::Identifier(id) => Some(ident_key(id)),
        ObjectNamePart::Function(_) => None,
    }
}

fn reject_query_extras(query: &Query) -> Result<()> {
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

fn bind_set_expr(expr: &SetExpr, ctx: &BindCtx<'_>) -> Result<LogicalPlan> {
    match expr {
        SetExpr::Select(select) => bind_select(select, ctx),
        SetExpr::Query(query) => bind_query(query, ctx),
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
            let left = bind_set_expr(left, ctx)?;
            let right = bind_set_expr(right, ctx)?;
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

fn bind_select(select: &Select, ctx: &BindCtx<'_>) -> Result<LogicalPlan> {
    reject_select_extras(select)?;
    let from = bind_from(&select.from, ctx)?;
    let mut plan = from.plan;
    let mut scope = from.scope;

    if let Some(pred) = &select.selection {
        plan = apply_where(plan, &scope, pred, ctx)?;
        scope.schema = plan.schema().clone();
    }

    let grouped = is_grouped(select, ctx.udafs);
    if grouped {
        plan = bind_aggregate(plan, &scope, select)?;
        scope = Scope::from_schema_udafs(None, plan.schema().clone(), scope.udafs.clone());
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
        plan = bind_project(plan, &scope, &select.projection, &select.named_window, ctx)?;
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

fn bind_from(from: &[TableWithJoins], ctx: &BindCtx<'_>) -> Result<BoundFrom> {
    if from.is_empty() {
        let schema = Arc::new(Schema::empty());
        return Ok(BoundFrom {
            plan: LogicalPlan::Empty {
                schema: schema.clone(),
            },
            scope: Scope::from_schema_udafs(None, schema, ctx.udafs.clone()),
        });
    }
    let mut bound = bind_table_with_joins(&from[0], ctx)?;
    for extra in &from[1..] {
        bound = append_from_item(bound, extra, ctx)?;
    }
    Ok(bound)
}

fn append_from_item(
    left: BoundFrom,
    extra: &TableWithJoins,
    ctx: &BindCtx<'_>,
) -> Result<BoundFrom> {
    if is_lateral(&extra.relation) {
        if !extra.joins.is_empty() {
            return Err(Error::NotImplemented("join after LATERAL"));
        }
        return apply_lateral(left, &extra.relation, ctx);
    }
    let right = bind_table_with_joins(extra, ctx)?;
    cross_join(left, right)
}

fn is_lateral(factor: &TableFactor) -> bool {
    matches!(factor, TableFactor::Derived { lateral: true, .. })
}

fn bind_table_with_joins(twj: &TableWithJoins, ctx: &BindCtx<'_>) -> Result<BoundFrom> {
    let mut left = bind_table_factor(&twj.relation, ctx)?;
    for join in &twj.joins {
        if join.global {
            return Err(Error::NotImplemented("GLOBAL JOIN"));
        }
        let right = bind_table_factor(&join.relation, ctx)?;
        left = bind_join(left, right, &join.join_operator)?;
    }
    Ok(left)
}

fn bind_table_factor(factor: &TableFactor, ctx: &BindCtx<'_>) -> Result<BoundFrom> {
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
            let ident = TableIdent::parse(&table)?;
            let scan_name = ident.sql_name();
            let alias_name = alias.as_ref().map(|a| ident_key(&a.name));
            if let Some(alias) = alias
                && !alias.columns.is_empty()
            {
                return Err(Error::NotImplemented("column aliases on table"));
            }
            let qualifier = alias_name.clone().unwrap_or_else(|| ident.table.clone());
            if let Some(cte_name) = cte_from_factor_name(name) {
                if let Some(plan) = ctx.ctes.get(&cte_name) {
                    let plan = plan.clone();
                    let schema = plan.schema().clone();
                    return Ok(BoundFrom {
                        plan,
                        scope: Scope::from_schema_udafs(Some(qualifier), schema, ctx.udafs.clone()),
                    });
                }
                if let Some(schema) = ctx.work.get(&cte_name) {
                    let schema = schema.clone();
                    return Ok(BoundFrom {
                        plan: LogicalPlan::WorkScan {
                            name: cte_name,
                            schema: schema.clone(),
                            alias: alias_name,
                        },
                        scope: Scope::from_schema_udafs(Some(qualifier), schema, ctx.udafs.clone()),
                    });
                }
            }
            let schema = ctx.catalog.table(&scan_name)?.clone();
            Ok(BoundFrom {
                plan: LogicalPlan::Scan {
                    table: scan_name.clone(),
                    schema: schema.clone(),
                    alias: alias_name,
                    projection: None,
                    filters: Vec::new(),
                    stats: ctx.catalog.stats(&scan_name).cloned(),
                },
                scope: Scope::from_schema_udafs(Some(qualifier), schema, ctx.udafs.clone()),
            })
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
            sample,
        } => {
            if *lateral {
                return Err(Error::NotImplemented("LATERAL without a left input"));
            }
            if sample.is_some() {
                return Err(Error::NotImplemented("TABLESAMPLE"));
            }
            let plan = bind_query(subquery, ctx)?;
            let qualifier = alias.as_ref().map(|a| ident_key(&a.name));
            let schema = plan.schema().clone();
            Ok(BoundFrom {
                plan,
                scope: Scope::from_schema_udafs(qualifier, schema, ctx.udafs.clone()),
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
        JoinType::Semi | JoinType::Anti | JoinType::NullAwareAnti => {
            (left.plan.schema().clone(), left.scope)
        }
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

fn is_grouped(select: &Select, udafs: &BTreeMap<String, Arc<dyn CombineAgg>>) -> bool {
    match &select.group_by {
        GroupByExpr::Expressions(exprs, _) if !exprs.is_empty() => true,
        GroupByExpr::All(_) => true,
        _ => projection_has_agg(&select.projection, udafs),
    }
}

fn projection_has_agg(items: &[SelectItem], udafs: &BTreeMap<String, Arc<dyn CombineAgg>>) -> bool {
    items.iter().any(|item| match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
            is_agg_expr(e, udafs)
        }
        _ => false,
    })
}

fn is_agg_expr(expr: &SqlExpr, udafs: &BTreeMap<String, Arc<dyn CombineAgg>>) -> bool {
    match expr {
        SqlExpr::Function(func) => classify_agg(func, udafs).is_some(),
        SqlExpr::Nested(inner) => is_agg_expr(inner, udafs),
        _ => false,
    }
}

#[derive(Clone)]
enum AggKind {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    User(Arc<dyn CombineAgg>),
}

fn classify_agg(func: &Function, udafs: &BTreeMap<String, Arc<dyn CombineAgg>>) -> Option<AggKind> {
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
        other => AggKind::User(udafs.get(other)?.clone()),
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
        (AggKind::User(agg), Some(e)) => Ok(AggCall::User {
            name: agg.name().to_string(),
            arg: e,
            agg,
        }),
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
                    && let Some(kind) = classify_agg(func, &scope.udafs)
                {
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
                if is_agg_expr(expr, &scope.udafs) {
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

fn bind_project(
    input: LogicalPlan,
    scope: &Scope,
    items: &[SelectItem],
    named: &[NamedWindowDefinition],
    ctx: &BindCtx<'_>,
) -> Result<LogicalPlan> {
    if projection_has_window(items) {
        return bind_window_project(input, scope, items, named);
    }
    if !named.is_empty() {
        return Err(Error::Plan(
            "WINDOW clause requires a window function".into(),
        ));
    }
    bind_scalar_project(input, scope, items, ctx)
}

fn projection_has_window(items: &[SelectItem]) -> bool {
    items.iter().any(|item| match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
            as_window_func(e).is_some()
        }
        _ => false,
    })
}

fn as_window_func(expr: &SqlExpr) -> Option<&Function> {
    match expr {
        SqlExpr::Function(func) if func.over.is_some() => Some(func),
        SqlExpr::Nested(inner) => as_window_func(inner),
        _ => None,
    }
}

fn bind_window_project(
    input: LogicalPlan,
    scope: &Scope,
    items: &[SelectItem],
    named: &[NamedWindowDefinition],
) -> Result<LogicalPlan> {
    let names = bind_named_windows(named)?;
    let mut fns = Vec::new();
    let mut spec: Option<(Vec<Expr>, Vec<SortExpr>, WindowFrame)> = None;
    for item in items {
        let expr = match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
            _ => continue,
        };
        let Some(func) = as_window_func(expr) else {
            continue;
        };
        let bound = bind_window_func(func, scope, &names)?;
        let name = match item {
            SelectItem::ExprWithAlias { alias, .. } => ident_key(alias),
            _ => infer_sql_name(expr),
        };
        let this = (bound.partition, bound.order, bound.frame);
        match &spec {
            None => spec = Some(this),
            Some(prev) => {
                if !same_window_spec(prev, &this) {
                    return Err(Error::NotImplemented("mixed window specifications"));
                }
            }
        }
        fns.push(WindowCall {
            name,
            func: bound.func,
        });
    }
    if fns.is_empty() {
        return Err(Error::Plan("window function required".into()));
    }
    let (partition, order, frame) =
        spec.ok_or_else(|| Error::Plan("window function required".into()))?;
    let mut fields: Vec<Field> = input
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    for call in &fns {
        let dt = window_result_type(&call.func, input.schema())?;
        fields.push(Field::new(&call.name, dt, true));
    }
    let window_schema = schema_from_fields(fields)?;
    let window = LogicalPlan::Window {
        input: Box::new(input),
        partition,
        order,
        frame,
        fns,
        schema: window_schema.clone(),
    };
    let window_scope = Scope::from_schema_udafs(None, window_schema, scope.udafs.clone());
    bind_window_output_project(window, scope, &window_scope, items)
}

fn same_window_spec(
    a: &(Vec<Expr>, Vec<SortExpr>, WindowFrame),
    b: &(Vec<Expr>, Vec<SortExpr>, WindowFrame),
) -> bool {
    a.2 == b.2
        && a.0.len() == b.0.len()
        && a.1.len() == b.1.len()
        && a.0
            .iter()
            .zip(&b.0)
            .all(|(l, r)| format!("{l:?}") == format!("{r:?}"))
        && a.1.iter().zip(&b.1).all(|(l, r)| {
            l.asc == r.asc
                && l.nulls_first == r.nulls_first
                && format!("{:?}", l.expr) == format!("{:?}", r.expr)
        })
}

struct BoundWindow {
    func: WindowFn,
    partition: Vec<Expr>,
    order: Vec<SortExpr>,
    frame: WindowFrame,
}

fn bind_named_windows(
    named: &[NamedWindowDefinition],
) -> Result<BTreeMap<String, sqlparser::ast::WindowSpec>> {
    let mut raw: BTreeMap<String, NamedWindowExpr> = BTreeMap::new();
    for NamedWindowDefinition(id, expr) in named {
        let name = ident_key(id);
        if raw.insert(name.clone(), expr.clone()).is_some() {
            return Err(Error::Plan(format!("duplicate window name {name}")));
        }
    }
    let mut out = BTreeMap::new();
    for name in raw.keys() {
        out.insert(name.clone(), resolve_named_window(name, &raw, 0)?);
    }
    Ok(out)
}

fn resolve_named_window(
    name: &str,
    raw: &BTreeMap<String, NamedWindowExpr>,
    depth: usize,
) -> Result<sqlparser::ast::WindowSpec> {
    if depth > 8 {
        return Err(Error::Plan("WINDOW name cycle".into()));
    }
    match raw.get(name) {
        Some(NamedWindowExpr::WindowSpec(spec)) => Ok(spec.clone()),
        Some(NamedWindowExpr::NamedWindow(id)) => {
            resolve_named_window(&ident_key(id), raw, depth + 1)
        }
        None => Err(Error::Plan(format!("unknown window name {name}"))),
    }
}

fn bind_window_func(
    func: &Function,
    scope: &Scope,
    named: &BTreeMap<String, sqlparser::ast::WindowSpec>,
) -> Result<BoundWindow> {
    if func.filter.is_some() {
        return Err(Error::NotImplemented("FILTER on window function"));
    }
    if func.null_treatment.is_some() {
        return Err(Error::NotImplemented("null treatment on window function"));
    }
    if !func.within_group.is_empty() {
        return Err(Error::NotImplemented("WITHIN GROUP"));
    }
    let over = func
        .over
        .as_ref()
        .ok_or_else(|| Error::Plan("window function requires OVER".into()))?;
    let spec = match over {
        WindowType::WindowSpec(spec) => spec.clone(),
        WindowType::NamedWindow(id) => named
            .get(&ident_key(id))
            .cloned()
            .ok_or_else(|| Error::Plan(format!("unknown window name {}", ident_key(id))))?,
    };
    if spec.window_name.is_some() {
        return Err(Error::NotImplemented("inherited window name"));
    }
    let partition = spec
        .partition_by
        .iter()
        .map(|e| bind_scalar(e, scope))
        .collect::<Result<Vec<_>>>()?;
    let order = bind_order_exprs(&spec.order_by, scope)?;
    let frame = bind_frame(spec.window_frame.as_ref(), !order.is_empty())?;
    let name = func_name(func).ok_or(Error::NotImplemented("function name"))?;
    let wfn = match name.as_str() {
        "row_number" => {
            require_window_no_args(func, &name)?;
            WindowFn::RowNumber
        }
        "rank" => {
            require_window_no_args(func, &name)?;
            WindowFn::Rank
        }
        "dense_rank" => {
            require_window_no_args(func, &name)?;
            WindowFn::DenseRank
        }
        "count" | "sum" => {
            let kind = classify_window_agg(func, scope)?;
            let arg = agg_arg(func, scope)?;
            let distinct = agg_distinct(func)?;
            WindowFn::Agg(make_agg(kind, arg, distinct)?)
        }
        _ => return Err(Error::NotImplemented("window function")),
    };
    Ok(BoundWindow {
        func: wfn,
        partition,
        order,
        frame,
    })
}

fn require_window_no_args(func: &Function, name: &str) -> Result<()> {
    match &func.args {
        FunctionArguments::None => Ok(()),
        FunctionArguments::List(list) if list.args.is_empty() => Ok(()),
        _ => Err(Error::Plan(format!("{name} does not take arguments"))),
    }
}

fn classify_window_agg(func: &Function, scope: &Scope) -> Result<AggKind> {
    let name = func_name(func).ok_or(Error::NotImplemented("function name"))?;
    Ok(match name.as_str() {
        "count" => AggKind::Count,
        "sum" => AggKind::Sum,
        other => {
            if let Some(agg) = scope.udafs.get(other) {
                AggKind::User(agg.clone())
            } else {
                return Err(Error::NotImplemented("window function"));
            }
        }
    })
}

fn bind_frame(frame: Option<&SqlWindowFrame>, ordered: bool) -> Result<WindowFrame> {
    let Some(frame) = frame else {
        return Ok(if ordered {
            WindowFrame {
                units: FrameUnits::Range,
                start: FrameBound::UnboundedPreceding,
                end: FrameBound::CurrentRow,
            }
        } else {
            WindowFrame {
                units: FrameUnits::Range,
                start: FrameBound::UnboundedPreceding,
                end: FrameBound::UnboundedFollowing,
            }
        });
    };
    let units = match frame.units {
        WindowFrameUnits::Rows => FrameUnits::Rows,
        WindowFrameUnits::Range => FrameUnits::Range,
        WindowFrameUnits::Groups => return Err(Error::NotImplemented("GROUPS frame")),
    };
    Ok(WindowFrame {
        units,
        start: bind_frame_bound(&frame.start_bound)?,
        end: match &frame.end_bound {
            Some(b) => bind_frame_bound(b)?,
            None => FrameBound::CurrentRow,
        },
    })
}

fn bind_frame_bound(bound: &WindowFrameBound) -> Result<FrameBound> {
    match bound {
        WindowFrameBound::CurrentRow => Ok(FrameBound::CurrentRow),
        WindowFrameBound::Preceding(None) => Ok(FrameBound::UnboundedPreceding),
        WindowFrameBound::Following(None) => Ok(FrameBound::UnboundedFollowing),
        WindowFrameBound::Preceding(Some(_)) | WindowFrameBound::Following(Some(_)) => {
            Err(Error::NotImplemented("offset window bound"))
        }
    }
}

fn window_result_type(func: &WindowFn, schema: &Schema) -> Result<DataType> {
    match func {
        WindowFn::RowNumber | WindowFn::Rank | WindowFn::DenseRank => Ok(DataType::Int64),
        WindowFn::Agg(call) => {
            let dt = call.result_type(schema)?;
            if matches!(call, AggCall::Sum(_)) && !matches!(dt, DataType::Int32 | DataType::Int64) {
                return Err(Error::NotImplemented("SUM window type"));
            }
            Ok(dt)
        }
    }
}

fn bind_window_output_project(
    window: LogicalPlan,
    input_scope: &Scope,
    window_scope: &Scope,
    items: &[SelectItem],
) -> Result<LogicalPlan> {
    let input_width = input_scope.schema.fields().len();
    let mut exprs = Vec::new();
    let mut window_i = 0usize;
    for item in items {
        match item {
            SelectItem::Wildcard(opts) => {
                reject_wildcard_opts(opts)?;
                for col in &input_scope.columns {
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
                for col in &input_scope.columns {
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
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                let name = match item {
                    SelectItem::ExprWithAlias { alias, .. } => ident_key(alias),
                    _ => infer_sql_name(expr),
                };
                if as_window_func(expr).is_some() {
                    exprs.push(NamedExpr {
                        expr: Expr::nth(input_width + window_i),
                        name,
                    });
                    window_i += 1;
                } else {
                    exprs.push(NamedExpr {
                        expr: bind_scalar(expr, input_scope)?,
                        name,
                    });
                }
            }
            _ => return Err(Error::NotImplemented("select item")),
        }
    }
    if exprs.is_empty() {
        return Err(Error::Plan("empty projection".into()));
    }
    let mut fields = Vec::new();
    for e in &exprs {
        let dt = expr_type(&e.expr, window_scope.schema.as_ref())?;
        fields.push(Field::new(&e.name, dt, true));
    }
    Ok(LogicalPlan::Project {
        input: Box::new(window),
        exprs,
        schema: schema_from_fields(fields)?,
    })
}

fn bind_scalar_project(
    input: LogicalPlan,
    scope: &Scope,
    items: &[SelectItem],
    ctx: &BindCtx<'_>,
) -> Result<LogicalPlan> {
    let mut plan = input;
    let mut scope = scope.clone();
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
                let name = infer_sql_name(expr);
                let (next, bound, next_scope) = bind_scalar_or_subquery(plan, scope, expr, ctx)?;
                plan = next;
                scope = next_scope;
                exprs.push(NamedExpr { name, expr: bound });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let name = ident_key(alias);
                let (next, bound, next_scope) = bind_scalar_or_subquery(plan, scope, expr, ctx)?;
                plan = next;
                scope = next_scope;
                exprs.push(NamedExpr { name, expr: bound });
            }
            _ => return Err(Error::NotImplemented("select item")),
        }
    }
    if exprs.is_empty() {
        return Err(Error::Plan("empty projection".into()));
    }
    let mut fields = Vec::new();
    for e in &exprs {
        let dt = expr_type(&e.expr, plan.schema())?;
        fields.push(Field::new(&e.name, dt, true));
    }
    Ok(LogicalPlan::Project {
        input: Box::new(plan),
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
    let order = bind_order_exprs(exprs, &scope)?;
    Ok(LogicalPlan::Sort {
        input: Box::new(input),
        order,
    })
}

fn bind_order_exprs(exprs: &[OrderByExpr], scope: &Scope) -> Result<Vec<SortExpr>> {
    let mut order = Vec::new();
    for e in exprs {
        if e.with_fill.is_some() {
            return Err(Error::NotImplemented("WITH FILL"));
        }
        let expr = bind_scalar(&e.expr, scope)?;
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
    Ok(order)
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

fn bind_compound(parts: &[Ident], scope: &Scope) -> Result<Expr> {
    if parts.len() < 2 || parts.len() > 4 {
        return Err(Error::Plan("invalid qualified name".into()));
    }
    let col = ident_key(parts.last().expect("len checked"));
    let prefix: Vec<String> = parts[..parts.len() - 1].iter().map(ident_key).collect();
    let full = prefix.join(".");
    match resolve_name(Some(&full), &col, scope) {
        Ok(expr) => Ok(expr),
        Err(_) if prefix.len() > 1 => resolve_name(Some(prefix.last().expect("len")), &col, scope),
        Err(err) => Err(err),
    }
}

fn bind_scalar(expr: &SqlExpr, scope: &Scope) -> Result<Expr> {
    match expr {
        SqlExpr::Identifier(id) => resolve_name(None, &ident_key(id), scope),
        SqlExpr::CompoundIdentifier(parts) => bind_compound(parts, scope),
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
        SqlExpr::Extract { field, expr, .. } => bind_extract(field.clone(), expr, scope),
        SqlExpr::Ceil { expr, field } => bind_ceil_floor(UnaryOp::Ceil, expr, field, scope),
        SqlExpr::Floor { expr, field } => bind_ceil_floor(UnaryOp::Floor, expr, field, scope),
        SqlExpr::Trim {
            trim_where,
            trim_what,
            expr,
            trim_characters,
        } => bind_trim(
            *trim_where,
            trim_what.as_deref(),
            expr,
            trim_characters.as_ref(),
            scope,
        ),
        SqlExpr::Array(SqlArray { elem, .. }) => {
            let args = elem
                .iter()
                .map(|e| bind_scalar(e, scope))
                .collect::<Result<Vec<_>>>()?;
            let out = Expr::Array(args);
            let _ = expr_type(&out, &scope.schema)?;
            Ok(out)
        }
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
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => bind_in_list(expr, list, *negated, scope),
        SqlExpr::InSubquery { .. } => Err(Error::NotImplemented("IN subquery in expression")),
        SqlExpr::Subquery(_) => Err(Error::NotImplemented("scalar subquery in expression")),
        SqlExpr::Exists { .. } => Err(Error::NotImplemented("EXISTS in expression")),
        SqlExpr::Like { .. } | SqlExpr::ILike { .. } => Err(Error::NotImplemented("LIKE")),
        _ => Err(Error::NotImplemented("expression")),
    }
}

fn bind_function(func: &Function, scope: &Scope) -> Result<Expr> {
    if func.over.is_some() {
        return Err(Error::NotImplemented("window function"));
    }
    if classify_agg(func, &scope.udafs).is_some() {
        return Err(Error::Plan(
            "aggregate function not allowed in this expression".into(),
        ));
    }
    let name = func_name(func).ok_or(Error::NotImplemented("function name"))?;
    let args = function_args(func, scope)?;
    let unary = |op: UnaryOp, args: Vec<Expr>| -> Result<Expr> {
        if args.len() != 1 {
            return Err(Error::Plan(format!("{name} expects 1 argument")));
        }
        Ok(Expr::Unary(op, Box::new(args.into_iter().next().unwrap())))
    };
    match name.as_str() {
        "upper" => unary(UnaryOp::Upper, args),
        "lower" => unary(UnaryOp::Lower, args),
        "abs" => unary(UnaryOp::Abs, args),
        "ceil" | "ceiling" => unary(UnaryOp::Ceil, args),
        "floor" => unary(UnaryOp::Floor, args),
        "sqrt" => unary(UnaryOp::Sqrt, args),
        "round" if args.len() == 1 => unary(UnaryOp::Round, args),
        "round" if args.len() == 2 => {
            let digits = literal_i64_expr(&args[1], "round digits")?;
            if digits != 0 {
                return Err(Error::NotImplemented("round scale"));
            }
            Ok(Expr::Unary(
                UnaryOp::Round,
                Box::new(args.into_iter().next().unwrap()),
            ))
        }
        "length" | "char_length" | "character_length" => unary(UnaryOp::CharLength, args),
        "trim" => unary(UnaryOp::Trim, args),
        "ltrim" => unary(UnaryOp::Ltrim, args),
        "rtrim" => unary(UnaryOp::Rtrim, args),
        "array_length" if args.len() == 2 => {
            let dim = literal_i64_expr(&args[1], "array_length dimension")?;
            if dim != 1 {
                return Err(Error::NotImplemented("array_length dimension"));
            }
            Ok(Expr::Unary(
                UnaryOp::ArrayLength,
                Box::new(args.into_iter().next().unwrap()),
            ))
        }
        "date_part" if args.len() == 2 => bind_date_part(&args[0], args[1].clone()),
        "nullif" if args.len() == 2 => {
            let mut args = args.into_iter();
            let a = args.next().unwrap();
            let b = args.next().unwrap();
            let dt = expr_type(&a, &scope.schema)?;
            Ok(Expr::Case {
                operand: None,
                when_then: vec![(
                    Expr::Binary(BinaryOp::Eq, Box::new(a.clone()), Box::new(b)),
                    Expr::lit_null(dt),
                )],
                else_expr: Some(Box::new(a)),
            })
        }
        "coalesce" => Ok(Expr::Coalesce(args)),
        "concat" => Ok(Expr::Concat(args)),
        _ => Err(Error::NotImplemented("function")),
    }
}

fn bind_extract(field: DateTimeField, expr: &SqlExpr, scope: &Scope) -> Result<Expr> {
    let inner = bind_scalar(expr, scope)?;
    let op = extract_op(field)?;
    Ok(Expr::Unary(op, Box::new(inner)))
}

fn bind_date_part(part: &Expr, value: Expr) -> Result<Expr> {
    let Expr::Literal(arr) = part else {
        return Err(Error::Plan(
            "date_part field must be a string literal".into(),
        ));
    };
    let name = arr
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .ok_or_else(|| Error::Plan("date_part field must be a string literal".into()))?;
    if name.len() != 1 || name.is_null(0) {
        return Err(Error::Plan(
            "date_part field must be a string literal".into(),
        ));
    }
    let field = match name.value(0).to_ascii_lowercase().as_str() {
        "year" => DateTimeField::Year,
        "month" => DateTimeField::Month,
        "day" => DateTimeField::Day,
        "epoch" => DateTimeField::Epoch,
        _ => return Err(Error::NotImplemented("date_part field")),
    };
    Ok(Expr::Unary(extract_op(field)?, Box::new(value)))
}

fn extract_op(field: DateTimeField) -> Result<UnaryOp> {
    Ok(match field {
        DateTimeField::Year | DateTimeField::Years => UnaryOp::ExtractYear,
        DateTimeField::Month | DateTimeField::Months => UnaryOp::ExtractMonth,
        DateTimeField::Day | DateTimeField::Days => UnaryOp::ExtractDay,
        DateTimeField::Epoch => UnaryOp::ExtractEpoch,
        _ => return Err(Error::NotImplemented("EXTRACT field")),
    })
}

fn bind_ceil_floor(
    op: UnaryOp,
    expr: &SqlExpr,
    field: &CeilFloorKind,
    scope: &Scope,
) -> Result<Expr> {
    match field {
        CeilFloorKind::DateTimeField(DateTimeField::NoDateTime) => {}
        _ => return Err(Error::NotImplemented("ceil/floor scale")),
    }
    Ok(Expr::Unary(op, Box::new(bind_scalar(expr, scope)?)))
}

fn bind_trim(
    trim_where: Option<TrimWhereField>,
    trim_what: Option<&SqlExpr>,
    expr: &SqlExpr,
    trim_characters: Option<&Vec<SqlExpr>>,
    scope: &Scope,
) -> Result<Expr> {
    if trim_what.is_some() || trim_characters.is_some() {
        return Err(Error::NotImplemented("TRIM characters"));
    }
    let op = match trim_where {
        None | Some(TrimWhereField::Both) => UnaryOp::Trim,
        Some(TrimWhereField::Leading) => UnaryOp::Ltrim,
        Some(TrimWhereField::Trailing) => UnaryOp::Rtrim,
    };
    Ok(Expr::Unary(op, Box::new(bind_scalar(expr, scope)?)))
}

fn literal_i64_expr(expr: &Expr, what: &str) -> Result<i64> {
    let Expr::Literal(arr) = expr else {
        return Err(Error::Plan(format!("{what} must be an integer literal")));
    };
    let a = arr
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .ok_or_else(|| Error::Plan(format!("{what} must be an integer literal")))?;
    if a.len() != 1 || a.is_null(0) {
        return Err(Error::Plan(format!("{what} must be an integer literal")));
    }
    Ok(a.value(0))
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
        SqlExpr::Function(func) if classify_agg(func, &scope.udafs).is_some() => {
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
    if name.0.is_empty() || name.0.len() > 3 {
        return Err(Error::Plan("invalid table name".into()));
    }
    let mut parts = Vec::with_capacity(name.0.len());
    for part in &name.0 {
        match part {
            ObjectNamePart::Identifier(id) => parts.push(ident_key(id)),
            ObjectNamePart::Function(_) => {
                return Err(Error::NotImplemented("function object name"));
            }
        }
    }
    Ok(parts.join("."))
}

fn flatten_and(expr: &SqlExpr) -> Vec<&SqlExpr> {
    match expr {
        SqlExpr::Nested(inner) => flatten_and(inner),
        SqlExpr::BinaryOp {
            op: BinaryOperator::And,
            left,
            right,
        } => {
            let mut out = flatten_and(left);
            out.extend(flatten_and(right));
            out
        }
        other => vec![other],
    }
}

fn unwrap_subquery(expr: &SqlExpr) -> Option<&Query> {
    match expr {
        SqlExpr::Nested(inner) => unwrap_subquery(inner),
        SqlExpr::Subquery(q) => Some(q),
        _ => None,
    }
}

fn bind_in_list(expr: &SqlExpr, list: &[SqlExpr], negated: bool, scope: &Scope) -> Result<Expr> {
    if list.is_empty() {
        let pred = Expr::lit_bool(false);
        return Ok(if negated {
            Expr::Unary(UnaryOp::Not, Box::new(pred))
        } else {
            pred
        });
    }
    let left = bind_scalar(expr, scope)?;
    let mut acc = Expr::Binary(
        BinaryOp::Eq,
        Box::new(left.clone()),
        Box::new(bind_scalar(&list[0], scope)?),
    );
    for item in &list[1..] {
        let eq = Expr::Binary(
            BinaryOp::Eq,
            Box::new(left.clone()),
            Box::new(bind_scalar(item, scope)?),
        );
        acc = Expr::Binary(BinaryOp::Or, Box::new(acc), Box::new(eq));
    }
    let pred = acc;
    let _ = expr_type(&pred, &scope.schema)?;
    if negated {
        Ok(Expr::Unary(UnaryOp::Not, Box::new(pred)))
    } else {
        Ok(pred)
    }
}

fn apply_where(
    mut plan: LogicalPlan,
    scope: &Scope,
    pred: &SqlExpr,
    ctx: &BindCtx<'_>,
) -> Result<LogicalPlan> {
    let mut leftover: Option<Expr> = None;
    for conjunct in flatten_and(pred) {
        match conjunct {
            SqlExpr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                plan = apply_in_subquery(plan, scope, expr, subquery, *negated, ctx)?;
            }
            SqlExpr::Exists { subquery, negated } => {
                plan = apply_exists(plan, scope, subquery, *negated, ctx)?;
            }
            other => {
                let bound = bind_scalar(other, scope)?;
                require_boolean(expr_type(&bound, &scope.schema)?, "WHERE")?;
                leftover = Some(match leftover {
                    None => bound,
                    Some(prev) => Expr::Binary(BinaryOp::And, Box::new(prev), Box::new(bound)),
                });
            }
        }
    }
    if let Some(predicate) = leftover {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        };
    }
    Ok(plan)
}

fn apply_in_subquery(
    plan: LogicalPlan,
    scope: &Scope,
    expr: &SqlExpr,
    subquery: &Query,
    negated: bool,
    ctx: &BindCtx<'_>,
) -> Result<LogicalPlan> {
    let needle = bind_scalar(expr, scope)?;
    let inner = bind_query(subquery, ctx)?;
    if inner.schema().fields().len() != 1 {
        return Err(Error::Plan("IN subquery must return one column".into()));
    }
    let left_len = plan.schema().fields().len();
    let on = Expr::Binary(
        BinaryOp::Eq,
        Box::new(needle),
        Box::new(Expr::nth(left_len)),
    );
    let join_schema = join_schema(plan.schema(), inner.schema());
    let _ = expr_type(&on, join_schema.as_ref())?;
    Ok(LogicalPlan::Join {
        schema: plan.schema().clone(),
        left: Box::new(plan),
        right: Box::new(inner),
        join_type: if negated {
            JoinType::NullAwareAnti
        } else {
            JoinType::Semi
        },
        on: Some(on),
    })
}

fn apply_exists(
    plan: LogicalPlan,
    outer: &Scope,
    subquery: &Query,
    negated: bool,
    ctx: &BindCtx<'_>,
) -> Result<LogicalPlan> {
    if subquery.with.is_some() {
        return Err(Error::NotImplemented("WITH inside EXISTS"));
    }
    reject_query_extras(subquery)?;
    let select = match unwrap_set(&subquery.body) {
        SetExpr::Select(s) => s,
        _ => return Err(Error::NotImplemented("EXISTS set operation")),
    };
    reject_select_extras(select)?;
    if is_grouped(select, ctx.udafs) {
        return Err(Error::NotImplemented("EXISTS aggregate"));
    }
    if select.having.is_some() {
        return Err(Error::NotImplemented("EXISTS HAVING"));
    }
    let inner_from = bind_from(&select.from, ctx)?;
    let outer_len = outer.schema.fields().len();
    let left_len = plan.schema().fields().len();
    let combined = Scope::join(outer.clone(), inner_from.scope.clone());
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                let _ = bind_scalar(expr, &combined)?;
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {}
            _ => return Err(Error::NotImplemented("EXISTS select item")),
        }
    }
    let mut on_eqs: Vec<Expr> = Vec::new();
    let mut inner_preds: Vec<Expr> = Vec::new();
    if let Some(pred) = &select.selection {
        for conjunct in flatten_and(pred) {
            let bound = bind_scalar(conjunct, &combined)?;
            classify_corr(bound, outer_len, left_len, &mut on_eqs, &mut inner_preds)?;
        }
    }
    let mut inner = inner_from.plan;
    if let Some(predicate) = fold_and(inner_preds) {
        inner = LogicalPlan::Filter {
            input: Box::new(inner),
            predicate,
        };
    }
    if let Some(order_by) = &subquery.order_by {
        inner = bind_order_by(inner, order_by)?;
    }
    let on = fold_and(on_eqs);
    if let Some(limit) = &subquery.limit_clause {
        let uses_inner = on
            .as_ref()
            .is_some_and(|e| nth_bounds(e).1.is_some_and(|m| m >= left_len));
        if uses_inner {
            return Err(Error::NotImplemented("LIMIT in correlated EXISTS"));
        }
        inner = bind_limit(inner, limit)?;
    }
    Ok(LogicalPlan::Join {
        schema: plan.schema().clone(),
        left: Box::new(plan),
        right: Box::new(inner),
        join_type: if negated {
            JoinType::Anti
        } else {
            JoinType::Semi
        },
        on,
    })
}

fn fold_and(mut parts: Vec<Expr>) -> Option<Expr> {
    let mut acc = parts.pop()?;
    while let Some(next) = parts.pop() {
        acc = Expr::Binary(BinaryOp::And, Box::new(next), Box::new(acc));
    }
    Some(acc)
}

fn classify_corr(
    expr: Expr,
    outer_len: usize,
    left_len: usize,
    on_eqs: &mut Vec<Expr>,
    inner_preds: &mut Vec<Expr>,
) -> Result<()> {
    let (min, max) = nth_bounds(&expr);
    if min.is_some_and(|m| m >= outer_len) {
        inner_preds.push(map_nths(expr, &|i| i - outer_len));
    } else if max.is_none_or(|m| m < outer_len) {
        on_eqs.push(expr);
    } else {
        on_eqs.push(map_nths(expr, &|i| {
            if i >= outer_len {
                left_len + (i - outer_len)
            } else {
                i
            }
        }));
    }
    Ok(())
}

fn nth_bounds(expr: &Expr) -> (Option<usize>, Option<usize>) {
    let mut min = None;
    let mut max = None;
    walk_nth(expr, &mut |i| {
        min = Some(min.map_or(i, |m: usize| m.min(i)));
        max = Some(max.map_or(i, |m: usize| m.max(i)));
    });
    (min, max)
}

fn walk_nth(expr: &Expr, f: &mut impl FnMut(usize)) {
    match expr {
        Expr::Nth(i) => f(*i),
        Expr::Column(_) | Expr::Literal(_) => {}
        Expr::Binary(_, l, r) => {
            walk_nth(l, f);
            walk_nth(r, f);
        }
        Expr::Unary(_, e) | Expr::Cast { expr: e, .. } | Expr::Substring { expr: e, .. } => {
            walk_nth(e, f);
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(o) = operand {
                walk_nth(o, f);
            }
            for (w, t) in when_then {
                walk_nth(w, f);
                walk_nth(t, f);
            }
            if let Some(e) = else_expr {
                walk_nth(e, f);
            }
        }
        Expr::Coalesce(args) | Expr::Concat(args) | Expr::Array(args) => {
            for a in args {
                walk_nth(a, f);
            }
        }
    }
}

fn map_nths(expr: Expr, f: &impl Fn(usize) -> usize) -> Expr {
    match expr {
        Expr::Nth(i) => Expr::Nth(f(i)),
        Expr::Column(_) | Expr::Literal(_) => expr,
        Expr::Binary(op, l, r) => {
            Expr::Binary(op, Box::new(map_nths(*l, f)), Box::new(map_nths(*r, f)))
        }
        Expr::Unary(op, e) => Expr::Unary(op, Box::new(map_nths(*e, f))),
        Expr::Cast { expr, data_type } => Expr::Cast {
            expr: Box::new(map_nths(*expr, f)),
            data_type,
        },
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => Expr::Case {
            operand: operand.map(|e| Box::new(map_nths(*e, f))),
            when_then: when_then
                .into_iter()
                .map(|(w, t)| (map_nths(w, f), map_nths(t, f)))
                .collect(),
            else_expr: else_expr.map(|e| Box::new(map_nths(*e, f))),
        },
        Expr::Coalesce(args) => Expr::Coalesce(args.into_iter().map(|e| map_nths(e, f)).collect()),
        Expr::Concat(args) => Expr::Concat(args.into_iter().map(|e| map_nths(e, f)).collect()),
        Expr::Substring {
            expr,
            start,
            length,
        } => Expr::Substring {
            expr: Box::new(map_nths(*expr, f)),
            start,
            length,
        },
        Expr::Array(args) => Expr::Array(args.into_iter().map(|e| map_nths(e, f)).collect()),
    }
}

fn bind_scalar_or_subquery(
    plan: LogicalPlan,
    scope: Scope,
    expr: &SqlExpr,
    ctx: &BindCtx<'_>,
) -> Result<(LogicalPlan, Expr, Scope)> {
    if let Some(query) = unwrap_subquery(expr) {
        attach_scalar(plan, scope, query, ctx)
    } else {
        let bound = bind_scalar(expr, &scope)?;
        Ok((plan, bound, scope))
    }
}

fn attach_scalar(
    plan: LogicalPlan,
    scope: Scope,
    query: &Query,
    ctx: &BindCtx<'_>,
) -> Result<(LogicalPlan, Expr, Scope)> {
    let inner = bind_query(query, ctx)?;
    if inner.schema().fields().len() != 1 {
        return Err(Error::Plan("scalar subquery must return one column".into()));
    }
    let schema = inner.schema().clone();
    let scalar = LogicalPlan::Scalar {
        input: Box::new(inner),
        schema: schema.clone(),
    };
    let nth = plan.schema().fields().len();
    let joined = join_schema(plan.schema(), &schema);
    let plan = LogicalPlan::Join {
        left: Box::new(plan),
        right: Box::new(scalar),
        join_type: JoinType::Cross,
        on: None,
        schema: joined.clone(),
    };
    let mut next_scope = scope;
    next_scope.schema = joined;
    Ok((plan, Expr::nth(nth), next_scope))
}

fn apply_lateral(left: BoundFrom, factor: &TableFactor, ctx: &BindCtx<'_>) -> Result<BoundFrom> {
    let TableFactor::Derived {
        subquery,
        alias,
        sample,
        ..
    } = factor
    else {
        return Err(Error::NotImplemented("LATERAL"));
    };
    if sample.is_some() {
        return Err(Error::NotImplemented("TABLESAMPLE"));
    }
    if subquery.with.is_some() {
        return Err(Error::NotImplemented("WITH inside LATERAL"));
    }
    if subquery.order_by.is_some() {
        return Err(Error::NotImplemented("LATERAL ORDER BY"));
    }
    if subquery.limit_clause.is_some() {
        return Err(Error::NotImplemented("LATERAL LIMIT"));
    }
    reject_query_extras(subquery)?;
    let select = match unwrap_set(&subquery.body) {
        SetExpr::Select(s) => s,
        _ => return Err(Error::NotImplemented("LATERAL set operation")),
    };
    reject_select_extras(select)?;
    if !select.from.is_empty() {
        return Err(Error::NotImplemented("LATERAL with FROM"));
    }
    if select.selection.is_some() {
        return Err(Error::NotImplemented("LATERAL WHERE"));
    }
    if is_grouped(select, ctx.udafs) {
        return Err(Error::NotImplemented("LATERAL aggregate"));
    }
    let qualifier = alias.as_ref().map(|a| ident_key(&a.name));
    let mut exprs = Vec::new();
    for col in &left.scope.columns {
        exprs.push(NamedExpr {
            expr: Expr::nth(col.index),
            name: col.name.clone(),
        });
    }
    let mut extra_cols = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                extra_cols.push(NamedExpr {
                    name: infer_sql_name(expr),
                    expr: bind_scalar(expr, &left.scope)?,
                });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                extra_cols.push(NamedExpr {
                    name: ident_key(alias),
                    expr: bind_scalar(expr, &left.scope)?,
                });
            }
            _ => return Err(Error::NotImplemented("LATERAL select item")),
        }
    }
    if extra_cols.is_empty() {
        return Err(Error::Plan("empty LATERAL projection".into()));
    }
    let extra_start = exprs.len();
    exprs.extend(extra_cols.iter().cloned());
    let mut fields = Vec::new();
    for e in &exprs {
        let dt = expr_type(&e.expr, left.plan.schema())?;
        fields.push(Field::new(&e.name, dt, true));
    }
    let schema = schema_from_fields(fields)?;
    let mut columns = left.scope.columns.clone();
    for (i, extra) in extra_cols.iter().enumerate() {
        columns.push(ScopeCol {
            qualifier: qualifier.clone(),
            name: extra.name.clone(),
            index: extra_start + i,
        });
    }
    Ok(BoundFrom {
        plan: LogicalPlan::Project {
            input: Box::new(left.plan),
            exprs,
            schema: schema.clone(),
        },
        scope: Scope {
            columns,
            schema,
            udafs: left.scope.udafs,
        },
    })
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
