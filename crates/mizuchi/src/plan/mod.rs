//! Logical plan: resolved operators, catalog, and SQL binding.
//!
//! Parser output is bound in one pass: name resolution and type checking
//! produce a typed tree. Unsupported SQL fails closed.

mod bind;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::error::{Error, Result};
use crate::expr::Expr;
use crate::schema::from_arrow_schema;
use crate::sql::parse_sql;

pub use bind::plan_sql;

/// Registered table schemas used to bind SQL.
#[derive(Clone, Debug, Default)]
pub struct Catalog {
    tables: BTreeMap<String, SchemaRef>,
}

impl Catalog {
    /// Empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `name` with an Arrow schema. Names are matched case-insensitively.
    pub fn register(&mut self, name: impl Into<String>, schema: SchemaRef) -> Result<()> {
        let schema = from_arrow_schema(schema)?;
        self.tables.insert(normalize_name(&name.into()), schema);
        Ok(())
    }

    /// Look up a table schema.
    pub fn table(&self, name: &str) -> Result<&SchemaRef> {
        self.tables
            .get(&normalize_name(name))
            .ok_or_else(|| Error::Plan(format!("table not found: {name}")))
    }
}

pub(crate) fn normalize_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

/// Join kind on a logical join.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    Semi,
    Anti,
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Inner => "Inner",
            Self::Left => "Left",
            Self::Right => "Right",
            Self::Full => "Full",
            Self::Cross => "Cross",
            Self::Semi => "Semi",
            Self::Anti => "Anti",
        })
    }
}

/// Named scalar expression in a projection or grouping list.
#[derive(Clone, Debug)]
pub struct NamedExpr {
    /// Bound expression.
    pub expr: Expr,
    /// Output field name.
    pub name: String,
}

/// Aggregate call. Scalar [`Expr`] does not include aggregates.
#[derive(Clone, Debug)]
pub enum AggCall {
    /// `COUNT(*)`.
    CountStar,
    /// `COUNT(expr)`.
    Count(Expr),
    /// `COUNT(DISTINCT expr)`.
    CountDistinct(Expr),
    /// `SUM(expr)`.
    Sum(Expr),
    /// `AVG(expr)`.
    Avg(Expr),
    /// `MIN(expr)`.
    Min(Expr),
    /// `MAX(expr)`.
    Max(Expr),
}

impl AggCall {
    pub(crate) fn result_type(&self, input: &Schema) -> Result<DataType> {
        match self {
            Self::CountStar | Self::Count(_) | Self::CountDistinct(_) => Ok(DataType::Int64),
            Self::Avg(expr) => {
                require_numeric(expr_type(expr, input)?, "AVG")?;
                Ok(DataType::Float64)
            }
            Self::Sum(expr) => {
                let dt = expr_type(expr, input)?;
                require_numeric(dt.clone(), "SUM")?;
                Ok(dt)
            }
            Self::Min(expr) | Self::Max(expr) => Ok(expr_type(expr, input)?),
        }
    }
}

impl fmt::Display for AggCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CountStar => f.write_str("count(*)"),
            Self::Count(e) => write!(f, "count({e:?})"),
            Self::CountDistinct(e) => write!(f, "count(distinct {e:?})"),
            Self::Sum(e) => write!(f, "sum({e:?})"),
            Self::Avg(e) => write!(f, "avg({e:?})"),
            Self::Min(e) => write!(f, "min({e:?})"),
            Self::Max(e) => write!(f, "max({e:?})"),
        }
    }
}

/// Sort key.
#[derive(Clone, Debug)]
pub struct SortExpr {
    /// Bound sort expression.
    pub expr: Expr,
    /// True for ASC (the default).
    pub asc: bool,
    /// True for NULLS FIRST.
    pub nulls_first: bool,
}

/// Resolved logical plan.
#[derive(Clone, Debug)]
pub enum LogicalPlan {
    /// One-row empty relation, used for `SELECT` without `FROM`.
    Empty {
        /// Output schema, usually empty.
        schema: SchemaRef,
    },
    /// Table scan.
    Scan {
        /// Catalog table name.
        table: String,
        /// Scan schema.
        schema: SchemaRef,
        /// Optional alias used in name resolution.
        alias: Option<String>,
    },
    /// Filter rows by a boolean predicate.
    Filter {
        /// Child plan.
        input: Box<LogicalPlan>,
        /// Boolean predicate.
        predicate: Expr,
    },
    /// Project and optionally rename columns.
    Project {
        /// Child plan.
        input: Box<LogicalPlan>,
        /// Output expressions.
        exprs: Vec<NamedExpr>,
        /// Output schema.
        schema: SchemaRef,
    },
    /// Hash-style grouping aggregate.
    Aggregate {
        /// Child plan.
        input: Box<LogicalPlan>,
        /// Grouping expressions.
        group: Vec<NamedExpr>,
        /// Aggregate calls with output names.
        aggs: Vec<(String, AggCall)>,
        /// Output schema: groups then aggregates.
        schema: SchemaRef,
    },
    /// Binary join.
    Join {
        /// Left input.
        left: Box<LogicalPlan>,
        /// Right input.
        right: Box<LogicalPlan>,
        /// Join kind.
        join_type: JoinType,
        /// Join condition; `None` for a cross join.
        on: Option<Expr>,
        /// Concatenated output schema.
        schema: SchemaRef,
    },
    /// Sort.
    Sort {
        /// Child plan.
        input: Box<LogicalPlan>,
        /// Sort keys.
        order: Vec<SortExpr>,
    },
    /// Limit and optional offset.
    Limit {
        /// Child plan.
        input: Box<LogicalPlan>,
        /// Rows to skip.
        skip: u64,
        /// Rows to keep; `None` means no fetch limit.
        fetch: Option<u64>,
    },
    /// Union of inputs with a shared schema.
    Union {
        /// Inputs, at least two.
        inputs: Vec<LogicalPlan>,
        /// Output schema from the first input.
        schema: SchemaRef,
        /// `true` for `UNION ALL`.
        all: bool,
    },
}

impl LogicalPlan {
    /// Schema of this node's output.
    pub fn schema(&self) -> &SchemaRef {
        match self {
            Self::Empty { schema }
            | Self::Scan { schema, .. }
            | Self::Project { schema, .. }
            | Self::Aggregate { schema, .. }
            | Self::Join { schema, .. }
            | Self::Union { schema, .. } => schema,
            Self::Filter { input, .. } | Self::Sort { input, .. } | Self::Limit { input, .. } => {
                input.schema()
            }
        }
    }

    /// Indented EXPLAIN tree.
    pub fn explain(&self) -> String {
        let mut out = String::new();
        explain_into(self, 0, &mut out);
        out
    }
}

fn explain_into(plan: &LogicalPlan, indent: usize, out: &mut String) {
    let pad = "  ".repeat(indent);
    match plan {
        LogicalPlan::Empty { schema } => {
            out.push_str(&format!("{pad}Empty {}\n", fmt_schema(schema)));
        }
        LogicalPlan::Scan {
            table,
            schema,
            alias,
        } => match alias {
            Some(alias) if alias != table => {
                out.push_str(&format!(
                    "{pad}Scan {table} AS {alias} {}\n",
                    fmt_schema(schema)
                ));
            }
            _ => out.push_str(&format!("{pad}Scan {table} {}\n", fmt_schema(schema))),
        },
        LogicalPlan::Filter { input, predicate } => {
            out.push_str(&format!(
                "{pad}Filter {}\n",
                fmt_expr(predicate, input.schema())
            ));
            explain_into(input, indent + 1, out);
        }
        LogicalPlan::Project {
            input,
            exprs,
            schema: _,
        } => {
            let list = exprs
                .iter()
                .map(|e| format!("{}: {}", e.name, fmt_expr(&e.expr, input.schema())))
                .collect::<Vec<_>>()
                .join(", ");
            out.push_str(&format!("{pad}Project [{list}]\n"));
            explain_into(input, indent + 1, out);
        }
        LogicalPlan::Aggregate {
            input,
            group,
            aggs,
            schema: _,
        } => {
            let g = group
                .iter()
                .map(|e| format!("{}: {}", e.name, fmt_expr(&e.expr, input.schema())))
                .collect::<Vec<_>>()
                .join(", ");
            let a = aggs
                .iter()
                .map(|(name, call)| format!("{name}: {call}"))
                .collect::<Vec<_>>()
                .join(", ");
            out.push_str(&format!("{pad}Aggregate group=[{g}] aggs=[{a}]\n"));
            explain_into(input, indent + 1, out);
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            on,
            schema: _,
        } => {
            let cond = match on {
                Some(expr) => fmt_expr(expr, &join_schema(left.schema(), right.schema())),
                None => "true".into(),
            };
            out.push_str(&format!("{pad}Join {join_type} {cond}\n"));
            explain_into(left, indent + 1, out);
            explain_into(right, indent + 1, out);
        }
        LogicalPlan::Sort { input, order } => {
            let keys = order
                .iter()
                .map(|s| {
                    let dir = if s.asc { "ASC" } else { "DESC" };
                    let nulls = if s.nulls_first {
                        "NULLS FIRST"
                    } else {
                        "NULLS LAST"
                    };
                    format!("{} {dir} {nulls}", fmt_expr(&s.expr, input.schema()))
                })
                .collect::<Vec<_>>()
                .join(", ");
            out.push_str(&format!("{pad}Sort [{keys}]\n"));
            explain_into(input, indent + 1, out);
        }
        LogicalPlan::Limit { input, skip, fetch } => {
            match fetch {
                Some(n) => out.push_str(&format!("{pad}Limit skip={skip} fetch={n}\n")),
                None => out.push_str(&format!("{pad}Limit skip={skip}\n")),
            }
            explain_into(input, indent + 1, out);
        }
        LogicalPlan::Union { inputs, all, .. } => {
            let kind = if *all { "all" } else { "distinct" };
            out.push_str(&format!("{pad}Union {kind}\n"));
            for input in inputs {
                explain_into(input, indent + 1, out);
            }
        }
    }
}

fn fmt_schema(schema: &Schema) -> String {
    let fields = schema
        .fields()
        .iter()
        .map(|f| format!("{}: {}", f.name(), f.data_type()))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{fields}]")
}

pub(crate) fn fmt_expr(expr: &Expr, schema: &Schema) -> String {
    match expr {
        Expr::Nth(i) => {
            if *i < schema.fields().len() {
                schema.field(*i).name().to_string()
            } else {
                format!("#{i}")
            }
        }
        Expr::Column(name) => name.clone(),
        Expr::Literal(arr) => match arr.data_type() {
            DataType::Null => "NULL".into(),
            DataType::Boolean => format!("{:?}", arr),
            _ => format!("{arr:?}"),
        },
        Expr::Binary(op, l, r) => {
            format!("({} {op:?} {})", fmt_expr(l, schema), fmt_expr(r, schema))
        }
        Expr::Unary(op, e) => format!("({op:?} {})", fmt_expr(e, schema)),
        Expr::Cast { expr, data_type } => {
            format!("CAST({} AS {data_type})", fmt_expr(expr, schema))
        }
        Expr::Case { .. } => "CASE ...".into(),
        Expr::Coalesce(args) => {
            let inner = args
                .iter()
                .map(|e| fmt_expr(e, schema))
                .collect::<Vec<_>>()
                .join(", ");
            format!("coalesce({inner})")
        }
        Expr::Concat(args) => {
            let inner = args
                .iter()
                .map(|e| fmt_expr(e, schema))
                .collect::<Vec<_>>()
                .join(", ");
            format!("concat({inner})")
        }
        Expr::Substring {
            expr,
            start,
            length,
        } => match length {
            Some(len) => format!("substring({}, {start}, {len})", fmt_expr(expr, schema)),
            None => format!("substring({}, {start})", fmt_expr(expr, schema)),
        },
    }
}

pub(crate) fn join_schema(left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Field> = left.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.extend(right.fields().iter().map(|f| f.as_ref().clone()));
    Arc::new(Schema::new(fields))
}

pub(crate) fn schema_from_fields(fields: Vec<Field>) -> Result<SchemaRef> {
    from_arrow_schema(Arc::new(Schema::new(fields)))
}

pub(crate) fn expr_type(expr: &Expr, schema: &Schema) -> Result<DataType> {
    match expr {
        Expr::Nth(i) => {
            if *i >= schema.fields().len() {
                return Err(Error::Plan(format!("column index {i} out of range")));
            }
            Ok(schema.field(*i).data_type().clone())
        }
        Expr::Column(name) => schema
            .column_with_name(name)
            .map(|(_, f)| f.data_type().clone())
            .ok_or_else(|| Error::Plan(format!("column not found: {name}"))),
        Expr::Literal(arr) => Ok(arr.data_type().clone()),
        Expr::Binary(op, l, r) => binary_type(*op, expr_type(l, schema)?, expr_type(r, schema)?),
        Expr::Unary(op, e) => unary_type(*op, expr_type(e, schema)?),
        Expr::Cast { data_type, .. } => Ok(data_type.clone()),
        Expr::Case {
            when_then,
            else_expr,
            ..
        } => {
            let mut dt = expr_type(&when_then[0].1, schema)?;
            if let Some(else_expr) = else_expr {
                dt = widen(dt, expr_type(else_expr, schema)?)?;
            }
            Ok(dt)
        }
        Expr::Coalesce(args) => {
            let mut dt = expr_type(&args[0], schema)?;
            for arg in &args[1..] {
                dt = widen(dt, expr_type(arg, schema)?)?;
            }
            Ok(dt)
        }
        Expr::Concat(_) => Ok(DataType::Utf8),
        Expr::Substring { .. } => Ok(DataType::Utf8),
    }
}

fn binary_type(op: crate::expr::BinaryOp, l: DataType, r: DataType) -> Result<DataType> {
    use crate::expr::BinaryOp::*;
    match op {
        Add | Sub | Mul | Div => {
            require_numeric(l.clone(), "arithmetic")?;
            require_numeric(r.clone(), "arithmetic")?;
            widen(l, r)
        }
        Eq | NotEq | Lt | LtEq | Gt | GtEq => Ok(DataType::Boolean),
        And | Or => {
            require_boolean(l, "boolean")?;
            require_boolean(r, "boolean")?;
            Ok(DataType::Boolean)
        }
    }
}

fn unary_type(op: crate::expr::UnaryOp, dt: DataType) -> Result<DataType> {
    use crate::expr::UnaryOp::*;
    match op {
        Not => {
            require_boolean(dt, "NOT")?;
            Ok(DataType::Boolean)
        }
        Neg => {
            require_numeric(dt.clone(), "negation")?;
            Ok(dt)
        }
        IsNull | IsNotNull => Ok(DataType::Boolean),
        Upper | Lower => Ok(DataType::Utf8),
    }
}

pub(crate) fn require_boolean(dt: DataType, what: &str) -> Result<()> {
    if dt == DataType::Boolean {
        Ok(())
    } else {
        Err(Error::Plan(format!("{what} requires boolean, got {dt}")))
    }
}

pub(crate) fn require_numeric(dt: DataType, what: &str) -> Result<()> {
    if matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
    ) {
        Ok(())
    } else {
        Err(Error::Plan(format!("{what} requires numeric, got {dt}")))
    }
}

fn widen(l: DataType, r: DataType) -> Result<DataType> {
    if l == r {
        return Ok(l);
    }
    if l == DataType::Null {
        return Ok(r);
    }
    if r == DataType::Null {
        return Ok(l);
    }
    if is_float(&l) || is_float(&r) {
        return Ok(DataType::Float64);
    }
    if is_int(&l) && is_int(&r) {
        return Ok(DataType::Int64);
    }
    Err(Error::Plan(format!("incompatible types {l} and {r}")))
}

fn is_float(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

fn is_int(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

pub(crate) fn parse_statements(sql: &str) -> Result<sqlparser::ast::Statement> {
    let mut stmts = parse_sql(sql)?;
    if stmts.len() != 1 {
        return Err(Error::Plan("plan_sql accepts exactly one statement".into()));
    }
    Ok(stmts.pop().expect("len checked"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    fn schema(fields: &[(&str, DataType)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(n, dt)| Field::new(*n, dt.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }

    fn catalog() -> Catalog {
        let mut c = Catalog::new();
        c.register(
            "t",
            schema(&[("a", DataType::Int64), ("b", DataType::Utf8)]),
        )
        .unwrap();
        c.register(
            "u",
            schema(&[("id", DataType::Int64), ("a", DataType::Int64)]),
        )
        .unwrap();
        c
    }

    fn explain(sql: &str) -> String {
        plan_sql(sql, &catalog()).unwrap().explain()
    }

    #[test]
    fn select_literal_is_project_over_empty() {
        let plan = plan_sql("SELECT 1", &Catalog::new()).unwrap();
        let text = plan.explain();
        assert!(text.contains("Project ["), "{text}");
        assert!(text.contains("Empty"), "{text}");
        assert_eq!(plan.schema().fields().len(), 1);
        assert_eq!(plan.schema().field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn select_from_where_order_limit_snapshot() {
        let text = explain("SELECT a FROM t WHERE a > 1 ORDER BY a LIMIT 10 OFFSET 2");
        assert!(text.contains("Limit skip=2 fetch=10"), "{text}");
        assert!(text.contains("Sort ["), "{text}");
        assert!(text.contains("Project [a:"), "{text}");
        assert!(text.contains("Filter"), "{text}");
        assert!(text.contains("Scan t"), "{text}");
    }

    #[test]
    fn join_group_by_union_corpus() {
        let join = explain("SELECT t.a, u.a FROM t JOIN u ON t.a = u.id");
        assert!(join.contains("Join Inner"), "{join}");
        assert!(join.contains("Scan t"), "{join}");
        assert!(join.contains("Scan u"), "{join}");

        let agg = explain("SELECT a, COUNT(*) FROM t GROUP BY a");
        assert!(agg.contains("Aggregate"), "{agg}");
        assert!(agg.contains("count(*)"), "{agg}");

        let distinct = explain("SELECT COUNT(DISTINCT a) FROM t");
        assert!(distinct.contains("count(distinct"), "{distinct}");

        let union = explain("SELECT a FROM t UNION ALL SELECT a FROM t");
        assert!(union.contains("Union all"), "{union}");

        let having = explain("SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1");
        assert!(having.contains("Filter"), "{having}");
        assert!(having.contains("Aggregate"), "{having}");
    }

    #[test]
    fn unknown_column_and_table_fail_closed() {
        assert!(matches!(
            plan_sql("SELECT missing FROM t", &catalog()),
            Err(Error::Plan(_))
        ));
        assert!(matches!(
            plan_sql("SELECT * FROM missing", &catalog()),
            Err(Error::Plan(_))
        ));
    }

    #[test]
    fn ambiguous_column_fails_closed() {
        let err = plan_sql("SELECT a FROM t JOIN u ON t.a = u.id", &catalog()).unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
        assert!(err.to_string().contains("ambiguous"), "{err}");
    }

    #[test]
    fn unsupported_sql_fails_closed() {
        assert!(matches!(
            plan_sql("INSERT INTO t VALUES (1)", &catalog()),
            Err(Error::NotImplemented(_))
        ));
        assert!(matches!(
            plan_sql("SELECT DISTINCT a FROM t", &catalog()),
            Err(Error::NotImplemented(_))
        ));
        assert!(matches!(
            plan_sql("SELECT COUNT(*) OVER () FROM t", &catalog()),
            Err(Error::NotImplemented(_))
        ));
        assert!(matches!(
            plan_sql("WITH c AS (SELECT 1) SELECT * FROM c", &Catalog::new()),
            Err(Error::NotImplemented(_))
        ));
    }

    #[test]
    fn syntax_errors_are_parse() {
        assert!(matches!(
            plan_sql("SELECT FROM", &Catalog::new()),
            Err(Error::Parse(_))
        ));
    }
}
