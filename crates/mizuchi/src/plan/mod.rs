//! Logical plan: resolved operators, catalog, and SQL binding.
//!
//! Parser output is bound in one pass: name resolution and type checking
//! produce a typed tree. Unsupported SQL fails closed.

mod bind;
mod catalog;
mod stats;

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::error::{Error, Result};
use crate::expr::{Expr, numeric_arith_type, widen_decimal};
use crate::schema::from_arrow_schema;
use crate::sql::parse_sql;
use crate::udaf::CombineAgg;

pub use bind::{bind_ddl, bind_merge, merge_target_table, plan_sql, plan_sql_with_udafs};
pub(crate) use catalog::normalize_name;
pub use catalog::{
    AlterTable, Catalog, CatalogProvider, DEFAULT_CATALOG, DEFAULT_NAMESPACE, Ddl, NamespaceIdent,
    TableIdent, TableMeta, apply_alter,
};
pub use stats::{ColumnStats, TableStats, inner_equi_rows, table_stats_from_batches};

/// Which `WHEN` arm a bound MERGE clause applies to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeMatch {
    /// `WHEN MATCHED`
    Matched,
    /// `WHEN NOT MATCHED` / `WHEN NOT MATCHED BY TARGET`
    NotMatched,
    /// `WHEN NOT MATCHED BY SOURCE`
    NotMatchedBySource,
}

/// Action for one MERGE clause.
#[derive(Clone, Debug)]
pub enum MergeActionPlan {
    /// Drop the row.
    Delete,
    /// Replace target columns. Each pair is `(target_ordinal, expr on the
    /// clause's input schema)`.
    Update {
        /// Assignments in clause order.
        assignments: Vec<(usize, Expr)>,
    },
    /// Build a target row. `values[i]` is the expression for target column
    /// `i`, or `None` for null.
    Insert {
        /// Per-column expressions, aligned with the target schema.
        values: Vec<Option<Expr>>,
    },
}

/// One bound `WHEN` clause.
#[derive(Clone, Debug)]
pub struct MergeClausePlan {
    /// Match kind.
    pub kind: MergeMatch,
    /// Extra predicate; `None` matches every row of `kind`.
    pub predicate: Option<Expr>,
    /// Action when the predicate holds.
    pub action: MergeActionPlan,
}

/// Bound `MERGE` statement. Executed by [`crate::exec::Session::merge_sql`].
#[derive(Clone, Debug)]
pub struct MergePlan {
    /// Target table name (normalized).
    pub target: String,
    /// Source relation.
    pub source: LogicalPlan,
    /// Join condition with the target on the left and the source on the right.
    pub on_ts: Expr,
    /// Join condition with the source on the left and the target on the right.
    pub on_st: Expr,
    /// Clauses in source order; the first match wins.
    pub clauses: Vec<MergeClausePlan>,
    /// Target schema after the merge.
    pub target_schema: SchemaRef,
    /// Source output schema.
    pub source_schema: SchemaRef,
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
    /// `NOT IN (SELECT ...)`: unmatched non-null left keys, empty if the
    /// subquery produced a null. Null keys never match a regular anti join.
    NullAwareAnti,
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
            Self::NullAwareAnti => "NullAwareAnti",
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
#[derive(Clone)]
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
    /// Registered combinable aggregate. Buffer type equals result type.
    User {
        /// SQL name.
        name: String,
        /// Input expression.
        arg: Expr,
        /// Combiner used at execution, including spill merge.
        agg: Arc<dyn CombineAgg>,
    },
}

impl fmt::Debug for AggCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CountStar => f.write_str("CountStar"),
            Self::Count(e) => f.debug_tuple("Count").field(e).finish(),
            Self::CountDistinct(e) => f.debug_tuple("CountDistinct").field(e).finish(),
            Self::Sum(e) => f.debug_tuple("Sum").field(e).finish(),
            Self::Avg(e) => f.debug_tuple("Avg").field(e).finish(),
            Self::Min(e) => f.debug_tuple("Min").field(e).finish(),
            Self::Max(e) => f.debug_tuple("Max").field(e).finish(),
            Self::User { name, arg, .. } => f
                .debug_struct("User")
                .field("name", name)
                .field("arg", arg)
                .finish(),
        }
    }
}

impl AggCall {
    pub(crate) fn result_type(&self, input: &Schema) -> Result<DataType> {
        match self {
            Self::CountStar | Self::Count(_) | Self::CountDistinct(_) => Ok(DataType::Int64),
            Self::Avg(expr) => {
                require_int_or_float(expr_type(expr, input)?, "AVG")?;
                Ok(DataType::Float64)
            }
            Self::Sum(expr) => {
                let dt = expr_type(expr, input)?;
                require_int_or_float(dt.clone(), "SUM")?;
                Ok(dt)
            }
            Self::Min(expr) | Self::Max(expr) => Ok(expr_type(expr, input)?),
            Self::User { agg, arg, .. } => {
                let _ = expr_type(arg, input)?;
                Ok(agg.result_type())
            }
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
            Self::User { name, arg, .. } => write!(f, "{name}({arg:?})"),
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

/// Window function evaluated after partitioning and ordering.
#[derive(Clone, Debug)]
pub enum WindowFn {
    /// `ROW_NUMBER()`
    RowNumber,
    /// `RANK()`
    Rank,
    /// `DENSE_RANK()`
    DenseRank,
    /// Aggregate with `OVER`.
    Agg(AggCall),
}

/// One window function in a [`LogicalPlan::Window`].
#[derive(Clone, Debug)]
pub struct WindowCall {
    /// Output field name.
    pub name: String,
    /// Function.
    pub func: WindowFn,
}

/// Frame units for a window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameUnits {
    /// Physical row offsets.
    Rows,
    /// Peer groups on the order keys.
    Range,
}

/// One end of a window frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameBound {
    /// Unbounded start of the partition.
    UnboundedPreceding,
    /// The current row (or peer group for RANGE).
    CurrentRow,
    /// Unbounded end of the partition.
    UnboundedFollowing,
}

/// Frame of a window function.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WindowFrame {
    /// Rows vs range.
    pub units: FrameUnits,
    /// Inclusive start.
    pub start: FrameBound,
    /// Inclusive end.
    pub end: FrameBound,
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
        /// Output schema after any pushed projection.
        schema: SchemaRef,
        /// Optional alias used in name resolution.
        alias: Option<String>,
        /// Column names to read; `None` reads every table column.
        projection: Option<Vec<String>>,
        /// Predicates pushed into the scan, evaluated on the table schema.
        filters: Vec<Expr>,
        /// Table statistics from the catalog, if known.
        stats: Option<TableStats>,
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
    /// Rows that appear in both inputs. Schema is the left input.
    Intersect {
        /// Left input; output order follows first-seen rows here.
        left: Box<LogicalPlan>,
        /// Right input, used as the probe table.
        right: Box<LogicalPlan>,
        /// Output schema from the left input.
        schema: SchemaRef,
        /// `true` for `INTERSECT ALL` (bag counts); `false` is DISTINCT.
        all: bool,
    },
    /// Left rows that do not appear in the right input. Schema is the left.
    Except {
        /// Left input; output order follows first-seen rows here.
        left: Box<LogicalPlan>,
        /// Right input, used as the probe table.
        right: Box<LogicalPlan>,
        /// Output schema from the left input.
        schema: SchemaRef,
        /// `true` for `EXCEPT ALL` (bag counts); `false` is DISTINCT.
        all: bool,
    },
    /// Window functions: input columns plus one column per function.
    Window {
        /// Child plan.
        input: Box<LogicalPlan>,
        /// Partition expressions; empty is one partition.
        partition: Vec<Expr>,
        /// Order keys within each partition.
        order: Vec<SortExpr>,
        /// Frame shared by every function in `fns`.
        frame: WindowFrame,
        /// Window functions, in output-column order after the input fields.
        fns: Vec<WindowCall>,
        /// Output schema: input fields then window results.
        schema: SchemaRef,
    },
    /// Recursive CTE: concatenate the seeds with successive evaluations of
    /// `step` over a working table named `name` until the step is empty.
    Recursive {
        /// Working-table name the recursive step scans.
        name: String,
        /// Non-recursive UNION ALL seeds, in query order.
        anchors: Vec<LogicalPlan>,
        /// Recursive branch; [`WorkScan`] of `name` reads the previous iteration.
        step: Box<LogicalPlan>,
        /// Output schema, taken from the first seed.
        schema: SchemaRef,
    },
    /// Previous iteration of a recursive CTE. Distinct from [`Scan`] so a
    /// catalog table of the same name is not overlaid.
    WorkScan {
        /// Working-table name of the recursive CTE.
        name: String,
        /// Output schema, matching the recursive CTE.
        schema: SchemaRef,
        /// Optional alias used in name resolution.
        alias: Option<String>,
    },
    /// Scalar subquery: exactly one row. Zero rows become a null row;
    /// more than one row fails at execution.
    Scalar {
        /// Inner query, which must have one column.
        input: Box<LogicalPlan>,
        /// Output schema, matching `input`.
        schema: SchemaRef,
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
            | Self::Union { schema, .. }
            | Self::Intersect { schema, .. }
            | Self::Except { schema, .. }
            | Self::Window { schema, .. }
            | Self::Recursive { schema, .. }
            | Self::WorkScan { schema, .. }
            | Self::Scalar { schema, .. } => schema,
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
            projection,
            filters,
            stats,
        } => {
            let mut extra = String::new();
            if let Some(names) = projection {
                extra.push_str(&format!(" projection=[{}]", names.join(", ")));
            }
            if !filters.is_empty() {
                let preds = filters
                    .iter()
                    .map(|f| fmt_expr(f, schema))
                    .collect::<Vec<_>>()
                    .join(", ");
                extra.push_str(&format!(" filters=[{preds}]"));
            }
            if let Some(stats) = stats {
                extra.push_str(&format!(" rowcount={}", stats.row_count));
            }
            match alias {
                Some(alias) if alias != table => {
                    out.push_str(&format!(
                        "{pad}Scan {table} AS {alias}{extra} {}\n",
                        fmt_schema(schema)
                    ));
                }
                _ => out.push_str(&format!(
                    "{pad}Scan {table}{extra} {}\n",
                    fmt_schema(schema)
                )),
            }
        }
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
        LogicalPlan::Intersect {
            left, right, all, ..
        } => {
            let kind = if *all { "all" } else { "distinct" };
            out.push_str(&format!("{pad}Intersect {kind}\n"));
            explain_into(left, indent + 1, out);
            explain_into(right, indent + 1, out);
        }
        LogicalPlan::Except {
            left, right, all, ..
        } => {
            let kind = if *all { "all" } else { "distinct" };
            out.push_str(&format!("{pad}Except {kind}\n"));
            explain_into(left, indent + 1, out);
            explain_into(right, indent + 1, out);
        }
        LogicalPlan::Window {
            input,
            partition,
            order,
            fns,
            ..
        } => {
            let p = partition
                .iter()
                .map(|e| fmt_expr(e, input.schema()))
                .collect::<Vec<_>>()
                .join(", ");
            let o = order
                .iter()
                .map(|s| fmt_expr(&s.expr, input.schema()))
                .collect::<Vec<_>>()
                .join(", ");
            let f = fns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            out.push_str(&format!(
                "{pad}Window partition=[{p}] order=[{o}] fns=[{f}]\n"
            ));
            explain_into(input, indent + 1, out);
        }
        LogicalPlan::Recursive {
            name,
            anchors,
            step,
            schema,
        } => {
            out.push_str(&format!("{pad}Recursive {name} {}\n", fmt_schema(schema)));
            for anchor in anchors {
                explain_into(anchor, indent + 1, out);
            }
            explain_into(step, indent + 1, out);
        }
        LogicalPlan::WorkScan {
            name,
            schema,
            alias,
        } => match alias {
            Some(alias) if alias != name => {
                out.push_str(&format!(
                    "{pad}WorkScan {name} AS {alias} {}\n",
                    fmt_schema(schema)
                ));
            }
            _ => out.push_str(&format!("{pad}WorkScan {name} {}\n", fmt_schema(schema))),
        },
        LogicalPlan::Scalar { input, schema } => {
            out.push_str(&format!("{pad}Scalar {}\n", fmt_schema(schema)));
            explain_into(input, indent + 1, out);
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
        Expr::Array(args) => {
            let inner = args
                .iter()
                .map(|e| fmt_expr(e, schema))
                .collect::<Vec<_>>()
                .join(", ");
            format!("ARRAY[{inner}]")
        }
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
        Expr::Array(args) => {
            if args.is_empty() {
                return Err(Error::Plan("ARRAY requires at least one element".into()));
            }
            let dt = expr_type(&args[0], schema)?;
            for arg in &args[1..] {
                let next = expr_type(arg, schema)?;
                if next != dt {
                    return Err(Error::Plan("ARRAY elements must share a type".into()));
                }
            }
            Ok(DataType::List(Arc::new(Field::new("item", dt, true))))
        }
    }
}

fn binary_type(op: crate::expr::BinaryOp, l: DataType, r: DataType) -> Result<DataType> {
    use crate::expr::BinaryOp::*;
    match op {
        Add | Sub | Mul | Div => {
            require_numeric(l.clone(), "arithmetic")?;
            require_numeric(r.clone(), "arithmetic")?;
            numeric_arith_type(op, l, r)
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
        Upper | Lower | Trim | Ltrim | Rtrim => Ok(DataType::Utf8),
        Abs => {
            require_numeric(dt.clone(), "abs")?;
            Ok(dt)
        }
        Ceil | Floor | Sqrt | Round => {
            require_numeric(dt, "numeric function")?;
            Ok(DataType::Float64)
        }
        CharLength => Ok(DataType::Int64),
        ExtractYear | ExtractMonth | ExtractDay => Ok(DataType::Int64),
        ArrayLength => {
            require_list(dt, "array_length")?;
            Ok(DataType::Int64)
        }
        ExtractEpoch => Ok(DataType::Float64),
    }
}

pub(crate) fn require_boolean(dt: DataType, what: &str) -> Result<()> {
    if dt == DataType::Boolean {
        Ok(())
    } else {
        Err(Error::Plan(format!("{what} requires boolean, got {dt}")))
    }
}

fn require_list(dt: DataType, what: &str) -> Result<()> {
    if matches!(
        dt,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
    ) {
        Ok(())
    } else {
        Err(Error::Plan(format!("{what} requires list, got {dt}")))
    }
}

pub(crate) fn require_numeric(dt: DataType, what: &str) -> Result<()> {
    if dt.is_numeric() {
        Ok(())
    } else {
        Err(Error::Plan(format!("{what} requires numeric, got {dt}")))
    }
}

fn require_int_or_float(dt: DataType, what: &str) -> Result<()> {
    if dt.is_integer() || dt.is_floating() {
        Ok(())
    } else {
        Err(Error::Plan(format!(
            "{what} requires integer or float, got {dt}"
        )))
    }
}

pub(crate) fn widen(l: DataType, r: DataType) -> Result<DataType> {
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
    if let Some(dt) = widen_decimal(&l, &r) {
        return dt;
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
    use std::collections::BTreeMap;

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
    fn register_clears_previous_stats() {
        let mut c = Catalog::new();
        c.register("t", schema(&[("a", DataType::Int64)])).unwrap();
        c.set_stats(
            "t",
            TableStats {
                row_count: 7,
                columns: BTreeMap::new(),
                ..TableStats::default()
            },
        )
        .unwrap();
        assert_eq!(c.stats("t").map(|s| s.row_count), Some(7));
        c.register("t", schema(&[("b", DataType::Int64)])).unwrap();
        assert!(c.stats("t").is_none());
        assert_eq!(c.table("t").unwrap().field(0).name(), "b");
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
        let intersect = explain("SELECT a FROM t INTERSECT SELECT a FROM t");
        assert!(intersect.contains("Intersect distinct"), "{intersect}");
        let except = explain("SELECT a FROM t EXCEPT ALL SELECT a FROM t");
        assert!(except.contains("Except all"), "{except}");

        let window = explain("SELECT ROW_NUMBER() OVER (ORDER BY a) FROM t");
        assert!(window.contains("Window"), "{window}");
        assert!(window.contains("Project"), "{window}");

        let cte = explain("WITH c AS (SELECT 1 AS n) SELECT n FROM c");
        assert!(cte.contains("Project"), "{cte}");
        assert!(cte.contains("Empty"), "{cte}");

        let rec = plan_sql(
            "WITH RECURSIVE c AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM c WHERE n < 3) SELECT n FROM c",
            &Catalog::new(),
        )
        .unwrap()
        .explain();
        assert!(rec.contains("Recursive c"), "{rec}");
        assert!(rec.contains("WorkScan c"), "{rec}");

        let in_list = explain("SELECT a FROM t WHERE a IN (1, 2)");
        assert!(in_list.contains("Filter"), "{in_list}");
        let in_sub = explain("SELECT a FROM t WHERE a IN (SELECT a FROM t)");
        assert!(in_sub.contains("Join Semi"), "{in_sub}");
        let not_in = explain("SELECT a FROM t WHERE a NOT IN (SELECT a FROM t)");
        assert!(not_in.contains("Join NullAwareAnti"), "{not_in}");
        let exists = explain("SELECT a FROM t WHERE EXISTS (SELECT 1 FROM t u WHERE u.a = t.a)");
        assert!(exists.contains("Join Semi"), "{exists}");
        let scalar = plan_sql("SELECT (SELECT a FROM t WHERE a = 1)", &catalog())
            .unwrap()
            .explain();
        assert!(scalar.contains("Scalar"), "{scalar}");
        let lateral = explain("SELECT a FROM t, LATERAL (SELECT a AS x) s WHERE t.a = s.x");
        assert!(lateral.contains("Project"), "{lateral}");

        let err = plan_sql(
            "WITH t AS (SELECT 1 AS n) SELECT n FROM sales.t",
            &Catalog::new(),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
        let err = plan_sql(
            "WITH t AS (SELECT 1 AS n) SELECT n FROM public.t",
            &Catalog::new(),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
        let err = plan_sql(
            "WITH RECURSIVE c AS (\
             SELECT 1 AS n UNION ALL SELECT n + 1 FROM c WHERE n < 3 \
             UNION ALL SELECT n FROM c WHERE n < 0) SELECT n FROM c",
            &Catalog::new(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("non-recursive term"), "{err}");
        assert!(matches!(
            plan_sql(
                "WITH RECURSIVE c AS (\
                 SELECT 1 AS n UNION ALL SELECT n + 1 FROM c WHERE n < 3 \
                 FETCH FIRST 1 ROWS ONLY) SELECT n FROM c",
                &Catalog::new(),
            ),
            Err(Error::NotImplemented(_))
        ));

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
            plan_sql("SELECT lag(a) OVER (ORDER BY a) FROM t", &catalog()),
            Err(Error::NotImplemented(_))
        ));
        assert!(matches!(
            plan_sql("WITH c (n) AS (SELECT 1) SELECT * FROM c", &Catalog::new()),
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

    #[test]
    fn decimal_sql_maps_and_arithmetic_types() {
        let plan = plan_sql("SELECT CAST(1 AS DECIMAL(10, 2))", &Catalog::new()).unwrap();
        assert_eq!(
            plan.schema().field(0).data_type(),
            &DataType::Decimal64(10, 2)
        );
        let plan = plan_sql("SELECT CAST(1 AS NUMERIC)", &Catalog::new()).unwrap();
        assert_eq!(
            plan.schema().field(0).data_type(),
            &DataType::Decimal64(10, 0)
        );
        let plan = plan_sql(
            "SELECT CAST('1.50' AS DECIMAL(10, 2)) + CAST('2.25' AS DECIMAL(10, 2))",
            &Catalog::new(),
        )
        .unwrap();
        assert_eq!(
            plan.schema().field(0).data_type(),
            &DataType::Decimal64(11, 2)
        );
        let plan = plan_sql(
            "SELECT CAST(1 AS DECIMAL(4, 1)) * CAST(2 AS DECIMAL(4, 1))",
            &Catalog::new(),
        )
        .unwrap();
        assert_eq!(
            plan.schema().field(0).data_type(),
            &DataType::Decimal32(9, 2)
        );
        let err = plan_sql("SELECT CAST(1 AS DECIMAL(39, 0))", &Catalog::new()).unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
        let err = plan_sql("SELECT CAST(1 AS DECIMAL(10, 11))", &Catalog::new()).unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
        let err = plan_sql(
            "SELECT CAST(1 AS DECIMAL(10, 2)) * CAST(1 AS DECIMAL(30, 2))",
            &Catalog::new(),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
        let err = plan_sql("SELECT SUM(CAST(1 AS DECIMAL(10, 2)))", &Catalog::new()).unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
    }
}
