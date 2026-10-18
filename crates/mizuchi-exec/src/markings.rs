//! Opaque marking labels propagated through a logical plan.
//!
//! Mizuchi does not interpret labels. It unions them through expressions and
//! reports them on results and writes.

use std::collections::{BTreeMap, BTreeSet};

use arrow::datatypes::Schema;
use mizuchi_core::error::{Error, Result};
use mizuchi_core::expr::Expr;
use mizuchi_plan::{AggCall, JoinType, LogicalPlan, WindowFn, normalize_name};

use super::Session;
use super::policy::PolicyContext;
use super::report::ColumnLabels;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PlanMarks {
    columns: Vec<(String, BTreeSet<String>)>,
    row: BTreeSet<String>,
}

impl PlanMarks {
    fn from_schema(schema: &Schema) -> Self {
        Self {
            columns: schema
                .fields()
                .iter()
                .map(|f| (f.name().clone(), BTreeSet::new()))
                .collect(),
            row: BTreeSet::new(),
        }
    }

    fn reported(&self) -> Vec<ColumnLabels> {
        self.columns
            .iter()
            .map(|(name, labels)| {
                let mut all: Vec<String> = labels.union(&self.row).cloned().collect();
                all.sort();
                all.dedup();
                ColumnLabels {
                    name: name.clone(),
                    labels: all,
                }
            })
            .collect()
    }
}

impl PolicyContext {
    /// Opaque labels on every row of `table`.
    pub fn mark_table(mut self, table: impl Into<String>, label: impl Into<String>) -> Self {
        self.table_markings
            .entry(normalize_name(&table.into()))
            .or_default()
            .insert(label.into());
        self
    }

    /// Opaque labels on one column of `table`.
    pub fn mark_column(
        mut self,
        table: impl Into<String>,
        column: impl Into<String>,
        label: impl Into<String>,
    ) -> Self {
        self.column_markings
            .entry(normalize_name(&table.into()))
            .or_default()
            .entry(column.into().to_ascii_lowercase())
            .or_default()
            .insert(label.into());
        self
    }

    fn table_labels(&self, table: &str) -> BTreeSet<String> {
        self.table_markings
            .get(&normalize_name(table))
            .cloned()
            .unwrap_or_default()
    }

    fn column_labels_for(&self, table: &str, column: &str) -> BTreeSet<String> {
        self.column_markings
            .get(&normalize_name(table))
            .and_then(|cols| cols.get(&column.to_ascii_lowercase()))
            .cloned()
            .unwrap_or_default()
    }

    pub(super) fn covers_write(&self, dest_table: &str, columns: &[ColumnLabels]) -> Result<()> {
        let dest = normalize_name(dest_table);
        let table_labels = self.table_labels(&dest);
        let col_map = self.column_markings.get(&dest);
        for col in columns {
            let mut allowed = table_labels.clone();
            if let Some(map) = col_map
                && let Some(extra) = map.get(&col.name.to_ascii_lowercase())
            {
                allowed.extend(extra.iter().cloned());
            }
            for label in &col.labels {
                if !allowed.contains(label) {
                    return Err(Error::Plan(format!(
                        "write to {dest} would drop marking {label} of column {}",
                        col.name
                    )));
                }
            }
        }
        Ok(())
    }
}

impl Session {
    pub(super) fn refuse_bare_governed_write(&self) -> Result<()> {
        if self.governed {
            return Err(Error::Plan(
                "governed writes require a destination table".into(),
            ));
        }
        Ok(())
    }

    /// Governed writes refuse a dest whose declared labels do not cover `columns`.
    pub fn check_write_markings(&self, dest_table: &str, columns: &[ColumnLabels]) -> Result<()> {
        if !self.governed {
            return Ok(());
        }
        self.require_policy()?;
        let Some(policy) = &self.query_policy else {
            return Ok(());
        };
        policy.covers_write(dest_table, columns)
    }

    /// [`Self::write_parquet_reported`] after a dest-table marking check.
    pub fn write_parquet_reported_to(
        &self,
        dest: impl AsRef<std::path::Path>,
        dest_table: &str,
        batches: &[mizuchi_core::batch::Batch],
        column_markings: &[ColumnLabels],
        opts: mizuchi_io::ParquetWriteOptions<'_>,
    ) -> Result<super::report::QueryReport> {
        if self.governed {
            Self::require_write_markings_match_batches(batches, column_markings)?;
        }
        self.check_write_markings(dest_table, column_markings)?;
        let mut report = self.write_parquet_reported_inner(dest, batches, opts)?;
        report.column_markings = column_markings.to_vec();
        Ok(report)
    }

    fn require_write_markings_match_batches(
        batches: &[mizuchi_core::batch::Batch],
        column_markings: &[ColumnLabels],
    ) -> Result<()> {
        let Some(first) = batches.first() else {
            return Ok(());
        };
        let schema = first.schema();
        let fields = schema.fields();
        if column_markings.len() != fields.len() {
            return Err(Error::Plan(
                "governed writes require column markings for every output column".into(),
            ));
        }
        for (field, mark) in fields.iter().zip(column_markings) {
            if !field.name().eq_ignore_ascii_case(&mark.name) {
                return Err(Error::Plan(format!(
                    "governed write markings do not match column {}",
                    field.name()
                )));
            }
        }
        Ok(())
    }
}

pub(super) fn propagate(plan: &LogicalPlan, policy: &PolicyContext) -> Vec<ColumnLabels> {
    let mut work = BTreeMap::new();
    walk(plan, policy, &mut work).reported()
}

fn walk(
    plan: &LogicalPlan,
    policy: &PolicyContext,
    work: &mut BTreeMap<String, PlanMarks>,
) -> PlanMarks {
    match plan {
        LogicalPlan::Empty { schema } => PlanMarks::from_schema(schema),
        LogicalPlan::Scan {
            table,
            schema,
            projection,
            filters,
            ..
        } => scan_marks(table, schema, projection.as_deref(), filters, policy),
        LogicalPlan::Filter { input, predicate } => {
            let mut marks = walk(input, policy, work);
            marks
                .row
                .extend(expr_labels(predicate, &marks, input.schema().as_ref()));
            marks
        }
        LogicalPlan::Project { input, exprs, .. } => {
            let input_marks = walk(input, policy, work);
            let columns = exprs
                .iter()
                .map(|ne| {
                    (
                        ne.name.clone(),
                        expr_labels(&ne.expr, &input_marks, input.schema().as_ref()),
                    )
                })
                .collect();
            PlanMarks {
                columns,
                row: input_marks.row,
            }
        }
        LogicalPlan::Aggregate {
            input,
            group,
            aggs,
            schema,
        } => {
            let input_marks = walk(input, policy, work);
            let mut row = input_marks.row.clone();
            let mut columns: Vec<(String, BTreeSet<String>)> = group
                .iter()
                .map(|ne| {
                    let labels = expr_labels(&ne.expr, &input_marks, input.schema().as_ref());
                    row.extend(labels.iter().cloned());
                    (ne.name.clone(), labels)
                })
                .collect();
            for (name, call) in aggs {
                columns.push((
                    name.clone(),
                    agg_labels(call, &input_marks, input.schema().as_ref()),
                ));
            }
            PlanMarks {
                columns: zip_named(columns, schema),
                row,
            }
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            on,
            schema,
        } => {
            let left_marks = walk(left, policy, work);
            let right_marks = walk(right, policy, work);
            let mut row: BTreeSet<String> =
                left_marks.row.union(&right_marks.row).cloned().collect();
            let concat = PlanMarks {
                columns: left_marks
                    .columns
                    .iter()
                    .chain(right_marks.columns.iter())
                    .cloned()
                    .collect(),
                row: BTreeSet::new(),
            };
            if let Some(pred) = on {
                row.extend(expr_labels(pred, &concat, schema.as_ref()));
            }
            let src = match join_type {
                JoinType::Semi | JoinType::Anti | JoinType::NullAwareAnti => left_marks.columns,
                _ => concat.columns,
            };
            PlanMarks {
                columns: zip_named(src, schema),
                row,
            }
        }
        LogicalPlan::Sort { input, order } => {
            let mut marks = walk(input, policy, work);
            for key in order {
                marks
                    .row
                    .extend(expr_labels(&key.expr, &marks, input.schema().as_ref()));
            }
            marks
        }
        LogicalPlan::Limit { input, .. } => walk(input, policy, work),
        LogicalPlan::Union {
            inputs,
            schema,
            all,
        } => {
            let mut acc = PlanMarks::from_schema(schema);
            for input in inputs {
                acc = union_marks(acc, walk(input, policy, work));
            }
            if !*all {
                promote_columns_to_row(&mut acc);
            }
            acc
        }
        LogicalPlan::Intersect {
            left,
            right,
            schema,
            ..
        }
        | LogicalPlan::Except {
            left,
            right,
            schema,
            ..
        } => {
            let marks = union_marks(walk(left, policy, work), walk(right, policy, work));
            let mut aligned = PlanMarks::from_schema(schema);
            aligned.row = marks.row;
            for (slot, src) in aligned.columns.iter_mut().zip(marks.columns) {
                slot.1 = src.1;
            }
            promote_columns_to_row(&mut aligned);
            aligned
        }
        LogicalPlan::Window {
            input,
            partition,
            order,
            fns,
            schema,
            ..
        } => {
            let input_marks = walk(input, policy, work);
            let mut window_labels = BTreeSet::new();
            for expr in partition {
                window_labels.extend(expr_labels(expr, &input_marks, input.schema().as_ref()));
            }
            for key in order {
                window_labels.extend(expr_labels(
                    &key.expr,
                    &input_marks,
                    input.schema().as_ref(),
                ));
            }
            let mut columns = input_marks.columns.clone();
            for call in fns {
                let mut labels = window_labels.clone();
                if let WindowFn::Agg(agg) = &call.func {
                    labels.extend(agg_labels(agg, &input_marks, input.schema().as_ref()));
                }
                columns.push((call.name.clone(), labels));
            }
            let mut row = input_marks.row;
            row.extend(window_labels);
            PlanMarks {
                columns: zip_named(columns, schema),
                row,
            }
        }
        LogicalPlan::Recursive {
            name,
            anchors,
            step,
            schema,
        } => {
            let mut acc = PlanMarks::from_schema(schema);
            for anchor in anchors {
                acc = union_marks(acc, walk(anchor, policy, work));
            }
            let key = normalize_name(name);
            work.insert(key.clone(), acc.clone());
            loop {
                let next = union_marks(acc.clone(), walk(step, policy, work));
                if next == acc {
                    break;
                }
                acc = next;
                work.insert(key.clone(), acc.clone());
            }
            acc
        }
        LogicalPlan::WorkScan { name, schema, .. } => work
            .get(&normalize_name(name))
            .cloned()
            .unwrap_or_else(|| PlanMarks::from_schema(schema)),
        LogicalPlan::Scalar { input, schema } => {
            let marks = walk(input, policy, work);
            let mut out = PlanMarks::from_schema(schema);
            out.row = marks.row;
            if let (Some(dst), Some(src)) = (out.columns.first_mut(), marks.columns.first()) {
                dst.1 = src.1.clone();
            }
            out
        }
    }
}

fn zip_named(
    columns: Vec<(String, BTreeSet<String>)>,
    schema: &Schema,
) -> Vec<(String, BTreeSet<String>)> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let labels = columns.get(i).map(|(_, l)| l.clone()).unwrap_or_default();
            (f.name().clone(), labels)
        })
        .collect()
}

fn promote_columns_to_row(marks: &mut PlanMarks) {
    for (_, labels) in &marks.columns {
        marks.row.extend(labels.iter().cloned());
    }
}

fn union_marks(mut left: PlanMarks, right: PlanMarks) -> PlanMarks {
    left.row.extend(right.row);
    for (i, (_, labels)) in right.columns.into_iter().enumerate() {
        if let Some(slot) = left.columns.get_mut(i) {
            slot.1.extend(labels);
        }
    }
    left
}

fn scan_marks(
    table: &str,
    schema: &Schema,
    projection: Option<&[String]>,
    filters: &[Expr],
    policy: &PolicyContext,
) -> PlanMarks {
    let names: Vec<String> = match projection {
        Some(cols) if !cols.is_empty() => cols.to_vec(),
        _ => schema.fields().iter().map(|f| f.name().clone()).collect(),
    };
    let mut row = policy.table_labels(table);
    row.extend(filter_labels(filters, table, schema, policy));
    let columns = names
        .into_iter()
        .map(|name| {
            let labels = policy.column_labels_for(table, &name);
            (name, labels)
        })
        .collect();
    PlanMarks { columns, row }
}

fn filter_labels(
    filters: &[Expr],
    table: &str,
    schema: &Schema,
    policy: &PolicyContext,
) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for pred in filters {
        collect_column_names(pred, schema, &mut names);
    }
    let mut out = BTreeSet::new();
    for name in names {
        out.extend(policy.column_labels_for(table, &name));
    }
    out
}

fn agg_labels(call: &AggCall, input: &PlanMarks, schema: &Schema) -> BTreeSet<String> {
    match call {
        AggCall::CountStar => BTreeSet::new(),
        AggCall::Count(e)
        | AggCall::CountDistinct(e)
        | AggCall::Sum(e)
        | AggCall::Avg(e)
        | AggCall::Min(e)
        | AggCall::Max(e) => expr_labels(e, input, schema),
        AggCall::User { arg, .. } => expr_labels(arg, input, schema),
    }
}

fn expr_labels(expr: &Expr, input: &PlanMarks, _schema: &Schema) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    add_expr_labels(expr, input, &mut out);
    out
}

fn add_expr_labels(expr: &Expr, input: &PlanMarks, out: &mut BTreeSet<String>) {
    match expr {
        Expr::Nth(i) => {
            if let Some((_, labels)) = input.columns.get(*i) {
                out.extend(labels.iter().cloned());
            }
        }
        Expr::Column(name) => {
            if let Some((_, labels)) = input
                .columns
                .iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(name))
            {
                out.extend(labels.iter().cloned());
            }
        }
        Expr::Literal(_) => {}
        Expr::Binary(_, left, right) => {
            add_expr_labels(left, input, out);
            add_expr_labels(right, input, out);
        }
        Expr::Unary(_, child) | Expr::Cast { expr: child, .. } => {
            add_expr_labels(child, input, out);
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                add_expr_labels(op, input, out);
            }
            for (w, t) in when_then {
                add_expr_labels(w, input, out);
                add_expr_labels(t, input, out);
            }
            if let Some(e) = else_expr {
                add_expr_labels(e, input, out);
            }
        }
        Expr::Coalesce(items) | Expr::Concat(items) | Expr::Array(items) => {
            for item in items {
                add_expr_labels(item, input, out);
            }
        }
        Expr::Substring { expr, .. } => add_expr_labels(expr, input, out),
    }
}

fn collect_column_names(expr: &Expr, schema: &Schema, out: &mut BTreeSet<String>) {
    match expr {
        Expr::Column(name) => {
            out.insert(name.clone());
        }
        Expr::Nth(i) => {
            if let Some(field) = schema.fields().get(*i) {
                out.insert(field.name().clone());
            }
        }
        Expr::Literal(_) => {}
        Expr::Binary(_, left, right) => {
            collect_column_names(left, schema, out);
            collect_column_names(right, schema, out);
        }
        Expr::Unary(_, child) | Expr::Cast { expr: child, .. } => {
            collect_column_names(child, schema, out);
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_column_names(op, schema, out);
            }
            for (w, t) in when_then {
                collect_column_names(w, schema, out);
                collect_column_names(t, schema, out);
            }
            if let Some(e) = else_expr {
                collect_column_names(e, schema, out);
            }
        }
        Expr::Coalesce(items) | Expr::Concat(items) | Expr::Array(items) => {
            for item in items {
                collect_column_names(item, schema, out);
            }
        }
        Expr::Substring { expr, .. } => collect_column_names(expr, schema, out),
    }
}
