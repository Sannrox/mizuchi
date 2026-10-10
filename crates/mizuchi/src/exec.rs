//! Pull-based vectorized operators over Arrow record batches.
//!
//! See `docs/decisions/0004-pull-vectorized-execution.md`.

use std::collections::BTreeMap;

use arrow::array::{Array, BooleanArray, RecordBatch, RecordBatchOptions};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::boolean::{and, is_not_null};
use arrow::datatypes::SchemaRef;

use crate::batch::{Batch, BatchPolicy};
use crate::error::{Error, Result};
use crate::expr::Expr;
use crate::plan::{Catalog, LogicalPlan, NamedExpr, normalize_name};

/// In-memory tables plus a batch-size policy.
#[derive(Clone, Debug, Default)]
pub struct Session {
    tables: BTreeMap<String, MemTable>,
    policy: BatchPolicy,
}

#[derive(Clone, Debug)]
struct MemTable {
    schema: SchemaRef,
    batches: Vec<Batch>,
}

impl Session {
    /// Empty session with the default morsel size.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an in-memory table. All batches must share `schema`.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        schema: SchemaRef,
        batches: Vec<Batch>,
    ) -> Result<()> {
        let schema = crate::schema::from_arrow_schema(schema)?;
        for batch in &batches {
            if batch.schema().as_ref() != schema.as_ref() {
                return Err(Error::InvalidBatch(
                    "registered batch schema does not match the table".into(),
                ));
            }
        }
        self.tables
            .insert(normalize_name(&name.into()), MemTable { schema, batches });
        Ok(())
    }

    /// Plan and run `sql` against registered tables.
    pub fn execute_sql(&self, sql: &str) -> Result<Vec<Batch>> {
        let mut catalog = Catalog::new();
        for (name, table) in &self.tables {
            catalog.register(name, table.schema.clone())?;
        }
        let plan = crate::plan::plan_sql(sql, &catalog)?;
        collect(physical(&plan, self)?)
    }
}

/// Pull iterator of morsels.
enum Exec {
    Empty {
        schema: SchemaRef,
        done: bool,
    },
    Scan {
        batches: Vec<Batch>,
        index: usize,
    },
    Filter {
        input: Box<Exec>,
        predicate: Expr,
    },
    Project {
        input: Box<Exec>,
        exprs: Vec<NamedExpr>,
        schema: SchemaRef,
    },
}

impl Exec {
    fn next_batch(&mut self) -> Result<Option<Batch>> {
        match self {
            Self::Empty { schema, done } => {
                if *done {
                    return Ok(None);
                }
                *done = true;
                Ok(Some(one_row_empty(schema.clone())?))
            }
            Self::Scan { batches, index } => {
                if *index >= batches.len() {
                    return Ok(None);
                }
                let batch = batches[*index].clone();
                *index += 1;
                Ok(Some(batch))
            }
            Self::Filter { input, predicate } => loop {
                let Some(batch) = input.next_batch()? else {
                    return Ok(None);
                };
                if let Some(out) = filter_batch(&batch, predicate)? {
                    return Ok(Some(out));
                }
            },
            Self::Project {
                input,
                exprs,
                schema,
            } => {
                let Some(batch) = input.next_batch()? else {
                    return Ok(None);
                };
                Ok(Some(project_batch(&batch, exprs, schema.clone())?))
            }
        }
    }
}

fn physical(plan: &LogicalPlan, session: &Session) -> Result<Exec> {
    match plan {
        LogicalPlan::Empty { schema } => Ok(Exec::Empty {
            schema: schema.clone(),
            done: false,
        }),
        LogicalPlan::Scan { table, .. } => {
            let table = session
                .tables
                .get(&normalize_name(table))
                .ok_or_else(|| Error::Plan(format!("table not found at execution: {table}")))?;
            let mut morsels = Vec::new();
            for batch in &table.batches {
                morsels.extend(batch.split_by_policy(&session.policy));
            }
            Ok(Exec::Scan {
                batches: morsels,
                index: 0,
            })
        }
        LogicalPlan::Filter { input, predicate } => Ok(Exec::Filter {
            input: Box::new(physical(input, session)?),
            predicate: predicate.clone(),
        }),
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => Ok(Exec::Project {
            input: Box::new(physical(input, session)?),
            exprs: exprs.clone(),
            schema: schema.clone(),
        }),
        LogicalPlan::Aggregate { .. } => Err(Error::NotImplemented("physical aggregate")),
        LogicalPlan::Join { .. } => Err(Error::NotImplemented("physical join")),
        LogicalPlan::Sort { .. } => Err(Error::NotImplemented("physical sort")),
        LogicalPlan::Limit { .. } => Err(Error::NotImplemented("physical limit")),
        LogicalPlan::Union { .. } => Err(Error::NotImplemented("physical union")),
    }
}

fn collect(mut exec: Exec) -> Result<Vec<Batch>> {
    let mut out = Vec::new();
    while let Some(batch) = exec.next_batch()? {
        if batch.num_rows() > 0 {
            out.push(batch);
        }
    }
    Ok(out)
}

fn filter_batch(batch: &Batch, predicate: &Expr) -> Result<Option<Batch>> {
    let mask = predicate.eval(batch)?;
    let bools = mask
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| Error::Eval("filter predicate is not boolean".into()))?;
    let not_null = is_not_null(bools).map_err(|e| Error::Eval(e.to_string()))?;
    let keep = and(bools, &not_null).map_err(|e| Error::Eval(e.to_string()))?;
    let filtered =
        filter_record_batch(batch.as_arrow(), &keep).map_err(|e| Error::Eval(e.to_string()))?;
    if filtered.num_rows() == 0 {
        return Ok(None);
    }
    Ok(Some(Batch::from_arrow(filtered)?))
}

fn project_batch(batch: &Batch, exprs: &[NamedExpr], schema: SchemaRef) -> Result<Batch> {
    let mut columns = Vec::with_capacity(exprs.len());
    for named in exprs {
        columns.push(named.expr.eval(batch)?);
    }
    Batch::try_new(schema, columns)
}

fn one_row_empty(schema: SchemaRef) -> Result<Batch> {
    let inner = RecordBatch::try_new_with_options(
        schema,
        Vec::new(),
        &RecordBatchOptions::default().with_row_count(Some(1)),
    )
    .map_err(|err| Error::InvalidBatch(err.to_string()))?;
    Batch::from_arrow(inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn table() -> (SchemaRef, Vec<Batch>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), None])),
                Arc::new(StringArray::from(vec![
                    Some("x"),
                    Some("y"),
                    Some("z"),
                    Some("n"),
                ])),
            ],
        )
        .unwrap();
        (schema, vec![batch])
    }

    fn session() -> Session {
        let (schema, batches) = table();
        let mut s = Session::new();
        s.register("t", schema, batches).unwrap();
        s
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
                if col.is_null(i) {
                    out.push(None);
                } else {
                    out.push(Some(col.value(i)));
                }
            }
        }
        out
    }

    #[test]
    fn scan_projects_and_filters_end_to_end() {
        let out = session()
            .execute_sql("SELECT a FROM t WHERE a > 1")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(2), Some(3)]);
    }

    #[test]
    fn filter_drops_null_predicates() {
        let out = session()
            .execute_sql("SELECT a FROM t WHERE a = 1")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1)]);
    }

    #[test]
    fn projection_renames() {
        let out = session()
            .execute_sql("SELECT a + 1 AS x FROM t WHERE a IS NOT NULL")
            .unwrap();
        assert_eq!(col_i64(&out, "x"), vec![Some(2), Some(3), Some(4)]);
    }

    #[test]
    fn select_literal_from_empty_relation() {
        let out = Session::new().execute_sql("SELECT 1 AS x").unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 1);
        assert_eq!(col_i64(&out, "x"), vec![Some(1)]);
    }

    #[test]
    fn join_is_not_physically_implemented() {
        let (schema, batches) = table();
        let mut s = session();
        s.register("u", schema, batches).unwrap();
        assert!(matches!(
            s.execute_sql("SELECT t.a FROM t JOIN u ON t.a = u.a"),
            Err(Error::NotImplemented("physical join"))
        ));
    }
}
