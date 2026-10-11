//! Pull-based vectorized operators over Arrow record batches.
//!
//! See `docs/decisions/0004-pull-vectorized-execution.md`.

mod agg;
mod join;
mod key;
mod sort;
mod stage;

use std::collections::BTreeMap;
use std::path::PathBuf;

use arrow::array::{Array, BooleanArray, RecordBatch, RecordBatchOptions};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::boolean::{and, is_not_null};
use arrow::datatypes::SchemaRef;

use crate::batch::{Batch, BatchPolicy};
use crate::error::{Error, Result};
use crate::expr::Expr;
use crate::plan::{Catalog, LogicalPlan, NamedExpr, normalize_name};

/// In-memory tables plus a batch-size policy.
#[derive(Clone, Debug)]
pub struct Session {
    tables: BTreeMap<String, MemTable>,
    policy: BatchPolicy,
    /// Parallel tasks per stage. Default 1 keeps the pull iterator path.
    workers: usize,
    /// Caller-provided directory for Arrow IPC shuffle files.
    shuffle_dir: Option<PathBuf>,
}

impl Default for Session {
    fn default() -> Self {
        Self {
            tables: BTreeMap::new(),
            policy: BatchPolicy::default(),
            workers: 1,
            shuffle_dir: None,
        }
    }
}

#[derive(Clone, Debug)]
struct MemTable {
    schema: SchemaRef,
    batches: Vec<Batch>,
    stats: crate::plan::TableStats,
}

impl Session {
    /// Empty session with the default morsel size.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of worker threads used by staged execution. `n` must be at least 1.
    pub fn with_workers(mut self, n: usize) -> Result<Self> {
        if n == 0 {
            return Err(Error::Plan("workers must be greater than zero".into()));
        }
        self.workers = n;
        Ok(self)
    }

    /// Directory for shuffle files written as Arrow IPC. The engine never
    /// opens network paths; the caller supplies the directory.
    pub fn with_shuffle_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.shuffle_dir = Some(dir.into());
        self
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
        let stats = crate::plan::table_stats_from_batches(schema.as_ref(), &batches);
        self.tables.insert(
            normalize_name(&name.into()),
            MemTable {
                schema,
                batches,
                stats,
            },
        );
        Ok(())
    }

    /// Plan and run `sql` against registered tables.
    pub fn execute_sql(&self, sql: &str) -> Result<Vec<Batch>> {
        collect(physical(&self.bind_optimized(sql)?, self)?)
    }

    /// Split the optimized plan into stages at hash-partition exchanges.
    pub fn explain_stages(&self, sql: &str) -> Result<String> {
        stage::explain_stages(&self.bind_optimized(sql)?, self.workers.max(1))
    }

    /// Run `sql` as shuffle-map and result stages on `workers` threads.
    pub fn execute_staged(&self, sql: &str) -> Result<Vec<Batch>> {
        stage::execute_staged(&self.bind_optimized(sql)?, self)
    }

    fn bind_optimized(&self, sql: &str) -> Result<LogicalPlan> {
        let mut catalog = Catalog::new();
        for (name, table) in &self.tables {
            catalog.register(name, table.schema.clone())?;
            catalog.set_stats(name, table.stats.clone())?;
        }
        let plan = crate::plan::plan_sql(sql, &catalog)?;
        crate::opt::optimize(plan)
    }
}

/// Pull iterator of morsels.
pub(super) enum Exec {
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
    Limit {
        input: Box<Exec>,
        skip: u64,
        fetch: Option<u64>,
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
            Self::Limit { input, skip, fetch } => limit_next(input, skip, fetch),
        }
    }
}

fn limit_next(input: &mut Exec, skip: &mut u64, fetch: &mut Option<u64>) -> Result<Option<Batch>> {
    if matches!(fetch, Some(0)) {
        return Ok(None);
    }
    loop {
        let Some(batch) = input.next_batch()? else {
            return Ok(None);
        };
        let n = batch.num_rows() as u64;
        if *skip >= n {
            *skip -= n;
            continue;
        }
        let start = *skip as usize;
        *skip = 0;
        let available = batch.num_rows() - start;
        let take_n = match *fetch {
            None => available,
            Some(f) => available.min(usize::try_from(f).unwrap_or(usize::MAX)),
        };
        if take_n == 0 {
            return Ok(None);
        }
        if let Some(f) = fetch {
            *f -= take_n as u64;
        }
        return Ok(Some(batch.slice(start, take_n)?));
    }
}

pub(super) fn physical(plan: &LogicalPlan, session: &Session) -> Result<Exec> {
    match plan {
        LogicalPlan::Empty { schema } => Ok(Exec::Empty {
            schema: schema.clone(),
            done: false,
        }),
        LogicalPlan::Scan {
            table,
            projection,
            filters,
            ..
        } => {
            let table = session
                .tables
                .get(&normalize_name(table))
                .ok_or_else(|| Error::Plan(format!("table not found at execution: {table}")))?;
            let mut morsels = Vec::new();
            for batch in &table.batches {
                let mut batch = batch.clone();
                for pred in filters {
                    match filter_batch(&batch, pred)? {
                        Some(kept) => batch = kept,
                        None => {
                            batch = Batch::empty(batch.schema())?;
                            break;
                        }
                    }
                }
                if let Some(names) = projection {
                    let mut indices = Vec::with_capacity(names.len());
                    for name in names {
                        let idx = batch
                            .schema()
                            .index_of(name)
                            .map_err(|e| Error::Plan(format!("scan projection: {e}")))?;
                        indices.push(idx);
                    }
                    batch = batch.project(&indices)?;
                }
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
        LogicalPlan::Aggregate {
            input,
            group,
            aggs,
            schema,
        } => {
            let child = physical(input, session)?;
            let batches = agg::run(child, group, aggs, schema.clone(), &session.policy)?;
            Ok(Exec::Scan { batches, index: 0 })
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            on,
            schema,
        } => {
            let batches = join::run(
                physical(left, session)?,
                physical(right, session)?,
                join::JoinSpec {
                    join_type: *join_type,
                    on: on.as_ref(),
                    left_schema: left.schema().clone(),
                    right_schema: right.schema().clone(),
                    schema: schema.clone(),
                },
                &session.policy,
            )?;
            Ok(Exec::Scan { batches, index: 0 })
        }
        LogicalPlan::Sort { input, order } => {
            let batches = sort::ordered(
                physical(input, session)?,
                input.schema().clone(),
                order,
                0,
                None,
                &session.policy,
            )?;
            Ok(Exec::Scan { batches, index: 0 })
        }
        LogicalPlan::Limit { input, skip, fetch } => match input.as_ref() {
            LogicalPlan::Sort {
                input: sorted,
                order,
            } => {
                let batches = sort::ordered(
                    physical(sorted, session)?,
                    sorted.schema().clone(),
                    order,
                    *skip,
                    *fetch,
                    &session.policy,
                )?;
                Ok(Exec::Scan { batches, index: 0 })
            }
            _ => Ok(Exec::Limit {
                input: Box::new(physical(input, session)?),
                skip: *skip,
                fetch: *fetch,
            }),
        },
        LogicalPlan::Union { .. } => Err(Error::NotImplemented("physical union")),
    }
}

pub(super) fn collect(mut exec: Exec) -> Result<Vec<Batch>> {
    let mut out = Vec::new();
    while let Some(batch) = exec.next_batch()? {
        if batch.num_rows() > 0 {
            out.push(batch);
        }
    }
    Ok(out)
}

pub(super) fn filter_batch(batch: &Batch, predicate: &Expr) -> Result<Option<Batch>> {
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
    use arrow::array::{Float64Array, Int64Array, LargeStringArray, StringArray, UInt32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::{BTreeMap, HashSet};
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

    fn col_f64(batches: &[Batch], name: &str) -> Vec<Option<f64>> {
        let mut out = Vec::new();
        for batch in batches {
            let col = batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
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

    fn col_utf8(batches: &[Batch], name: &str) -> Vec<Option<String>> {
        let mut out = Vec::new();
        for batch in batches {
            let col = batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..col.len() {
                if col.is_null(i) {
                    out.push(None);
                } else {
                    out.push(Some(col.value(i).to_owned()));
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
    fn coalesce_filter_after_reordering_projection() {
        let out = session()
            .execute_sql("SELECT a FROM (SELECT b, a FROM t) s WHERE COALESCE(a, 0) > 1")
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

    fn join_session() -> Session {
        let t_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let t = Batch::try_new(
            t_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(2),
                Some(3),
                None,
            ]))],
        )
        .unwrap();
        let u_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("x", DataType::Int64, true),
        ]));
        let u = Batch::try_new(
            u_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(2), Some(2), Some(4), None])),
                Arc::new(Int64Array::from(vec![
                    Some(10),
                    Some(30),
                    Some(40),
                    Some(99),
                ])),
            ],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", t_schema, vec![t]).unwrap();
        s.register("u", u_schema, vec![u]).unwrap();
        s
    }

    fn sorted_pairs(batches: &[Batch], left: &str, right: &str) -> Vec<(Option<i64>, Option<i64>)> {
        let mut rows: Vec<(Option<i64>, Option<i64>)> = col_i64(batches, left)
            .into_iter()
            .zip(col_i64(batches, right))
            .collect();
        rows.sort_by_key(|(l, r)| (l.unwrap_or(i64::MIN), r.unwrap_or(i64::MIN)));
        rows
    }

    /// Independent nested-loop reference with SQL null-equality (null never matches).
    fn reference_join(kind: &str) -> Vec<(Option<i64>, Option<i64>)> {
        let left = [Some(1i64), Some(2), Some(2), Some(3), None];
        let right = [Some(2i64), Some(2), Some(4), None];
        let mut out = Vec::new();
        let mut right_hit = [false; 4];
        for l in left {
            let mut matched = false;
            for (ri, r) in right.iter().enumerate() {
                let eq = matches!((l, r), (Some(a), Some(b)) if a == *b);
                if eq {
                    matched = true;
                    right_hit[ri] = true;
                    if kind != "semi" && kind != "anti" {
                        out.push((l, *r));
                    }
                }
            }
            match kind {
                "semi" if matched => out.push((l, l)),
                "anti" if !matched => out.push((l, l)),
                "left" | "full" if !matched => out.push((l, None)),
                _ => {}
            }
        }
        if kind == "right" || kind == "full" {
            for (ri, r) in right.iter().enumerate() {
                if !right_hit[ri] {
                    out.push((None, *r));
                }
            }
        }
        out.sort_by_key(|(l, r)| (l.unwrap_or(i64::MIN), r.unwrap_or(i64::MIN)));
        out
    }

    #[test]
    fn join_type_matrix_matches_reference() {
        let s = join_session();
        let cases = [
            ("inner", "SELECT t.a, u.id FROM t JOIN u ON t.a = u.id"),
            ("left", "SELECT t.a, u.id FROM t LEFT JOIN u ON t.a = u.id"),
            (
                "right",
                "SELECT t.a, u.id FROM t RIGHT JOIN u ON t.a = u.id",
            ),
            (
                "full",
                "SELECT t.a, u.id FROM t FULL OUTER JOIN u ON t.a = u.id",
            ),
        ];
        for (kind, sql) in cases {
            let out = s.execute_sql(sql).unwrap();
            assert_eq!(
                sorted_pairs(&out, "a", "id"),
                reference_join(kind),
                "{kind}"
            );
        }
    }

    #[test]
    fn semi_and_anti_keep_left_rows() {
        let s = join_session();
        let semi = s
            .execute_sql("SELECT t.a FROM t LEFT SEMI JOIN u ON t.a = u.id")
            .unwrap();
        let mut got = col_i64(&semi, "a");
        got.sort_by_key(|v| v.unwrap_or(i64::MIN));
        let mut want: Vec<Option<i64>> =
            reference_join("semi").into_iter().map(|(l, _)| l).collect();
        want.sort_by_key(|v| v.unwrap_or(i64::MIN));
        assert_eq!(got, want);

        let anti = s
            .execute_sql("SELECT t.a FROM t LEFT ANTI JOIN u ON t.a = u.id")
            .unwrap();
        let mut got = col_i64(&anti, "a");
        got.sort_by_key(|v| v.unwrap_or(i64::MIN));
        let mut want: Vec<Option<i64>> =
            reference_join("anti").into_iter().map(|(l, _)| l).collect();
        want.sort_by_key(|v| v.unwrap_or(i64::MIN));
        assert_eq!(got, want);
    }

    #[test]
    fn cross_join_is_cartesian() {
        let s = join_session();
        let out = s
            .execute_sql("SELECT t.a, u.id FROM t CROSS JOIN u")
            .unwrap();
        assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 5 * 4);
    }

    #[test]
    fn join_residual_predicate() {
        let s = join_session();
        let out = s
            .execute_sql("SELECT t.a, u.id FROM t JOIN u ON t.a = u.id AND u.x > 10")
            .unwrap();
        assert_eq!(
            sorted_pairs(&out, "a", "id"),
            vec![(Some(2), Some(2)), (Some(2), Some(2))]
        );
    }

    #[test]
    fn three_table_join_matches_after_reorder() {
        let fact_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("amt", DataType::Int64, true),
        ]));
        let id_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let fact = Batch::try_new(
            fact_schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    (0..100).map(|i| Some(i % 10)).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from((0..100).map(Some).collect::<Vec<_>>())),
            ],
        )
        .unwrap();
        let d0 = Batch::try_new(
            id_schema.clone(),
            vec![Arc::new(Int64Array::from(
                (0..10).map(Some).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        let d1 = Batch::try_new(
            id_schema.clone(),
            vec![Arc::new(Int64Array::from(
                (0..100).map(Some).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("fact", fact_schema, vec![fact]).unwrap();
        s.register("d0", id_schema.clone(), vec![d0]).unwrap();
        s.register("d1", id_schema.clone(), vec![d1]).unwrap();
        let out = s
            .execute_sql(
                "SELECT fact.amt FROM fact JOIN d1 ON fact.id = d1.id JOIN d0 ON d1.id = d0.id",
            )
            .unwrap();
        let amts = col_i64(&out, "amt");
        assert_eq!(amts.len(), 100);
        assert_eq!(
            amts.iter().map(|v| v.unwrap_or(0)).sum::<i64>(),
            (0..100).sum()
        );
        let filtered = s
            .execute_sql(
                "SELECT fact.amt FROM fact JOIN d1 ON fact.id = d1.id AND d1.id > 5 JOIN d0 ON d1.id = d0.id",
            )
            .unwrap();
        let famts = col_i64(&filtered, "amt");
        assert_eq!(famts.len(), 40);
        assert!(famts.iter().all(|v| v.unwrap() % 10 > 5));

        let extra = Batch::try_new(
            id_schema.clone(),
            vec![Arc::new(Int64Array::from(
                (0..10).map(Some).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        s.register("d2", id_schema, vec![extra]).unwrap();
        let four = s
            .execute_sql(
                "SELECT fact.amt FROM fact JOIN d1 ON fact.id = d1.id JOIN d0 ON d1.id = d0.id JOIN d2 ON d0.id = d2.id",
            )
            .unwrap();
        assert_eq!(col_i64(&four, "amt").len(), 100);
    }

    #[test]
    fn same_leaf_on_equality_rejects_nulls_after_reorder() {
        let fact_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("flag", DataType::Int64, true),
        ]));
        let id_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let fact = Batch::try_new(
            fact_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(0), Some(1), Some(2)])),
                Arc::new(Int64Array::from(vec![Some(1), None, Some(1)])),
            ],
        )
        .unwrap();
        let dim = Batch::try_new(
            id_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(0), Some(1), Some(2)]))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("fact", fact_schema, vec![fact]).unwrap();
        s.register("d0", id_schema.clone(), vec![dim.clone()])
            .unwrap();
        s.register("d1", id_schema, vec![dim]).unwrap();
        let out = s
            .execute_sql(
                "SELECT fact.id FROM fact JOIN d1 ON fact.id = d1.id AND fact.flag = fact.flag JOIN d0 ON d1.id = d0.id ORDER BY id",
            )
            .unwrap();
        assert_eq!(col_i64(&out, "id"), vec![Some(0), Some(2)]);
    }

    #[test]
    fn join_coalesce_filter_pushes_to_right() {
        let s = join_session();
        let out = s
            .execute_sql("SELECT t.a, u.x FROM t JOIN u ON t.a = u.id WHERE COALESCE(u.x, 0) > 20")
            .unwrap();
        assert_eq!(
            sorted_pairs(&out, "a", "x"),
            vec![(Some(2), Some(30)), (Some(2), Some(30))]
        );
    }

    #[test]
    fn ungrouped_aggs_skip_nulls_except_count_star() {
        let out = session()
            .execute_sql(
                "SELECT COUNT(*) AS cs, COUNT(a) AS cv, SUM(a) AS s, MIN(a) AS mn, MAX(a) AS mx, AVG(a) AS av, COUNT(DISTINCT a) AS cd FROM t",
            )
            .unwrap();
        assert_eq!(col_i64(&out, "cs"), vec![Some(4)]);
        assert_eq!(col_i64(&out, "cv"), vec![Some(3)]);
        assert_eq!(col_i64(&out, "s"), vec![Some(6)]);
        assert_eq!(col_i64(&out, "mn"), vec![Some(1)]);
        assert_eq!(col_i64(&out, "mx"), vec![Some(3)]);
        let av = col_f64(&out, "av");
        assert_eq!(av.len(), 1);
        assert!((av[0].unwrap() - 2.0).abs() < 1e-12);
        assert_eq!(col_i64(&out, "cd"), vec![Some(3)]);
    }

    #[test]
    fn grouped_agg_puts_null_keys_in_one_group() {
        let out = session()
            .execute_sql("SELECT a, COUNT(*) AS c FROM t GROUP BY a")
            .unwrap();
        let mut rows: Vec<(Option<i64>, Option<i64>)> = col_i64(&out, "a")
            .into_iter()
            .zip(col_i64(&out, "c"))
            .collect();
        rows.sort_by_key(|(k, _)| k.unwrap_or(i64::MIN));
        assert_eq!(
            rows,
            vec![
                (None, Some(1)),
                (Some(1), Some(1)),
                (Some(2), Some(1)),
                (Some(3), Some(1)),
            ]
        );
    }

    #[test]
    fn count_distinct_ignores_nulls() {
        let out = session()
            .execute_sql("SELECT COUNT(DISTINCT a) AS cd FROM t")
            .unwrap();
        assert_eq!(col_i64(&out, "cd"), vec![Some(3)]);
    }

    #[test]
    fn empty_table_ungrouped_count_zero_sum_null() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let mut s = Session::new();
        s.register("t", schema, vec![]).unwrap();
        let out = s
            .execute_sql(
                "SELECT COUNT(*) AS c, SUM(a) AS s, AVG(a) AS av, COUNT(DISTINCT a) AS cd FROM t",
            )
            .unwrap();
        assert_eq!(col_i64(&out, "c"), vec![Some(0)]);
        assert_eq!(col_i64(&out, "s"), vec![None]);
        assert_eq!(col_f64(&out, "av"), vec![None]);
        assert_eq!(col_i64(&out, "cd"), vec![Some(0)]);
    }

    #[test]
    fn empty_table_grouped_emits_no_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let mut s = Session::new();
        s.register("t", schema, vec![]).unwrap();
        let out = s
            .execute_sql("SELECT a, COUNT(*) AS c FROM t GROUP BY a")
            .unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn count_star_without_from_is_one() {
        let out = Session::new().execute_sql("SELECT COUNT(*) AS c").unwrap();
        assert_eq!(col_i64(&out, "c"), vec![Some(1)]);
    }

    #[test]
    fn having_filters_groups() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2, 3, 3, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 1, 2, 3])),
            ],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", schema, vec![batch]).unwrap();
        let out = s
            .execute_sql("SELECT g, COUNT(*) FROM t GROUP BY g HAVING COUNT(*) > 1")
            .unwrap();
        let mut rows: Vec<(Option<i64>, Option<i64>)> = col_i64(&out, "g")
            .into_iter()
            .zip(col_i64(&out, "count"))
            .collect();
        rows.sort_by_key(|(k, _)| k.unwrap_or(i64::MIN));
        assert_eq!(rows, vec![(Some(1), Some(2)), (Some(3), Some(3))]);
    }

    #[test]
    fn min_max_strings() {
        let out = session()
            .execute_sql("SELECT MIN(b) AS mn, MAX(b) AS mx FROM t")
            .unwrap();
        assert_eq!(col_utf8(&out, "mn"), vec![Some("n".into())]);
        assert_eq!(col_utf8(&out, "mx"), vec![Some("z".into())]);
    }

    #[test]
    fn sum_uint32_keeps_unsigned_type() {
        let schema = Arc::new(Schema::new(vec![Field::new("u", DataType::UInt32, true)]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(UInt32Array::from(vec![Some(1), None, Some(2)]))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", schema, vec![batch]).unwrap();
        let out = s.execute_sql("SELECT SUM(u) AS s FROM t").unwrap();
        let col = out[0]
            .column_by_name("s")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(col.value(0), 3);
    }

    #[test]
    fn float_signed_zero_is_one_group() {
        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(vec![0.0, -0.0, 1.0]))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", schema, vec![batch]).unwrap();
        let grouped = s
            .execute_sql("SELECT f, COUNT(*) AS c FROM t GROUP BY f")
            .unwrap();
        assert_eq!(grouped.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        let distinct = s
            .execute_sql("SELECT COUNT(DISTINCT f) AS cd FROM t")
            .unwrap();
        assert_eq!(col_i64(&distinct, "cd"), vec![Some(2)]);
    }

    #[test]
    fn large_utf8_group_keys_round_trip() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::LargeUtf8,
            true,
        )]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(LargeStringArray::from(vec![
                Some("a"),
                Some("a"),
                Some("b"),
            ]))],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", schema, vec![batch]).unwrap();
        let out = s
            .execute_sql("SELECT s, COUNT(*) AS c FROM t GROUP BY s")
            .unwrap();
        let mut rows: Vec<(Option<String>, Option<i64>)> = Vec::new();
        for batch in &out {
            let col = batch
                .column_by_name("s")
                .unwrap()
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap();
            let counts = batch
                .column_by_name("c")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..col.len() {
                let key = if col.is_null(i) {
                    None
                } else {
                    Some(col.value(i).to_owned())
                };
                rows.push((
                    key,
                    if counts.is_null(i) {
                        None
                    } else {
                        Some(counts.value(i))
                    },
                ));
            }
        }
        rows.sort();
        assert_eq!(
            rows,
            vec![(Some("a".into()), Some(2)), (Some("b".into()), Some(1))]
        );
    }

    #[test]
    fn distinct_sum_fails_closed() {
        assert!(matches!(
            session().execute_sql("SELECT SUM(DISTINCT a) FROM t"),
            Err(Error::NotImplemented("DISTINCT aggregate"))
        ));
    }

    #[derive(Default)]
    struct RefAgg {
        count_star: i64,
        count_val: i64,
        sum: i64,
        sum_seen: bool,
        min: Option<i64>,
        max: Option<i64>,
        avg_sum: f64,
        avg_n: i64,
        distinct: HashSet<i64>,
    }

    impl RefAgg {
        fn add(&mut self, v: Option<i64>) {
            self.count_star += 1;
            if let Some(v) = v {
                self.count_val += 1;
                self.sum += v;
                self.sum_seen = true;
                self.min = Some(self.min.map_or(v, |m| m.min(v)));
                self.max = Some(self.max.map_or(v, |m| m.max(v)));
                self.avg_sum += v as f64;
                self.avg_n += 1;
                self.distinct.insert(v);
            }
        }
    }

    fn sweep_session(n_groups: i64, rows_per_group: i64) -> (Session, BTreeMap<i64, RefAgg>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let mut gs = Vec::new();
        let mut vs = Vec::new();
        let mut expected: BTreeMap<i64, RefAgg> = BTreeMap::new();
        let mut i = 0i64;
        for _ in 0..rows_per_group {
            for g in 0..n_groups {
                let v = if i % 7 == 0 { None } else { Some(i % 11) };
                gs.push(Some(g));
                vs.push(v);
                expected.entry(g).or_default().add(v);
                i += 1;
            }
        }
        let batch = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(gs)),
                Arc::new(Int64Array::from(vs)),
            ],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", schema, vec![batch]).unwrap();
        (s, expected)
    }

    struct GotAgg {
        count_star: i64,
        count_val: i64,
        sum: Option<i64>,
        min: Option<i64>,
        max: Option<i64>,
        avg: Option<f64>,
        distinct: i64,
    }

    fn grouped_map(batches: &[Batch]) -> BTreeMap<i64, GotAgg> {
        let g = col_i64(batches, "g");
        let cs = col_i64(batches, "cs");
        let cv = col_i64(batches, "cv");
        let s = col_i64(batches, "s");
        let mn = col_i64(batches, "mn");
        let mx = col_i64(batches, "mx");
        let av = col_f64(batches, "av");
        let cd = col_i64(batches, "cd");
        let mut map = BTreeMap::new();
        for i in 0..g.len() {
            map.insert(
                g[i].unwrap(),
                GotAgg {
                    count_star: cs[i].unwrap(),
                    count_val: cv[i].unwrap(),
                    sum: s[i],
                    min: mn[i],
                    max: mx[i],
                    avg: av[i],
                    distinct: cd[i].unwrap(),
                },
            );
        }
        map
    }

    fn assert_matches_reference(n_groups: i64, rows_per_group: i64) {
        let (s, expected) = sweep_session(n_groups, rows_per_group);
        let out = s
            .execute_sql(
                "SELECT g, COUNT(*) AS cs, COUNT(v) AS cv, SUM(v) AS s, MIN(v) AS mn, MAX(v) AS mx, AVG(v) AS av, COUNT(DISTINCT v) AS cd FROM t GROUP BY g",
            )
            .unwrap();
        let got = grouped_map(&out);
        assert_eq!(got.len(), expected.len(), "group cardinality {n_groups}");
        for (g, exp) in expected {
            let row = got.get(&g).unwrap();
            assert_eq!(row.count_star, exp.count_star, "count(*) group {g}");
            assert_eq!(row.count_val, exp.count_val, "count(v) group {g}");
            assert_eq!(
                row.sum,
                if exp.sum_seen { Some(exp.sum) } else { None },
                "sum group {g}"
            );
            assert_eq!(row.min, exp.min, "min group {g}");
            assert_eq!(row.max, exp.max, "max group {g}");
            match (row.avg, exp.avg_n) {
                (None, 0) => {}
                (Some(got_av), n) if n > 0 => {
                    let want = exp.avg_sum / n as f64;
                    assert!(
                        (got_av - want).abs() < 1e-9,
                        "avg group {g}: {got_av} vs {want}"
                    );
                }
                other => panic!("avg group {g}: {other:?}"),
            }
            assert_eq!(
                row.distinct,
                exp.distinct.len() as i64,
                "count distinct group {g}"
            );
        }
    }

    #[test]
    fn group_cardinality_sweep_matches_reference() {
        for n_groups in [1, 16, 257, 4097] {
            assert_matches_reference(n_groups, 3);
        }
    }

    #[test]
    fn limit_keeps_input_order() {
        let out = session().execute_sql("SELECT a FROM t LIMIT 2").unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1), Some(2)]);
    }

    #[test]
    fn limit_offset_skips_then_takes() {
        let out = session()
            .execute_sql("SELECT a FROM t LIMIT 2 OFFSET 1")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(2), Some(3)]);
    }

    #[test]
    fn offset_past_end_is_empty() {
        let out = session().execute_sql("SELECT a FROM t OFFSET 10").unwrap();
        assert!(out.iter().all(|b| b.num_rows() == 0) || out.is_empty());
    }

    #[test]
    fn limit_zero_is_empty() {
        let out = session().execute_sql("SELECT a FROM t LIMIT 0").unwrap();
        assert!(out.iter().all(|b| b.num_rows() == 0) || out.is_empty());
    }

    #[test]
    fn order_by_asc_nulls_last() {
        let out = session().execute_sql("SELECT a FROM t ORDER BY a").unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1), Some(2), Some(3), None]);
    }

    #[test]
    fn order_by_desc_nulls_first() {
        let out = session()
            .execute_sql("SELECT a FROM t ORDER BY a DESC")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![None, Some(3), Some(2), Some(1)]);
    }

    #[test]
    fn order_by_nulls_first_and_last() {
        let first = session()
            .execute_sql("SELECT a FROM t ORDER BY a NULLS FIRST")
            .unwrap();
        assert_eq!(col_i64(&first, "a"), vec![None, Some(1), Some(2), Some(3)]);
        let last = session()
            .execute_sql("SELECT a FROM t ORDER BY a DESC NULLS LAST")
            .unwrap();
        assert_eq!(col_i64(&last, "a"), vec![Some(3), Some(2), Some(1), None]);
    }

    #[test]
    fn order_by_two_keys() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
        ]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("b"), Some("a"), Some("a")])),
                Arc::new(Int64Array::from(vec![Some(0), Some(2), Some(1)])),
            ],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("k", schema, vec![batch]).unwrap();
        let out = s.execute_sql("SELECT g, n FROM k ORDER BY g, n").unwrap();
        assert_eq!(
            col_utf8(&out, "g"),
            vec![Some("a".into()), Some("a".into()), Some("b".into())]
        );
        assert_eq!(col_i64(&out, "n"), vec![Some(1), Some(2), Some(0)]);
    }

    #[test]
    fn order_by_limit_is_topk() {
        let out = session()
            .execute_sql("SELECT a FROM t ORDER BY a LIMIT 2")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(1), Some(2)]);
        let out = session()
            .execute_sql("SELECT a FROM t ORDER BY a DESC LIMIT 2 OFFSET 1")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), vec![Some(3), Some(2)]);
    }

    #[test]
    fn topk_on_large_input_matches_reference() {
        let n = 5000i64;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let values: Vec<Option<i64>> = (0..n).rev().map(Some).collect();
        let batch =
            Batch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap();
        let mut s = Session::new();
        s.register("big", schema, vec![batch]).unwrap();
        let out = s
            .execute_sql("SELECT a FROM big ORDER BY a LIMIT 7 OFFSET 3")
            .unwrap();
        assert_eq!(col_i64(&out, "a"), (3..10).map(Some).collect::<Vec<_>>());
        let desc = s
            .execute_sql("SELECT a FROM big ORDER BY a DESC LIMIT 5")
            .unwrap();
        assert_eq!(
            col_i64(&desc, "a"),
            (n - 5..n).rev().map(Some).collect::<Vec<_>>()
        );
    }
}
