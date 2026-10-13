//! Micro-batch incremental aggregation (ADR 0005).
//!
//! Append-only batches are folded into the same hash aggregate the pull
//! operator uses. `complete` emits the full group table without dropping
//! running state.

use arrow::datatypes::SchemaRef;

use super::agg::HashAgg;
use mizuchi_core::batch::{Batch, BatchPolicy};
use mizuchi_core::error::Result;
use mizuchi_plan::{AggCall, NamedExpr};

/// Running grouped aggregate over a stream of record batches.
pub struct MicroBatchAgg {
    table: HashAgg,
    policy: BatchPolicy,
}

impl MicroBatchAgg {
    /// `group` keys and `aggs` match a `SELECT keys..., aggs... GROUP BY keys` plan.
    pub fn try_new(
        group: Vec<NamedExpr>,
        aggs: Vec<(String, AggCall)>,
        schema: SchemaRef,
    ) -> Result<Self> {
        Ok(Self {
            table: HashAgg::new(&group, &aggs, schema)?,
            policy: BatchPolicy::default(),
        })
    }

    /// Fold one micro-batch into the running state.
    pub fn ingest(&mut self, batch: &Batch) -> Result<()> {
        self.table.update(batch)
    }

    /// Complete output mode: every group accumulated so far.
    pub fn complete(&self) -> Result<Vec<Batch>> {
        self.table.snapshot(&self.policy)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::Session;
    use mizuchi_core::batch::Batch;
    use mizuchi_core::expr::Expr;
    use mizuchi_plan::AggCall;

    fn batch(k: &[&str], v: &[i64]) -> Batch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("v", DataType::Int64, true),
        ]));
        Batch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    k.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(v.to_vec())),
            ],
        )
        .unwrap()
    }

    fn agg() -> MicroBatchAgg {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("c", DataType::Int64, true),
            Field::new("s", DataType::Int64, true),
        ]));
        MicroBatchAgg::try_new(
            vec![NamedExpr {
                expr: Expr::column("k"),
                name: "k".into(),
            }],
            vec![
                ("c".into(), AggCall::CountStar),
                ("s".into(), AggCall::Sum(Expr::column("v"))),
            ],
            schema,
        )
        .unwrap()
    }

    fn rows(batches: &[Batch]) -> Vec<(Option<String>, i64, i64)> {
        let mut out = Vec::new();
        for b in batches {
            let k = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let c = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            let s = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                out.push((
                    if k.is_null(i) {
                        None
                    } else {
                        Some(k.value(i).to_string())
                    },
                    c.value(i),
                    s.value(i),
                ));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    fn pull(first: Batch, rest: &[Batch]) -> Vec<(Option<String>, i64, i64)> {
        let schema = first.schema();
        let mut all = vec![first];
        all.extend(rest.iter().cloned());
        let mut s = Session::new();
        s.register("t", schema, all).unwrap();
        let got = s
            .execute_sql("SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k")
            .unwrap();
        rows(&got)
    }

    #[test]
    fn two_micro_batches_match_full_recompute() {
        let b1 = batch(&["x", "x", "y"], &[1, 2, 10]);
        let b2 = batch(&["y", "z"], &[5, 7]);
        let mut inc = agg();
        inc.ingest(&b1).unwrap();
        assert_eq!(rows(&inc.complete().unwrap()), pull(b1.clone(), &[]));
        inc.ingest(&b2).unwrap();
        assert_eq!(rows(&inc.complete().unwrap()), pull(b1, &[b2]));
    }

    #[test]
    fn null_group_key_is_one_group_across_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let b1 = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![None::<&str>, Some("x")])),
                Arc::new(Int64Array::from(vec![3, 1])),
            ],
        )
        .unwrap();
        let b2 = Batch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(Int64Array::from(vec![4])),
            ],
        )
        .unwrap();
        let mut inc = agg();
        inc.ingest(&b1).unwrap();
        inc.ingest(&b2).unwrap();
        assert_eq!(rows(&inc.complete().unwrap()), pull(b1, &[b2]));
    }
}
