//! Query result cache keyed by optimized plan and table generations.
//!
//! After a query runs, later identical plans reuse the stored batches while
//! every scanned table still has the generation recorded at store time.
//! Re-registering a table bumps its generation and drops entries that
//! mentioned it.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::batch::Batch;
use crate::plan::{LogicalPlan, normalize_name};

#[derive(Clone, Debug, Default)]
pub(super) struct QueryCache {
    entries: HashMap<CacheKey, Vec<Batch>>,
    pub(super) hits: u64,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(super) struct CacheKey {
    plan: String,
    versions: BTreeMap<String, u64>,
}

impl QueryCache {
    pub(super) fn lookup(&mut self, key: &CacheKey) -> Option<Vec<Batch>> {
        let hit = self.entries.get(key)?.clone();
        self.hits += 1;
        Some(hit)
    }

    pub(super) fn store(&mut self, key: CacheKey, batches: &[Batch]) {
        self.entries.insert(key, batches.to_vec());
    }

    pub(super) fn invalidate_table(&mut self, table: &str) {
        self.entries
            .retain(|key, _| !key.versions.contains_key(table));
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
    }
}

pub(super) fn key(plan: &LogicalPlan, generations: &BTreeMap<String, u64>) -> CacheKey {
    let mut tables = BTreeSet::new();
    collect_tables(plan, &mut tables);
    let mut versions = BTreeMap::new();
    for table in tables {
        let generation = generations.get(&table).copied().unwrap_or(0);
        versions.insert(table, generation);
    }
    CacheKey {
        // Debug is lossless for CASE and other expressions; EXPLAIN is not.
        plan: format!("{plan:?}"),
        versions,
    }
}

fn collect_tables(plan: &LogicalPlan, out: &mut BTreeSet<String>) {
    match plan {
        LogicalPlan::Empty { .. } => {}
        LogicalPlan::Scan { table, .. } => {
            out.insert(normalize_name(table));
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Window { input, .. } => collect_tables(input, out),
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::Intersect { left, right, .. }
        | LogicalPlan::Except { left, right, .. } => {
            collect_tables(left, out);
            collect_tables(right, out);
        }
        LogicalPlan::Union { inputs, .. } => {
            for input in inputs {
                collect_tables(input, out);
            }
        }
        LogicalPlan::Recursive { anchors, step, .. } => {
            for anchor in anchors {
                collect_tables(anchor, out);
            }
            collect_tables(step, out);
        }
        LogicalPlan::WorkScan { .. } => {}
        LogicalPlan::Scalar { input, .. } => collect_tables(input, out),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::batch::Batch;
    use crate::exec::Session;

    fn table(values: &[i64]) -> (Arc<Schema>, Vec<Batch>) {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap();
        (schema, vec![batch])
    }

    fn col(batches: &[Batch]) -> Vec<i64> {
        let mut out = Vec::new();
        for batch in batches {
            let a = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            out.extend(a.iter().map(|v| v.unwrap()));
        }
        out
    }

    #[test]
    fn repeat_query_hits_cache() {
        let (schema, batches) = table(&[1, 2, 3]);
        let mut s = Session::new();
        s.register("t", schema, batches).unwrap();
        let sql = "SELECT a, COUNT(*) AS c FROM t GROUP BY a";
        let first = s.execute_sql(sql).unwrap();
        assert_eq!(s.cache_hits(), 0);
        let second = s.execute_sql(sql).unwrap();
        assert_eq!(s.cache_hits(), 1);
        assert_eq!(col(&first), col(&second));
        let _ = s.execute_sql("SELECT a FROM t WHERE a > 1").unwrap();
        assert_eq!(s.cache_hits(), 1);
    }

    #[test]
    fn register_invalidates_entries_for_that_table() {
        let (schema, batches) = table(&[1, 2]);
        let mut s = Session::new();
        s.register("t", schema.clone(), batches).unwrap();
        let sql = "SELECT SUM(a) AS s FROM t";
        let first = col(&s.execute_sql(sql).unwrap());
        assert_eq!(first, vec![3]);
        let _ = s.execute_sql(sql).unwrap();
        assert_eq!(s.cache_hits(), 1);

        let (_, next) = table(&[10, 20, 30]);
        s.register("t", schema, next).unwrap();
        let after = col(&s.execute_sql(sql).unwrap());
        assert_eq!(after, vec![60]);
        assert_eq!(s.cache_hits(), 1);
        let _ = s.execute_sql(sql).unwrap();
        assert_eq!(s.cache_hits(), 2);
    }

    #[test]
    fn distinct_case_plans_do_not_share_a_key() {
        let (schema, batches) = table(&[1, 2, 3]);
        let mut s = Session::new();
        s.register("t", schema, batches).unwrap();
        let first = col(&s
            .execute_sql("SELECT CASE WHEN a > 1 THEN 1 ELSE 0 END AS x FROM t")
            .unwrap());
        let second = col(&s
            .execute_sql("SELECT CASE WHEN a > 2 THEN 1 ELSE 0 END AS x FROM t")
            .unwrap());
        assert_eq!(first, vec![0, 1, 1]);
        assert_eq!(second, vec![0, 0, 1]);
        assert_eq!(s.cache_hits(), 0);
        let again = col(&s
            .execute_sql("SELECT CASE WHEN a > 1 THEN 1 ELSE 0 END AS x FROM t")
            .unwrap());
        assert_eq!(again, first);
        assert_eq!(s.cache_hits(), 1);
    }

    #[test]
    fn quoted_table_name_invalidates_on_register() {
        let (schema, batches) = table(&[1, 2]);
        let mut s = Session::new();
        s.register("T", schema.clone(), batches).unwrap();
        let sql = r#"SELECT SUM(a) AS s FROM "T""#;
        assert_eq!(col(&s.execute_sql(sql).unwrap()), vec![3]);
        let (_, next) = table(&[10]);
        s.register("T", schema, next).unwrap();
        assert_eq!(col(&s.execute_sql(sql).unwrap()), vec![10]);
        assert_eq!(s.cache_hits(), 0);
    }

    #[test]
    fn parallel_pull_hits_on_repeat() {
        let (schema, batches) = table(&[4, 5]);
        let mut s = Session::new().with_workers(2).unwrap();
        s.register("t", schema, batches).unwrap();
        let sql = "SELECT COUNT(*) AS c FROM t";
        let first = col(&s.execute_sql(sql).unwrap());
        let second = col(&s.execute_sql(sql).unwrap());
        assert_eq!(first, second);
        assert_eq!(s.cache_hits(), 1);
    }
}
