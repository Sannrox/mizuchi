//! Query result cache keyed by optimized plan and table generations.
//!
//! After a query runs, later identical plans reuse the stored batches while
//! every scanned table still has the generation recorded at store time.
//! Re-registering a table bumps its generation and drops entries that
//! mentioned it.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::memory::batch_bytes;
use mizuchi_core::batch::Batch;
use mizuchi_plan::{LogicalPlan, normalize_name};

/// Byte budget of a session's result cache unless it sets its own with
/// [`crate::Session::with_result_cache_bytes`].
pub const DEFAULT_RESULT_CACHE_BYTES: u64 = 64 * 1024 * 1024;

/// Stored results, least recently used evicted first once their Arrow bytes
/// would pass the budget given to [`QueryCache::store`].
#[derive(Clone, Debug, Default)]
pub(super) struct QueryCache {
    entries: HashMap<CacheKey, Entry>,
    /// Arrow bytes held by `entries`.
    bytes: u64,
    /// Use counter; an entry's `used` is the tick of its last store or hit.
    tick: u64,
    pub(super) hits: u64,
}

#[derive(Clone, Debug)]
struct Entry {
    batches: Vec<Batch>,
    bytes: u64,
    used: u64,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(super) struct CacheKey {
    plan: String,
    versions: BTreeMap<String, u64>,
}

impl QueryCache {
    pub(super) fn lookup(&mut self, key: &CacheKey) -> Option<Vec<Batch>> {
        self.tick += 1;
        let entry = self.entries.get_mut(key)?;
        entry.used = self.tick;
        self.hits += 1;
        Some(entry.batches.clone())
    }

    /// Keeps `batches` under `key` if they fit in `budget` bytes, evicting
    /// the least recently used entries to make room. A result larger than
    /// the whole budget is not kept, and a budget of 0 keeps nothing.
    pub(super) fn store(&mut self, key: CacheKey, batches: &[Batch], budget: u64) {
        let bytes: u64 = batches.iter().map(batch_bytes).sum();
        if let Some(old) = self.entries.remove(&key) {
            self.bytes -= old.bytes;
        }
        if bytes > budget {
            return;
        }
        while self.bytes + bytes > budget {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.used)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            self.remove(&oldest);
        }
        self.tick += 1;
        self.bytes += bytes;
        self.entries.insert(
            key,
            Entry {
                batches: batches.to_vec(),
                bytes,
                used: self.tick,
            },
        );
    }

    /// Arrow bytes the cache holds.
    pub(super) fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(super) fn invalidate_table(&mut self, table: &str) {
        let stale: Vec<CacheKey> = self
            .entries
            .keys()
            .filter(|key| key.versions.contains_key(table))
            .cloned()
            .collect();
        for key in stale {
            self.remove(&key);
        }
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
        self.bytes = 0;
    }

    fn remove(&mut self, key: &CacheKey) {
        if let Some(entry) = self.entries.remove(key) {
            self.bytes -= entry.bytes;
        }
    }
}

/// Names of the tables `plan` scans, as the cache keys them.
pub(super) fn scanned_tables(plan: &LogicalPlan) -> BTreeSet<String> {
    let mut tables = BTreeSet::new();
    collect_tables(plan, &mut tables);
    tables
}

pub(super) fn key(plan: &LogicalPlan, generations: &BTreeMap<String, u64>) -> CacheKey {
    let tables = scanned_tables(plan);
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

    use crate::Session;
    use mizuchi_core::batch::Batch;

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
    fn cached_bytes_stay_within_the_budget_and_evicted_queries_recompute() {
        let (schema, batches) = table(&(0..64).collect::<Vec<_>>());
        let mut s = Session::new();
        s.register("t", schema, batches).unwrap();
        let sql = |k: i64| format!("SELECT a + {k} AS a FROM t");
        let one = crate::memory::batch_bytes(&s.execute_sql(&sql(0)).unwrap()[0]);
        // Room for three results; run many distinct ones through it.
        let budget = 3 * one + one / 2;
        let mut s = s.with_result_cache_bytes(budget);
        for k in 0..20 {
            let out = col(&s.execute_sql(&sql(k)).unwrap());
            assert_eq!(out, (k..64 + k).collect::<Vec<_>>());
            assert!(s.cached_result_bytes() <= budget, "{k}");
        }
        assert_eq!(s.cached_result_bytes(), 3 * one);
        // The newest result is still cached; the oldest was evicted and is
        // computed again, correctly.
        let _ = s.execute_sql(&sql(19)).unwrap();
        assert_eq!(s.cache_hits(), 1);
        assert_eq!(
            col(&s.execute_sql(&sql(0)).unwrap()),
            (0..64).collect::<Vec<_>>()
        );
        assert_eq!(s.cache_hits(), 1);
        s.register("t", table(&[1]).0, table(&[1]).1).unwrap();
        assert_eq!(s.cached_result_bytes(), 0, "invalidation releases bytes");
    }

    #[test]
    fn a_zero_budget_disables_the_cache() {
        let (schema, batches) = table(&[1, 2, 3]);
        let mut s = Session::new().with_result_cache_bytes(0);
        s.register("t", schema, batches).unwrap();
        let sql = "SELECT a FROM t";
        assert_eq!(col(&s.execute_sql(sql).unwrap()), vec![1, 2, 3]);
        assert_eq!(col(&s.execute_sql(sql).unwrap()), vec![1, 2, 3]);
        assert_eq!(s.cache_hits(), 0);
        assert_eq!(s.cached_result_bytes(), 0);
    }

    #[test]
    fn the_cache_budget_never_passes_the_memory_limit() {
        let (schema, batches) = table(&[1, 2, 3]);
        let mut s = Session::new().with_memory_limit(1);
        s.register("t", schema, batches).unwrap();
        let _ = s.execute_sql("SELECT a FROM t").unwrap();
        assert_eq!(s.cached_result_bytes(), 0);
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
