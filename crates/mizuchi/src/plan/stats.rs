//! Table and column statistics used for cardinality estimates.

use std::collections::{BTreeMap, HashSet};

use arrow::array::{Array, Int64Array, LargeStringArray, StringArray};
use arrow::datatypes::{DataType, Schema};

use crate::batch::Batch;

/// Per-column summary: distinct count, optional Int64 range, nulls.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnStats {
    /// Exact or estimated number of distinct values.
    pub ndv: Option<u64>,
    /// Minimum Int64 value, when the column is Int64.
    pub min_i64: Option<i64>,
    /// Maximum Int64 value, when the column is Int64.
    pub max_i64: Option<i64>,
    /// Null row count.
    pub null_count: u64,
}

/// Table-level statistics attached to a catalog entry and its scans.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableStats {
    /// Number of rows.
    pub row_count: u64,
    /// Per-column summaries keyed by field name.
    pub columns: BTreeMap<String, ColumnStats>,
    /// Hive-style partition column names, in directory order.
    pub partition_columns: Vec<String>,
    /// Known partition directories (`col=value` paths) used for pruning.
    pub partitions: Vec<String>,
}

/// Build exact in-memory stats from `batches` that match `schema`.
pub fn table_stats_from_batches(schema: &Schema, batches: &[Batch]) -> TableStats {
    let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    let mut columns = BTreeMap::new();
    for field in schema.fields() {
        let name = field.name().as_str();
        let stats = match field.data_type() {
            DataType::Int64 => i64_column_stats(batches, name),
            DataType::Utf8 | DataType::LargeUtf8 => utf8_column_stats(batches, name),
            _ => ColumnStats {
                ndv: None,
                min_i64: None,
                max_i64: None,
                null_count: nulls(batches, name),
            },
        };
        columns.insert(name.to_string(), stats);
    }
    TableStats {
        row_count,
        columns,
        ..TableStats::default()
    }
}

fn nulls(batches: &[Batch], name: &str) -> u64 {
    let mut n = 0u64;
    for batch in batches {
        if let Some(col) = batch.column_by_name(name) {
            n += col.null_count() as u64;
        }
    }
    n
}

fn i64_column_stats(batches: &[Batch], name: &str) -> ColumnStats {
    let mut ndv = HashSet::new();
    let mut min_i64 = None;
    let mut max_i64 = None;
    let mut null_count = 0u64;
    for batch in batches {
        let Some(col) = batch.column_by_name(name) else {
            continue;
        };
        let Some(arr) = col.as_any().downcast_ref::<Int64Array>() else {
            continue;
        };
        for i in 0..arr.len() {
            if arr.is_null(i) {
                null_count += 1;
                continue;
            }
            let v = arr.value(i);
            ndv.insert(v);
            min_i64 = Some(min_i64.map_or(v, |m: i64| m.min(v)));
            max_i64 = Some(max_i64.map_or(v, |m: i64| m.max(v)));
        }
    }
    ColumnStats {
        ndv: Some(ndv.len() as u64),
        min_i64,
        max_i64,
        null_count,
    }
}

fn utf8_column_stats(batches: &[Batch], name: &str) -> ColumnStats {
    let mut ndv = HashSet::new();
    let mut null_count = 0u64;
    let mut seen = false;
    for batch in batches {
        let Some(col) = batch.column_by_name(name) else {
            continue;
        };
        if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
            seen = true;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    null_count += 1;
                } else {
                    ndv.insert(arr.value(i).to_string());
                }
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<LargeStringArray>() {
            seen = true;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    null_count += 1;
                } else {
                    ndv.insert(arr.value(i).to_string());
                }
            }
        }
    }
    ColumnStats {
        ndv: seen.then_some(ndv.len() as u64),
        min_i64: None,
        max_i64: None,
        null_count,
    }
}

/// Inner equijoin row estimate: `left * right / max(ndv_l, ndv_r)`.
pub fn inner_equi_rows(left_rows: f64, right_rows: f64, left_ndv: f64, right_ndv: f64) -> f64 {
    let den = left_ndv.max(right_ndv).max(1.0);
    (left_rows * right_rows / den).max(1.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::LargeStringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn large_utf8_ndv_and_nulls() {
        let schema = Schema::new(vec![Field::new("s", DataType::LargeUtf8, true)]);
        let batch = Batch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(LargeStringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("a"),
                None,
            ]))],
        )
        .unwrap();
        let stats = table_stats_from_batches(&schema, &[batch]);
        let col = &stats.columns["s"];
        assert_eq!(col.ndv, Some(2));
        assert_eq!(col.null_count, 1);
        assert_eq!(stats.row_count, 4);
    }
}
