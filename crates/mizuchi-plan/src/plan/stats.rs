//! Table and column statistics used for cardinality estimates.

use std::collections::{BTreeMap, HashSet};

use arrow::array::{Array, DictionaryArray, Int64Array, LargeStringArray, StringArray};
use arrow::datatypes::{
    DataType, Int8Type, Int16Type, Int32Type, Int64Type, Schema, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};

use mizuchi_core::batch::Batch;

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
            DataType::Dictionary(_, value)
                if matches!(value.as_ref(), DataType::Utf8 | DataType::LargeUtf8) =>
            {
                utf8_column_stats(batches, name)
            }
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

fn dictionary_value_utf8(values: &dyn Array, idx: usize) -> Option<Option<&str>> {
    if let Some(a) = values.as_any().downcast_ref::<StringArray>() {
        return Some(if a.is_null(idx) {
            None
        } else {
            Some(a.value(idx))
        });
    }
    if let Some(a) = values.as_any().downcast_ref::<LargeStringArray>() {
        return Some(if a.is_null(idx) {
            None
        } else {
            Some(a.value(idx))
        });
    }
    None
}

fn dictionary_utf8_referenced<K: arrow::datatypes::ArrowDictionaryKeyType>(
    col: &dyn Array,
    ndv: &mut HashSet<String>,
    null_count: &mut u64,
) -> bool {
    let Some(dict) = col.as_any().downcast_ref::<DictionaryArray<K>>() else {
        return false;
    };
    let values = dict.values();
    if !(values.as_any().is::<StringArray>() || values.as_any().is::<LargeStringArray>()) {
        return false;
    }
    for i in 0..dict.len() {
        if dict.is_null(i) {
            *null_count += 1;
            continue;
        }
        let Some(idx) = dict.key(i) else {
            *null_count += 1;
            continue;
        };
        match dictionary_value_utf8(values.as_ref(), idx) {
            Some(Some(s)) => {
                ndv.insert(s.to_string());
            }
            Some(None) | None => {
                *null_count += 1;
            }
        }
    }
    true
}

fn dictionary_utf8_ndv(col: &dyn Array, ndv: &mut HashSet<String>, null_count: &mut u64) -> bool {
    dictionary_utf8_referenced::<Int8Type>(col, ndv, null_count)
        || dictionary_utf8_referenced::<Int16Type>(col, ndv, null_count)
        || dictionary_utf8_referenced::<Int32Type>(col, ndv, null_count)
        || dictionary_utf8_referenced::<Int64Type>(col, ndv, null_count)
        || dictionary_utf8_referenced::<UInt8Type>(col, ndv, null_count)
        || dictionary_utf8_referenced::<UInt16Type>(col, ndv, null_count)
        || dictionary_utf8_referenced::<UInt32Type>(col, ndv, null_count)
        || dictionary_utf8_referenced::<UInt64Type>(col, ndv, null_count)
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
        } else if dictionary_utf8_ndv(col.as_ref(), &mut ndv, &mut null_count) {
            seen = true;
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
    use arrow::array::{LargeStringArray, StringArray};
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

    #[test]
    fn dictionary_ndv_counts_referenced_values_only() {
        use arrow::array::{DictionaryArray, Int32Array};
        use arrow::datatypes::Int32Type;
        use std::sync::Arc;

        let schema = Schema::new(vec![Field::new(
            "s",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )]);
        let keys = Int32Array::from(vec![Some(0), Some(0), None]);
        let values: arrow::array::ArrayRef = Arc::new(StringArray::from(vec!["A", "B", "C"]));
        let dict = DictionaryArray::<Int32Type>::try_new(keys, values).unwrap();
        let batch = Batch::try_new(Arc::new(schema.clone()), vec![Arc::new(dict)]).unwrap();
        let stats = table_stats_from_batches(&schema, &[batch]);
        let col = &stats.columns["s"];
        assert_eq!(col.ndv, Some(1));
        assert_eq!(col.null_count, 1);
        assert_eq!(stats.row_count, 3);
    }

    #[test]
    fn large_utf8_dictionary_ndv_and_nulls() {
        use arrow::array::{DictionaryArray, Int32Array, LargeStringArray};
        use arrow::datatypes::Int32Type;
        use std::sync::Arc;

        let schema = Schema::new(vec![Field::new(
            "s",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeUtf8)),
            true,
        )]);
        let keys = Int32Array::from(vec![Some(0), None, Some(0)]);
        let values: arrow::array::ArrayRef = Arc::new(LargeStringArray::from(vec!["A", "unused"]));
        let dict = DictionaryArray::<Int32Type>::try_new(keys, values).unwrap();
        let batch = Batch::try_new(Arc::new(schema.clone()), vec![Arc::new(dict)]).unwrap();
        let stats = table_stats_from_batches(&schema, &[batch]);
        let col = &stats.columns["s"];
        assert_eq!(col.ndv, Some(1));
        assert_eq!(col.null_count, 1);
        assert_eq!(stats.row_count, 3);
    }
}
