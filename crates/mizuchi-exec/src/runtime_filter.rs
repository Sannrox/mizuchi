//! Runtime join-key filters for the probe-side scan.
//!
//! After an inner hash-join build, integer equality keys become a min/max
//! range or a small IN-list. The probe scan may skip morsels and rows. A scan
//! that ignores the filter is still correct.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{
    Array, AsArray, BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{
    DataType, Int8Type, Int16Type, Int32Type, Int64Type, Schema, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};

use super::filter_batch;
use super::join;
use mizuchi_core::batch::Batch;
use mizuchi_core::error::Result;
use mizuchi_core::expr::{BinaryOp, Expr};
use mizuchi_plan::NamedExpr;

/// Distinct values at or below this count become an IN-list (OR of equalities).
const IN_LIST_MAX: usize = 32;

/// Predicates on the probe (left) schema plus EXPLAIN text.
#[derive(Clone, Debug)]
pub(crate) struct RuntimeFilter {
    pub preds: Vec<Expr>,
    pub summary: String,
}

/// Derive a probe-side filter from inner-join build keys. `None` means
/// fail-open: the join stays correct without a filter.
pub(crate) fn from_build(
    on: Option<&Expr>,
    left_ncols: usize,
    left_schema: &arrow::datatypes::Schema,
    build_batches: &[Batch],
) -> Option<RuntimeFilter> {
    let on = on?;
    let pairs = join::equi_key_pairs(on, left_ncols).ok()?;
    if pairs.is_empty() {
        return None;
    }
    let probe_empty = Batch::empty(Arc::new(left_schema.clone())).ok()?;
    let mut preds = Vec::new();
    let mut parts = Vec::new();
    for (left_key, right_key) in pairs {
        let probe_dt = left_key.eval(&probe_empty).ok()?.data_type().clone();
        match collect_ints(&right_key, build_batches) {
            IntCollect::FailOpen => {}
            IntCollect::Empty => {
                return Some(RuntimeFilter {
                    preds: vec![Expr::lit_bool(false)],
                    summary: "false".into(),
                });
            }
            IntCollect::Values {
                distinct,
                min,
                max,
                dtype,
            } => {
                if dtype != probe_dt {
                    continue;
                }
                let Some(pred) = (if let Some(values) = &distinct {
                    in_list(&left_key, values, &dtype)
                } else {
                    between(&left_key, min, max, &dtype)
                }) else {
                    continue;
                };
                parts.push(summarize_pred(&left_key, distinct.as_ref(), min, max));
                preds.push(pred);
            }
        }
    }
    if preds.is_empty() {
        return None;
    }
    Some(RuntimeFilter {
        preds,
        summary: parts.join(", "),
    })
}

/// Rewrite `Nth` / `Column` through a Project onto the child schema.
pub(crate) fn rewrite_through_project(expr: &Expr, project: &[NamedExpr]) -> Option<Expr> {
    map_leaves(expr, &|e| match e {
        Expr::Nth(i) => project.get(*i).map(|n| n.expr.clone()),
        Expr::Column(name) => project
            .iter()
            .find(|n| n.name == *name)
            .map(|n| n.expr.clone()),
        other => Some(other.clone()),
    })
}

/// True when footer-style min/max (or a false literal) prove `preds` cannot
/// hold for any row. Conservative: unknown shapes keep the batch.
pub(crate) fn batch_cannot_match(batch: &Batch, preds: &[Expr]) -> bool {
    preds.iter().any(|p| pred_cannot_match(batch, p))
}

/// Apply `preds` as a conjunction, dropping empty results.
pub(crate) fn apply_preds(mut batch: Batch, preds: &[Expr]) -> Result<Option<Batch>> {
    if batch_cannot_match(&batch, preds) {
        return Ok(None);
    }
    for pred in preds {
        match filter_batch(&batch, pred)? {
            Some(kept) => batch = kept,
            None => return Ok(None),
        }
    }
    Ok(Some(batch))
}

/// Filter already-produced batches. Used when a probe plan is not a scan
/// pipeline.
pub(crate) fn filter_batches(batches: Vec<Batch>, preds: &[Expr]) -> Result<Vec<Batch>> {
    if preds.is_empty() {
        return Ok(batches);
    }
    let mut out = Vec::new();
    for batch in batches {
        if let Some(kept) = apply_preds(batch, preds)?
            && kept.num_rows() > 0
        {
            out.push(kept);
        }
    }
    Ok(out)
}

/// Replace `Nth(i)` with `Column(schema[i])` so Parquet prune can use names.
pub(crate) fn nth_to_column(expr: &Expr, schema: &Schema) -> Option<Expr> {
    map_leaves(expr, &|e| match e {
        Expr::Nth(i) => schema.fields().get(*i).map(|f| Expr::column(f.name())),
        other => Some(other.clone()),
    })
}

/// Human-readable form for `EXPLAIN ANALYZE`, using field names when `Nth`
/// is in range.
pub(crate) fn format_detail(filter: &RuntimeFilter, schema: &Schema) -> String {
    let named: Vec<String> = filter
        .preds
        .iter()
        .map(|p| match nth_to_column(p, schema) {
            Some(e) => pred_text(&e),
            None => pred_text(p),
        })
        .collect();
    let body = if named.iter().any(|s| s == "false") {
        "false".to_string()
    } else if !named.is_empty() {
        named.join(", ")
    } else {
        filter.summary.clone()
    };
    format!("runtime_filter=[{body}]")
}

enum IntCollect {
    FailOpen,
    Empty,
    Values {
        distinct: Option<Vec<i64>>,
        min: i64,
        max: i64,
        dtype: DataType,
    },
}

fn collect_ints(expr: &Expr, batches: &[Batch]) -> IntCollect {
    let mut distinct = BTreeSet::new();
    let mut overflow = false;
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    let mut n = 0usize;
    let mut dtype: Option<DataType> = None;
    for batch in batches {
        let arr = match expr.eval(batch) {
            Ok(a) => a,
            Err(_) => return IntCollect::FailOpen,
        };
        match &dtype {
            None => dtype = Some(arr.data_type().clone()),
            Some(dt) if dt == arr.data_type() => {}
            Some(_) => return IntCollect::FailOpen,
        }
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let Some(v) = int_at(arr.as_ref(), i) else {
                return IntCollect::FailOpen;
            };
            n += 1;
            min = min.min(v);
            max = max.max(v);
            if !overflow {
                distinct.insert(v);
                if distinct.len() > IN_LIST_MAX {
                    overflow = true;
                    distinct.clear();
                }
            }
        }
    }
    if n == 0 {
        return IntCollect::Empty;
    }
    let Some(dtype) = dtype else {
        return IntCollect::FailOpen;
    };
    let distinct = if overflow {
        None
    } else {
        Some(distinct.into_iter().collect())
    };
    IntCollect::Values {
        distinct,
        min,
        max,
        dtype,
    }
}

fn int_at(array: &dyn Array, i: usize) -> Option<i64> {
    match array.data_type() {
        DataType::Int8 => Some(i64::from(array.as_primitive::<Int8Type>().value(i))),
        DataType::Int16 => Some(i64::from(array.as_primitive::<Int16Type>().value(i))),
        DataType::Int32 => Some(i64::from(array.as_primitive::<Int32Type>().value(i))),
        DataType::Int64 => Some(array.as_primitive::<Int64Type>().value(i)),
        DataType::UInt8 => Some(i64::from(array.as_primitive::<UInt8Type>().value(i))),
        DataType::UInt16 => Some(i64::from(array.as_primitive::<UInt16Type>().value(i))),
        DataType::UInt32 => Some(i64::from(array.as_primitive::<UInt32Type>().value(i))),
        DataType::UInt64 => i64::try_from(array.as_primitive::<UInt64Type>().value(i)).ok(),
        _ => None,
    }
}

fn in_list(key: &Expr, values: &[i64], dtype: &DataType) -> Option<Expr> {
    let eqs: Option<Vec<Expr>> = values
        .iter()
        .copied()
        .map(|v| eq_lit(key, v, dtype))
        .collect();
    eqs?.into_iter()
        .reduce(|a, b| Expr::Binary(BinaryOp::Or, Box::new(a), Box::new(b)))
}

fn between(key: &Expr, min: i64, max: i64, dtype: &DataType) -> Option<Expr> {
    Some(Expr::Binary(
        BinaryOp::And,
        Box::new(Expr::Binary(
            BinaryOp::GtEq,
            Box::new(key.clone()),
            Box::new(lit_int(min, dtype)?),
        )),
        Box::new(Expr::Binary(
            BinaryOp::LtEq,
            Box::new(key.clone()),
            Box::new(lit_int(max, dtype)?),
        )),
    ))
}

fn eq_lit(key: &Expr, v: i64, dtype: &DataType) -> Option<Expr> {
    Some(Expr::Binary(
        BinaryOp::Eq,
        Box::new(key.clone()),
        Box::new(lit_int(v, dtype)?),
    ))
}

fn lit_int(v: i64, dtype: &DataType) -> Option<Expr> {
    Some(Expr::Literal(match dtype {
        DataType::Int8 => Arc::new(Int8Array::from(vec![i8::try_from(v).ok()?])),
        DataType::Int16 => Arc::new(Int16Array::from(vec![i16::try_from(v).ok()?])),
        DataType::Int32 => Arc::new(Int32Array::from(vec![i32::try_from(v).ok()?])),
        DataType::Int64 => Arc::new(Int64Array::from(vec![v])),
        DataType::UInt8 => Arc::new(UInt8Array::from(vec![u8::try_from(v).ok()?])),
        DataType::UInt16 => Arc::new(UInt16Array::from(vec![u16::try_from(v).ok()?])),
        DataType::UInt32 => Arc::new(UInt32Array::from(vec![u32::try_from(v).ok()?])),
        DataType::UInt64 => Arc::new(UInt64Array::from(vec![u64::try_from(v).ok()?])),
        _ => return None,
    }))
}

fn summarize_pred(key: &Expr, distinct: Option<&Vec<i64>>, min: i64, max: i64) -> String {
    let name = key_text(key);
    match distinct {
        Some(values) if values.len() == 1 => format!("{name} = {}", values[0]),
        Some(values) => {
            let list = values
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            format!("{name} IN ({list})")
        }
        None => format!("{name} BETWEEN {min} AND {max}"),
    }
}

fn key_text(expr: &Expr) -> String {
    match expr {
        Expr::Nth(i) => format!("nth{i}"),
        Expr::Column(n) => n.clone(),
        _ => "?".into(),
    }
}

fn pred_text(expr: &Expr) -> String {
    match expr {
        Expr::Literal(arr) => {
            if let Some(b) = arr.as_any().downcast_ref::<BooleanArray>()
                && b.len() == 1
                && !b.value(0)
            {
                return "false".into();
            }
            "?".into()
        }
        Expr::Binary(BinaryOp::Eq, l, r) => match (key_text(l).as_str(), int_lit(r)) {
            (name, Some(v)) if name != "?" => format!("{name} = {v}"),
            _ => "?".into(),
        },
        Expr::Binary(BinaryOp::Or, _, _) => {
            let mut vals = Vec::new();
            let mut key = None;
            if flatten_or_eq(expr, &mut key, &mut vals)
                && let Some(k) = key
            {
                let name = key_text(&k);
                if vals.len() == 1 {
                    return format!("{name} = {}", vals[0]);
                }
                let list = vals
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                return format!("{name} IN ({list})");
            }
            "?".into()
        }
        Expr::Binary(BinaryOp::And, l, r) => match (cmp_bound(l), cmp_bound(r)) {
            (Some((name, BinaryOp::GtEq, lo)), Some((n2, BinaryOp::LtEq, hi))) if name == n2 => {
                format!("{name} BETWEEN {lo} AND {hi}")
            }
            (Some((name, BinaryOp::LtEq, hi)), Some((n2, BinaryOp::GtEq, lo))) if name == n2 => {
                format!("{name} BETWEEN {lo} AND {hi}")
            }
            _ => "?".into(),
        },
        _ => "?".into(),
    }
}

fn flatten_or_eq(expr: &Expr, key: &mut Option<Expr>, vals: &mut Vec<i64>) -> bool {
    match expr {
        Expr::Binary(BinaryOp::Or, l, r) => {
            flatten_or_eq(l, key, vals) && flatten_or_eq(r, key, vals)
        }
        Expr::Binary(BinaryOp::Eq, l, r) => {
            let Some(v) = int_lit(r) else {
                return false;
            };
            match key {
                None => *key = Some(l.as_ref().clone()),
                Some(k) if key_text(k) == key_text(l) => {}
                _ => return false,
            }
            vals.push(v);
            true
        }
        _ => false,
    }
}

fn cmp_bound(expr: &Expr) -> Option<(String, BinaryOp, i64)> {
    let Expr::Binary(op, l, r) = expr else {
        return None;
    };
    if !matches!(
        op,
        BinaryOp::GtEq | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::Lt
    ) {
        return None;
    }
    let v = int_lit(r)?;
    Some((key_text(l), *op, v))
}

fn int_lit(expr: &Expr) -> Option<i64> {
    let Expr::Literal(arr) = expr else {
        return None;
    };
    if arr.len() != 1 || arr.is_null(0) {
        return None;
    }
    int_at(arr.as_ref(), 0)
}

fn pred_cannot_match(batch: &Batch, pred: &Expr) -> bool {
    match pred {
        Expr::Literal(arr) => {
            if let Some(b) = arr.as_any().downcast_ref::<BooleanArray>() {
                b.len() == 1 && !b.is_null(0) && !b.value(0)
            } else {
                false
            }
        }
        Expr::Binary(BinaryOp::And, l, r) => {
            pred_cannot_match(batch, l) || pred_cannot_match(batch, r)
        }
        Expr::Binary(BinaryOp::Or, l, r) => {
            pred_cannot_match(batch, l) && pred_cannot_match(batch, r)
        }
        Expr::Binary(op, l, r) => {
            let Some(v) = int_lit(r) else {
                return false;
            };
            let Some((lo, hi)) = expr_int_range(batch, l) else {
                return false;
            };
            match op {
                BinaryOp::Eq => v < lo || v > hi,
                BinaryOp::GtEq => hi < v,
                BinaryOp::LtEq => lo > v,
                BinaryOp::Gt => hi <= v,
                BinaryOp::Lt => lo >= v,
                _ => false,
            }
        }
        _ => false,
    }
}

fn expr_int_range(batch: &Batch, expr: &Expr) -> Option<(i64, i64)> {
    let arr = expr.eval(batch).ok()?;
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    let mut n = 0usize;
    for i in 0..arr.len() {
        if arr.is_null(i) {
            continue;
        }
        let v = int_at(arr.as_ref(), i)?;
        n += 1;
        min = min.min(v);
        max = max.max(v);
    }
    if n == 0 { None } else { Some((min, max)) }
}

fn map_leaves(expr: &Expr, leaf: &impl Fn(&Expr) -> Option<Expr>) -> Option<Expr> {
    match expr {
        Expr::Nth(_) | Expr::Column(_) | Expr::Literal(_) => leaf(expr),
        Expr::Binary(op, l, r) => Some(Expr::Binary(
            *op,
            Box::new(map_leaves(l, leaf)?),
            Box::new(map_leaves(r, leaf)?),
        )),
        Expr::Unary(op, e) => Some(Expr::Unary(*op, Box::new(map_leaves(e, leaf)?))),
        Expr::Cast { expr, data_type } => Some(Expr::Cast {
            expr: Box::new(map_leaves(expr, leaf)?),
            data_type: data_type.clone(),
        }),
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => Some(Expr::Case {
            operand: match operand.as_deref() {
                Some(e) => Some(Box::new(map_leaves(e, leaf)?)),
                None => None,
            },
            when_then: when_then
                .iter()
                .map(|(w, t)| Some((map_leaves(w, leaf)?, map_leaves(t, leaf)?)))
                .collect::<Option<_>>()?,
            else_expr: match else_expr.as_deref() {
                Some(e) => Some(Box::new(map_leaves(e, leaf)?)),
                None => None,
            },
        }),
        Expr::Coalesce(args) => Some(Expr::Coalesce(
            args.iter()
                .map(|e| map_leaves(e, leaf))
                .collect::<Option<_>>()?,
        )),
        Expr::Concat(args) => Some(Expr::Concat(
            args.iter()
                .map(|e| map_leaves(e, leaf))
                .collect::<Option<_>>()?,
        )),
        Expr::Substring {
            expr,
            start,
            length,
        } => Some(Expr::Substring {
            expr: Box::new(map_leaves(expr, leaf)?),
            start: *start,
            length: *length,
        }),
        Expr::Array(args) => Some(Expr::Array(
            args.iter()
                .map(|e| map_leaves(e, leaf))
                .collect::<Option<_>>()?,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn i64_batch(name: &str, values: Vec<Option<i64>>) -> Batch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, true)]));
        Batch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    fn on_nth() -> Expr {
        Expr::Binary(BinaryOp::Eq, Box::new(Expr::nth(0)), Box::new(Expr::nth(1)))
    }

    fn derive(build: Batch) -> Option<RuntimeFilter> {
        let schema = build.schema();
        from_build(
            Some(&on_nth()),
            1,
            schema.as_ref(),
            std::slice::from_ref(&build),
        )
    }

    #[test]
    fn small_distinct_becomes_in_list() {
        let build = i64_batch("id", vec![Some(10), Some(20), Some(10), Some(30)]);
        let rf = derive(build).expect("filter");
        assert_eq!(rf.preds.len(), 1);
        assert!(rf.summary.contains("IN (10, 20, 30)"), "{}", rf.summary);
        let probe = i64_batch("k", vec![Some(1), Some(20), Some(99)]);
        assert!(!batch_cannot_match(&probe, &rf.preds));
        let kept = apply_preds(probe, &rf.preds).unwrap().unwrap();
        assert_eq!(kept.num_rows(), 1);
    }

    #[test]
    fn wide_distinct_becomes_range() {
        let values: Vec<Option<i64>> = (0..=IN_LIST_MAX as i64).map(Some).collect();
        let build = i64_batch("id", values);
        let rf = derive(build).expect("filter");
        assert!(rf.summary.contains("BETWEEN 0 AND 32"), "{}", rf.summary);
    }

    #[test]
    fn empty_build_is_false() {
        let build = i64_batch("id", vec![]);
        let rf = derive(build).expect("filter");
        assert_eq!(rf.summary, "false");
        let probe = i64_batch("k", vec![Some(1)]);
        assert!(batch_cannot_match(&probe, &rf.preds));
    }

    #[test]
    fn utf8_keys_fail_open() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let build = Batch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef],
        )
        .unwrap();
        assert!(derive(build).is_none());
    }

    #[test]
    fn skipped_morsel_outside_range() {
        let build = i64_batch("id", vec![Some(5), Some(6)]);
        let rf = derive(build).unwrap();
        let other = i64_batch("k", vec![Some(100), Some(101)]);
        assert!(batch_cannot_match(&other, &rf.preds));
    }

    #[test]
    fn nth_to_column_for_parquet() {
        let schema = Schema::new(vec![Field::new("ss_item_sk", DataType::Int64, true)]);
        let pred = eq_lit(&Expr::nth(0), 3, &DataType::Int64).unwrap();
        let named = nth_to_column(&pred, &schema).unwrap();
        match named {
            Expr::Binary(BinaryOp::Eq, l, _) => match l.as_ref() {
                Expr::Column(n) => assert_eq!(n, "ss_item_sk"),
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn int32_keys_emit_int32_literals() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let build = Batch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![Some(10), Some(20)])) as ArrayRef],
        )
        .unwrap();
        let rf = derive(build).expect("filter");
        let Expr::Binary(BinaryOp::Or, l, r) = &rf.preds[0] else {
            panic!("{:?}", rf.preds[0]);
        };
        for side in [l.as_ref(), r.as_ref()] {
            let Expr::Binary(BinaryOp::Eq, _, lit) = side else {
                panic!("{side:?}");
            };
            let Expr::Literal(arr) = lit.as_ref() else {
                panic!("{lit:?}");
            };
            assert_eq!(arr.data_type(), &DataType::Int32);
        }
        let probe_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]));
        let probe = Batch::try_new(
            probe_schema,
            vec![Arc::new(Int32Array::from(vec![Some(10), Some(99)])) as ArrayRef],
        )
        .unwrap();
        let kept = apply_preds(probe, &rf.preds).unwrap().unwrap();
        assert_eq!(kept.num_rows(), 1);
    }

    #[test]
    fn mixed_integer_widths_fail_open() {
        let probe_schema = Schema::new(vec![Field::new("k", DataType::Int32, true)]);
        let build_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let build = Batch::try_new(
            build_schema,
            vec![Arc::new(Int64Array::from(vec![Some(10), Some(20)])) as ArrayRef],
        )
        .unwrap();
        assert!(from_build(Some(&on_nth()), 1, &probe_schema, &[build]).is_none());
    }
}
