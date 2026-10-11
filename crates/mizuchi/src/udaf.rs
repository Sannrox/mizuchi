//! Combinable user-defined aggregates.
//!
//! The buffer type equals the result type, so a partial aggregate written
//! during spill or a parallel combine is merged with the same operation used
//! to fold rows. `update` folds one input value into the buffer; `merge`
//! folds two buffers. Skip-null aggregates use a null identity (no values
//! seen).

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::DataType;

use crate::error::{Error, Result};

/// Associative aggregate with a mergeable buffer.
pub trait CombineAgg: Send + Sync + std::fmt::Debug {
    /// SQL name, matched case-insensitively.
    fn name(&self) -> &str;
    /// Buffer and result Arrow type.
    fn result_type(&self) -> DataType;
    /// Length-1 empty-group buffer.
    fn identity(&self) -> ArrayRef;
    /// Fold one input value into the buffer. Both arrays have length 1.
    fn update(&self, state: &dyn Array, value: &dyn Array) -> Result<ArrayRef>;
    /// Fold two buffers. Both arrays have length 1.
    fn merge(&self, left: &dyn Array, right: &dyn Array) -> Result<ArrayRef>;
}

/// Int64 product. Nulls are skipped; no non-null values yields null.
#[derive(Debug, Clone, Copy)]
pub struct ProductI64;

impl CombineAgg for ProductI64 {
    fn name(&self) -> &str {
        "product"
    }

    fn result_type(&self) -> DataType {
        DataType::Int64
    }

    fn identity(&self) -> ArrayRef {
        Arc::new(Int64Array::from(vec![None::<i64>]))
    }

    fn update(&self, state: &dyn Array, value: &dyn Array) -> Result<ArrayRef> {
        mul_i64(state, value, false)
    }

    fn merge(&self, left: &dyn Array, right: &dyn Array) -> Result<ArrayRef> {
        mul_i64(left, right, true)
    }
}

fn i64_at(array: &dyn Array) -> Result<Option<i64>> {
    if array.len() != 1 {
        return Err(Error::Eval("UDAF buffer must have length 1".into()));
    }
    let arr = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::Eval(format!("expected Int64, got {}", array.data_type())))?;
    if arr.is_null(0) {
        Ok(None)
    } else {
        Ok(Some(arr.value(0)))
    }
}

fn mul_i64(left: &dyn Array, right: &dyn Array, both_buffers: bool) -> Result<ArrayRef> {
    let a = i64_at(left)?;
    let b = i64_at(right)?;
    let out = match (a, b) {
        (None, x) | (x, None) => x,
        (Some(l), Some(r)) => Some(l.checked_mul(r).ok_or_else(|| {
            Error::Eval(if both_buffers {
                "product merge overflow".into()
            } else {
                "product overflow".into()
            })
        })?),
    };
    Ok(Arc::new(Int64Array::from(vec![out])))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn product_skips_nulls_and_overflows() {
        let p = ProductI64;
        let n = p.identity();
        let two = Arc::new(Int64Array::from(vec![Some(2i64)])) as ArrayRef;
        let three = Arc::new(Int64Array::from(vec![Some(3i64)])) as ArrayRef;
        let six = p.update(n.as_ref(), two.as_ref()).unwrap();
        let six = p.update(six.as_ref(), three.as_ref()).unwrap();
        assert_eq!(i64_at(six.as_ref()).unwrap(), Some(6));
        let skipped = p.update(six.as_ref(), n.as_ref()).unwrap();
        assert_eq!(i64_at(skipped.as_ref()).unwrap(), Some(6));
        let huge = Arc::new(Int64Array::from(vec![Some(i64::MAX)])) as ArrayRef;
        assert!(p.update(two.as_ref(), huge.as_ref()).is_err());
    }
}
