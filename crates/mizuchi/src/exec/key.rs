//! Hashable values for grouping and equi-join keys.

use std::hash::{Hash, Hasher};

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};

use crate::error::{Error, Result};

#[derive(Clone)]
pub(crate) enum Scalar {
    Null,
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(u32),
    F64(u64),
    Utf8(String),
}

impl PartialEq for Scalar {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::I8(a), Self::I8(b)) => a == b,
            (Self::I16(a), Self::I16(b)) => a == b,
            (Self::I32(a), Self::I32(b)) => a == b,
            (Self::I64(a), Self::I64(b)) => a == b,
            (Self::U8(a), Self::U8(b)) => a == b,
            (Self::U16(a), Self::U16(b)) => a == b,
            (Self::U32(a), Self::U32(b)) => a == b,
            (Self::U64(a), Self::U64(b)) => a == b,
            (Self::F32(a), Self::F32(b)) => canonical_f32(*a) == canonical_f32(*b),
            (Self::F64(a), Self::F64(b)) => canonical_f64(*a) == canonical_f64(*b),
            (Self::Utf8(a), Self::Utf8(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Scalar {}

impl Hash for Scalar {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Null => {}
            Self::Bool(v) => v.hash(state),
            Self::I8(v) => v.hash(state),
            Self::I16(v) => v.hash(state),
            Self::I32(v) => v.hash(state),
            Self::I64(v) => v.hash(state),
            Self::U8(v) => v.hash(state),
            Self::U16(v) => v.hash(state),
            Self::U32(v) => v.hash(state),
            Self::U64(v) => v.hash(state),
            Self::F32(v) => canonical_f32(*v).hash(state),
            Self::F64(v) => canonical_f64(*v).hash(state),
            Self::Utf8(v) => v.hash(state),
        }
    }
}

/// SQL grouping / equality: +0.0 equals -0.0; all NaNs form one group.
fn canonical_f32(bits: u32) -> u32 {
    let v = f32::from_bits(bits);
    if v == 0.0 {
        0.0f32.to_bits()
    } else if v.is_nan() {
        f32::NAN.to_bits()
    } else {
        bits
    }
}

fn canonical_f64(bits: u64) -> u64 {
    let v = f64::from_bits(bits);
    if v == 0.0 {
        0.0f64.to_bits()
    } else if v.is_nan() {
        f64::NAN.to_bits()
    } else {
        bits
    }
}

pub(crate) fn extract_key(cols: &[ArrayRef], row: usize) -> Result<Vec<Scalar>> {
    cols.iter().map(|c| scalar_at(c.as_ref(), row)).collect()
}

pub(super) fn scalar_at(array: &dyn Array, i: usize) -> Result<Scalar> {
    if array.is_null(i) {
        return Ok(Scalar::Null);
    }
    Ok(match array.data_type() {
        arrow::datatypes::DataType::Boolean => Scalar::Bool(array.as_boolean().value(i)),
        arrow::datatypes::DataType::Int8 => Scalar::I8(array.as_primitive::<Int8Type>().value(i)),
        arrow::datatypes::DataType::Int16 => {
            Scalar::I16(array.as_primitive::<Int16Type>().value(i))
        }
        arrow::datatypes::DataType::Int32 => {
            Scalar::I32(array.as_primitive::<Int32Type>().value(i))
        }
        arrow::datatypes::DataType::Int64 => {
            Scalar::I64(array.as_primitive::<Int64Type>().value(i))
        }
        arrow::datatypes::DataType::UInt8 => Scalar::U8(array.as_primitive::<UInt8Type>().value(i)),
        arrow::datatypes::DataType::UInt16 => {
            Scalar::U16(array.as_primitive::<UInt16Type>().value(i))
        }
        arrow::datatypes::DataType::UInt32 => {
            Scalar::U32(array.as_primitive::<UInt32Type>().value(i))
        }
        arrow::datatypes::DataType::UInt64 => {
            Scalar::U64(array.as_primitive::<UInt64Type>().value(i))
        }
        arrow::datatypes::DataType::Float32 => {
            Scalar::F32(array.as_primitive::<Float32Type>().value(i).to_bits())
        }
        arrow::datatypes::DataType::Float64 => {
            Scalar::F64(array.as_primitive::<Float64Type>().value(i).to_bits())
        }
        arrow::datatypes::DataType::Utf8 => {
            Scalar::Utf8(array.as_string::<i32>().value(i).to_owned())
        }
        arrow::datatypes::DataType::LargeUtf8 => {
            Scalar::Utf8(array.as_string::<i64>().value(i).to_owned())
        }
        dt => {
            return Err(Error::Eval(format!(
                "unsupported grouping or join key type: {dt}"
            )));
        }
    })
}

pub(super) fn eval_all(
    exprs: &[crate::expr::Expr],
    batch: &crate::batch::Batch,
) -> Result<Vec<ArrayRef>> {
    exprs.iter().map(|e| e.eval(batch)).collect()
}
