//! Hashable values for grouping and equi-join keys.
//!
//! Grouping and equi-join lookups use a cheap non-crypto hasher. Precomputed
//! row hashes are stored in an identity map so the bucket step does not hash
//! them again.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hash, Hasher};

use arrow::array::{Array, ArrayRef, AsArray, DictionaryArray, StringArray};
use arrow::datatypes::{
    ArrowDictionaryKeyType, DataType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};

use crate::error::{Error, Result};

/// rustc-hash style multiply. Fast and well distributed for in-memory maps.
const FX_K: u64 = 0x517cc1b727220a95;

/// Cheap hasher for grouping keys, join keys, and integer build maps.
#[derive(Clone, Copy, Default)]
pub(super) struct FastHasher {
    state: u64,
}

impl Hasher for FastHasher {
    fn write(&mut self, bytes: &[u8]) {
        let mut i = 0;
        while i + 8 <= bytes.len() {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes[i..i + 8]);
            self.write_u64(u64::from_le_bytes(buf));
            i += 8;
        }
        if i < bytes.len() {
            let mut buf = [0u8; 8];
            buf[..bytes.len() - i].copy_from_slice(&bytes[i..]);
            self.write_u64(u64::from_le_bytes(buf));
        }
    }

    fn write_u8(&mut self, i: u8) {
        self.write_u64(u64::from(i));
    }

    fn write_u16(&mut self, i: u16) {
        self.write_u64(u64::from(i));
    }

    fn write_u32(&mut self, i: u32) {
        self.write_u64(u64::from(i));
    }

    fn write_u64(&mut self, i: u64) {
        self.state = (self.state.rotate_left(5) ^ i).wrapping_mul(FX_K);
    }

    fn write_usize(&mut self, i: usize) {
        self.write_u64(i as u64);
    }

    fn finish(&self) -> u64 {
        self.state
    }
}

/// Use a precomputed `u64` hash as the bucket key without hashing it again.
#[derive(Clone, Copy, Default)]
pub(super) struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn write(&mut self, bytes: &[u8]) {
        let mut v = 0u64;
        for (i, b) in bytes.iter().take(8).enumerate() {
            v |= u64::from(*b) << (8 * i);
        }
        self.0 = v;
    }

    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }

    fn write_u32(&mut self, i: u32) {
        self.0 = u64::from(i);
    }

    fn write_u16(&mut self, i: u16) {
        self.0 = u64::from(i);
    }

    fn write_u8(&mut self, i: u8) {
        self.0 = u64::from(i);
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

pub(super) type FastBuild = BuildHasherDefault<FastHasher>;
pub(super) type FastMap<K, V> = HashMap<K, V, FastBuild>;
pub(super) type IdentityBuild = BuildHasherDefault<IdentityHasher>;
pub(super) type IdentityMap<V> = HashMap<u64, V, IdentityBuild>;

pub(super) fn fast_map_with_capacity<K, V>(n: usize) -> FastMap<K, V> {
    HashMap::with_capacity_and_hasher(n, FastBuild::default())
}

pub(super) fn identity_map_with_capacity<V>(n: usize) -> IdentityMap<V> {
    HashMap::with_capacity_and_hasher(n, IdentityBuild::default())
}

#[derive(Clone, Debug)]
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

/// Variant tags for [`Scalar::hash`] and in-place cell hashing. Must stay in
/// lockstep so a grouping lookup without allocating `String` matches an
/// inserted key.
pub(super) const HASH_NULL: u8 = 0;
pub(super) const HASH_BOOL: u8 = 1;
pub(super) const HASH_I8: u8 = 2;
pub(super) const HASH_I16: u8 = 3;
pub(super) const HASH_I32: u8 = 4;
pub(super) const HASH_I64: u8 = 5;
pub(super) const HASH_U8: u8 = 6;
pub(super) const HASH_U16: u8 = 7;
pub(super) const HASH_U32: u8 = 8;
pub(super) const HASH_U64: u8 = 9;
pub(super) const HASH_F32: u8 = 10;
pub(super) const HASH_F64: u8 = 11;
pub(super) const HASH_UTF8: u8 = 12;

impl Hash for Scalar {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Null => HASH_NULL.hash(state),
            Self::Bool(v) => {
                HASH_BOOL.hash(state);
                v.hash(state);
            }
            Self::I8(v) => {
                HASH_I8.hash(state);
                v.hash(state);
            }
            Self::I16(v) => {
                HASH_I16.hash(state);
                v.hash(state);
            }
            Self::I32(v) => {
                HASH_I32.hash(state);
                v.hash(state);
            }
            Self::I64(v) => {
                HASH_I64.hash(state);
                v.hash(state);
            }
            Self::U8(v) => {
                HASH_U8.hash(state);
                v.hash(state);
            }
            Self::U16(v) => {
                HASH_U16.hash(state);
                v.hash(state);
            }
            Self::U32(v) => {
                HASH_U32.hash(state);
                v.hash(state);
            }
            Self::U64(v) => {
                HASH_U64.hash(state);
                v.hash(state);
            }
            Self::F32(v) => {
                HASH_F32.hash(state);
                canonical_f32(*v).hash(state);
            }
            Self::F64(v) => {
                HASH_F64.hash(state);
                canonical_f64(*v).hash(state);
            }
            Self::Utf8(v) => {
                HASH_UTF8.hash(state);
                v.hash(state);
            }
        }
    }
}

/// SQL grouping / equality: +0.0 equals -0.0; all NaNs form one group.
pub(super) fn canonical_f32(bits: u32) -> u32 {
    let v = f32::from_bits(bits);
    if v == 0.0 {
        0.0f32.to_bits()
    } else if v.is_nan() {
        f32::NAN.to_bits()
    } else {
        bits
    }
}

pub(super) fn canonical_f64(bits: u64) -> u64 {
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
        DataType::Utf8 => Scalar::Utf8(array.as_string::<i32>().value(i).to_owned()),
        DataType::LargeUtf8 => Scalar::Utf8(array.as_string::<i64>().value(i).to_owned()),
        DataType::Dictionary(_, _) => Scalar::Utf8(dict_utf8_at(array, i)?.to_owned()),
        DataType::Null => Scalar::Null,
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

/// Utf8 (or LargeUtf8) value at `row` in a dictionary array, without allocating.
pub(super) fn dict_utf8_at(array: &dyn Array, row: usize) -> Result<&str> {
    let DataType::Dictionary(key, value) = array.data_type() else {
        return Err(Error::Eval("expected dictionary grouping key".into()));
    };
    match (key.as_ref(), value.as_ref()) {
        (DataType::Int8, DataType::Utf8) => dict_utf8_typed::<Int8Type>(array, row),
        (DataType::Int16, DataType::Utf8) => dict_utf8_typed::<Int16Type>(array, row),
        (DataType::Int32, DataType::Utf8) => dict_utf8_typed::<Int32Type>(array, row),
        (DataType::Int64, DataType::Utf8) => dict_utf8_typed::<Int64Type>(array, row),
        (DataType::UInt8, DataType::Utf8) => dict_utf8_typed::<UInt8Type>(array, row),
        (DataType::UInt16, DataType::Utf8) => dict_utf8_typed::<UInt16Type>(array, row),
        (DataType::UInt32, DataType::Utf8) => dict_utf8_typed::<UInt32Type>(array, row),
        (DataType::UInt64, DataType::Utf8) => dict_utf8_typed::<UInt64Type>(array, row),
        (DataType::Int32, DataType::LargeUtf8) => dict_large_utf8_typed::<Int32Type>(array, row),
        dt => Err(Error::Eval(format!(
            "unsupported dictionary grouping key type: {}[{}]",
            dt.0, dt.1
        ))),
    }
}

fn dict_utf8_typed<K: ArrowDictionaryKeyType>(array: &dyn Array, row: usize) -> Result<&str> {
    let dict = array
        .as_any()
        .downcast_ref::<DictionaryArray<K>>()
        .ok_or_else(|| Error::Eval("dictionary key downcast failed".into()))?;
    let idx = dict
        .key(row)
        .ok_or_else(|| Error::Eval("null dictionary key".into()))?;
    let values = dict
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Eval("expected Utf8 dictionary values".into()))?;
    Ok(values.value(idx))
}

fn dict_large_utf8_typed<K: ArrowDictionaryKeyType>(array: &dyn Array, row: usize) -> Result<&str> {
    let dict = array
        .as_any()
        .downcast_ref::<DictionaryArray<K>>()
        .ok_or_else(|| Error::Eval("dictionary key downcast failed".into()))?;
    let idx = dict
        .key(row)
        .ok_or_else(|| Error::Eval("null dictionary key".into()))?;
    let values = dict
        .values()
        .as_any()
        .downcast_ref::<arrow::array::LargeStringArray>()
        .ok_or_else(|| Error::Eval("expected LargeUtf8 dictionary values".into()))?;
    Ok(values.value(idx))
}

pub(super) fn hash_scalars(key: &[Scalar]) -> u64 {
    let mut hasher = FastHasher::default();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Hash one grouping or join key row without allocating `Scalar` / `String`.
pub(super) fn hash_row(cols: &[ArrayRef], row: usize) -> Result<u64> {
    let mut hasher = FastHasher::default();
    cols.len().hash(&mut hasher);
    for col in cols {
        hash_cell(col.as_ref(), row, &mut hasher)?;
    }
    Ok(hasher.finish())
}

pub(super) fn hash_cell(array: &dyn Array, row: usize, hasher: &mut impl Hasher) -> Result<()> {
    if array.is_null(row) {
        HASH_NULL.hash(hasher);
        return Ok(());
    }
    match array.data_type() {
        DataType::Null => {
            HASH_NULL.hash(hasher);
        }
        DataType::Boolean => {
            HASH_BOOL.hash(hasher);
            array.as_boolean().value(row).hash(hasher);
        }
        DataType::Int8 => {
            HASH_I8.hash(hasher);
            array.as_primitive::<Int8Type>().value(row).hash(hasher);
        }
        DataType::Int16 => {
            HASH_I16.hash(hasher);
            array.as_primitive::<Int16Type>().value(row).hash(hasher);
        }
        DataType::Int32 => {
            HASH_I32.hash(hasher);
            array.as_primitive::<Int32Type>().value(row).hash(hasher);
        }
        DataType::Int64 => {
            HASH_I64.hash(hasher);
            array.as_primitive::<Int64Type>().value(row).hash(hasher);
        }
        DataType::UInt8 => {
            HASH_U8.hash(hasher);
            array.as_primitive::<UInt8Type>().value(row).hash(hasher);
        }
        DataType::UInt16 => {
            HASH_U16.hash(hasher);
            array.as_primitive::<UInt16Type>().value(row).hash(hasher);
        }
        DataType::UInt32 => {
            HASH_U32.hash(hasher);
            array.as_primitive::<UInt32Type>().value(row).hash(hasher);
        }
        DataType::UInt64 => {
            HASH_U64.hash(hasher);
            array.as_primitive::<UInt64Type>().value(row).hash(hasher);
        }
        DataType::Float32 => {
            HASH_F32.hash(hasher);
            canonical_f32(array.as_primitive::<Float32Type>().value(row).to_bits()).hash(hasher);
        }
        DataType::Float64 => {
            HASH_F64.hash(hasher);
            canonical_f64(array.as_primitive::<Float64Type>().value(row).to_bits()).hash(hasher);
        }
        DataType::Utf8 => {
            HASH_UTF8.hash(hasher);
            array.as_string::<i32>().value(row).hash(hasher);
        }
        DataType::LargeUtf8 => {
            HASH_UTF8.hash(hasher);
            array.as_string::<i64>().value(row).hash(hasher);
        }
        DataType::Dictionary(_, _) => {
            HASH_UTF8.hash(hasher);
            dict_utf8_at(array, row)?.hash(hasher);
        }
        dt => {
            return Err(Error::Eval(format!(
                "unsupported grouping or join key type: {dt}"
            )));
        }
    }
    Ok(())
}

pub(super) fn row_has_null(cols: &[ArrayRef], row: usize) -> bool {
    cols.iter()
        .any(|c| c.data_type() == &DataType::Null || c.is_null(row))
}

pub(super) fn row_matches(key: &[Scalar], cols: &[ArrayRef], row: usize) -> Result<bool> {
    if key.len() != cols.len() {
        return Ok(false);
    }
    for (part, col) in key.iter().zip(cols) {
        if !cell_matches(part, col.as_ref(), row)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn cell_matches(part: &Scalar, array: &dyn Array, row: usize) -> Result<bool> {
    if array.is_null(row) {
        return Ok(*part == Scalar::Null);
    }
    Ok(match (part, array.data_type()) {
        (Scalar::Bool(v), DataType::Boolean) => *v == array.as_boolean().value(row),
        (Scalar::I8(v), DataType::Int8) => *v == array.as_primitive::<Int8Type>().value(row),
        (Scalar::I16(v), DataType::Int16) => *v == array.as_primitive::<Int16Type>().value(row),
        (Scalar::I32(v), DataType::Int32) => *v == array.as_primitive::<Int32Type>().value(row),
        (Scalar::I64(v), DataType::Int64) => *v == array.as_primitive::<Int64Type>().value(row),
        (Scalar::U8(v), DataType::UInt8) => *v == array.as_primitive::<UInt8Type>().value(row),
        (Scalar::U16(v), DataType::UInt16) => *v == array.as_primitive::<UInt16Type>().value(row),
        (Scalar::U32(v), DataType::UInt32) => *v == array.as_primitive::<UInt32Type>().value(row),
        (Scalar::U64(v), DataType::UInt64) => *v == array.as_primitive::<UInt64Type>().value(row),
        (Scalar::F32(v), DataType::Float32) => {
            Scalar::F32(*v) == Scalar::F32(array.as_primitive::<Float32Type>().value(row).to_bits())
        }
        (Scalar::F64(v), DataType::Float64) => {
            Scalar::F64(*v) == Scalar::F64(array.as_primitive::<Float64Type>().value(row).to_bits())
        }
        (Scalar::Utf8(v), DataType::Utf8) => v == array.as_string::<i32>().value(row),
        (Scalar::Utf8(v), DataType::LargeUtf8) => v == array.as_string::<i64>().value(row),
        (Scalar::Utf8(v), DataType::Dictionary(_, _)) => v == dict_utf8_at(array, row)?,
        _ => false,
    })
}

/// Compare one key row of `left` to one key row of `right` without allocating.
pub(super) fn rows_equal(
    left: &[ArrayRef],
    lrow: usize,
    right: &[ArrayRef],
    rrow: usize,
) -> Result<bool> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (l, r) in left.iter().zip(right) {
        if !cells_equal(l.as_ref(), lrow, r.as_ref(), rrow)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn cells_equal(a: &dyn Array, ai: usize, b: &dyn Array, bi: usize) -> Result<bool> {
    let an = a.is_null(ai);
    let bn = b.is_null(bi);
    if an || bn {
        return Ok(an && bn);
    }
    if a.data_type() == b.data_type() {
        return Ok(match a.data_type() {
            DataType::Null => true,
            DataType::Boolean => a.as_boolean().value(ai) == b.as_boolean().value(bi),
            DataType::Int8 => {
                a.as_primitive::<Int8Type>().value(ai) == b.as_primitive::<Int8Type>().value(bi)
            }
            DataType::Int16 => {
                a.as_primitive::<Int16Type>().value(ai) == b.as_primitive::<Int16Type>().value(bi)
            }
            DataType::Int32 => {
                a.as_primitive::<Int32Type>().value(ai) == b.as_primitive::<Int32Type>().value(bi)
            }
            DataType::Int64 => {
                a.as_primitive::<Int64Type>().value(ai) == b.as_primitive::<Int64Type>().value(bi)
            }
            DataType::UInt8 => {
                a.as_primitive::<UInt8Type>().value(ai) == b.as_primitive::<UInt8Type>().value(bi)
            }
            DataType::UInt16 => {
                a.as_primitive::<UInt16Type>().value(ai) == b.as_primitive::<UInt16Type>().value(bi)
            }
            DataType::UInt32 => {
                a.as_primitive::<UInt32Type>().value(ai) == b.as_primitive::<UInt32Type>().value(bi)
            }
            DataType::UInt64 => {
                a.as_primitive::<UInt64Type>().value(ai) == b.as_primitive::<UInt64Type>().value(bi)
            }
            DataType::Float32 => {
                canonical_f32(a.as_primitive::<Float32Type>().value(ai).to_bits())
                    == canonical_f32(b.as_primitive::<Float32Type>().value(bi).to_bits())
            }
            DataType::Float64 => {
                canonical_f64(a.as_primitive::<Float64Type>().value(ai).to_bits())
                    == canonical_f64(b.as_primitive::<Float64Type>().value(bi).to_bits())
            }
            DataType::Utf8 => a.as_string::<i32>().value(ai) == b.as_string::<i32>().value(bi),
            DataType::LargeUtf8 => a.as_string::<i64>().value(ai) == b.as_string::<i64>().value(bi),
            DataType::Dictionary(_, _) => dict_utf8_at(a, ai)? == dict_utf8_at(b, bi)?,
            _ => scalar_at(a, ai)? == scalar_at(b, bi)?,
        });
    }
    Ok(scalar_at(a, ai)? == scalar_at(b, bi)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn hash_row_matches_extracted_scalar_key() {
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
        ];
        for row in 0..3 {
            let key = extract_key(&cols, row).unwrap();
            assert_eq!(hash_row(&cols, row).unwrap(), hash_scalars(&key));
        }
        assert_eq!(hash_row(&[], 0).unwrap(), hash_scalars(&[]));
    }

    #[test]
    fn rows_equal_matches_scalar_eq_including_nulls() {
        let left: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
        ];
        let right = left.clone();
        for row in 0..3 {
            assert!(rows_equal(&left, row, &right, row).unwrap());
            let key = extract_key(&left, row).unwrap();
            assert!(row_matches(&key, &right, row).unwrap());
        }
        assert!(!rows_equal(&left, 0, &right, 1).unwrap());
        assert!(row_has_null(&left, 1));
        assert!(row_has_null(&left, 2));
        assert!(!row_has_null(&left, 0));
        let nulls: Vec<ArrayRef> = vec![Arc::new(arrow::array::NullArray::new(2))];
        assert!(row_has_null(&nulls, 0));
        assert!(row_has_null(&nulls, 1));
    }

    #[test]
    fn identity_map_uses_precomputed_hash() {
        let mut map = identity_map_with_capacity::<usize>(4);
        let cols: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![7i64]))];
        let h = hash_row(&cols, 0).unwrap();
        map.insert(h, 3);
        assert_eq!(map.get(&h), Some(&3));
        assert_eq!(map.get(&(h ^ 1)), None);
    }
}
