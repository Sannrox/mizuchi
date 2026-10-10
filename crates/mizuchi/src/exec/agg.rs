//! Hash aggregate over Arrow batches.
//!
//! Grouping expressions and aggregate arguments are evaluated column-at-a-time.
//! Groups live in a hash table keyed by those grouping values. COUNT(expr),
//! SUM, AVG, MIN, and MAX skip nulls; COUNT(*) counts every input row.
//! COUNT(DISTINCT expr) counts unique non-null values per group. Ungrouped
//! aggregation of empty input still emits one row (COUNT is 0, the rest null).
//! Grouped aggregation of empty input emits no rows. Null grouping keys form
//! one group.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeStringArray, StringArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, SchemaRef,
    UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};

use super::Exec;
use crate::batch::{Batch, BatchPolicy};
use crate::error::{Error, Result};
use crate::expr::Expr;
use crate::plan::{AggCall, NamedExpr};

pub(super) fn run(
    mut input: Exec,
    group: &[NamedExpr],
    aggs: &[(String, AggCall)],
    schema: SchemaRef,
    policy: &BatchPolicy,
) -> Result<Vec<Batch>> {
    let mut table = HashAgg::new(group, aggs, schema)?;
    while let Some(batch) = input.next_batch()? {
        table.update(&batch)?;
    }
    table.finish(policy)
}

struct HashAgg {
    group_exprs: Vec<Expr>,
    aggs: Vec<AggCall>,
    schema: SchemaRef,
    grouped: bool,
    slots: HashMap<Vec<Scalar>, usize>,
    keys: Vec<Vec<Scalar>>,
    accums: Vec<Vec<Accum>>,
}

impl HashAgg {
    fn new(group: &[NamedExpr], aggs: &[(String, AggCall)], schema: SchemaRef) -> Result<Self> {
        Ok(Self {
            group_exprs: group.iter().map(|g| g.expr.clone()).collect(),
            aggs: aggs.iter().map(|(_, c)| c.clone()).collect(),
            schema,
            grouped: !group.is_empty(),
            slots: HashMap::new(),
            keys: Vec::new(),
            accums: Vec::new(),
        })
    }

    fn update(&mut self, batch: &Batch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let key_cols = eval_all(&self.group_exprs, batch)?;
        let mut arg_cols: Vec<Option<ArrayRef>> = Vec::with_capacity(self.aggs.len());
        for call in &self.aggs {
            arg_cols.push(match call {
                AggCall::CountStar => None,
                AggCall::Count(e)
                | AggCall::CountDistinct(e)
                | AggCall::Sum(e)
                | AggCall::Avg(e)
                | AggCall::Min(e)
                | AggCall::Max(e) => Some(e.eval(batch)?),
            });
        }
        for row in 0..batch.num_rows() {
            let key = extract_key(&key_cols, row)?;
            let slot = self.ensure_group(key)?;
            for (accum, arg) in self.accums[slot].iter_mut().zip(arg_cols.iter()) {
                accum.update(arg.as_deref(), row)?;
            }
        }
        Ok(())
    }

    fn ensure_group(&mut self, key: Vec<Scalar>) -> Result<usize> {
        if let Some(&slot) = self.slots.get(&key) {
            return Ok(slot);
        }
        let slot = self.keys.len();
        let n_group = self.group_exprs.len();
        let accums = self
            .aggs
            .iter()
            .enumerate()
            .map(|(i, call)| Accum::from_call(call, self.schema.field(n_group + i).data_type()))
            .collect::<Result<Vec<_>>>()?;
        self.slots.insert(key.clone(), slot);
        self.keys.push(key);
        self.accums.push(accums);
        Ok(slot)
    }

    fn finish(mut self, policy: &BatchPolicy) -> Result<Vec<Batch>> {
        if !self.grouped && self.keys.is_empty() {
            self.ensure_group(Vec::new())?;
        }
        if self.keys.is_empty() {
            return Ok(Vec::new());
        }
        let n = self.keys.len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for (g, field) in self
            .schema
            .fields()
            .iter()
            .enumerate()
            .take(self.group_exprs.len())
        {
            let values: Vec<Scalar> = self.keys.iter().map(|k| k[g].clone()).collect();
            columns.push(build_array(&values, field.data_type())?);
        }
        for (a, field) in self
            .schema
            .fields()
            .iter()
            .enumerate()
            .skip(self.group_exprs.len())
        {
            let ai = a - self.group_exprs.len();
            let values: Vec<Scalar> = (0..n).map(|i| self.accums[i][ai].finish()).collect();
            columns.push(build_array(&values, field.data_type())?);
        }
        let full = Batch::try_new(self.schema, columns)?;
        Ok(full
            .split_by_policy(policy)
            .into_iter()
            .filter(|b| b.num_rows() > 0)
            .collect())
    }
}

fn eval_all(exprs: &[Expr], batch: &Batch) -> Result<Vec<ArrayRef>> {
    exprs.iter().map(|e| e.eval(batch)).collect()
}

fn extract_key(cols: &[ArrayRef], row: usize) -> Result<Vec<Scalar>> {
    cols.iter().map(|c| scalar_at(c.as_ref(), row)).collect()
}

#[derive(Clone)]
enum Scalar {
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

/// SQL grouping: +0.0 equals -0.0; all NaNs form one group.
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

enum Accum {
    Count(i64),
    CountDistinct(HashSet<Scalar>),
    SumI64 { sum: i64, seen: bool },
    SumU64 { sum: u64, seen: bool },
    SumF64 { sum: f64, seen: bool },
    Avg { sum: f64, count: i64 },
    Min { current: Option<Scalar> },
    Max { current: Option<Scalar> },
}

impl Accum {
    fn from_call(call: &AggCall, result_type: &DataType) -> Result<Self> {
        Ok(match call {
            AggCall::CountStar | AggCall::Count(_) => Self::Count(0),
            AggCall::CountDistinct(_) => Self::CountDistinct(HashSet::new()),
            AggCall::Sum(_) => match result_type {
                DataType::Float32 | DataType::Float64 => Self::SumF64 {
                    sum: 0.0,
                    seen: false,
                },
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    Self::SumU64 {
                        sum: 0,
                        seen: false,
                    }
                }
                _ => Self::SumI64 {
                    sum: 0,
                    seen: false,
                },
            },
            AggCall::Avg(_) => Self::Avg { sum: 0.0, count: 0 },
            AggCall::Min(_) => Self::Min { current: None },
            AggCall::Max(_) => Self::Max { current: None },
        })
    }

    fn update(&mut self, arg: Option<&dyn Array>, row: usize) -> Result<()> {
        match self {
            Self::Count(n) => {
                if let Some(array) = arg {
                    if !array.is_null(row) {
                        *n += 1;
                    }
                } else {
                    *n += 1;
                }
                Ok(())
            }
            Self::CountDistinct(seen) => {
                let array =
                    arg.ok_or_else(|| Error::Eval("COUNT DISTINCT needs an argument".into()))?;
                let value = scalar_at(array, row)?;
                if value != Scalar::Null {
                    seen.insert(value);
                }
                Ok(())
            }
            Self::SumI64 { sum, seen } => update_sum_i64(sum, seen, arg, row),
            Self::SumU64 { sum, seen } => update_sum_u64(sum, seen, arg, row),
            Self::SumF64 { sum, seen } => update_sum_f64(sum, seen, arg, row),
            Self::Avg { sum, count } => update_avg(sum, count, arg, row),
            Self::Min { current } => update_minmax(current, arg, row, true),
            Self::Max { current } => update_minmax(current, arg, row, false),
        }
    }

    fn finish(&self) -> Scalar {
        match self {
            Self::Count(n) => Scalar::I64(*n),
            Self::CountDistinct(seen) => Scalar::I64(seen.len() as i64),
            Self::SumI64 { sum, seen } => {
                if *seen {
                    Scalar::I64(*sum)
                } else {
                    Scalar::Null
                }
            }
            Self::SumU64 { sum, seen } => {
                if *seen {
                    Scalar::U64(*sum)
                } else {
                    Scalar::Null
                }
            }
            Self::SumF64 { sum, seen } => {
                if *seen {
                    Scalar::F64(sum.to_bits())
                } else {
                    Scalar::Null
                }
            }
            Self::Avg { sum, count } => {
                if *count == 0 {
                    Scalar::Null
                } else {
                    Scalar::F64((*sum / *count as f64).to_bits())
                }
            }
            Self::Min { current } | Self::Max { current } => {
                current.clone().unwrap_or(Scalar::Null)
            }
        }
    }
}

fn update_sum_i64(
    sum: &mut i64,
    seen: &mut bool,
    arg: Option<&dyn Array>,
    row: usize,
) -> Result<()> {
    let array = arg.ok_or_else(|| Error::Eval("SUM needs an argument".into()))?;
    if array.is_null(row) {
        return Ok(());
    }
    let v = integer_value(array, row)?;
    *sum = sum
        .checked_add(v)
        .ok_or_else(|| Error::Eval("SUM overflow".into()))?;
    *seen = true;
    Ok(())
}

fn update_sum_u64(
    sum: &mut u64,
    seen: &mut bool,
    arg: Option<&dyn Array>,
    row: usize,
) -> Result<()> {
    let array = arg.ok_or_else(|| Error::Eval("SUM needs an argument".into()))?;
    if array.is_null(row) {
        return Ok(());
    }
    let v = unsigned_value(array, row)?;
    *sum = sum
        .checked_add(v)
        .ok_or_else(|| Error::Eval("SUM overflow".into()))?;
    *seen = true;
    Ok(())
}

fn update_sum_f64(
    sum: &mut f64,
    seen: &mut bool,
    arg: Option<&dyn Array>,
    row: usize,
) -> Result<()> {
    let array = arg.ok_or_else(|| Error::Eval("SUM needs an argument".into()))?;
    if array.is_null(row) {
        return Ok(());
    }
    *sum += float_value(array, row)?;
    *seen = true;
    Ok(())
}

fn update_avg(sum: &mut f64, count: &mut i64, arg: Option<&dyn Array>, row: usize) -> Result<()> {
    let array = arg.ok_or_else(|| Error::Eval("AVG needs an argument".into()))?;
    if array.is_null(row) {
        return Ok(());
    }
    *sum += numeric_as_f64(array, row)?;
    *count += 1;
    Ok(())
}

fn update_minmax(
    current: &mut Option<Scalar>,
    arg: Option<&dyn Array>,
    row: usize,
    is_min: bool,
) -> Result<()> {
    let array = arg.ok_or_else(|| Error::Eval("MIN/MAX needs an argument".into()))?;
    let value = scalar_at(array, row)?;
    if value == Scalar::Null {
        return Ok(());
    }
    match current {
        None => *current = Some(value),
        Some(cur) => {
            let ord = cmp_scalar(cur, &value)?;
            if (is_min && ord == Ordering::Greater) || (!is_min && ord == Ordering::Less) {
                *current = Some(value);
            }
        }
    }
    Ok(())
}

fn integer_value(array: &dyn Array, row: usize) -> Result<i64> {
    Ok(match array.data_type() {
        DataType::Int8 => i64::from(array.as_primitive::<Int8Type>().value(row)),
        DataType::Int16 => i64::from(array.as_primitive::<Int16Type>().value(row)),
        DataType::Int32 => i64::from(array.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => array.as_primitive::<Int64Type>().value(row),
        DataType::UInt8 => i64::from(array.as_primitive::<UInt8Type>().value(row)),
        DataType::UInt16 => i64::from(array.as_primitive::<UInt16Type>().value(row)),
        DataType::UInt32 => i64::from(array.as_primitive::<UInt32Type>().value(row)),
        dt => {
            return Err(Error::Eval(format!(
                "SUM does not support {dt} in the integer accumulator"
            )));
        }
    })
}

fn unsigned_value(array: &dyn Array, row: usize) -> Result<u64> {
    Ok(match array.data_type() {
        DataType::UInt8 => u64::from(array.as_primitive::<UInt8Type>().value(row)),
        DataType::UInt16 => u64::from(array.as_primitive::<UInt16Type>().value(row)),
        DataType::UInt32 => u64::from(array.as_primitive::<UInt32Type>().value(row)),
        DataType::UInt64 => array.as_primitive::<UInt64Type>().value(row),
        dt => {
            return Err(Error::Eval(format!(
                "SUM does not support {dt} in the unsigned accumulator"
            )));
        }
    })
}

fn float_value(array: &dyn Array, row: usize) -> Result<f64> {
    Ok(match array.data_type() {
        DataType::Float32 => f64::from(array.as_primitive::<Float32Type>().value(row)),
        DataType::Float64 => array.as_primitive::<Float64Type>().value(row),
        dt => return Err(Error::Eval(format!("float SUM does not support {dt}"))),
    })
}

fn numeric_as_f64(array: &dyn Array, row: usize) -> Result<f64> {
    match array.data_type() {
        DataType::Float32 | DataType::Float64 => float_value(array, row),
        DataType::UInt64 => Ok(array.as_primitive::<UInt64Type>().value(row) as f64),
        _ => Ok(integer_value(array, row)? as f64),
    }
}

fn scalar_at(array: &dyn Array, i: usize) -> Result<Scalar> {
    if array.is_null(i) {
        return Ok(Scalar::Null);
    }
    Ok(match array.data_type() {
        DataType::Boolean => Scalar::Bool(array.as_boolean().value(i)),
        DataType::Int8 => Scalar::I8(array.as_primitive::<Int8Type>().value(i)),
        DataType::Int16 => Scalar::I16(array.as_primitive::<Int16Type>().value(i)),
        DataType::Int32 => Scalar::I32(array.as_primitive::<Int32Type>().value(i)),
        DataType::Int64 => Scalar::I64(array.as_primitive::<Int64Type>().value(i)),
        DataType::UInt8 => Scalar::U8(array.as_primitive::<UInt8Type>().value(i)),
        DataType::UInt16 => Scalar::U16(array.as_primitive::<UInt16Type>().value(i)),
        DataType::UInt32 => Scalar::U32(array.as_primitive::<UInt32Type>().value(i)),
        DataType::UInt64 => Scalar::U64(array.as_primitive::<UInt64Type>().value(i)),
        DataType::Float32 => Scalar::F32(array.as_primitive::<Float32Type>().value(i).to_bits()),
        DataType::Float64 => Scalar::F64(array.as_primitive::<Float64Type>().value(i).to_bits()),
        DataType::Utf8 => Scalar::Utf8(array.as_string::<i32>().value(i).to_owned()),
        DataType::LargeUtf8 => Scalar::Utf8(array.as_string::<i64>().value(i).to_owned()),
        dt => {
            return Err(Error::Eval(format!(
                "unsupported grouping or aggregate type: {dt}"
            )));
        }
    })
}

fn cmp_scalar(a: &Scalar, b: &Scalar) -> Result<Ordering> {
    Ok(match (a, b) {
        (Scalar::Bool(x), Scalar::Bool(y)) => x.cmp(y),
        (Scalar::I8(x), Scalar::I8(y)) => x.cmp(y),
        (Scalar::I16(x), Scalar::I16(y)) => x.cmp(y),
        (Scalar::I32(x), Scalar::I32(y)) => x.cmp(y),
        (Scalar::I64(x), Scalar::I64(y)) => x.cmp(y),
        (Scalar::U8(x), Scalar::U8(y)) => x.cmp(y),
        (Scalar::U16(x), Scalar::U16(y)) => x.cmp(y),
        (Scalar::U32(x), Scalar::U32(y)) => x.cmp(y),
        (Scalar::U64(x), Scalar::U64(y)) => x.cmp(y),
        (Scalar::F32(x), Scalar::F32(y)) => f32::from_bits(*x).total_cmp(&f32::from_bits(*y)),
        (Scalar::F64(x), Scalar::F64(y)) => f64::from_bits(*x).total_cmp(&f64::from_bits(*y)),
        (Scalar::Utf8(x), Scalar::Utf8(y)) => x.cmp(y),
        _ => {
            return Err(Error::Eval(
                "MIN/MAX compared values of different types".into(),
            ));
        }
    })
}

fn build_array(values: &[Scalar], dt: &DataType) -> Result<ArrayRef> {
    Ok(match dt {
        DataType::Boolean => Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|v| match v {
                    Scalar::Null => Ok(None),
                    Scalar::Bool(b) => Ok(Some(*b)),
                    _ => Err(Error::Eval("expected boolean aggregate value".into())),
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        DataType::Int8 => Arc::new(Int8Array::from(map_i8(values)?)),
        DataType::Int16 => Arc::new(Int16Array::from(map_i16(values)?)),
        DataType::Int32 => Arc::new(Int32Array::from(map_i32(values)?)),
        DataType::Int64 => Arc::new(Int64Array::from(map_i64(values)?)),
        DataType::UInt8 => Arc::new(UInt8Array::from(map_u8(values)?)),
        DataType::UInt16 => Arc::new(UInt16Array::from(map_u16(values)?)),
        DataType::UInt32 => Arc::new(UInt32Array::from(map_u32(values)?)),
        DataType::UInt64 => Arc::new(UInt64Array::from(map_u64(values)?)),
        DataType::Float32 => Arc::new(Float32Array::from(map_f32(values)?)),
        DataType::Float64 => Arc::new(Float64Array::from(map_f64(values)?)),
        DataType::Utf8 => Arc::new(StringArray::from(map_utf8(values)?)),
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(map_utf8(values)?)),
        dt => {
            return Err(Error::Eval(format!(
                "unsupported aggregate output type: {dt}"
            )));
        }
    })
}

fn map_i8(values: &[Scalar]) -> Result<Vec<Option<i8>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::I8(x) => Ok(Some(*x)),
            other => downcast_int(other, |x| i8::try_from(x).ok()),
        })
        .collect()
}

fn map_i16(values: &[Scalar]) -> Result<Vec<Option<i16>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::I16(x) => Ok(Some(*x)),
            other => downcast_int(other, |x| i16::try_from(x).ok()),
        })
        .collect()
}

fn map_i32(values: &[Scalar]) -> Result<Vec<Option<i32>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::I32(x) => Ok(Some(*x)),
            other => downcast_int(other, |x| i32::try_from(x).ok()),
        })
        .collect()
}

fn map_i64(values: &[Scalar]) -> Result<Vec<Option<i64>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::I64(x) => Ok(Some(*x)),
            Scalar::I8(x) => Ok(Some(i64::from(*x))),
            Scalar::I16(x) => Ok(Some(i64::from(*x))),
            Scalar::I32(x) => Ok(Some(i64::from(*x))),
            _ => Err(Error::Eval("expected integer aggregate value".into())),
        })
        .collect()
}

fn map_u8(values: &[Scalar]) -> Result<Vec<Option<u8>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::U8(x) => Ok(Some(*x)),
            other => downcast_uint(other, |x| u8::try_from(x).ok()),
        })
        .collect()
}

fn map_u16(values: &[Scalar]) -> Result<Vec<Option<u16>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::U16(x) => Ok(Some(*x)),
            other => downcast_uint(other, |x| u16::try_from(x).ok()),
        })
        .collect()
}

fn map_u32(values: &[Scalar]) -> Result<Vec<Option<u32>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::U32(x) => Ok(Some(*x)),
            other => downcast_uint(other, |x| u32::try_from(x).ok()),
        })
        .collect()
}

fn map_u64(values: &[Scalar]) -> Result<Vec<Option<u64>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::U64(x) => Ok(Some(*x)),
            _ => Err(Error::Eval("expected u64 aggregate value".into())),
        })
        .collect()
}

fn map_f32(values: &[Scalar]) -> Result<Vec<Option<f32>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::F32(x) => Ok(Some(f32::from_bits(*x))),
            Scalar::F64(x) => Ok(Some(f64::from_bits(*x) as f32)),
            _ => Err(Error::Eval("expected float aggregate value".into())),
        })
        .collect()
}

fn map_f64(values: &[Scalar]) -> Result<Vec<Option<f64>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::F64(x) => Ok(Some(f64::from_bits(*x))),
            Scalar::F32(x) => Ok(Some(f64::from(f32::from_bits(*x)))),
            _ => Err(Error::Eval("expected float64 aggregate value".into())),
        })
        .collect()
}

fn map_utf8(values: &[Scalar]) -> Result<Vec<Option<&str>>> {
    values
        .iter()
        .map(|v| match v {
            Scalar::Null => Ok(None),
            Scalar::Utf8(s) => Ok(Some(s.as_str())),
            _ => Err(Error::Eval("expected utf8 aggregate value".into())),
        })
        .collect()
}

fn downcast_int<T>(v: &Scalar, f: impl Fn(i64) -> Option<T>) -> Result<Option<T>> {
    let n = match v {
        Scalar::I64(x) => *x,
        Scalar::I32(x) => i64::from(*x),
        Scalar::I16(x) => i64::from(*x),
        Scalar::I8(x) => i64::from(*x),
        _ => return Err(Error::Eval("expected integer aggregate value".into())),
    };
    f(n).map(Some)
        .ok_or_else(|| Error::Eval("integer SUM does not fit the result type".into()))
}

fn downcast_uint<T>(v: &Scalar, f: impl Fn(u64) -> Option<T>) -> Result<Option<T>> {
    let n = match v {
        Scalar::U64(x) => *x,
        Scalar::U32(x) => u64::from(*x),
        Scalar::U16(x) => u64::from(*x),
        Scalar::U8(x) => u64::from(*x),
        _ => return Err(Error::Eval("expected unsigned aggregate value".into())),
    };
    f(n).map(Some)
        .ok_or_else(|| Error::Eval("unsigned SUM does not fit the result type".into()))
}
