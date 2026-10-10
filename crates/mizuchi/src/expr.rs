//! Scalar expressions evaluated column-at-a-time with Arrow compute kernels.
//!
//! The tree is the engine's analogue of a bound expression: columns (by name or
//! ordinal), literals, arithmetic, comparison, boolean, cast, CASE, null
//! handling, and basic string functions. Evaluation never walks rows in the
//! engine; kernels operate on whole arrays.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, StringArray, UInt32Array, new_null_array,
};
use arrow::compute::kernels::boolean::{and_kleene, is_not_null, is_null, not, or_kleene};
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::compute::kernels::concat_elements::concat_elements_utf8_many;
use arrow::compute::kernels::numeric::{add, div, mul, neg, sub};
use arrow::compute::kernels::substring::substring;
use arrow::compute::kernels::zip::zip;
use arrow::compute::{cast, take};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;

use crate::batch::Batch;
use crate::error::{Error, Result};

/// Binary operator in a scalar expression.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

/// Unary operator in a scalar expression.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
    IsNull,
    IsNotNull,
    Upper,
    Lower,
}

/// Scalar expression tree.
#[derive(Clone, Debug)]
pub enum Expr {
    /// Column by field name.
    Column(String),
    /// Column by ordinal, after name resolution.
    Nth(usize),
    /// Literal stored as a length-1 Arrow array.
    Literal(ArrayRef),
    Binary(BinaryOp, Box<Expr>, Box<Expr>),
    Unary(UnaryOp, Box<Expr>),
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    /// `CASE [operand] WHEN ... THEN ... [ELSE ...] END`.
    Case {
        operand: Option<Box<Expr>>,
        when_then: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    /// First non-null argument.
    Coalesce(Vec<Expr>),
    /// Element-wise UTF-8 concatenation.
    Concat(Vec<Expr>),
    /// SQL `substring(expr FROM start [FOR length])` with 1-based `start`.
    Substring {
        expr: Box<Expr>,
        start: i64,
        length: Option<u64>,
    },
}

impl Expr {
    /// Named column.
    pub fn column(name: impl Into<String>) -> Self {
        Self::Column(name.into())
    }

    /// Bound column ordinal.
    pub fn nth(index: usize) -> Self {
        Self::Nth(index)
    }

    /// Boolean literal.
    pub fn lit_bool(value: bool) -> Self {
        Self::Literal(Arc::new(BooleanArray::from(vec![value])))
    }

    /// 64-bit integer literal.
    pub fn lit_i64(value: i64) -> Self {
        Self::Literal(Arc::new(Int64Array::from(vec![value])))
    }

    /// UTF-8 literal.
    pub fn lit_utf8(value: impl Into<String>) -> Self {
        Self::Literal(Arc::new(StringArray::from(vec![value.into()])))
    }

    /// Typed null literal.
    pub fn lit_null(data_type: DataType) -> Self {
        Self::Literal(new_null_array(&data_type, 1))
    }

    /// Evaluate this expression on `batch`, returning one array of `batch.num_rows()`.
    pub fn eval(&self, batch: &Batch) -> Result<ArrayRef> {
        eval_expr(self, batch)
    }
}

fn eval_expr(expr: &Expr, batch: &Batch) -> Result<ArrayRef> {
    match expr {
        Expr::Column(name) => batch
            .column_by_name(name)
            .cloned()
            .ok_or_else(|| Error::Eval(format!("column {name} not found"))),
        Expr::Nth(index) => {
            if *index >= batch.num_columns() {
                return Err(Error::Eval(format!("column index {index} out of range")));
            }
            Ok(batch.column(*index).clone())
        }
        Expr::Literal(value) => broadcast(value, batch.num_rows()),
        Expr::Binary(op, left, right) => {
            let left = eval_expr(left, batch)?;
            let right = eval_expr(right, batch)?;
            eval_binary(*op, &left, &right)
        }
        Expr::Unary(op, child) => {
            let child = eval_expr(child, batch)?;
            eval_unary(*op, &child)
        }
        Expr::Cast { expr, data_type } => {
            let child = eval_expr(expr, batch)?;
            cast(child.as_ref(), data_type).map_err(eval_err)
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => eval_case(batch, operand.as_deref(), when_then, else_expr.as_deref()),
        Expr::Coalesce(args) => eval_coalesce(batch, args),
        Expr::Concat(args) => eval_concat(batch, args),
        Expr::Substring {
            expr,
            start,
            length,
        } => {
            let child = eval_expr(expr, batch)?;
            let arrow_start = if *start >= 1 { *start - 1 } else { *start };
            substring(child.as_ref(), arrow_start, *length).map_err(eval_err)
        }
    }
}

fn eval_binary(op: BinaryOp, left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
    match op {
        BinaryOp::Add => add(&left, &right).map_err(eval_err),
        BinaryOp::Sub => sub(&left, &right).map_err(eval_err),
        BinaryOp::Mul => mul(&left, &right).map_err(eval_err),
        BinaryOp::Div => div(&left, &right).map_err(eval_err),
        BinaryOp::Eq => cmp_array(eq(left, right)),
        BinaryOp::NotEq => cmp_array(neq(left, right)),
        BinaryOp::Lt => cmp_array(lt(left, right)),
        BinaryOp::LtEq => cmp_array(lt_eq(left, right)),
        BinaryOp::Gt => cmp_array(gt(left, right)),
        BinaryOp::GtEq => cmp_array(gt_eq(left, right)),
        BinaryOp::And => {
            let out = and_kleene(as_bool(left)?, as_bool(right)?).map_err(eval_err)?;
            Ok(Arc::new(out))
        }
        BinaryOp::Or => {
            let out = or_kleene(as_bool(left)?, as_bool(right)?).map_err(eval_err)?;
            Ok(Arc::new(out))
        }
    }
}

fn eval_unary(op: UnaryOp, child: &ArrayRef) -> Result<ArrayRef> {
    match op {
        UnaryOp::Not => Ok(Arc::new(not(as_bool(child)?).map_err(eval_err)?)),
        UnaryOp::Neg => neg(child.as_ref()).map_err(eval_err),
        UnaryOp::IsNull => Ok(Arc::new(is_null(child.as_ref()).map_err(eval_err)?)),
        UnaryOp::IsNotNull => Ok(Arc::new(is_not_null(child.as_ref()).map_err(eval_err)?)),
        UnaryOp::Upper => map_utf8(child, str::to_uppercase),
        UnaryOp::Lower => map_utf8(child, str::to_lowercase),
    }
}

fn eval_case(
    batch: &Batch,
    operand: Option<&Expr>,
    when_then: &[(Expr, Expr)],
    else_expr: Option<&Expr>,
) -> Result<ArrayRef> {
    if when_then.is_empty() {
        return Err(Error::Eval("CASE requires at least one WHEN".into()));
    }
    let operand = match operand {
        Some(expr) => Some(eval_expr(expr, batch)?),
        None => None,
    };
    let mut else_array = match else_expr {
        Some(expr) => eval_expr(expr, batch)?,
        None => new_null_array(when_then[0].1.eval(batch)?.data_type(), batch.num_rows()),
    };
    for (when, then) in when_then.iter().rev() {
        let when_array = eval_expr(when, batch)?;
        let mask = match &operand {
            Some(op) => Arc::new(eq(op, &when_array).map_err(eval_err)?) as ArrayRef,
            None => when_array,
        };
        let then_array = eval_expr(then, batch)?;
        else_array = zip(&predicate(&mask)?, &then_array, &else_array).map_err(eval_err)?;
    }
    Ok(else_array)
}

fn eval_coalesce(batch: &Batch, args: &[Expr]) -> Result<ArrayRef> {
    if args.is_empty() {
        return Err(Error::Eval(
            "coalesce requires at least one argument".into(),
        ));
    }
    let mut out = eval_expr(&args[args.len() - 1], batch)?;
    for arg in args[..args.len() - 1].iter().rev() {
        let next = eval_expr(arg, batch)?;
        let mask = is_not_null(next.as_ref()).map_err(eval_err)?;
        out = zip(&mask, &next, &out).map_err(eval_err)?;
    }
    Ok(out)
}

fn eval_concat(batch: &Batch, args: &[Expr]) -> Result<ArrayRef> {
    if args.len() < 2 {
        return Err(Error::Eval("concat requires at least two arguments".into()));
    }
    let mut arrays = Vec::with_capacity(args.len());
    for arg in args {
        arrays.push(eval_expr(arg, batch)?);
    }
    let mut refs: Vec<&StringArray> = Vec::with_capacity(arrays.len());
    for array in &arrays {
        refs.push(as_utf8(array)?);
    }
    let out = concat_elements_utf8_many(&refs).map_err(eval_err)?;
    Ok(Arc::new(out))
}

fn broadcast(value: &ArrayRef, rows: usize) -> Result<ArrayRef> {
    if value.len() != 1 {
        return Err(Error::Eval("literal must be a single-row array".into()));
    }
    if rows == 1 {
        return Ok(value.clone());
    }
    if rows == 0 {
        return Ok(value.slice(0, 0));
    }
    let indices = UInt32Array::from(vec![0; rows]);
    take(value.as_ref(), &indices, None).map_err(eval_err)
}

fn predicate(mask: &ArrayRef) -> Result<BooleanArray> {
    let mask = as_bool(mask)?;
    let not_null = is_not_null(mask).map_err(eval_err)?;
    let bits = mask.values() & not_null.values();
    Ok(BooleanArray::new(bits, None))
}

fn as_bool(array: &ArrayRef) -> Result<&BooleanArray> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| Error::Eval(format!("expected Boolean, got {}", array.data_type())))
}

fn as_utf8(array: &ArrayRef) -> Result<&StringArray> {
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Eval(format!("expected Utf8, got {}", array.data_type())))
}

fn map_utf8(array: &ArrayRef, f: impl Fn(&str) -> String) -> Result<ArrayRef> {
    let strings = as_utf8(array)?;
    let out: StringArray = strings.iter().map(|v| v.map(&f)).collect();
    Ok(Arc::new(out))
}

fn cmp_array(result: std::result::Result<BooleanArray, ArrowError>) -> Result<ArrayRef> {
    Ok(Arc::new(result.map_err(eval_err)?))
}

fn eval_err(err: ArrowError) -> Error {
    Error::Eval(err.to_string())
}

#[cfg(test)]
mod tests {
    use arrow::array::{AsArray, Int64Array};
    use arrow::datatypes::{Field, Int64Type, Schema};

    use super::*;

    fn batch_i64_utf8(ids: Vec<Option<i64>>, names: Vec<Option<&str>>) -> Batch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        Batch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    fn i64_values(array: &ArrayRef) -> Vec<Option<i64>> {
        array.as_primitive::<Int64Type>().iter().collect()
    }

    fn bool_values(array: &ArrayRef) -> Vec<Option<bool>> {
        array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect()
    }

    fn utf8_values(array: &ArrayRef) -> Vec<Option<String>> {
        array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect()
    }

    #[test]
    fn column_and_literal_including_nulls() {
        let batch = batch_i64_utf8(
            vec![Some(1), None, Some(3)],
            vec![Some("a"), None, Some("c")],
        );
        assert_eq!(
            i64_values(&Expr::column("id").eval(&batch).unwrap()),
            vec![Some(1), None, Some(3)]
        );
        assert_eq!(
            i64_values(&Expr::nth(0).eval(&batch).unwrap()),
            vec![Some(1), None, Some(3)]
        );
        assert_eq!(
            i64_values(&Expr::lit_i64(7).eval(&batch).unwrap()),
            vec![Some(7), Some(7), Some(7)]
        );
        assert_eq!(
            i64_values(&Expr::lit_null(DataType::Int64).eval(&batch).unwrap()),
            vec![None, None, None]
        );
    }

    #[test]
    fn arithmetic_propagates_nulls() {
        let batch = batch_i64_utf8(vec![Some(10), None, Some(3)], vec![None, None, None]);
        let add = Expr::Binary(
            BinaryOp::Add,
            Box::new(Expr::column("id")),
            Box::new(Expr::lit_i64(2)),
        );
        assert_eq!(
            i64_values(&add.eval(&batch).unwrap()),
            vec![Some(12), None, Some(5)]
        );
        let neg = Expr::Unary(UnaryOp::Neg, Box::new(Expr::column("id")));
        assert_eq!(
            i64_values(&neg.eval(&batch).unwrap()),
            vec![Some(-10), None, Some(-3)]
        );
    }

    #[test]
    fn comparison_and_boolean_use_sql_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
            Field::new("n", DataType::Int64, true),
        ]));
        let batch = Batch::try_new(
            schema,
            vec![
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
                Arc::new(BooleanArray::from(vec![None, Some(true), Some(false)])),
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
            ],
        )
        .unwrap();
        let eq = Expr::Binary(
            BinaryOp::Eq,
            Box::new(Expr::column("n")),
            Box::new(Expr::lit_i64(1)),
        );
        assert_eq!(
            bool_values(&eq.eval(&batch).unwrap()),
            vec![Some(true), None, Some(false)]
        );
        let and = Expr::Binary(
            BinaryOp::And,
            Box::new(Expr::column("a")),
            Box::new(Expr::column("b")),
        );
        // Kleene: true AND null = null; false AND true = false; null AND false = false
        assert_eq!(
            bool_values(&and.eval(&batch).unwrap()),
            vec![None, Some(false), Some(false)]
        );
        let is_null = Expr::Unary(UnaryOp::IsNull, Box::new(Expr::column("n")));
        assert_eq!(
            bool_values(&is_null.eval(&batch).unwrap()),
            vec![Some(false), Some(true), Some(false)]
        );
    }

    #[test]
    fn cast_and_case_and_coalesce() {
        let batch = batch_i64_utf8(
            vec![Some(1), None, Some(3)],
            vec![Some("x"), None, Some("z")],
        );
        let casted = Expr::Cast {
            expr: Box::new(Expr::column("id")),
            data_type: DataType::Utf8,
        };
        assert_eq!(
            utf8_values(&casted.eval(&batch).unwrap()),
            vec![Some("1".into()), None, Some("3".into())]
        );

        let case = Expr::Case {
            operand: None,
            when_then: vec![(
                Expr::Binary(
                    BinaryOp::Gt,
                    Box::new(Expr::column("id")),
                    Box::new(Expr::lit_i64(2)),
                ),
                Expr::lit_utf8("big"),
            )],
            else_expr: Some(Box::new(Expr::lit_utf8("small"))),
        };
        assert_eq!(
            utf8_values(&case.eval(&batch).unwrap()),
            vec![
                Some("small".into()),
                Some("small".into()),
                Some("big".into())
            ]
        );

        let simple_case = Expr::Case {
            operand: Some(Box::new(Expr::column("id"))),
            when_then: vec![(Expr::lit_i64(3), Expr::lit_utf8("three"))],
            else_expr: Some(Box::new(Expr::lit_utf8("other"))),
        };
        assert_eq!(
            utf8_values(&simple_case.eval(&batch).unwrap()),
            vec![
                Some("other".into()),
                Some("other".into()),
                Some("three".into())
            ]
        );

        let coalesced = Expr::Coalesce(vec![Expr::column("id"), Expr::lit_i64(99)]);
        assert_eq!(
            i64_values(&coalesced.eval(&batch).unwrap()),
            vec![Some(1), Some(99), Some(3)]
        );
    }

    #[test]
    fn string_basics_including_nulls() {
        let batch = batch_i64_utf8(
            vec![Some(1), Some(2), Some(3)],
            vec![Some("Ab"), None, Some("Cd")],
        );
        let upper = Expr::Unary(UnaryOp::Upper, Box::new(Expr::column("name")));
        assert_eq!(
            utf8_values(&upper.eval(&batch).unwrap()),
            vec![Some("AB".into()), None, Some("CD".into())]
        );
        let concat = Expr::Concat(vec![Expr::column("name"), Expr::lit_utf8("!")]);
        assert_eq!(
            utf8_values(&concat.eval(&batch).unwrap()),
            vec![Some("Ab!".into()), None, Some("Cd!".into())]
        );
        let sub = Expr::Substring {
            expr: Box::new(Expr::column("name")),
            start: 2,
            length: Some(1),
        };
        assert_eq!(
            utf8_values(&sub.eval(&batch).unwrap()),
            vec![Some("b".into()), None, Some("d".into())]
        );
    }

    #[test]
    fn evaluates_one_million_rows_with_kernels() {
        const N: i64 = 1_000_000;
        let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..N));
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = Batch::try_new(schema, vec![ids]).unwrap();
        let expr = Expr::Binary(
            BinaryOp::Add,
            Box::new(Expr::column("id")),
            Box::new(Expr::lit_i64(1)),
        );
        let out = expr.eval(&batch).unwrap();
        let arr = out.as_primitive::<Int64Type>();
        assert_eq!(arr.len(), N as usize);
        assert_eq!(arr.value(0), 1);
        assert_eq!(arr.value((N as usize) - 1), N);
    }
}
