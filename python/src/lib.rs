//! Lazy DataFrame API over mizuchi plans, exposed to Python via PyO3.
//!
//! Expressions are SQL fragments bound by the engine. collect() runs the
//! composed query and returns rows as tuples.

use std::sync::{Arc, Mutex};

use ::mizuchi::{ArrayRef, Batch, Error as EngineError, Session, VERSION};
use arrow::array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeStringArray, StringArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use pyo3::BoundObject;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyInt, PyList, PyTuple};

fn py_err(e: EngineError) -> PyErr {
    PyValueError::new_err(e.to_string())
}

fn lock_session(session: &Mutex<Session>) -> PyResult<std::sync::MutexGuard<'_, Session>> {
    session
        .lock()
        .map_err(|_| PyValueError::new_err("session lock poisoned"))
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[pyclass(name = "Session")]
struct PySession {
    inner: Arc<Mutex<Session>>,
}

#[pymethods]
impl PySession {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Session::new())),
        }
    }

    /// Register an in-memory table from a dict of column name -> list of values.
    fn register(&self, name: &str, columns: Bound<'_, PyDict>) -> PyResult<()> {
        let batch = dict_to_batch(&columns)?;
        let schema = batch.schema();
        lock_session(&self.inner)?
            .register(name, schema, vec![batch])
            .map_err(py_err)
    }

    fn table(&self, name: &str) -> PyDataFrame {
        let ident = quote_ident(name);
        PyDataFrame {
            session: self.inner.clone(),
            sql: format!("SELECT * FROM {ident}"),
            n: 0,
        }
    }

    fn sql(&self, sql: &str) -> PyDataFrame {
        PyDataFrame {
            session: self.inner.clone(),
            sql: sql.to_string(),
            n: 0,
        }
    }
}

#[pyclass(name = "DataFrame")]
#[derive(Clone)]
struct PyDataFrame {
    session: Arc<Mutex<Session>>,
    sql: String,
    n: u32,
}

impl PyDataFrame {
    fn wrap(&self, sql: String) -> Self {
        Self {
            session: self.session.clone(),
            sql,
            n: self.n + 1,
        }
    }

    fn alias(&self) -> String {
        format!("_m{}", self.n)
    }
}

#[pymethods]
impl PyDataFrame {
    #[pyo3(signature = (*cols))]
    fn select(&self, cols: Vec<String>) -> PyResult<Self> {
        if cols.is_empty() {
            return Err(PyValueError::new_err("select requires at least one column"));
        }
        let list = cols.join(", ");
        let a = self.alias();
        Ok(self.wrap(format!("SELECT {list} FROM ({}) AS {a}", self.sql)))
    }

    fn filter(&self, pred: &str) -> Self {
        let a = self.alias();
        self.wrap(format!("SELECT * FROM ({}) AS {a} WHERE {pred}", self.sql))
    }

    /// Join `other` with SQL predicate `on`. The sides are in scope as `l` and `r`.
    fn join(&self, other: PyRef<'_, PyDataFrame>, on: &str) -> PyResult<Self> {
        if !Arc::ptr_eq(&self.session, &other.session) {
            return Err(PyValueError::new_err(
                "join requires DataFrames from the same Session",
            ));
        }
        Ok(self.wrap(format!(
            "SELECT * FROM ({}) AS l JOIN ({}) AS r ON {on}",
            self.sql, other.sql
        )))
    }

    #[pyo3(signature = (*keys))]
    fn sort(&self, keys: Vec<String>) -> PyResult<Self> {
        if keys.is_empty() {
            return Err(PyValueError::new_err("sort requires at least one key"));
        }
        let list = keys.join(", ");
        let a = self.alias();
        Ok(self.wrap(format!(
            "SELECT * FROM ({}) AS {a} ORDER BY {list}",
            self.sql
        )))
    }

    #[pyo3(signature = (*keys))]
    fn group_by(&self, keys: Vec<String>) -> PyResult<PyGrouped> {
        if keys.is_empty() {
            return Err(PyValueError::new_err("group_by requires at least one key"));
        }
        Ok(PyGrouped {
            df: self.clone(),
            keys,
        })
    }

    fn collect(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let session = lock_session(&self.session)?;
        let batches = session.execute_sql(&self.sql).map_err(py_err)?;
        rows_to_pylist(py, &batches)
    }

    fn explain(&self) -> PyResult<String> {
        let session = lock_session(&self.session)?;
        session.explain_stages(&self.sql).map_err(py_err)
    }

    fn __repr__(&self) -> String {
        format!("DataFrame[{}]", self.sql)
    }
}

#[pyclass(name = "Grouped")]
struct PyGrouped {
    df: PyDataFrame,
    keys: Vec<String>,
}

#[pymethods]
impl PyGrouped {
    #[pyo3(signature = (*aggs))]
    fn agg(&self, aggs: Vec<String>) -> PyResult<PyDataFrame> {
        if aggs.is_empty() {
            return Err(PyValueError::new_err(
                "agg requires at least one expression",
            ));
        }
        let keys = self.keys.join(", ");
        let aggs = aggs.join(", ");
        let a = self.df.alias();
        Ok(self.df.wrap(format!(
            "SELECT {keys}, {aggs} FROM ({}) AS {a} GROUP BY {keys}",
            self.df.sql
        )))
    }
}

fn dict_to_batch(columns: &Bound<'_, PyDict>) -> PyResult<Batch> {
    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    let mut nrows: Option<usize> = None;
    for (k, v) in columns.iter() {
        let name: String = k.extract()?;
        let seq = v.downcast::<PyList>()?;
        let list: Vec<Option<Bound<PyAny>>> = seq
            .iter()
            .map(|item| if item.is_none() { None } else { Some(item) })
            .collect();
        let n = list.len();
        if let Some(expect) = nrows {
            if n != expect {
                return Err(PyValueError::new_err("column lengths must match"));
            }
        } else {
            nrows = Some(n);
        }
        let (field, arr) = column_to_array(&name, &list)?;
        fields.push(field);
        arrays.push(arr);
    }
    if fields.is_empty() {
        return Err(PyValueError::new_err(
            "register requires at least one column",
        ));
    }
    let schema = std::sync::Arc::new(Schema::new(fields));
    Batch::try_new(schema, arrays).map_err(py_err)
}

fn column_to_array(name: &str, list: &[Option<Bound<PyAny>>]) -> PyResult<(Field, ArrayRef)> {
    let mut kind = ColKind::Empty;
    for cell in list {
        let Some(v) = cell else { continue };
        if v.is_instance_of::<PyBool>() {
            kind = ColKind::Bool;
            break;
        }
        if v.is_instance_of::<PyInt>() {
            if v.extract::<i64>().is_err() {
                return Err(PyValueError::new_err(format!(
                    "integer in column {name} exceeds Int64 range"
                )));
            }
            kind = ColKind::I64;
            break;
        }
        if v.extract::<f64>().is_ok() {
            kind = ColKind::F64;
            break;
        }
        if v.extract::<String>().is_ok() {
            kind = ColKind::Utf8;
            break;
        }
        return Err(PyValueError::new_err(format!(
            "unsupported value in column {name}"
        )));
    }
    match kind {
        ColKind::Empty => Err(PyValueError::new_err(format!(
            "cannot infer type of all-null column {name}"
        ))),
        ColKind::I64 => {
            let vals: Vec<Option<i64>> = list
                .iter()
                .map(|c| match c {
                    None => Ok(None),
                    Some(v) => v.extract().map(Some),
                })
                .collect::<PyResult<_>>()?;
            Ok((
                Field::new(name, DataType::Int64, true),
                std::sync::Arc::new(Int64Array::from(vals)),
            ))
        }
        ColKind::Bool => {
            let vals: Vec<Option<bool>> = list
                .iter()
                .map(|c| match c {
                    None => Ok(None),
                    Some(v) => v.extract().map(Some),
                })
                .collect::<PyResult<_>>()?;
            Ok((
                Field::new(name, DataType::Boolean, true),
                std::sync::Arc::new(BooleanArray::from(vals)),
            ))
        }
        ColKind::F64 => {
            let vals: Vec<Option<f64>> = list
                .iter()
                .map(|c| match c {
                    None => Ok(None),
                    Some(v) => v.extract().map(Some),
                })
                .collect::<PyResult<_>>()?;
            Ok((
                Field::new(name, DataType::Float64, true),
                std::sync::Arc::new(Float64Array::from(vals)),
            ))
        }
        ColKind::Utf8 => {
            let vals: Vec<Option<String>> = list
                .iter()
                .map(|c| match c {
                    None => Ok(None),
                    Some(v) => v.extract().map(Some),
                })
                .collect::<PyResult<_>>()?;
            Ok((
                Field::new(name, DataType::Utf8, true),
                std::sync::Arc::new(StringArray::from(vals)),
            ))
        }
    }
}

enum ColKind {
    Empty,
    Bool,
    I64,
    F64,
    Utf8,
}

fn rows_to_pylist(py: Python<'_>, batches: &[Batch]) -> PyResult<Py<PyList>> {
    let list = PyList::empty(py);
    for batch in batches {
        for r in 0..batch.num_rows() {
            let mut cells: Vec<Py<PyAny>> = Vec::with_capacity(batch.num_columns());
            for c in 0..batch.num_columns() {
                cells.push(cell_to_py(py, batch.column(c), r)?);
            }
            list.append(PyTuple::new(py, cells)?)?;
        }
    }
    Ok(list.unbind())
}

fn downcast<'a, T: 'static>(arr: &'a ArrayRef, what: &str) -> PyResult<&'a T> {
    arr.as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| PyValueError::new_err(format!("expected {what} array")))
}

fn cell_to_py(py: Python<'_>, arr: &ArrayRef, row: usize) -> PyResult<Py<PyAny>> {
    if arr.is_null(row) {
        return Ok(py.None());
    }
    match arr.data_type() {
        DataType::Null => Ok(py.None()),
        DataType::Boolean => to_py(py, downcast::<BooleanArray>(arr, "Boolean")?.value(row)),
        DataType::Int8 => to_py(
            py,
            i64::from(downcast::<Int8Array>(arr, "Int8")?.value(row)),
        ),
        DataType::Int16 => to_py(
            py,
            i64::from(downcast::<Int16Array>(arr, "Int16")?.value(row)),
        ),
        DataType::Int32 => to_py(
            py,
            i64::from(downcast::<Int32Array>(arr, "Int32")?.value(row)),
        ),
        DataType::Int64 => to_py(py, downcast::<Int64Array>(arr, "Int64")?.value(row)),
        DataType::UInt8 => to_py(
            py,
            u64::from(downcast::<UInt8Array>(arr, "UInt8")?.value(row)),
        ),
        DataType::UInt16 => to_py(
            py,
            u64::from(downcast::<UInt16Array>(arr, "UInt16")?.value(row)),
        ),
        DataType::UInt32 => to_py(
            py,
            u64::from(downcast::<UInt32Array>(arr, "UInt32")?.value(row)),
        ),
        DataType::UInt64 => to_py(py, downcast::<UInt64Array>(arr, "UInt64")?.value(row)),
        DataType::Float32 => to_py(
            py,
            f64::from(downcast::<Float32Array>(arr, "Float32")?.value(row)),
        ),
        DataType::Float64 => to_py(py, downcast::<Float64Array>(arr, "Float64")?.value(row)),
        DataType::Utf8 => to_py(py, downcast::<StringArray>(arr, "Utf8")?.value(row)),
        DataType::LargeUtf8 => to_py(
            py,
            downcast::<LargeStringArray>(arr, "LargeUtf8")?.value(row),
        ),
        DataType::Binary => to_py(py, downcast::<BinaryArray>(arr, "Binary")?.value(row)),
        other => Err(PyValueError::new_err(format!(
            "unsupported result type {other}"
        ))),
    }
}

fn to_py<'py, T>(py: Python<'py>, v: T) -> PyResult<Py<PyAny>>
where
    T: IntoPyObject<'py>,
    PyErr: From<T::Error>,
{
    Ok(v.into_pyobject(py)?.into_any().unbind())
}

#[pymodule]
fn mizuchi(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySession>()?;
    m.add_class::<PyDataFrame>()?;
    m.add_class::<PyGrouped>()?;
    m.add("version", VERSION)?;
    Ok(())
}
