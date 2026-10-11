//! Lazy DataFrame API over mizuchi plans, exposed to Python via PyO3.
//!
//! Expressions are SQL fragments bound by the engine. collect() runs the
//! composed query and returns rows as tuples. Vectorized Python UDFs and
//! Arrow C Data Interface interchange live in this crate.

mod arrow_c;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use ::mizuchi::{ArrayRef, Batch, CombineAgg, Error as EngineError, Session, VERSION};
use arrow::array::{
    Array, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeListArray, LargeStringArray, ListArray, StringArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
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

    /// Register a table from an Arrow C Data Interface producer
    /// (pyarrow, Polars, pandas with Arrow dtypes, or any `__arrow_c_stream__`).
    fn register_arrow(&self, name: &str, obj: Bound<'_, PyAny>) -> PyResult<()> {
        let batches = arrow_c::import_batches(&obj)?;
        if batches.is_empty() {
            return Err(PyValueError::new_err(
                "register_arrow requires at least one batch",
            ));
        }
        let schema = batches[0].schema();
        lock_session(&self.inner)?
            .register(name, schema, batches)
            .map_err(py_err)
    }

    /// Register a combinable SQL aggregate. `update(state, value)` folds one
    /// input; `merge(left, right)` folds two buffers. `identity` is the
    /// empty-group buffer (None is a typed null Int64).
    fn register_udaf(
        &self,
        name: &str,
        update: Bound<'_, PyAny>,
        merge: Bound<'_, PyAny>,
        identity: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let (identity, result_type) = py_identity(&identity)?;
        let agg = PyCombineAgg {
            name: name.to_string(),
            update: update.unbind(),
            merge: merge.unbind(),
            identity,
            result_type,
        };
        lock_session(&self.inner)?
            .register_udaf(Arc::new(agg))
            .map_err(py_err)
    }
}

struct PyCombineAgg {
    name: String,
    update: Py<PyAny>,
    merge: Py<PyAny>,
    identity: ArrayRef,
    result_type: DataType,
}

impl std::fmt::Debug for PyCombineAgg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyCombineAgg")
            .field("name", &self.name)
            .finish()
    }
}

impl CombineAgg for PyCombineAgg {
    fn name(&self) -> &str {
        &self.name
    }

    fn result_type(&self) -> DataType {
        self.result_type.clone()
    }

    fn identity(&self) -> ArrayRef {
        self.identity.clone()
    }

    fn update(
        &self,
        state: &dyn Array,
        value: &dyn Array,
    ) -> std::result::Result<ArrayRef, EngineError> {
        self.call(&self.update, state, value)
    }

    fn merge(
        &self,
        left: &dyn Array,
        right: &dyn Array,
    ) -> std::result::Result<ArrayRef, EngineError> {
        self.call(&self.merge, left, right)
    }
}

impl PyCombineAgg {
    fn call(
        &self,
        func: &Py<PyAny>,
        left: &dyn Array,
        right: &dyn Array,
    ) -> std::result::Result<ArrayRef, EngineError> {
        Python::with_gil(|py| {
            let l = array0_to_py(py, left).map_err(|e| EngineError::Eval(e.to_string()))?;
            let r = array0_to_py(py, right).map_err(|e| EngineError::Eval(e.to_string()))?;
            let out = func
                .call1(py, (l, r))
                .map_err(|e| EngineError::Eval(e.to_string()))?;
            py_to_array0(py, out.bind(py), &self.result_type)
                .map_err(|e| EngineError::Eval(e.to_string()))
        })
    }
}

fn py_identity(value: &Bound<'_, PyAny>) -> PyResult<(ArrayRef, DataType)> {
    if value.is_none() {
        let dt = DataType::Int64;
        return Ok((Arc::new(Int64Array::from(vec![None::<i64>])), dt));
    }
    if value.is_instance_of::<PyBool>() {
        let v: bool = value.extract()?;
        return Ok((
            Arc::new(BooleanArray::from(vec![Some(v)])),
            DataType::Boolean,
        ));
    }
    if value.is_instance_of::<PyInt>() {
        let v: i64 = value.extract()?;
        return Ok((Arc::new(Int64Array::from(vec![Some(v)])), DataType::Int64));
    }
    if let Ok(v) = value.extract::<f64>() {
        return Ok((
            Arc::new(Float64Array::from(vec![Some(v)])),
            DataType::Float64,
        ));
    }
    if let Ok(v) = value.extract::<String>() {
        return Ok((Arc::new(StringArray::from(vec![Some(v)])), DataType::Utf8));
    }
    Err(PyValueError::new_err(
        "udaf identity must be None, bool, int, float, or str",
    ))
}

fn array0_to_py(py: Python<'_>, array: &dyn Array) -> PyResult<Py<PyAny>> {
    if array.len() != 1 {
        return Err(PyValueError::new_err("UDAF buffer must have length 1"));
    }
    let owned: ArrayRef = array.slice(0, 1);
    cell_to_py(py, &owned, 0)
}

fn py_to_array0(_py: Python<'_>, value: &Bound<'_, PyAny>, dt: &DataType) -> PyResult<ArrayRef> {
    if value.is_none() {
        return Ok(match dt {
            DataType::Boolean => Arc::new(BooleanArray::from(vec![None::<bool>])),
            DataType::Int64 => Arc::new(Int64Array::from(vec![None::<i64>])),
            DataType::Float64 => Arc::new(Float64Array::from(vec![None::<f64>])),
            DataType::Utf8 => Arc::new(StringArray::from(vec![None::<&str>])),
            other => {
                return Err(PyValueError::new_err(format!(
                    "unsupported UDAF type {other}"
                )));
            }
        });
    }
    Ok(match dt {
        DataType::Boolean => {
            let v: bool = value.extract()?;
            Arc::new(BooleanArray::from(vec![Some(v)]))
        }
        DataType::Int64 => {
            let v: i64 = value.extract()?;
            Arc::new(Int64Array::from(vec![Some(v)]))
        }
        DataType::Float64 => {
            let v: f64 = value.extract()?;
            Arc::new(Float64Array::from(vec![Some(v)]))
        }
        DataType::Utf8 => {
            let v: String = value.extract()?;
            Arc::new(StringArray::from(vec![Some(v)]))
        }
        other => {
            return Err(PyValueError::new_err(format!(
                "unsupported UDAF type {other}"
            )));
        }
    })
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

    /// Export result batches as an Arrow C stream (zero-copy capsules).
    fn to_arrow(&self) -> PyResult<PyArrowStream> {
        let session = lock_session(&self.session)?;
        let batches = session.execute_sql(&self.sql).map_err(py_err)?;
        let (schema, rbs) = batches_to_stream_parts(batches)?;
        Ok(PyArrowStream {
            schema,
            batches: rbs,
        })
    }

    /// Vectorized scalar UDF: `func` takes one pyarrow.Array per column and
    /// returns an array of the same length. Nulls are passed through.
    #[pyo3(signature = (name, func, *cols))]
    fn udf(
        &self,
        py: Python<'_>,
        name: &str,
        func: Bound<'_, PyAny>,
        cols: Vec<String>,
    ) -> PyResult<Self> {
        if cols.is_empty() {
            return Err(PyValueError::new_err("udf requires at least one column"));
        }
        let mut session = lock_session(&self.session)?;
        let batches = session.execute_sql(&self.sql).map_err(py_err)?;
        let out = apply_scalar_udf(py, &batches, name, &func, &cols)?;
        let table = format!("_mudf{}", self.n);
        let schema = out
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| PyValueError::new_err("udf produced no batches"))?;
        session.register(&table, schema, out).map_err(py_err)?;
        Ok(self.wrap(format!("SELECT * FROM {}", quote_ident(&table))))
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

    /// Vectorized aggregate UDF: `func` takes one pyarrow.Array per column for
    /// the group and returns a scalar (or a length-1 array).
    #[pyo3(signature = (name, func, *cols))]
    fn agg_udf(
        &self,
        py: Python<'_>,
        name: &str,
        func: Bound<'_, PyAny>,
        cols: Vec<String>,
    ) -> PyResult<PyDataFrame> {
        if cols.is_empty() {
            return Err(PyValueError::new_err(
                "agg_udf requires at least one column",
            ));
        }
        let mut session = lock_session(&self.df.session)?;
        let batches = session.execute_sql(&self.df.sql).map_err(py_err)?;
        let out = apply_agg_udf(py, &batches, &self.keys, name, &func, &cols)?;
        let table = format!("_magg{}", self.df.n);
        let schema = out
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| PyValueError::new_err("agg_udf produced no batches"))?;
        session.register(&table, schema, out).map_err(py_err)?;
        Ok(self
            .df
            .wrap(format!("SELECT * FROM {}", quote_ident(&table))))
    }
}

/// Arrow C stream producer. pyarrow, Polars, and pandas consume this via
/// `__arrow_c_stream__`.
#[pyclass(name = "ArrowStream")]
struct PyArrowStream {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

#[pymethods]
impl PyArrowStream {
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, pyo3::types::PyCapsule>> {
        let _ = requested_schema;
        arrow_c::export_stream(py, self.schema.clone(), self.batches.clone())
    }
}

fn batches_to_stream_parts(batches: Vec<Batch>) -> PyResult<(SchemaRef, Vec<RecordBatch>)> {
    if batches.is_empty() {
        return Ok((Arc::new(Schema::empty()), Vec::new()));
    }
    let schema = batches[0].schema();
    let rbs = batches.into_iter().map(Batch::into_arrow).collect();
    Ok((schema, rbs))
}

fn apply_scalar_udf(
    py: Python<'_>,
    batches: &[Batch],
    out_name: &str,
    func: &Bound<'_, PyAny>,
    cols: &[String],
) -> PyResult<Vec<Batch>> {
    if batches.is_empty() {
        return Err(PyValueError::new_err("udf input is empty"));
    }
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        let args = py_args_from_batch(py, batch, cols)?;
        let result = func.call1(PyTuple::new(py, args)?)?;
        let array = result_to_array(&result)?;
        if array.len() != batch.num_rows() {
            return Err(PyValueError::new_err(format!(
                "scalar UDF must return {} rows, got {}",
                batch.num_rows(),
                array.len()
            )));
        }
        out.push(with_column(batch, out_name, array)?);
    }
    Ok(out)
}

fn apply_agg_udf(
    py: Python<'_>,
    batches: &[Batch],
    keys: &[String],
    out_name: &str,
    func: &Bound<'_, PyAny>,
    cols: &[String],
) -> PyResult<Vec<Batch>> {
    if batches.is_empty() {
        return Err(PyValueError::new_err("agg_udf input is empty"));
    }
    let schema = batches[0].schema();
    let key_idx = column_indices(schema.as_ref(), keys)?;
    let arg_idx = column_indices(schema.as_ref(), cols)?;
    let mut groups: HashMap<Vec<Cell>, Vec<u32>> = HashMap::new();
    let mut order: Vec<Vec<Cell>> = Vec::new();
    let mut base: u32 = 0;
    for batch in batches {
        for row in 0..batch.num_rows() {
            let key: Vec<Cell> = key_idx
                .iter()
                .map(|&i| cell_at(batch.column(i), row))
                .collect::<PyResult<_>>()?;
            let global = base + row as u32;
            if let Some(rows) = groups.get_mut(&key) {
                rows.push(global);
            } else {
                groups.insert(key.clone(), vec![global]);
                order.push(key);
            }
        }
        base += batch.num_rows() as u32;
    }
    let mut key_builders: Vec<Vec<Cell>> = vec![Vec::new(); keys.len()];
    let mut out_arrays: Vec<ArrayRef> = Vec::new();
    let mut out_type: Option<DataType> = None;
    for key in &order {
        let rows = &groups[key];
        let args = take_group_args(py, batches, &arg_idx, rows)?;
        let result = func.call1(PyTuple::new(py, args)?)?;
        let array = result_to_array_or_scalar(&result)?;
        if array.len() != 1 {
            return Err(PyValueError::new_err(
                "aggregate UDF must return a scalar or a length-1 array",
            ));
        }
        match &out_type {
            None => out_type = Some(array.data_type().clone()),
            Some(dt) if dt != array.data_type() => {
                return Err(PyValueError::new_err(
                    "aggregate UDF return type must be the same for every group",
                ));
            }
            _ => {}
        }
        out_arrays.push(array);
        for (i, cell) in key.iter().enumerate() {
            key_builders[i].push(cell.clone());
        }
    }
    let dt = out_type.unwrap_or(DataType::Null);
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for (i, name) in keys.iter().enumerate() {
        let dt = schema.field(key_idx[i]).data_type().clone();
        fields.push(Field::new(name, dt.clone(), true));
        columns.push(cells_to_array(&key_builders[i], &dt)?);
    }
    fields.push(Field::new(out_name, dt.clone(), true));
    columns.push(concat_len1(&out_arrays, &dt, order.len())?);
    let schema = Arc::new(Schema::new(fields));
    Ok(vec![Batch::try_new(schema, columns).map_err(py_err)?])
}

fn column_indices(schema: &Schema, names: &[String]) -> PyResult<Vec<usize>> {
    names
        .iter()
        .map(|name| {
            schema
                .index_of(name)
                .or_else(|_| schema.index_of(&name.to_ascii_lowercase()))
                .map_err(|_| PyValueError::new_err(format!("column not found: {name}")))
        })
        .collect()
}

fn py_args_from_batch<'py>(
    py: Python<'py>,
    batch: &Batch,
    cols: &[String],
) -> PyResult<Vec<Bound<'py, PyAny>>> {
    let mut args = Vec::with_capacity(cols.len());
    for name in cols {
        let arr = batch
            .column_by_name(name)
            .or_else(|| batch.column_by_name(&name.to_ascii_lowercase()))
            .ok_or_else(|| PyValueError::new_err(format!("column not found: {name}")))?;
        args.push(arrow_c::to_pyarrow_array(py, arr.as_ref())?);
    }
    Ok(args)
}

fn take_group_args<'py>(
    py: Python<'py>,
    batches: &[Batch],
    arg_idx: &[usize],
    rows: &[u32],
) -> PyResult<Vec<Bound<'py, PyAny>>> {
    let indices = UInt32Array::from(rows.to_vec());
    let mut args = Vec::with_capacity(arg_idx.len());
    for &col in arg_idx {
        let full = concat_column(batches, col)?;
        let taken =
            take(&full, &indices, None).map_err(|e| PyValueError::new_err(e.to_string()))?;
        args.push(arrow_c::to_pyarrow_array(py, taken.as_ref())?);
    }
    Ok(args)
}

fn concat_column(batches: &[Batch], col: usize) -> PyResult<ArrayRef> {
    let refs: Vec<&dyn Array> = batches.iter().map(|b| b.column(col).as_ref()).collect();
    arrow::compute::concat(&refs).map_err(|e| PyValueError::new_err(e.to_string()))
}

fn concat_len1(arrays: &[ArrayRef], dt: &DataType, n: usize) -> PyResult<ArrayRef> {
    if arrays.is_empty() {
        return Err(PyValueError::new_err(format!(
            "cannot build empty {dt} aggregate column ({n} groups)"
        )));
    }
    let refs: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
    arrow::compute::concat(&refs).map_err(|e| PyValueError::new_err(e.to_string()))
}

fn result_to_array(obj: &Bound<'_, PyAny>) -> PyResult<ArrayRef> {
    if obj.hasattr("__arrow_c_array__")? {
        arrow_c::import_array(obj)
    } else {
        Err(PyValueError::new_err(
            "UDF must return an Arrow array (__arrow_c_array__)",
        ))
    }
}

fn result_to_array_or_scalar(obj: &Bound<'_, PyAny>) -> PyResult<ArrayRef> {
    if obj.hasattr("__arrow_c_array__")? {
        return arrow_c::import_array(obj);
    }
    if obj.hasattr("as_py")? {
        let inner = obj.call_method0("as_py")?;
        return python_scalar_array(&inner);
    }
    python_scalar_array(obj)
}

fn python_scalar_array(obj: &Bound<'_, PyAny>) -> PyResult<ArrayRef> {
    if obj.is_none() {
        return Ok(Arc::new(Int64Array::from(vec![None::<i64>])));
    }
    if obj.is_instance_of::<PyBool>() {
        let v: bool = obj.extract()?;
        return Ok(Arc::new(BooleanArray::from(vec![Some(v)])));
    }
    if obj.is_instance_of::<PyInt>() {
        let v: i64 = obj.extract()?;
        return Ok(Arc::new(Int64Array::from(vec![Some(v)])));
    }
    if let Ok(v) = obj.extract::<f64>() {
        return Ok(Arc::new(Float64Array::from(vec![Some(v)])));
    }
    if let Ok(v) = obj.extract::<String>() {
        return Ok(Arc::new(StringArray::from(vec![Some(v)])));
    }
    Err(PyValueError::new_err(
        "aggregate UDF returned an unsupported scalar",
    ))
}

fn with_column(batch: &Batch, name: &str, array: ArrayRef) -> PyResult<Batch> {
    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut columns: Vec<ArrayRef> = (0..batch.num_columns())
        .map(|i| batch.column(i).clone())
        .collect();
    match batch.schema().index_of(name) {
        Ok(i) => {
            fields[i] = Field::new(name, array.data_type().clone(), true);
            columns[i] = array;
        }
        Err(_) => {
            fields.push(Field::new(name, array.data_type().clone(), true));
            columns.push(array);
        }
    }
    let schema = Arc::new(Schema::new(fields));
    Batch::try_new(schema, columns).map_err(py_err)
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Cell {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(u64),
    Utf8(String),
    Bin(Vec<u8>),
}

fn cell_at(arr: &ArrayRef, row: usize) -> PyResult<Cell> {
    if arr.is_null(row) {
        return Ok(Cell::Null);
    }
    match arr.data_type() {
        DataType::Boolean => Ok(Cell::Bool(
            downcast::<BooleanArray>(arr, "Boolean")?.value(row),
        )),
        DataType::Int8 => Ok(Cell::I64(i64::from(
            downcast::<Int8Array>(arr, "Int8")?.value(row),
        ))),
        DataType::Int16 => Ok(Cell::I64(i64::from(
            downcast::<Int16Array>(arr, "Int16")?.value(row),
        ))),
        DataType::Int32 => Ok(Cell::I64(i64::from(
            downcast::<Int32Array>(arr, "Int32")?.value(row),
        ))),
        DataType::Int64 => Ok(Cell::I64(downcast::<Int64Array>(arr, "Int64")?.value(row))),
        DataType::UInt8 => Ok(Cell::U64(u64::from(
            downcast::<UInt8Array>(arr, "UInt8")?.value(row),
        ))),
        DataType::UInt16 => Ok(Cell::U64(u64::from(
            downcast::<UInt16Array>(arr, "UInt16")?.value(row),
        ))),
        DataType::UInt32 => Ok(Cell::U64(u64::from(
            downcast::<UInt32Array>(arr, "UInt32")?.value(row),
        ))),
        DataType::UInt64 => Ok(Cell::U64(
            downcast::<UInt64Array>(arr, "UInt64")?.value(row),
        )),
        DataType::Float32 => Ok(Cell::F64(
            f64::from(downcast::<Float32Array>(arr, "Float32")?.value(row)).to_bits(),
        )),
        DataType::Float64 => Ok(Cell::F64(
            downcast::<Float64Array>(arr, "Float64")?
                .value(row)
                .to_bits(),
        )),
        DataType::Utf8 => Ok(Cell::Utf8(
            downcast::<StringArray>(arr, "Utf8")?.value(row).to_string(),
        )),
        DataType::LargeUtf8 => Ok(Cell::Utf8(
            downcast::<LargeStringArray>(arr, "LargeUtf8")?
                .value(row)
                .to_string(),
        )),
        DataType::Binary => Ok(Cell::Bin(
            downcast::<BinaryArray>(arr, "Binary")?.value(row).to_vec(),
        )),
        other => Err(PyValueError::new_err(format!(
            "unsupported grouping type {other}"
        ))),
    }
}

fn cells_to_array(cells: &[Cell], dt: &DataType) -> PyResult<ArrayRef> {
    match dt {
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(map_cells(
            cells,
            |c| match c {
                Cell::Bool(v) => Some(*v),
                _ => None,
            },
        )))),
        DataType::Int8 => Ok(Arc::new(Int8Array::from(map_cells(cells, |c| match c {
            Cell::I64(v) => i8::try_from(*v).ok(),
            _ => None,
        })))),
        DataType::Int16 => Ok(Arc::new(Int16Array::from(map_cells(cells, |c| match c {
            Cell::I64(v) => i16::try_from(*v).ok(),
            _ => None,
        })))),
        DataType::Int32 => Ok(Arc::new(Int32Array::from(map_cells(cells, |c| match c {
            Cell::I64(v) => i32::try_from(*v).ok(),
            _ => None,
        })))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from(map_cells(cells, |c| match c {
            Cell::I64(v) => Some(*v),
            _ => None,
        })))),
        DataType::UInt8 => Ok(Arc::new(UInt8Array::from(map_cells(cells, |c| match c {
            Cell::U64(v) => u8::try_from(*v).ok(),
            _ => None,
        })))),
        DataType::UInt16 => Ok(Arc::new(UInt16Array::from(map_cells(cells, |c| match c {
            Cell::U64(v) => u16::try_from(*v).ok(),
            _ => None,
        })))),
        DataType::UInt32 => Ok(Arc::new(UInt32Array::from(map_cells(cells, |c| match c {
            Cell::U64(v) => u32::try_from(*v).ok(),
            _ => None,
        })))),
        DataType::UInt64 => Ok(Arc::new(UInt64Array::from(map_cells(cells, |c| match c {
            Cell::U64(v) => Some(*v),
            _ => None,
        })))),
        DataType::Float32 => Ok(Arc::new(Float32Array::from(map_cells(
            cells,
            |c| match c {
                Cell::F64(b) => Some(f64::from_bits(*b) as f32),
                _ => None,
            },
        )))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(map_cells(
            cells,
            |c| match c {
                Cell::F64(b) => Some(f64::from_bits(*b)),
                _ => None,
            },
        )))),
        DataType::Utf8 => Ok(Arc::new(StringArray::from(
            cells
                .iter()
                .map(|c| match c {
                    Cell::Utf8(v) => Some(v.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::LargeUtf8 => Ok(Arc::new(LargeStringArray::from(
            cells
                .iter()
                .map(|c| match c {
                    Cell::Utf8(v) => Some(v.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Binary => Ok(Arc::new(BinaryArray::from(
            cells
                .iter()
                .map(|c| match c {
                    Cell::Bin(v) => Some(v.as_slice()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        other => Err(PyValueError::new_err(format!(
            "unsupported grouping key type {other}"
        ))),
    }
}

fn map_cells<T>(cells: &[Cell], f: impl Fn(&Cell) -> Option<T>) -> Vec<Option<T>> {
    cells
        .iter()
        .map(|c| if matches!(c, Cell::Null) { None } else { f(c) })
        .collect()
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
        DataType::List(_) => list_values_to_py(py, downcast::<ListArray>(arr, "List")?.value(row)),
        DataType::LargeList(_) => {
            list_values_to_py(py, downcast::<LargeListArray>(arr, "LargeList")?.value(row))
        }
        DataType::FixedSizeList(_, _) => list_values_to_py(
            py,
            downcast::<FixedSizeListArray>(arr, "FixedSizeList")?.value(row),
        ),
        other => Err(PyValueError::new_err(format!(
            "unsupported result type {other}"
        ))),
    }
}

fn list_values_to_py(py: Python<'_>, values: ArrayRef) -> PyResult<Py<PyAny>> {
    let out = PyList::empty(py);
    for i in 0..values.len() {
        out.append(cell_to_py(py, &values, i)?)?;
    }
    Ok(out.into_any().unbind())
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
    m.add_class::<PyArrowStream>()?;
    m.add("version", VERSION)?;
    Ok(())
}
