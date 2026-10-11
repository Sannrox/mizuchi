//! Arrow C Data Interface via PyCapsule (`__arrow_c_array__` / `__arrow_c_stream__`).
//!
//! Import and export share buffers; they do not copy column data.

use std::ffi::CString;
use std::sync::Arc;

use ::mizuchi::Batch;
use arrow::array::{ArrayRef, RecordBatch, RecordBatchIterator, make_array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi, to_ffi};
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyCapsuleMethods, PyTuple};

const SCHEMA_NAME: &str = "arrow_schema";
const ARRAY_NAME: &str = "arrow_array";
const STREAM_NAME: &str = "arrow_array_stream";

fn arrow_err(err: ArrowError) -> PyErr {
    PyValueError::new_err(err.to_string())
}

fn cname(name: &str) -> CString {
    CString::new(name).expect("capsule name")
}

/// Export `array` as (`arrow_schema`, `arrow_array`) capsules.
pub fn export_array<'py>(
    py: Python<'py>,
    array: &dyn arrow::array::Array,
) -> PyResult<(Bound<'py, PyCapsule>, Bound<'py, PyCapsule>)> {
    let (ffi_array, ffi_schema) = to_ffi(&array.to_data()).map_err(arrow_err)?;
    let schema = PyCapsule::new(py, ffi_schema, Some(cname(SCHEMA_NAME)))?;
    let array = PyCapsule::new(py, ffi_array, Some(cname(ARRAY_NAME)))?;
    Ok((schema, array))
}

/// Import an array from an object that implements `__arrow_c_array__`.
pub fn import_array(obj: &Bound<'_, PyAny>) -> PyResult<ArrayRef> {
    let pair = obj.call_method0("__arrow_c_array__")?;
    let tuple = pair.downcast::<PyTuple>().map_err(|_| {
        PyValueError::new_err("__arrow_c_array__ must return (schema, array) capsules")
    })?;
    if tuple.len() != 2 {
        return Err(PyValueError::new_err(
            "__arrow_c_array__ must return two capsules",
        ));
    }
    let schema_cap = tuple.get_item(0)?;
    let array_cap = tuple.get_item(1)?;
    let schema_cap = schema_cap
        .downcast::<PyCapsule>()
        .map_err(|_| PyValueError::new_err("schema capsule missing"))?;
    let array_cap = array_cap
        .downcast::<PyCapsule>()
        .map_err(|_| PyValueError::new_err("array capsule missing"))?;
    check_name(schema_cap, SCHEMA_NAME)?;
    check_name(array_cap, ARRAY_NAME)?;
    // from_raw moves the C struct and writes an empty released one into the
    // capsule, so the producer destructor is a no-op (Arrow consume).
    let schema = unsafe { schema_cap.reference::<FFI_ArrowSchema>() };
    let array = unsafe { FFI_ArrowArray::from_raw(array_cap.pointer().cast()) };
    let data = unsafe { from_ffi(array, schema) }.map_err(arrow_err)?;
    Ok(make_array(data))
}

/// Export batches as an `arrow_array_stream` capsule.
pub fn export_stream<'py>(
    py: Python<'py>,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> PyResult<Bound<'py, PyCapsule>> {
    let iter = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
    let stream = FFI_ArrowArrayStream::new(Box::new(iter));
    PyCapsule::new(py, stream, Some(cname(STREAM_NAME)))
}

/// Import record batches from `__arrow_c_stream__` or `__arrow_c_array__`.
pub fn import_batches(obj: &Bound<'_, PyAny>) -> PyResult<Vec<Batch>> {
    if obj.hasattr("__arrow_c_stream__")? {
        import_stream(obj)
    } else if obj.hasattr("__arrow_c_array__")? {
        import_record_batch(obj)
    } else if obj.hasattr("to_arrow")? {
        let inner = obj.call_method0("to_arrow")?;
        import_batches(&inner)
    } else {
        Err(PyValueError::new_err(
            "object must implement the Arrow C Data Interface (__arrow_c_stream__ or __arrow_c_array__)",
        ))
    }
}

fn import_stream(obj: &Bound<'_, PyAny>) -> PyResult<Vec<Batch>> {
    let capsule = obj.call_method0("__arrow_c_stream__")?;
    let cap = capsule.downcast::<PyCapsule>().map_err(|_| {
        PyValueError::new_err("__arrow_c_stream__ must return an arrow_array_stream capsule")
    })?;
    check_name(cap, STREAM_NAME)?;
    let stream = unsafe { FFI_ArrowArrayStream::from_raw(cap.pointer().cast()) };
    let reader = ArrowArrayStreamReader::try_new(stream).map_err(arrow_err)?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(arrow_err)?;
        out.push(Batch::from_arrow(batch).map_err(|e| PyValueError::new_err(e.to_string()))?);
    }
    Ok(out)
}

fn import_record_batch(obj: &Bound<'_, PyAny>) -> PyResult<Vec<Batch>> {
    let array = import_array(obj)?;
    let batch = match array.data_type() {
        DataType::Struct(_) => {
            let sa = arrow::array::StructArray::from(array.to_data());
            RecordBatch::from(sa)
        }
        _ => {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                array.data_type().clone(),
                true,
            )]));
            RecordBatch::try_new(schema, vec![array])
                .map_err(|e| PyValueError::new_err(e.to_string()))?
        }
    };
    Ok(vec![
        Batch::from_arrow(batch).map_err(|e| PyValueError::new_err(e.to_string()))?,
    ])
}

fn check_name(cap: &Bound<'_, PyCapsule>, expect: &str) -> PyResult<()> {
    match cap.name()? {
        Some(name) if name.to_bytes() == expect.as_bytes() => Ok(()),
        Some(name) => Err(PyValueError::new_err(format!(
            "expected capsule {expect}, got {}",
            name.to_string_lossy()
        ))),
        None => Err(PyValueError::new_err(format!(
            "expected named capsule {expect}"
        ))),
    }
}

/// Convert a Rust array into a pyarrow.Array (zero-copy capsules).
pub fn to_pyarrow_array<'py>(
    py: Python<'py>,
    array: &dyn arrow::array::Array,
) -> PyResult<Bound<'py, PyAny>> {
    let (schema, array) = export_array(py, array)?;
    let pa = py
        .import("pyarrow")
        .map_err(|_| PyValueError::new_err("Python UDFs require the pyarrow package"))?;
    pa.getattr("Array")?
        .call_method1("_import_from_c_capsule", (schema, array))
}
