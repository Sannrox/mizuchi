//! Uncompressed ORC subset: DIRECT encoding, STRUCT, LIST, and primitives.
//!
//! Compression, dictionary encoding, encryption, and types outside the
//! Arrow mapping below fail closed. The file layout follows the published
//! ORC postscript/footer/stripe structure.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Fields, Schema};

use crate::batch::{Batch, BatchPolicy};
use crate::error::{Error, Result};
use crate::schema::from_arrow_schema;

const MAGIC: &[u8] = b"ORC";
const KIND_BOOLEAN: u32 = 0;
const KIND_BYTE: u32 = 1;
const KIND_SHORT: u32 = 2;
const KIND_INT: u32 = 3;
const KIND_LONG: u32 = 4;
const KIND_FLOAT: u32 = 5;
const KIND_DOUBLE: u32 = 6;
const KIND_STRING: u32 = 7;
const KIND_BINARY: u32 = 8;
const KIND_LIST: u32 = 10;
const KIND_STRUCT: u32 = 12;
const STREAM_PRESENT: u32 = 0;
const STREAM_DATA: u32 = 1;
const STREAM_LENGTH: u32 = 2;

#[derive(Clone, Debug)]
struct OrcType {
    kind: u32,
    subtypes: Vec<u32>,
    field_names: Vec<String>,
}

struct StreamOut {
    kind: u32,
    column: u32,
    bytes: Vec<u8>,
}

type ColumnStreams = BTreeMap<u32, Vec<(u32, Vec<u8>)>>;

/// Write Arrow batches as an uncompressed ORC file.
pub fn write_file(path: impl AsRef<Path>, batches: &[Batch]) -> Result<()> {
    let path = path.as_ref();
    let first = batches
        .first()
        .ok_or_else(|| Error::InvalidBatch("ORC write needs at least one batch".into()))?;
    let schema = first.schema();
    for other in &batches[1..] {
        if other.schema() != schema {
            return Err(Error::InvalidBatch(
                "write batches must share one schema".into(),
            ));
        }
    }
    let rbs: Vec<&RecordBatch> = batches.iter().map(Batch::as_arrow).collect();
    let combined = concat_batches(&schema, rbs).map_err(|e| Error::InvalidBatch(e.to_string()))?;
    let types = schema_to_types(schema.as_ref())?;
    let mut streams = Vec::new();
    for (j, sub) in types[0].subtypes.iter().enumerate() {
        encode_array(combined.column(j).as_ref(), *sub, &types, &mut streams)?;
    }

    let mut data_bytes = Vec::new();
    for stream in &streams {
        data_bytes.extend_from_slice(&stream.bytes);
    }
    let stripe_footer = encode_stripe_footer(&streams, types.len());
    let data_length = data_bytes.len() as u64;
    let footer_length = stripe_footer.len() as u64;

    let mut body = Vec::from(MAGIC);
    body.extend_from_slice(&data_bytes);
    body.extend_from_slice(&stripe_footer);

    let file_footer = encode_file_footer(
        MAGIC.len() as u64,
        data_length + footer_length,
        &types,
        MAGIC.len() as u64,
        data_length,
        footer_length,
        combined.num_rows() as u64,
    );
    body.extend_from_slice(&file_footer);
    let postscript = encode_postscript(file_footer.len() as u64);
    if postscript.len() > 255 {
        return Err(Error::Io("ORC postscript exceeds 255 bytes".into()));
    }
    body.extend_from_slice(&postscript);
    body.push(postscript.len() as u8);
    fs::write(path, body).map_err(|e| Error::Io(format!("{}: {e}", path.display())))
}

/// Read an uncompressed ORC file into engine batches.
pub fn read_file(
    path: impl AsRef<Path>,
    projection: Option<&[String]>,
    policy: BatchPolicy,
) -> Result<Vec<Batch>> {
    let path = path.as_ref();
    let bytes = fs::read(path).map_err(|e| Error::Io(format!("{}: {e}", path.display())))?;
    if bytes.len() < 4 || !bytes.starts_with(MAGIC) {
        return Err(Error::Io(format!("{}: not an ORC file", path.display())));
    }
    let ps_len = usize::from(*bytes.last().expect("length checked"));
    if ps_len + 1 > bytes.len() {
        return Err(Error::Io(format!(
            "{}: truncated postscript",
            path.display()
        )));
    }
    let ps_start = bytes.len() - 1 - ps_len;
    let ps = decode_postscript(&bytes[ps_start..bytes.len() - 1])?;
    if ps.compression != 0 {
        return Err(Error::Io(format!(
            "{}: compressed ORC is not supported",
            path.display()
        )));
    }
    let footer_len = ps.footer_length as usize;
    if footer_len > ps_start {
        return Err(Error::Io(format!("{}: truncated footer", path.display())));
    }
    let footer_start = ps_start - footer_len;
    let footer = decode_file_footer(&bytes[footer_start..ps_start])?;
    let (arrow_schema, selected) = types_to_schema(&footer.types, projection)?;
    let schema = from_arrow_schema(Arc::new(arrow_schema))?;
    if footer.stripes.is_empty() {
        return Ok(vec![Batch::empty(schema)?]);
    }
    let mut record_batches = Vec::new();
    for stripe in &footer.stripes {
        let start = stripe.offset as usize;
        let data_start = start + stripe.index_length as usize;
        let data_end = data_start + stripe.data_length as usize;
        let sf_end = data_end + stripe.footer_length as usize;
        if sf_end > bytes.len() {
            return Err(Error::Io(format!("{}: truncated stripe", path.display())));
        }
        let sf = decode_stripe_footer(&bytes[data_end..sf_end])?;
        let by_col = split_streams(&bytes[data_start..data_end], &sf)?;
        let mut cols = Vec::with_capacity(selected.len());
        for sub in &selected {
            cols.push(decode_array(
                *sub,
                stripe.number_of_rows as usize,
                &footer.types,
                &by_col,
            )?);
        }
        let rb = RecordBatch::try_new(schema.clone(), cols)
            .map_err(|e| Error::InvalidBatch(e.to_string()))?;
        record_batches.push(rb);
    }
    let mut out = Vec::new();
    for rb in record_batches {
        let batch = Batch::from_arrow(rb)?;
        out.extend(
            batch
                .split_by_policy(&policy)
                .into_iter()
                .filter(|b| b.num_rows() > 0),
        );
    }
    if out.is_empty() {
        out.push(Batch::empty(schema)?);
    }
    Ok(out)
}

fn schema_to_types(schema: &Schema) -> Result<Vec<OrcType>> {
    let mut types = vec![OrcType {
        kind: KIND_STRUCT,
        subtypes: Vec::new(),
        field_names: schema
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect(),
    }];
    let mut subtypes = Vec::new();
    for field in schema.fields() {
        subtypes.push(add_data_type(field.data_type(), &mut types)?);
    }
    types[0].subtypes = subtypes;
    Ok(types)
}

fn add_data_type(dt: &DataType, types: &mut Vec<OrcType>) -> Result<u32> {
    let id = types.len() as u32;
    match dt {
        DataType::Boolean => types.push(primitive(KIND_BOOLEAN)),
        DataType::Int8 => types.push(primitive(KIND_BYTE)),
        DataType::Int16 => types.push(primitive(KIND_SHORT)),
        DataType::Int32 => types.push(primitive(KIND_INT)),
        DataType::Int64 => types.push(primitive(KIND_LONG)),
        DataType::Float32 => types.push(primitive(KIND_FLOAT)),
        DataType::Float64 => types.push(primitive(KIND_DOUBLE)),
        DataType::Utf8 | DataType::LargeUtf8 => types.push(primitive(KIND_STRING)),
        DataType::Binary | DataType::LargeBinary => types.push(primitive(KIND_BINARY)),
        DataType::List(field) => {
            types.push(OrcType {
                kind: KIND_LIST,
                subtypes: Vec::new(),
                field_names: Vec::new(),
            });
            let child = add_data_type(field.data_type(), types)?;
            types[id as usize].subtypes.push(child);
        }
        DataType::Struct(fields) => {
            types.push(OrcType {
                kind: KIND_STRUCT,
                subtypes: Vec::new(),
                field_names: fields.iter().map(|f| f.name().to_string()).collect(),
            });
            let mut subs = Vec::new();
            for field in fields {
                subs.push(add_data_type(field.data_type(), types)?);
            }
            types[id as usize].subtypes = subs;
        }
        other => {
            return Err(Error::UnsupportedType(format!(
                "ORC write does not support {other}"
            )));
        }
    }
    Ok(id)
}

fn primitive(kind: u32) -> OrcType {
    OrcType {
        kind,
        subtypes: Vec::new(),
        field_names: Vec::new(),
    }
}

fn types_to_schema(types: &[OrcType], projection: Option<&[String]>) -> Result<(Schema, Vec<u32>)> {
    if types.is_empty() || types[0].kind != KIND_STRUCT {
        return Err(Error::Io("ORC root type must be STRUCT".into()));
    }
    let root = &types[0];
    if root.field_names.len() != root.subtypes.len() {
        return Err(Error::Io(
            "ORC struct field names do not match subtypes".into(),
        ));
    }
    let wanted: Vec<(String, u32)> = match projection {
        None => root
            .field_names
            .iter()
            .cloned()
            .zip(root.subtypes.iter().copied())
            .collect(),
        Some(names) => {
            let mut out = Vec::with_capacity(names.len());
            for name in names {
                let idx = root
                    .field_names
                    .iter()
                    .position(|n| n == name)
                    .ok_or_else(|| Error::Plan(format!("projection column not in file: {name}")))?;
                out.push((name.clone(), root.subtypes[idx]));
            }
            out
        }
    };
    let mut fields = Vec::new();
    let mut selected = Vec::new();
    for (name, sub) in wanted {
        fields.push(Field::new(name, orc_to_arrow(sub, types)?, true));
        selected.push(sub);
    }
    Ok((Schema::new(fields), selected))
}

fn orc_to_arrow(id: u32, types: &[OrcType]) -> Result<DataType> {
    let ty = types
        .get(id as usize)
        .ok_or_else(|| Error::Io(format!("ORC type {id} missing")))?;
    match ty.kind {
        KIND_BOOLEAN => Ok(DataType::Boolean),
        KIND_BYTE => Ok(DataType::Int8),
        KIND_SHORT => Ok(DataType::Int16),
        KIND_INT => Ok(DataType::Int32),
        KIND_LONG => Ok(DataType::Int64),
        KIND_FLOAT => Ok(DataType::Float32),
        KIND_DOUBLE => Ok(DataType::Float64),
        KIND_STRING => Ok(DataType::Utf8),
        KIND_BINARY => Ok(DataType::Binary),
        KIND_LIST => {
            let child = *ty
                .subtypes
                .first()
                .ok_or_else(|| Error::Io("ORC LIST missing child".into()))?;
            Ok(DataType::List(Arc::new(Field::new(
                "item",
                orc_to_arrow(child, types)?,
                true,
            ))))
        }
        KIND_STRUCT => {
            let mut fields = Vec::new();
            for (name, sub) in ty.field_names.iter().zip(ty.subtypes.iter()) {
                fields.push(Field::new(name, orc_to_arrow(*sub, types)?, true));
            }
            Ok(DataType::Struct(fields.into()))
        }
        other => Err(Error::UnsupportedType(format!("ORC type kind {other}"))),
    }
}

fn encode_array(
    arr: &dyn Array,
    col: u32,
    types: &[OrcType],
    streams: &mut Vec<StreamOut>,
) -> Result<()> {
    let ty = &types[col as usize];
    if arr.null_count() > 0 {
        let bits: Vec<bool> = (0..arr.len()).map(|i| arr.is_valid(i)).collect();
        streams.push(StreamOut {
            kind: STREAM_PRESENT,
            column: col,
            bytes: byte_rle_encode(&pack_bits(&bits)),
        });
    }
    match ty.kind {
        KIND_BOOLEAN => {
            let a = downcast::<BooleanArray>(arr, "BOOLEAN")?;
            let bits: Vec<bool> = (0..a.len())
                .filter(|&i| a.is_valid(i))
                .map(|i| a.value(i))
                .collect();
            push_data(streams, col, byte_rle_encode(&pack_bits(&bits)));
        }
        KIND_BYTE => {
            let a = downcast::<Int8Array>(arr, "BYTE")?;
            let bytes: Vec<u8> = (0..a.len())
                .filter(|&i| a.is_valid(i))
                .map(|i| a.value(i) as u8)
                .collect();
            push_data(streams, col, byte_rle_encode(&bytes));
        }
        KIND_SHORT => encode_ints(
            streams,
            col,
            downcast::<Int16Array>(arr, "SHORT")?,
            |a, i| i64::from(a.value(i)),
        )?,
        KIND_INT => encode_ints(streams, col, downcast::<Int32Array>(arr, "INT")?, |a, i| {
            i64::from(a.value(i))
        })?,
        KIND_LONG => encode_ints(
            streams,
            col,
            downcast::<Int64Array>(arr, "LONG")?,
            |a, i| a.value(i),
        )?,
        KIND_FLOAT => {
            let a = downcast::<Float32Array>(arr, "FLOAT")?;
            let mut data = Vec::new();
            for i in 0..a.len() {
                if a.is_valid(i) {
                    data.extend_from_slice(&a.value(i).to_le_bytes());
                }
            }
            push_data(streams, col, data);
        }
        KIND_DOUBLE => {
            let a = downcast::<Float64Array>(arr, "DOUBLE")?;
            let mut data = Vec::new();
            for i in 0..a.len() {
                if a.is_valid(i) {
                    data.extend_from_slice(&a.value(i).to_le_bytes());
                }
            }
            push_data(streams, col, data);
        }
        KIND_STRING => {
            let a = downcast::<StringArray>(arr, "STRING")?;
            let mut data = Vec::new();
            let mut lengths = Vec::new();
            for i in 0..a.len() {
                if a.is_valid(i) {
                    let v = a.value(i);
                    lengths.push(v.len() as i64);
                    data.extend_from_slice(v.as_bytes());
                }
            }
            push_data(streams, col, data);
            streams.push(StreamOut {
                kind: STREAM_LENGTH,
                column: col,
                bytes: int_rle_encode(&lengths, false),
            });
        }
        KIND_BINARY => {
            let a = downcast::<BinaryArray>(arr, "BINARY")?;
            let mut data = Vec::new();
            let mut lengths = Vec::new();
            for i in 0..a.len() {
                if a.is_valid(i) {
                    let v = a.value(i);
                    lengths.push(v.len() as i64);
                    data.extend_from_slice(v);
                }
            }
            push_data(streams, col, data);
            streams.push(StreamOut {
                kind: STREAM_LENGTH,
                column: col,
                bytes: int_rle_encode(&lengths, false),
            });
        }
        KIND_LIST => {
            let a = arr
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| Error::InvalidBatch("ORC LIST needs a ListArray".into()))?;
            let mut lengths = Vec::new();
            for i in 0..a.len() {
                if a.is_valid(i) {
                    lengths.push(i64::from(a.value_length(i)));
                }
            }
            streams.push(StreamOut {
                kind: STREAM_LENGTH,
                column: col,
                bytes: int_rle_encode(&lengths, false),
            });
            let child = *ty
                .subtypes
                .first()
                .ok_or_else(|| Error::Io("ORC LIST missing child".into()))?;
            encode_array(a.values().as_ref(), child, types, streams)?;
        }
        KIND_STRUCT => {
            let a = arr
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| Error::InvalidBatch("ORC STRUCT needs a StructArray".into()))?;
            for (j, sub) in ty.subtypes.iter().enumerate() {
                encode_array(a.column(j).as_ref(), *sub, types, streams)?;
            }
        }
        other => {
            return Err(Error::UnsupportedType(format!("ORC type kind {other}")));
        }
    }
    Ok(())
}

fn encode_ints<A: Array>(
    streams: &mut Vec<StreamOut>,
    col: u32,
    arr: &A,
    value: impl Fn(&A, usize) -> i64,
) -> Result<()> {
    let mut values = Vec::new();
    for i in 0..arr.len() {
        if arr.is_valid(i) {
            values.push(value(arr, i));
        }
    }
    push_data(streams, col, int_rle_encode(&values, true));
    Ok(())
}

fn push_data(streams: &mut Vec<StreamOut>, col: u32, bytes: Vec<u8>) {
    if !bytes.is_empty() {
        streams.push(StreamOut {
            kind: STREAM_DATA,
            column: col,
            bytes,
        });
    }
}

fn downcast<'a, T: 'static>(arr: &'a dyn Array, what: &str) -> Result<&'a T> {
    arr.as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| Error::InvalidBatch(format!("ORC {what} array type mismatch")))
}

fn decode_array(
    col: u32,
    num_rows: usize,
    types: &[OrcType],
    by_col: &ColumnStreams,
) -> Result<ArrayRef> {
    let ty = types
        .get(col as usize)
        .ok_or_else(|| Error::Io(format!("ORC type {col} missing")))?;
    let present = present_bits(by_col.get(&col), num_rows)?;
    let n_valid = present.iter().filter(|b| **b).count();
    match ty.kind {
        KIND_BOOLEAN => {
            let packed = stream_bytes(by_col.get(&col), STREAM_DATA);
            let decoded = byte_rle_decode(&packed)?;
            let bits = unpack_bits(&decoded, n_valid);
            Ok(Arc::new(BooleanArray::from(scatter(&present, bits))))
        }
        KIND_BYTE => {
            let decoded = byte_rle_decode(&stream_bytes(by_col.get(&col), STREAM_DATA))?;
            if decoded.len() < n_valid {
                return Err(Error::Io("ORC BYTE data truncated".into()));
            }
            let vals: Vec<i8> = decoded[..n_valid].iter().map(|b| *b as i8).collect();
            Ok(Arc::new(Int8Array::from(scatter(&present, vals))))
        }
        KIND_SHORT => {
            let vals = int_rle_decode(&stream_bytes(by_col.get(&col), STREAM_DATA), true)?;
            let shorts: Vec<i16> = take_n(vals, n_valid)?
                .into_iter()
                .map(|v| i16::try_from(v).map_err(|_| Error::Io("ORC SHORT overflow".into())))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(Int16Array::from(scatter(&present, shorts))))
        }
        KIND_INT => {
            let vals = int_rle_decode(&stream_bytes(by_col.get(&col), STREAM_DATA), true)?;
            let ints: Vec<i32> = take_n(vals, n_valid)?
                .into_iter()
                .map(|v| i32::try_from(v).map_err(|_| Error::Io("ORC INT overflow".into())))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(Int32Array::from(scatter(&present, ints))))
        }
        KIND_LONG => {
            let vals = take_n(
                int_rle_decode(&stream_bytes(by_col.get(&col), STREAM_DATA), true)?,
                n_valid,
            )?;
            Ok(Arc::new(Int64Array::from(scatter(&present, vals))))
        }
        KIND_FLOAT => {
            let data = stream_bytes(by_col.get(&col), STREAM_DATA);
            if data.len() < n_valid * 4 {
                return Err(Error::Io("ORC FLOAT data truncated".into()));
            }
            let mut vals = Vec::with_capacity(n_valid);
            for chunk in data.chunks_exact(4).take(n_valid) {
                vals.push(f32::from_le_bytes(chunk.try_into().expect("4 bytes")));
            }
            Ok(Arc::new(Float32Array::from(scatter(&present, vals))))
        }
        KIND_DOUBLE => {
            let data = stream_bytes(by_col.get(&col), STREAM_DATA);
            if data.len() < n_valid * 8 {
                return Err(Error::Io("ORC DOUBLE data truncated".into()));
            }
            let mut vals = Vec::with_capacity(n_valid);
            for chunk in data.chunks_exact(8).take(n_valid) {
                vals.push(f64::from_le_bytes(chunk.try_into().expect("8 bytes")));
            }
            Ok(Arc::new(Float64Array::from(scatter(&present, vals))))
        }
        KIND_STRING => {
            let lengths = take_n(
                int_rle_decode(&stream_bytes(by_col.get(&col), STREAM_LENGTH), false)?,
                n_valid,
            )?;
            let strings = split_bytes(&stream_bytes(by_col.get(&col), STREAM_DATA), &lengths)?;
            Ok(Arc::new(StringArray::from(scatter(&present, strings))))
        }
        KIND_BINARY => {
            let lengths = take_n(
                int_rle_decode(&stream_bytes(by_col.get(&col), STREAM_LENGTH), false)?,
                n_valid,
            )?;
            let bins = split_raw(&stream_bytes(by_col.get(&col), STREAM_DATA), &lengths)?;
            let scattered: Vec<Option<Vec<u8>>> = scatter(&present, bins);
            Ok(Arc::new(BinaryArray::from_iter(
                scattered.iter().map(|v| v.as_deref()),
            )))
        }
        KIND_LIST => {
            let lengths = take_n(
                int_rle_decode(&stream_bytes(by_col.get(&col), STREAM_LENGTH), false)?,
                n_valid,
            )?;
            let total: usize = lengths
                .iter()
                .map(|n| usize::try_from(*n).unwrap_or(0))
                .sum();
            let child_id = *ty
                .subtypes
                .first()
                .ok_or_else(|| Error::Io("ORC LIST missing child".into()))?;
            let values = decode_array(child_id, total, types, by_col)?;
            let mut offsets = Vec::with_capacity(num_rows + 1);
            offsets.push(0i32);
            let mut li = 0;
            for bit in &present {
                let next = if *bit {
                    let n = i32::try_from(lengths[li])
                        .map_err(|_| Error::Io("ORC LIST length overflow".into()))?;
                    li += 1;
                    offsets.last().copied().unwrap_or(0) + n
                } else {
                    offsets.last().copied().unwrap_or(0)
                };
                offsets.push(next);
            }
            let field = match orc_to_arrow(col, types)? {
                DataType::List(f) => f,
                _ => return Err(Error::Io("ORC LIST schema mismatch".into())),
            };
            let nulls = NullBuffer::from(present);
            let list = ListArray::try_new(
                field,
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                values,
                Some(nulls),
            )
            .map_err(|e| Error::InvalidBatch(e.to_string()))?;
            Ok(Arc::new(list))
        }
        KIND_STRUCT => {
            let mut child_fields = Vec::new();
            let mut child_cols = Vec::new();
            for (name, sub) in ty.field_names.iter().zip(ty.subtypes.iter()) {
                child_fields.push(Field::new(name, orc_to_arrow(*sub, types)?, true));
                child_cols.push(decode_array(*sub, num_rows, types, by_col)?);
            }
            let nulls = if present.iter().all(|b| *b) {
                None
            } else {
                Some(NullBuffer::from(present))
            };
            let arr = StructArray::new(Fields::from(child_fields), child_cols, nulls);
            Ok(Arc::new(arr))
        }
        other => Err(Error::UnsupportedType(format!("ORC type kind {other}"))),
    }
}

fn present_bits(streams: Option<&Vec<(u32, Vec<u8>)>>, num_rows: usize) -> Result<Vec<bool>> {
    match streams.and_then(|s| s.iter().find(|(k, _)| *k == STREAM_PRESENT)) {
        None => Ok(vec![true; num_rows]),
        Some((_, bytes)) => {
            let decoded = byte_rle_decode(bytes)?;
            Ok(unpack_bits(&decoded, num_rows))
        }
    }
}

fn stream_bytes(streams: Option<&Vec<(u32, Vec<u8>)>>, kind: u32) -> Vec<u8> {
    streams
        .and_then(|s| s.iter().find(|(k, _)| *k == kind))
        .map(|(_, b)| b.clone())
        .unwrap_or_default()
}

fn scatter<T: Clone>(present: &[bool], values: Vec<T>) -> Vec<Option<T>> {
    let mut it = values.into_iter();
    present
        .iter()
        .map(|bit| if *bit { it.next() } else { None })
        .collect()
}

fn take_n(mut vals: Vec<i64>, n: usize) -> Result<Vec<i64>> {
    if vals.len() < n {
        return Err(Error::Io("ORC integer stream truncated".into()));
    }
    vals.truncate(n);
    Ok(vals)
}

fn split_raw(data: &[u8], lengths: &[i64]) -> Result<Vec<Vec<u8>>> {
    let mut out = Vec::with_capacity(lengths.len());
    let mut off = 0usize;
    for len in lengths {
        let n = usize::try_from(*len).map_err(|_| Error::Io("ORC length overflow".into()))?;
        let end = off
            .checked_add(n)
            .ok_or_else(|| Error::Io("ORC length overflow".into()))?;
        if end > data.len() {
            return Err(Error::Io("ORC byte data truncated".into()));
        }
        out.push(data[off..end].to_vec());
        off = end;
    }
    Ok(out)
}

fn split_bytes(data: &[u8], lengths: &[i64]) -> Result<Vec<String>> {
    split_raw(data, lengths)?
        .into_iter()
        .map(|b| {
            String::from_utf8(b).map_err(|e| Error::Io(format!("ORC STRING is not UTF-8: {e}")))
        })
        .collect()
}

fn split_streams(data: &[u8], sf: &StripeFooter) -> Result<ColumnStreams> {
    let mut map = ColumnStreams::new();
    let mut off = 0usize;
    for stream in &sf.streams {
        let end = off
            .checked_add(stream.length as usize)
            .ok_or_else(|| Error::Io("ORC stream length overflow".into()))?;
        if end > data.len() {
            return Err(Error::Io("ORC stripe data truncated".into()));
        }
        map.entry(stream.column)
            .or_default()
            .push((stream.kind, data[off..end].to_vec()));
        off = end;
    }
    Ok(map)
}

struct PostScript {
    footer_length: u64,
    compression: u32,
}

struct StripeInfo {
    offset: u64,
    index_length: u64,
    data_length: u64,
    footer_length: u64,
    number_of_rows: u64,
}

struct FileFooter {
    types: Vec<OrcType>,
    stripes: Vec<StripeInfo>,
}

struct StripeFooter {
    streams: Vec<StreamMeta>,
}

struct StreamMeta {
    kind: u32,
    column: u32,
    length: u64,
}

fn encode_postscript(footer_length: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    put_varint_field(&mut buf, 1, footer_length);
    // version 0.12 packed
    let mut packed = Vec::new();
    put_varint(&mut packed, 0);
    put_varint(&mut packed, 12);
    put_bytes_field(&mut buf, 4, &packed);
    put_varint_field(&mut buf, 6, 1);
    put_bytes_field(&mut buf, 8, MAGIC);
    buf
}

fn decode_postscript(bytes: &[u8]) -> Result<PostScript> {
    let fields = decode_proto(bytes)?;
    Ok(PostScript {
        footer_length: varint_field(&fields, 1).unwrap_or(0),
        compression: varint_field(&fields, 2).unwrap_or(0) as u32,
    })
}

fn encode_file_footer(
    header_length: u64,
    content_length: u64,
    types: &[OrcType],
    stripe_offset: u64,
    data_length: u64,
    footer_length: u64,
    number_of_rows: u64,
) -> Vec<u8> {
    let mut buf = Vec::new();
    put_varint_field(&mut buf, 1, header_length);
    put_varint_field(&mut buf, 2, content_length);
    for ty in types {
        put_bytes_field(&mut buf, 3, &encode_type(ty));
    }
    let mut stripe = Vec::new();
    put_varint_field(&mut stripe, 1, stripe_offset);
    put_varint_field(&mut stripe, 3, data_length);
    put_varint_field(&mut stripe, 4, footer_length);
    put_varint_field(&mut stripe, 5, number_of_rows);
    put_bytes_field(&mut buf, 4, &stripe);
    put_varint_field(&mut buf, 6, number_of_rows);
    buf
}

fn encode_type(ty: &OrcType) -> Vec<u8> {
    let mut buf = Vec::new();
    put_varint_field(&mut buf, 1, u64::from(ty.kind));
    if !ty.subtypes.is_empty() {
        let mut packed = Vec::new();
        for s in &ty.subtypes {
            put_varint(&mut packed, u64::from(*s));
        }
        put_bytes_field(&mut buf, 2, &packed);
    }
    for name in &ty.field_names {
        put_bytes_field(&mut buf, 3, name.as_bytes());
    }
    buf
}

fn decode_file_footer(bytes: &[u8]) -> Result<FileFooter> {
    let fields = decode_proto(bytes)?;
    let mut types = Vec::new();
    for raw in bytes_fields(&fields, 3) {
        types.push(decode_type(&raw)?);
    }
    let mut stripes = Vec::new();
    for raw in bytes_fields(&fields, 4) {
        stripes.push(decode_stripe_info(&raw)?);
    }
    Ok(FileFooter { types, stripes })
}

fn decode_type(bytes: &[u8]) -> Result<OrcType> {
    let fields = decode_proto(bytes)?;
    let kind = varint_field(&fields, 1).unwrap_or(0) as u32;
    let mut subtypes = Vec::new();
    for packed in bytes_fields(&fields, 2) {
        let mut i = 0;
        while i < packed.len() {
            let (v, n) = read_varint(&packed[i..])?;
            subtypes.push(v as u32);
            i += n;
        }
    }
    for v in varint_fields(&fields, 2) {
        subtypes.push(v as u32);
    }
    let field_names = bytes_fields(&fields, 3)
        .into_iter()
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .collect();
    Ok(OrcType {
        kind,
        subtypes,
        field_names,
    })
}

fn decode_stripe_info(bytes: &[u8]) -> Result<StripeInfo> {
    let fields = decode_proto(bytes)?;
    Ok(StripeInfo {
        offset: varint_field(&fields, 1).unwrap_or(0),
        index_length: varint_field(&fields, 2).unwrap_or(0),
        data_length: varint_field(&fields, 3).unwrap_or(0),
        footer_length: varint_field(&fields, 4).unwrap_or(0),
        number_of_rows: varint_field(&fields, 5).unwrap_or(0),
    })
}

fn encode_stripe_footer(streams: &[StreamOut], n_columns: usize) -> Vec<u8> {
    let mut buf = Vec::new();
    for stream in streams {
        let mut msg = Vec::new();
        put_varint_field(&mut msg, 1, u64::from(stream.kind));
        put_varint_field(&mut msg, 2, u64::from(stream.column));
        put_varint_field(&mut msg, 3, stream.bytes.len() as u64);
        put_bytes_field(&mut buf, 1, &msg);
    }
    for _ in 0..n_columns {
        // ColumnEncoding.kind DIRECT = 0, omit field.
        put_bytes_field(&mut buf, 2, &[]);
    }
    buf
}

fn decode_stripe_footer(bytes: &[u8]) -> Result<StripeFooter> {
    let fields = decode_proto(bytes)?;
    let mut streams = Vec::new();
    for raw in bytes_fields(&fields, 1) {
        let f = decode_proto(&raw)?;
        streams.push(StreamMeta {
            kind: varint_field(&f, 1).unwrap_or(0) as u32,
            column: varint_field(&f, 2).unwrap_or(0) as u32,
            length: varint_field(&f, 3).unwrap_or(0),
        });
    }
    Ok(StripeFooter { streams })
}

enum ProtoVal {
    Varint(u64),
    Bytes(Vec<u8>),
}

fn decode_proto(bytes: &[u8]) -> Result<Vec<(u32, ProtoVal)>> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let (key, n) = read_varint(&bytes[i..])?;
        i += n;
        let field = (key >> 3) as u32;
        let wire = (key & 7) as u32;
        match wire {
            0 => {
                let (v, n) = read_varint(&bytes[i..])?;
                i += n;
                out.push((field, ProtoVal::Varint(v)));
            }
            1 => {
                i = i
                    .checked_add(8)
                    .ok_or_else(|| Error::Io("ORC protobuf overflow".into()))?;
            }
            2 => {
                let (len, n) = read_varint(&bytes[i..])?;
                i += n;
                let end = i
                    .checked_add(len as usize)
                    .ok_or_else(|| Error::Io("ORC protobuf overflow".into()))?;
                if end > bytes.len() {
                    return Err(Error::Io("ORC protobuf truncated".into()));
                }
                out.push((field, ProtoVal::Bytes(bytes[i..end].to_vec())));
                i = end;
            }
            5 => {
                i = i
                    .checked_add(4)
                    .ok_or_else(|| Error::Io("ORC protobuf overflow".into()))?;
            }
            other => {
                return Err(Error::Io(format!("ORC protobuf wire type {other}")));
            }
        }
    }
    Ok(out)
}

fn varint_field(fields: &[(u32, ProtoVal)], id: u32) -> Option<u64> {
    fields.iter().find_map(|(f, v)| match (f, v) {
        (fid, ProtoVal::Varint(n)) if *fid == id => Some(*n),
        _ => None,
    })
}

fn varint_fields(fields: &[(u32, ProtoVal)], id: u32) -> Vec<u64> {
    fields
        .iter()
        .filter_map(|(f, v)| match (f, v) {
            (fid, ProtoVal::Varint(n)) if *fid == id => Some(*n),
            _ => None,
        })
        .collect()
}

fn bytes_fields(fields: &[(u32, ProtoVal)], id: u32) -> Vec<Vec<u8>> {
    fields
        .iter()
        .filter_map(|(f, v)| match (f, v) {
            (fid, ProtoVal::Bytes(b)) if *fid == id => Some(b.clone()),
            _ => None,
        })
        .collect()
}

fn put_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut b = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            b |= 0x80;
            buf.push(b);
        } else {
            buf.push(b);
            break;
        }
    }
}

fn put_varint_field(buf: &mut Vec<u8>, field: u32, v: u64) {
    put_varint(buf, u64::from(field << 3));
    put_varint(buf, v);
}

fn put_bytes_field(buf: &mut Vec<u8>, field: u32, bytes: &[u8]) {
    put_varint(buf, u64::from((field << 3) | 2));
    put_varint(buf, bytes.len() as u64);
    buf.extend_from_slice(bytes);
}

fn read_varint(bytes: &[u8]) -> Result<(u64, usize)> {
    let mut result = 0u64;
    let mut shift = 0;
    for (i, b) in bytes.iter().enumerate() {
        if shift >= 64 {
            return Err(Error::Io("ORC varint overflow".into()));
        }
        result |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
    }
    Err(Error::Io("ORC varint truncated".into()))
}

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

fn byte_rle_encode(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let n = (bytes.len() - i).min(127);
        out.push((-(n as i8)) as u8);
        out.extend_from_slice(&bytes[i..i + n]);
        i += n;
    }
    out
}

fn byte_rle_decode(data: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < data.len() {
        let header = data[i] as i8;
        i += 1;
        if header < 0 {
            let n = literal_run_len(header);
            let end = i
                .checked_add(n)
                .ok_or_else(|| Error::Io("ORC byte RLE overflow".into()))?;
            if end > data.len() {
                return Err(Error::Io("ORC byte RLE truncated".into()));
            }
            out.extend_from_slice(&data[i..end]);
            i = end;
        } else {
            let n = header as usize + 3;
            if i >= data.len() {
                return Err(Error::Io("ORC byte RLE truncated".into()));
            }
            let v = data[i];
            i += 1;
            out.extend(std::iter::repeat_n(v, n));
        }
    }
    Ok(out)
}

fn int_rle_encode(values: &[i64], signed: bool) -> Vec<u8> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < values.len() {
        let n = (values.len() - i).min(127);
        out.push((-(n as i8)) as u8);
        for v in &values[i..i + n] {
            if signed {
                put_varint(&mut out, zigzag(*v));
            } else {
                put_varint(&mut out, *v as u64);
            }
        }
        i += n;
    }
    out
}

fn int_rle_decode(data: &[u8], signed: bool) -> Result<Vec<i64>> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < data.len() {
        let header = data[i] as i8;
        i += 1;
        let decode_one = |slice: &[u8]| -> Result<(i64, usize)> {
            let (v, n) = read_varint(slice)?;
            Ok((if signed { unzigzag(v) } else { v as i64 }, n))
        };
        if header < 0 {
            let n = literal_run_len(header);
            for _ in 0..n {
                let (v, used) = decode_one(&data[i..])?;
                out.push(v);
                i += used;
            }
        } else {
            let n = header as usize + 3;
            if i >= data.len() {
                return Err(Error::Io("ORC integer RLE truncated".into()));
            }
            let delta = data[i] as i8 as i64;
            i += 1;
            let (mut cur, used) = decode_one(&data[i..])?;
            i += used;
            out.push(cur);
            for _ in 1..n {
                cur += delta;
                out.push(cur);
            }
        }
    }
    Ok(out)
}

fn literal_run_len(header: i8) -> usize {
    if header == i8::MIN {
        128
    } else {
        (-header) as usize
    }
}

fn pack_bits(bits: &[bool]) -> Vec<u8> {
    let mut bytes = vec![0u8; bits.len().div_ceil(8)];
    for (i, bit) in bits.iter().enumerate() {
        if *bit {
            bytes[i / 8] |= 1 << (i % 8);
        }
    }
    bytes
}

fn unpack_bits(bytes: &[u8], n: usize) -> Vec<bool> {
    (0..n)
        .map(|i| bytes.get(i / 8).copied().unwrap_or(0) & (1 << (i % 8)) != 0)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringBuilder;
    use arrow::array::{ListBuilder, StructArray};

    fn tmp(suffix: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "mizuchi-orc-{}-{}-{suffix}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn nested_batch() -> Batch {
        let name = StringArray::from(vec![Some("a"), Some("b")]);
        let nested = StructArray::from(vec![(
            Arc::new(Field::new("name", DataType::Utf8, true)),
            Arc::new(name) as ArrayRef,
        )]);
        let mut tags = ListBuilder::new(StringBuilder::new());
        tags.values().append_value("x");
        tags.values().append_value("y");
        tags.append(true);
        tags.values().append_value("z");
        tags.append(true);
        let tags = tags.finish();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new(
                "nested",
                DataType::Struct(Fields::from(vec![Field::new("name", DataType::Utf8, true)])),
                true,
            ),
            Field::new("tags", tags.data_type().clone(), true),
        ]));
        Batch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
                Arc::new(nested),
                Arc::new(tags),
            ],
        )
        .unwrap()
    }

    #[test]
    fn orc_nested_round_trip_and_projection() {
        let batch = nested_batch();
        let path = tmp("nested.orc");
        write_file(&path, std::slice::from_ref(&batch)).unwrap();
        let out = read_file(&path, None, BatchPolicy::default()).unwrap();
        assert_eq!(out[0].num_rows(), 2);
        assert_eq!(out[0].num_columns(), 3);
        let names = ["id".to_string(), "tags".to_string()];
        let projected = read_file(&path, Some(&names), BatchPolicy::default()).unwrap();
        assert_eq!(projected[0].num_columns(), 2);
        assert!(projected[0].column_by_name("nested").is_none());
        assert_eq!(
            projected[0]
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[1, 2]
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn orc_integer_rle_round_trips_long_runs() {
        let n = 200i64;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = Batch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(
                (0..n).map(Some).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        let path = tmp("rle.orc");
        write_file(&path, std::slice::from_ref(&batch)).unwrap();
        let out = read_file(&path, None, BatchPolicy::unlimited()).unwrap();
        let col = out[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.len(), 200);
        assert_eq!(col.value(127), 127);
        assert_eq!(col.value(199), 199);
        let _ = fs::remove_file(path);
    }
}
