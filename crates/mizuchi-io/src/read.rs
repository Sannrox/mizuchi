//! File readers that yield Arrow record batches.
//!
//! Callers pass the path. Parquet projection uses the file column mask so
//! unread columns are not decoded. Predicates skip row groups from footer
//! min/max, skip pages from the column index, and skip equality misses in a
//! bloom filter when one is stored. Remaining rows are filtered while the
//! predicate columns are decoded, then unread surviving columns are
//! materialized. CSV schema inference is optional; without it every column is
//! Utf8. JSON is newline-delimited with inference. Avro is Object Container
//! Files. ORC is the uncompressed DIRECT subset in [`crate::orc`].

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, RecordBatch};
use arrow::csv::ReaderBuilder;
use arrow::csv::reader::Format;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowFilter, RowSelection,
    RowSelector,
};
use parquet::bloom_filter::Sbbf;
use parquet::file::metadata::PageIndexPolicy;
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::reader::ChunkReader;
use parquet::file::statistics::Statistics;

use crate::crypto::EncryptionSession;
use mizuchi_core::batch::{Batch, BatchPolicy, DEFAULT_MAX_ROWS};
use mizuchi_core::error::{Error, Result};
use mizuchi_core::expr::{BinaryOp, Expr};
use mizuchi_core::schema::from_arrow_schema;

/// Int64 min/max from a Parquet column chunk footer.
#[derive(Clone, Debug)]
pub struct ColumnRange {
    /// Column name.
    pub name: String,
    /// Minimum stored value, if present.
    pub min_i64: Option<i64>,
    /// Maximum stored value, if present.
    pub max_i64: Option<i64>,
}

/// Float64 min/max from a Parquet column chunk footer.
#[derive(Clone, Debug)]
pub struct FloatRange {
    /// Column name.
    pub name: String,
    /// Minimum stored value, if present and finite.
    pub min_f64: Option<f64>,
    /// Maximum stored value, if present and finite.
    pub max_f64: Option<f64>,
}

/// Footer facts for one Parquet row group, used to skip groups before decode.
#[derive(Clone, Debug)]
pub struct RowGroupStats {
    /// Zero-based index in the file.
    pub index: usize,
    /// Rows in this group.
    pub num_rows: i64,
    /// Root column names in file order.
    pub column_names: Vec<String>,
    /// Int64 (and Int32, widened) column ranges from footer statistics.
    pub i64_ranges: Vec<ColumnRange>,
    /// Float64 (and Float32, widened) column ranges from footer statistics.
    pub f64_ranges: Vec<FloatRange>,
}

/// Row groups and compressed bytes considered versus kept after prune.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ParquetScanStats {
    /// Row groups in the file.
    pub row_groups_total: usize,
    /// Row groups that may still match.
    pub row_groups_kept: usize,
    /// Compressed bytes of projected columns in every group.
    pub compressed_bytes_total: u64,
    /// Compressed bytes of projected columns that the scan may still read.
    pub compressed_bytes_kept: u64,
}

/// Options for [`read_parquet`].
#[derive(Default)]
pub struct ParquetReadOptions<'a> {
    /// Column names to decode. `None` reads every column.
    pub projection: Option<&'a [String]>,
    /// Return true to skip this row group. `None` keeps every group.
    pub prune_row_group: Option<&'a dyn Fn(&RowGroupStats) -> bool>,
    /// Comparison predicates used to skip row groups from footer min/max,
    /// skip pages from the column index, skip bloom misses, and filter rows
    /// while decoding predicate columns.
    pub filters: Option<&'a [Expr]>,
    /// Morsel size for decoded batches.
    pub policy: BatchPolicy,
    /// Unwrap grant used to recover DEKs from Parquet key metadata.
    pub encryption: Option<&'a EncryptionSession>,
}

/// Options for [`read_csv`].
#[derive(Clone, Debug, Default)]
pub struct CsvReadOptions<'a> {
    /// First row is column names.
    pub has_header: bool,
    /// Scan values to pick types. When false, every column is Utf8.
    pub infer_schema: bool,
    /// Column names to keep. `None` keeps every column.
    pub projection: Option<&'a [String]>,
    /// Morsel size for decoded batches.
    pub policy: BatchPolicy,
}

/// Options for [`read_json`].
#[derive(Clone, Debug, Default)]
pub struct JsonReadOptions<'a> {
    /// Column names to keep. `None` keeps every inferred column.
    pub projection: Option<&'a [String]>,
    /// Morsel size for decoded batches.
    pub policy: BatchPolicy,
}

/// Options for [`read_avro`].
#[derive(Clone, Debug, Default)]
pub struct AvroReadOptions<'a> {
    /// Column names to keep. `None` keeps every column.
    pub projection: Option<&'a [String]>,
    /// Morsel size for decoded batches.
    pub policy: BatchPolicy,
}

/// Options for [`read_orc`].
#[derive(Clone, Debug, Default)]
pub struct OrcReadOptions<'a> {
    /// Column names to keep. Unread ORC columns are not decoded.
    pub projection: Option<&'a [String]>,
    /// Morsel size for decoded batches.
    pub policy: BatchPolicy,
}

/// Load footer metadata including the optional page index.
pub fn parquet_reader_options() -> ArrowReaderOptions {
    parquet_reader_options_with(None).expect("plaintext reader options")
}

/// [`parquet_reader_options`] plus modular-encryption unwrap when `enc` is set.
pub fn parquet_reader_options_with(enc: Option<&EncryptionSession>) -> Result<ArrowReaderOptions> {
    let opts = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    match enc {
        Some(s) => Ok(opts.with_file_decryption_properties(s.decryption_properties()?)),
        None => Ok(opts),
    }
}

const PARE: &[u8; 4] = b"PARE";

/// Parquet 60 decrypts optionally: a plaintext file still opens when
/// decryption properties are set. Keyed reads require an encrypted footer.
pub(crate) fn require_encrypted_footer(
    bytes: &[u8],
    enc: Option<&EncryptionSession>,
) -> Result<()> {
    if enc.is_none() {
        return Ok(());
    }
    if bytes.len() < 4 || &bytes[bytes.len() - 4..] != PARE {
        return Err(Error::Io(
            "encrypted read requires encrypted parquet".into(),
        ));
    }
    Ok(())
}

pub(crate) fn require_encrypted_footer_file(
    file: &File,
    enc: Option<&EncryptionSession>,
) -> Result<()> {
    if enc.is_none() {
        return Ok(());
    }
    let mut magic = [0u8; 4];
    let mut peek = file.try_clone().map_err(|e| Error::Io(e.to_string()))?;
    peek.seek(SeekFrom::End(-4))
        .map_err(|e| Error::Io(e.to_string()))?;
    peek.read_exact(&mut magic)
        .map_err(|e| Error::Io(e.to_string()))?;
    if &magic != PARE {
        return Err(Error::Io(
            "encrypted read requires encrypted parquet".into(),
        ));
    }
    Ok(())
}

/// Read a Parquet file into engine batches.
pub fn read_parquet(path: impl AsRef<Path>, opts: ParquetReadOptions<'_>) -> Result<Vec<Batch>> {
    Ok(read_parquet_reported(path, opts)?.0)
}

/// [`read_parquet`] plus row-group and compressed-byte accounting after prune.
pub fn read_parquet_reported(
    path: impl AsRef<Path>,
    opts: ParquetReadOptions<'_>,
) -> Result<(Vec<Batch>, ParquetScanStats)> {
    let path = path.as_ref();
    let file = File::open(path).map_err(|e| io_err(path, e))?;
    require_encrypted_footer_file(&file, opts.encryption)?;
    let bloom = file.try_clone().ok();
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
        file,
        parquet_reader_options_with(opts.encryption)?,
    )
    .map_err(|e| Error::Io(format!("{}: {e}", path.display())))?;
    read_parquet_from_builder(builder, &opts, &path.display().to_string(), bloom.as_ref())
}

/// Decode a Parquet builder (file or object bytes) with the same prune rules.
pub fn read_parquet_from_builder<R>(
    builder: ParquetRecordBatchReaderBuilder<R>,
    opts: &ParquetReadOptions<'_>,
    label: &str,
    bloom: Option<&R>,
) -> Result<(Vec<Batch>, ParquetScanStats)>
where
    R: ChunkReader + 'static,
{
    let parquet_schema = builder.parquet_schema().clone();
    let arrow_schema = builder.schema().clone();
    from_arrow_schema(arrow_schema.clone())?;

    let column_names: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    let flat = builder
        .metadata()
        .row_groups()
        .first()
        .is_none_or(|rg| rg.columns().len() == column_names.len());

    let mut keep_groups = Vec::new();
    let mut compressed_bytes_total = 0u64;
    let mut compressed_bytes_kept = 0u64;
    for (index, rg) in builder.metadata().row_groups().iter().enumerate() {
        let group_bytes = projected_group_bytes(rg, opts.projection, &column_names);
        compressed_bytes_total += group_bytes;
        let (i64_ranges, f64_ranges) = footer_ranges(rg, &column_names);
        let stats = RowGroupStats {
            index,
            num_rows: rg.num_rows(),
            column_names: column_names.clone(),
            i64_ranges,
            f64_ranges,
        };
        let skip_hook = opts
            .prune_row_group
            .map(|prune| prune(&stats))
            .unwrap_or(false);
        let skip_stats = opts
            .filters
            .map(|preds| preds.iter().any(|p| row_group_cannot_match(&stats, p)))
            .unwrap_or(false);
        let skip_bloom = opts
            .filters
            .map(|preds| {
                bloom.is_some_and(|reader| bloom_cannot_match(rg, &column_names, preds, reader))
            })
            .unwrap_or(false);
        if skip_hook || skip_stats || skip_bloom {
            continue;
        }
        keep_groups.push(index);
        compressed_bytes_kept += group_bytes;
    }

    let scan = ParquetScanStats {
        row_groups_total: builder.metadata().num_row_groups(),
        row_groups_kept: keep_groups.len(),
        compressed_bytes_total,
        compressed_bytes_kept,
    };

    if keep_groups.is_empty() {
        return Ok((Vec::new(), scan));
    }

    let page_selectors = opts.filters.and_then(|preds| {
        page_selection(
            builder.metadata().as_ref(),
            &keep_groups,
            &column_names,
            preds,
            flat,
        )
    });

    let batch_size = opts.policy.max_rows().unwrap_or(DEFAULT_MAX_ROWS);
    let mut builder = builder
        .with_batch_size(batch_size)
        .with_row_groups(keep_groups);
    if let Some(selectors) = page_selectors {
        builder = builder.with_row_selection(RowSelection::from(selectors));
    }
    if let Some(names) = opts.projection {
        let mask = projection_mask(&parquet_schema, &arrow_schema, names)?;
        builder = builder.with_projection(mask);
    }
    if let Some(preds) = opts.filters
        && let Some(filter) = row_filter_from_preds(&parquet_schema, &arrow_schema, preds)
    {
        builder = builder.with_row_filter(filter);
    }

    let reader = builder
        .build()
        .map_err(|e| Error::Io(format!("{label}: {e}")))?;
    Ok((collect_reader(reader, &opts.policy)?, scan))
}

/// Read a CSV file into engine batches.
pub fn read_csv(path: impl AsRef<Path>, opts: CsvReadOptions<'_>) -> Result<Vec<Batch>> {
    let path = path.as_ref();
    let mut file = File::open(path).map_err(|e| io_err(path, e))?;
    let format = Format::default().with_header(opts.has_header);
    let (inferred, _) = format
        .infer_schema(&mut file, None)
        .map_err(|e| Error::Io(format!("{}: {e}", path.display())))?;
    let schema = if opts.infer_schema {
        inferred
    } else {
        Schema::new(
            inferred
                .fields()
                .iter()
                .map(|f| Field::new(f.name(), DataType::Utf8, true))
                .collect::<Vec<_>>(),
        )
    };
    let schema = from_arrow_schema(std::sync::Arc::new(schema))?;
    let projection = csv_projection_indices(schema.as_ref(), opts.projection)?;
    file.seek(SeekFrom::Start(0)).map_err(|e| io_err(path, e))?;
    let batch_size = opts.policy.max_rows().unwrap_or(DEFAULT_MAX_ROWS);
    let mut builder = ReaderBuilder::new(schema)
        .with_format(format)
        .with_batch_size(batch_size);
    if let Some(idx) = projection {
        builder = builder.with_projection(idx);
    }
    let reader = builder
        .build(file)
        .map_err(|e| Error::Io(format!("{}: {e}", path.display())))?;
    collect_csv(reader, &opts.policy)
}

/// Read newline-delimited JSON. Schema is inferred from the file; unread
/// projected columns are omitted from the decoder schema.
pub fn read_json(path: impl AsRef<Path>, opts: JsonReadOptions<'_>) -> Result<Vec<Batch>> {
    let path = path.as_ref();
    let file = File::open(path).map_err(|e| io_err(path, e))?;
    let mut reader = BufReader::new(file);
    let (inferred, _) = arrow::json::reader::infer_json_schema(&mut reader, None)
        .map_err(|e| Error::Io(format!("{}: {e}", path.display())))?;
    let schema = project_schema(Arc::new(inferred), opts.projection)?;
    let schema = from_arrow_schema(schema)?;
    reader
        .seek(SeekFrom::Start(0))
        .map_err(|e| io_err(path, e))?;
    let batch_size = opts.policy.max_rows().unwrap_or(DEFAULT_MAX_ROWS);
    let json_reader = arrow::json::ReaderBuilder::new(schema)
        .with_batch_size(batch_size)
        .build(reader)
        .map_err(|e| Error::Io(format!("{}: {e}", path.display())))?;
    collect_reader(json_reader, &opts.policy)
}

/// Read an Avro Object Container File. Schema comes from the file header.
pub fn read_avro(path: impl AsRef<Path>, opts: AvroReadOptions<'_>) -> Result<Vec<Batch>> {
    let path = path.as_ref();
    let file = File::open(path).map_err(|e| io_err(path, e))?;
    let header_reader = arrow_avro::reader::ReaderBuilder::new()
        .build(BufReader::new(file))
        .map_err(|e| Error::Io(format!("{}: {e}", path.display())))?;
    let indices = projection_indices(header_reader.schema().as_ref(), opts.projection)?;
    let file = File::open(path).map_err(|e| io_err(path, e))?;
    let batch_size = opts.policy.max_rows().unwrap_or(DEFAULT_MAX_ROWS);
    let mut builder = arrow_avro::reader::ReaderBuilder::new().with_batch_size(batch_size);
    if let Some(idx) = indices {
        builder = builder.with_projection(idx);
    }
    let reader = builder
        .build(BufReader::new(file))
        .map_err(|e| Error::Io(format!("{}: {e}", path.display())))?;
    collect_reader(reader, &opts.policy)
}

/// Read an uncompressed ORC file. Projection skips unread columns.
pub fn read_orc(path: impl AsRef<Path>, opts: OrcReadOptions<'_>) -> Result<Vec<Batch>> {
    crate::orc::read_file(path, opts.projection, opts.policy)
}

fn projection_mask(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    arrow_schema: &Schema,
    names: &[String],
) -> Result<ProjectionMask> {
    let mut indices = Vec::with_capacity(names.len());
    for name in names {
        let idx = arrow_schema
            .index_of(name)
            .map_err(|_| Error::Plan(format!("projection column not in file: {name}")))?;
        indices.push(idx);
    }
    Ok(ProjectionMask::roots(parquet_schema, indices))
}

fn csv_projection_indices(
    schema: &Schema,
    projection: Option<&[String]>,
) -> Result<Option<Vec<usize>>> {
    projection_indices(schema, projection)
}

fn projection_indices(
    schema: &Schema,
    projection: Option<&[String]>,
) -> Result<Option<Vec<usize>>> {
    let Some(names) = projection else {
        return Ok(None);
    };
    let mut indices = Vec::with_capacity(names.len());
    for name in names {
        let idx = schema
            .index_of(name)
            .map_err(|_| Error::Plan(format!("projection column not in file: {name}")))?;
        indices.push(idx);
    }
    Ok(Some(indices))
}

fn project_schema(schema: SchemaRef, projection: Option<&[String]>) -> Result<SchemaRef> {
    let Some(names) = projection else {
        return Ok(schema);
    };
    let fields: Vec<Field> = names
        .iter()
        .map(|name| {
            let idx = schema
                .index_of(name)
                .map_err(|_| Error::Plan(format!("projection column not in file: {name}")))?;
            Ok(schema.field(idx).clone())
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn collect_reader<I>(reader: I, policy: &BatchPolicy) -> Result<Vec<Batch>>
where
    I: Iterator<Item = arrow::error::Result<RecordBatch>>,
{
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| Error::Io(e.to_string()))?;
        let batch = Batch::from_arrow(batch)?;
        out.extend(
            batch
                .split_by_policy(policy)
                .into_iter()
                .filter(|b| b.num_rows() > 0),
        );
    }
    Ok(out)
}

fn collect_csv<R: std::io::Read>(
    reader: arrow::csv::Reader<R>,
    policy: &BatchPolicy,
) -> Result<Vec<Batch>> {
    collect_reader(reader, policy)
}

fn io_err(path: &Path, err: std::io::Error) -> Error {
    Error::Io(format!("{}: {err}", path.display()))
}

/// True when footer min/max prove `pred` cannot hold for any row in the group.
pub fn row_group_cannot_match(stats: &RowGroupStats, pred: &Expr) -> bool {
    match pred {
        Expr::Binary(BinaryOp::And, l, r) => {
            row_group_cannot_match(stats, l) || row_group_cannot_match(stats, r)
        }
        Expr::Binary(BinaryOp::Or, l, r) => {
            row_group_cannot_match(stats, l) && row_group_cannot_match(stats, r)
        }
        Expr::Binary(op, l, r) => {
            let Some((name, lit, swapped)) = col_cmp_lit(l, r) else {
                return false;
            };
            let op = if swapped { swap_cmp(*op) } else { *op };
            match lit {
                NumLit::I64(v) => {
                    let Some(range) = stats.i64_ranges.iter().find(|c| c.name == name) else {
                        return false;
                    };
                    i64_range_cannot_match(range.min_i64, range.max_i64, op, v)
                }
                NumLit::F64(v) => {
                    let Some(range) = stats.f64_ranges.iter().find(|c| c.name == name) else {
                        return false;
                    };
                    f64_range_cannot_match(range.min_f64, range.max_f64, op, v)
                }
            }
        }
        _ => false,
    }
}

fn swap_cmp(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        other => other,
    }
}

#[derive(Clone, Copy)]
enum NumLit {
    I64(i64),
    F64(f64),
}

enum EqValue {
    I64(i64),
    F64(f64),
    Utf8(String),
}

fn col_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(n) => Some(n.clone()),
        _ => None,
    }
}

fn col_cmp_lit<'a>(left: &'a Expr, right: &'a Expr) -> Option<(String, NumLit, bool)> {
    match (
        col_name(left),
        numeric_lit(right),
        col_name(right),
        numeric_lit(left),
    ) {
        (Some(n), Some(v), _, _) => Some((n, v, false)),
        (_, _, Some(n), Some(v)) => Some((n, v, true)),
        _ => None,
    }
}

fn numeric_lit(expr: &Expr) -> Option<NumLit> {
    let Expr::Literal(arr) = expr else {
        return None;
    };
    if arr.len() != 1 || arr.is_null(0) {
        return None;
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return Some(NumLit::I64(a.value(0)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int32Array>() {
        return Some(NumLit::I64(i64::from(a.value(0))));
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
        return Some(NumLit::F64(a.value(0)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Float32Array>() {
        return Some(NumLit::F64(f64::from(a.value(0))));
    }
    None
}

fn eq_value(expr: &Expr) -> Option<EqValue> {
    let Expr::Literal(arr) = expr else {
        return None;
    };
    if arr.len() != 1 || arr.is_null(0) {
        return None;
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return Some(EqValue::I64(a.value(0)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int32Array>() {
        return Some(EqValue::I64(i64::from(a.value(0))));
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
        return Some(EqValue::F64(a.value(0)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Float32Array>() {
        return Some(EqValue::F64(f64::from(a.value(0))));
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::StringArray>() {
        return Some(EqValue::Utf8(a.value(0).to_string()));
    }
    None
}

fn i64_range_cannot_match(min: Option<i64>, max: Option<i64>, op: BinaryOp, lit: i64) -> bool {
    match op {
        BinaryOp::Gt => max.is_some_and(|m| m <= lit),
        BinaryOp::GtEq => max.is_some_and(|m| m < lit),
        BinaryOp::Lt => min.is_some_and(|m| m >= lit),
        BinaryOp::LtEq => min.is_some_and(|m| m > lit),
        BinaryOp::Eq => max.is_some_and(|m| m < lit) || min.is_some_and(|m| m > lit),
        _ => false,
    }
}

fn f64_range_cannot_match(min: Option<f64>, max: Option<f64>, op: BinaryOp, lit: f64) -> bool {
    if !lit.is_finite() {
        return false;
    }
    let finite = |v: f64| v.is_finite().then_some(v);
    match op {
        BinaryOp::Gt => max.and_then(finite).is_some_and(|m| m <= lit),
        BinaryOp::GtEq => max.and_then(finite).is_some_and(|m| m < lit),
        BinaryOp::Lt => min.and_then(finite).is_some_and(|m| m >= lit),
        BinaryOp::LtEq => min.and_then(finite).is_some_and(|m| m > lit),
        BinaryOp::Eq => {
            max.and_then(finite).is_some_and(|m| m < lit)
                || min.and_then(finite).is_some_and(|m| m > lit)
        }
        _ => false,
    }
}

fn footer_ranges(
    rg: &parquet::file::metadata::RowGroupMetaData,
    column_names: &[String],
) -> (Vec<ColumnRange>, Vec<FloatRange>) {
    let mut i64_ranges = Vec::new();
    let mut f64_ranges = Vec::new();
    let cols = rg.columns();
    if cols.len() != column_names.len() {
        return (i64_ranges, f64_ranges);
    }
    for (ci, col) in cols.iter().enumerate() {
        let name = column_names[ci].clone();
        match col.statistics() {
            Some(Statistics::Int64(s)) => i64_ranges.push(ColumnRange {
                name,
                min_i64: s.min_opt().copied(),
                max_i64: s.max_opt().copied(),
            }),
            Some(Statistics::Int32(s)) => i64_ranges.push(ColumnRange {
                name,
                min_i64: s.min_opt().map(|v| i64::from(*v)),
                max_i64: s.max_opt().map(|v| i64::from(*v)),
            }),
            Some(Statistics::Double(s)) => f64_ranges.push(FloatRange {
                name,
                min_f64: s.min_opt().copied().filter(|v| v.is_finite()),
                max_f64: s.max_opt().copied().filter(|v| v.is_finite()),
            }),
            Some(Statistics::Float(s)) => f64_ranges.push(FloatRange {
                name,
                min_f64: s.min_opt().map(|v| f64::from(*v)).filter(|v| v.is_finite()),
                max_f64: s.max_opt().map(|v| f64::from(*v)).filter(|v| v.is_finite()),
            }),
            _ => {}
        }
    }
    (i64_ranges, f64_ranges)
}

fn projected_group_bytes(
    rg: &parquet::file::metadata::RowGroupMetaData,
    projection: Option<&[String]>,
    column_names: &[String],
) -> u64 {
    let cols = rg.columns();
    if cols.len() != column_names.len() {
        return rg.compressed_size().max(0) as u64;
    }
    let indices: Vec<usize> = match projection {
        None => (0..cols.len()).collect(),
        Some(names) => names
            .iter()
            .filter_map(|n| column_names.iter().position(|c| c == n))
            .collect(),
    };
    indices
        .into_iter()
        .map(|i| cols[i].compressed_size().max(0) as u64)
        .sum()
}

fn bloom_cannot_match<R: ChunkReader>(
    rg: &parquet::file::metadata::RowGroupMetaData,
    column_names: &[String],
    preds: &[Expr],
    reader: &R,
) -> bool {
    let cols = rg.columns();
    if cols.len() != column_names.len() {
        return false;
    }
    let mut eqs = Vec::new();
    for pred in preds {
        collect_eq(pred, &mut eqs);
    }
    for (name, value) in eqs {
        let Some(ci) = column_names.iter().position(|c| c == &name) else {
            continue;
        };
        let Ok(Some(filter)) = Sbbf::read_from_column_chunk(&cols[ci], reader) else {
            continue;
        };
        // Hash the physical width the writer inserted. Widened i64/f64
        // lookups on INT32/FLOAT chunks would miss present values.
        let miss = match (&value, cols[ci].column_type()) {
            (EqValue::I64(v), parquet::basic::Type::INT64) => !filter.check(v),
            (EqValue::I64(v), parquet::basic::Type::INT32) => match i32::try_from(*v) {
                Ok(narrow) => !filter.check(&narrow),
                Err(_) => true,
            },
            (EqValue::F64(v), parquet::basic::Type::DOUBLE) => !filter.check(v),
            (EqValue::F64(v), parquet::basic::Type::FLOAT) => {
                let narrow = *v as f32;
                if f64::from(narrow) != *v {
                    continue;
                }
                !filter.check(&narrow)
            }
            (EqValue::Utf8(v), parquet::basic::Type::BYTE_ARRAY) => !filter.check(v.as_str()),
            _ => continue,
        };
        if miss {
            return true;
        }
    }
    false
}

fn collect_eq(pred: &Expr, out: &mut Vec<(String, EqValue)>) {
    match pred {
        Expr::Binary(BinaryOp::And, l, r) => {
            collect_eq(l, out);
            collect_eq(r, out);
        }
        Expr::Binary(BinaryOp::Eq, l, r) => {
            if let (Some(n), Some(v)) = (col_name(l), eq_value(r)) {
                out.push((n, v));
            } else if let (Some(n), Some(v)) = (col_name(r), eq_value(l)) {
                out.push((n, v));
            }
        }
        _ => {}
    }
}

fn page_selection(
    metadata: &parquet::file::metadata::ParquetMetaData,
    keep_groups: &[usize],
    column_names: &[String],
    preds: &[Expr],
    flat: bool,
) -> Option<Vec<RowSelector>> {
    if !flat {
        return None;
    }
    let index = metadata.page_index()?;
    let mut selectors = Vec::new();
    let mut skipped_any = false;
    for &gi in keep_groups {
        let num_rows = metadata.row_group(gi).num_rows() as usize;
        let mut keep = vec![true; num_rows];
        for (ci, name) in column_names.iter().enumerate() {
            let Some(col_index) = index.column_index(gi, ci) else {
                continue;
            };
            let Some(offset) = index.offset_index(gi, ci) else {
                continue;
            };
            let locations = offset.page_locations();
            let n = col_index.num_pages() as usize;
            if n != locations.len() {
                continue;
            }
            for (page_i, loc) in locations.iter().enumerate() {
                if !page_cannot_match(col_index, page_i, name, preds) {
                    continue;
                }
                let start = loc.first_row_index as usize;
                let end = if page_i + 1 < locations.len() {
                    locations[page_i + 1].first_row_index as usize
                } else {
                    num_rows
                };
                let end = end.min(num_rows);
                if start < end {
                    keep[start..end].fill(false);
                    skipped_any = true;
                }
            }
        }
        selectors.extend(runs_from_keep(&keep));
    }
    if !skipped_any { None } else { Some(selectors) }
}

fn runs_from_keep(keep: &[bool]) -> Vec<RowSelector> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < keep.len() {
        let flag = keep[i];
        let mut n = 1usize;
        while i + n < keep.len() && keep[i + n] == flag {
            n += 1;
        }
        if flag {
            out.push(RowSelector::select(n));
        } else {
            out.push(RowSelector::skip(n));
        }
        i += n;
    }
    out
}

fn page_cannot_match(
    col_index: &ColumnIndexMetaData,
    page_i: usize,
    name: &str,
    preds: &[Expr],
) -> bool {
    preds
        .iter()
        .any(|p| page_pred_cannot_match(col_index, page_i, name, p))
}

fn page_pred_cannot_match(
    col_index: &ColumnIndexMetaData,
    page_i: usize,
    name: &str,
    pred: &Expr,
) -> bool {
    match pred {
        Expr::Binary(BinaryOp::And, l, r) => {
            page_pred_cannot_match(col_index, page_i, name, l)
                || page_pred_cannot_match(col_index, page_i, name, r)
        }
        Expr::Binary(op, l, r) => {
            let Some((col, lit, swapped)) = col_cmp_lit(l, r) else {
                return false;
            };
            if col != name {
                return false;
            }
            let op = if swapped { swap_cmp(*op) } else { *op };
            match (col_index, lit) {
                (ColumnIndexMetaData::INT64(idx), NumLit::I64(v)) => i64_range_cannot_match(
                    idx.min_value(page_i).copied(),
                    idx.max_value(page_i).copied(),
                    op,
                    v,
                ),
                (ColumnIndexMetaData::INT32(idx), NumLit::I64(v)) => i64_range_cannot_match(
                    idx.min_value(page_i).map(|x| i64::from(*x)),
                    idx.max_value(page_i).map(|x| i64::from(*x)),
                    op,
                    v,
                ),
                (ColumnIndexMetaData::DOUBLE(idx), NumLit::F64(v)) => f64_range_cannot_match(
                    idx.min_value(page_i).copied().filter(|x| x.is_finite()),
                    idx.max_value(page_i).copied().filter(|x| x.is_finite()),
                    op,
                    v,
                ),
                (ColumnIndexMetaData::FLOAT(idx), NumLit::F64(v)) => f64_range_cannot_match(
                    idx.min_value(page_i)
                        .map(|x| f64::from(*x))
                        .filter(|x| x.is_finite()),
                    idx.max_value(page_i)
                        .map(|x| f64::from(*x))
                        .filter(|x| x.is_finite()),
                    op,
                    v,
                ),
                (ColumnIndexMetaData::DOUBLE(idx), NumLit::I64(v)) => f64_range_cannot_match(
                    idx.min_value(page_i).copied().filter(|x| x.is_finite()),
                    idx.max_value(page_i).copied().filter(|x| x.is_finite()),
                    op,
                    v as f64,
                ),
                _ => false,
            }
        }
        _ => false,
    }
}

fn row_filter_from_preds(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    arrow_schema: &Schema,
    preds: &[Expr],
) -> Option<RowFilter> {
    let mut conjuncts = Vec::new();
    for pred in preds {
        flatten_and(pred, &mut conjuncts);
    }
    let mut boxed: Vec<Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>> = Vec::new();
    for pred in conjuncts {
        let mut names = Vec::new();
        expr_columns(&pred, &mut names);
        if names.is_empty() {
            continue;
        }
        let mut indices = Vec::new();
        for name in &names {
            if let Ok(idx) = arrow_schema.index_of(name) {
                indices.push(idx);
            }
        }
        if indices.is_empty() {
            continue;
        }
        let mask = ProjectionMask::roots(parquet_schema, indices);
        boxed.push(Box::new(ArrowPredicateFn::new(mask, move |batch| {
            let batch =
                Batch::from_arrow(batch).map_err(|e| ArrowError::ComputeError(e.to_string()))?;
            let arr = pred
                .eval(&batch)
                .map_err(|e| ArrowError::ComputeError(e.to_string()))?;
            arr.as_any()
                .downcast_ref::<BooleanArray>()
                .cloned()
                .ok_or_else(|| ArrowError::ComputeError("predicate did not yield Boolean".into()))
        })));
    }
    if boxed.is_empty() {
        None
    } else {
        Some(RowFilter::new(boxed))
    }
}

fn flatten_and(expr: &Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::Binary(BinaryOp::And, l, r) => {
            flatten_and(l, out);
            flatten_and(r, out);
        }
        other => out.push(other.clone()),
    }
}

fn expr_columns(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Column(n) => {
            if !out.iter().any(|c| c == n) {
                out.push(n.clone());
            }
        }
        Expr::Nth(_) | Expr::Literal(_) => {}
        Expr::Binary(_, l, r) => {
            expr_columns(l, out);
            expr_columns(r, out);
        }
        Expr::Unary(_, c) => expr_columns(c, out),
        Expr::Cast { expr, .. } => expr_columns(expr, out),
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                expr_columns(op, out);
            }
            for (w, t) in when_then {
                expr_columns(w, out);
                expr_columns(t, out);
            }
            if let Some(e) = else_expr {
                expr_columns(e, out);
            }
        }
        Expr::Coalesce(args) | Expr::Concat(args) | Expr::Array(args) => {
            for a in args {
                expr_columns(a, out);
            }
        }
        Expr::Substring { expr, .. } => expr_columns(expr, out),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_FILE: AtomicU64 = AtomicU64::new(0);

    fn test_path(ext: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "mizuchi-read-{}-{}-{}.{}",
            std::process::id(),
            TEST_FILE.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            ext
        ))
    }

    fn write_parquet(batches: &[RecordBatch], max_row_group: usize) -> std::path::PathBuf {
        write_parquet_with(
            batches,
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(max_row_group))
                .build(),
        )
    }

    fn write_parquet_with(batches: &[RecordBatch], props: WriterProperties) -> std::path::PathBuf {
        let path = test_path("parquet");
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batches[0].schema(), Some(props)).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();
        path
    }

    fn write_csv(body: &str) -> std::path::PathBuf {
        let path = test_path("csv");
        std::fs::write(&path, body).unwrap();
        path
    }

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("extra", DataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4)])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("c"),
                    Some("d"),
                ])),
                Arc::new(Int64Array::from(vec![
                    Some(10),
                    Some(20),
                    Some(30),
                    Some(40),
                ])),
            ],
        )
        .unwrap()
    }

    fn col_i64(batches: &[Batch], name: &str) -> Vec<Option<i64>> {
        let mut out = Vec::new();
        for batch in batches {
            let col = batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..col.len() {
                out.push(if col.is_null(i) {
                    None
                } else {
                    Some(col.value(i))
                });
            }
        }
        out
    }

    #[test]
    fn parquet_round_trip() {
        let path = write_parquet(&[sample_batch()], 1024);
        let out = read_parquet(&path, ParquetReadOptions::default()).unwrap();
        assert_eq!(
            col_i64(&out, "id"),
            vec![Some(1), Some(2), Some(3), Some(4)]
        );
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn parquet_projection_drops_unread_columns() {
        let path = write_parquet(&[sample_batch()], 1024);
        let names = ["id".to_string(), "name".to_string()];
        let out = read_parquet(
            &path,
            ParquetReadOptions {
                projection: Some(&names),
                ..ParquetReadOptions::default()
            },
        )
        .unwrap();
        assert_eq!(out[0].num_columns(), 2);
        assert!(out[0].column_by_name("extra").is_none());
        assert_eq!(
            col_i64(&out, "id"),
            vec![Some(1), Some(2), Some(3), Some(4)]
        );
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn parquet_row_group_prune_skips_groups() {
        let path = write_parquet(&[sample_batch()], 2);
        let prune = |s: &RowGroupStats| s.index == 0;
        let out = read_parquet(
            &path,
            ParquetReadOptions {
                prune_row_group: Some(&prune),
                ..ParquetReadOptions::default()
            },
        )
        .unwrap();
        assert_eq!(col_i64(&out, "id"), vec![Some(3), Some(4)]);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn csv_infers_types_with_header() {
        let path = write_csv("id,name\n1,alice\n2,bob\n");
        let out = read_csv(
            &path,
            CsvReadOptions {
                has_header: true,
                infer_schema: true,
                ..CsvReadOptions::default()
            },
        )
        .unwrap();
        assert_eq!(out[0].schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(col_i64(&out, "id"), vec![Some(1), Some(2)]);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn csv_without_infer_is_utf8() {
        let path = write_csv("id,name\n1,alice\n");
        let out = read_csv(
            &path,
            CsvReadOptions {
                has_header: true,
                infer_schema: false,
                ..CsvReadOptions::default()
            },
        )
        .unwrap();
        assert_eq!(out[0].schema().field(0).data_type(), &DataType::Utf8);
        let col = out[0].column_by_name("id").unwrap();
        let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "1");
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn csv_projection_keeps_requested_columns() {
        let path = write_csv("id,name,extra\n1,a,9\n2,b,8\n");
        let names = ["name".to_string()];
        let out = read_csv(
            &path,
            CsvReadOptions {
                has_header: true,
                infer_schema: true,
                projection: Some(&names),
                ..CsvReadOptions::default()
            },
        )
        .unwrap();
        assert_eq!(out[0].num_columns(), 1);
        assert!(out[0].column_by_name("id").is_none());
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn missing_file_fails_closed() {
        let err = read_parquet("/no/such/file.parquet", ParquetReadOptions::default()).unwrap_err();
        assert!(matches!(err, Error::Io(_)));
    }

    #[test]
    fn parquet_footer_stats_skip_row_groups() {
        let path = write_parquet(&[sample_batch()], 2);
        let pred = Expr::Binary(
            mizuchi_core::expr::BinaryOp::Gt,
            Box::new(Expr::column("id")),
            Box::new(Expr::lit_i64(2)),
        );
        let filters = [pred];
        let out = read_parquet(
            &path,
            ParquetReadOptions {
                filters: Some(&filters),
                projection: Some(&["id".to_string()]),
                ..ParquetReadOptions::default()
            },
        )
        .unwrap();
        assert_eq!(out[0].num_columns(), 1);
        assert_eq!(col_i64(&out, "id"), vec![Some(3), Some(4)]);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn parquet_nested_schema_does_not_prune_by_root_name() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new(
                "s",
                DataType::Struct(
                    vec![
                        Field::new("x", DataType::Int64, true),
                        Field::new("y", DataType::Int64, true),
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new("b", DataType::Int64, true),
        ]));
        let nested = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1)])),
                Arc::new(arrow::array::StructArray::from(vec![
                    (
                        Arc::new(Field::new("x", DataType::Int64, true)),
                        Arc::new(Int64Array::from(vec![Some(1)])) as arrow::array::ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("y", DataType::Int64, true)),
                        Arc::new(Int64Array::from(vec![Some(1)])) as arrow::array::ArrayRef,
                    ),
                ])),
                Arc::new(Int64Array::from(vec![Some(1000)])),
            ],
        )
        .unwrap();
        let path = write_parquet(&[nested], 1);
        let pred = Expr::Binary(
            mizuchi_core::expr::BinaryOp::Gt,
            Box::new(Expr::column("b")),
            Box::new(Expr::lit_i64(500)),
        );
        let filters = [pred];
        let out = read_parquet(
            &path,
            ParquetReadOptions {
                filters: Some(&filters),
                projection: Some(&["b".to_string()]),
                ..ParquetReadOptions::default()
            },
        )
        .unwrap();
        assert_eq!(col_i64(&out, "b"), vec![Some(1000)]);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn parquet_scan_stats_skips_row_groups_and_bytes() {
        let path = write_parquet(&[sample_batch()], 2);
        let pred = Expr::Binary(
            mizuchi_core::expr::BinaryOp::Gt,
            Box::new(Expr::column("id")),
            Box::new(Expr::lit_i64(2)),
        );
        let filters = [pred];
        let names = ["id".to_string()];
        let (out, stats) = read_parquet_reported(
            &path,
            ParquetReadOptions {
                filters: Some(&filters),
                projection: Some(&names),
                ..ParquetReadOptions::default()
            },
        )
        .unwrap();
        assert_eq!(stats.row_groups_total, 2);
        assert_eq!(stats.row_groups_kept, 1);
        assert!(stats.compressed_bytes_kept < stats.compressed_bytes_total);
        assert!(stats.compressed_bytes_kept > 0);
        assert_eq!(col_i64(&out, "id"), vec![Some(3), Some(4)]);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn parquet_or_eq_in_list_skips_row_groups() {
        let path = write_parquet(&[sample_batch()], 2);
        let pred = Expr::Binary(
            mizuchi_core::expr::BinaryOp::Or,
            Box::new(Expr::Binary(
                mizuchi_core::expr::BinaryOp::Eq,
                Box::new(Expr::column("id")),
                Box::new(Expr::lit_i64(1)),
            )),
            Box::new(Expr::Binary(
                mizuchi_core::expr::BinaryOp::Eq,
                Box::new(Expr::column("id")),
                Box::new(Expr::lit_i64(2)),
            )),
        );
        let filters = [pred];
        let names = ["id".to_string()];
        let (out, stats) = read_parquet_reported(
            &path,
            ParquetReadOptions {
                filters: Some(&filters),
                projection: Some(&names),
                ..ParquetReadOptions::default()
            },
        )
        .unwrap();
        assert_eq!(stats.row_groups_total, 2);
        assert_eq!(stats.row_groups_kept, 1);
        assert_eq!(col_i64(&out, "id"), vec![Some(1), Some(2)]);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn parquet_float_stats_skip_row_groups() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Float64, true),
            Field::new("qty", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::Float64Array::from(vec![
                    Some(0.01),
                    Some(0.02),
                    Some(0.06),
                    Some(0.07),
                ])),
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4)])),
            ],
        )
        .unwrap();
        let path = write_parquet(&[batch], 2);
        let pred = Expr::Binary(
            mizuchi_core::expr::BinaryOp::GtEq,
            Box::new(Expr::column("price")),
            Box::new(Expr::Literal(Arc::new(arrow::array::Float64Array::from(
                vec![0.05],
            )))),
        );
        let filters = [pred];
        let (out, stats) = read_parquet_reported(
            &path,
            ParquetReadOptions {
                filters: Some(&filters),
                projection: Some(&["price".to_string()]),
                ..ParquetReadOptions::default()
            },
        )
        .unwrap();
        assert_eq!(stats.row_groups_kept, 1);
        assert!(stats.compressed_bytes_kept < stats.compressed_bytes_total);
        let prices: Vec<f64> = out
            .iter()
            .flat_map(|b| {
                let col = b
                    .column_by_name("price")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .unwrap();
                (0..col.len()).map(|i| col.value(i))
            })
            .collect();
        assert_eq!(prices, vec![0.06, 0.07]);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn parquet_late_materialization_filters_mixed_group() {
        let path = write_parquet(&[sample_batch()], 1024);
        let pred = Expr::Binary(
            mizuchi_core::expr::BinaryOp::Gt,
            Box::new(Expr::column("id")),
            Box::new(Expr::lit_i64(2)),
        );
        let filters = [pred];
        let (out, stats) = read_parquet_reported(
            &path,
            ParquetReadOptions {
                filters: Some(&filters),
                projection: Some(&["id".to_string(), "name".to_string()]),
                ..ParquetReadOptions::default()
            },
        )
        .unwrap();
        assert_eq!(stats.row_groups_kept, stats.row_groups_total);
        assert_eq!(col_i64(&out, "id"), vec![Some(3), Some(4)]);
        assert_eq!(out[0].num_columns(), 2);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn parquet_page_index_skips_unmatched_pages() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let mut ids = Vec::new();
        let mut payload = Vec::new();
        for i in 0..40i64 {
            ids.push(Some(if i < 20 { i } else { 1000 + i }));
            payload.push(Some("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".to_string()));
        }
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(payload)),
            ],
        )
        .unwrap();
        let path = write_parquet_with(
            &[batch],
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(40))
                .set_data_page_row_count_limit(10)
                .build(),
        );
        let pred = Expr::Binary(
            mizuchi_core::expr::BinaryOp::GtEq,
            Box::new(Expr::column("id")),
            Box::new(Expr::lit_i64(1000)),
        );
        let filters = [pred];
        let names = ["id".to_string()];
        let (out, stats) = read_parquet_reported(
            &path,
            ParquetReadOptions {
                filters: Some(&filters),
                projection: Some(&names),
                ..ParquetReadOptions::default()
            },
        )
        .unwrap();
        assert_eq!(stats.row_groups_kept, 1);
        let values = col_i64(&out, "id");
        assert_eq!(values.len(), 20);
        assert!(values.iter().all(|v| v.unwrap() >= 1000));
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn parquet_bloom_skips_unmatched_equality() {
        let path = write_parquet_with(
            &[sample_batch()],
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(2))
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
                .set_bloom_filter_enabled(true)
                .build(),
        );
        let pred = Expr::Binary(
            mizuchi_core::expr::BinaryOp::Eq,
            Box::new(Expr::column("id")),
            Box::new(Expr::lit_i64(3)),
        );
        let filters = [pred];
        let (out, stats) = read_parquet_reported(
            &path,
            ParquetReadOptions {
                filters: Some(&filters),
                projection: Some(&["id".to_string()]),
                ..ParquetReadOptions::default()
            },
        )
        .unwrap();
        assert_eq!(stats.row_groups_total, 2);
        assert_eq!(stats.row_groups_kept, 1);
        assert!(stats.compressed_bytes_kept < stats.compressed_bytes_total);
        assert_eq!(col_i64(&out, "id"), vec![Some(3)]);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn parquet_bloom_int32_uses_physical_width() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                Some(4),
            ]))],
        )
        .unwrap();
        let path = write_parquet_with(
            &[batch],
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(2))
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
                .set_bloom_filter_enabled(true)
                .build(),
        );
        let pred = Expr::Binary(
            mizuchi_core::expr::BinaryOp::Eq,
            Box::new(Expr::column("id")),
            Box::new(Expr::Literal(Arc::new(arrow::array::Int32Array::from(
                vec![3],
            )))),
        );
        let filters = [pred];
        let (out, stats) = read_parquet_reported(
            &path,
            ParquetReadOptions {
                filters: Some(&filters),
                projection: Some(&["id".to_string()]),
                ..ParquetReadOptions::default()
            },
        )
        .unwrap();
        assert_eq!(stats.row_groups_total, 2);
        assert_eq!(stats.row_groups_kept, 1);
        let values: Vec<i32> = out
            .iter()
            .flat_map(|b| {
                let col = b
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .unwrap();
                (0..col.len()).map(|i| col.value(i))
            })
            .collect();
        assert_eq!(values, vec![3]);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn parquet_bloom_float32_uses_physical_width() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Float32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Float32Array::from(vec![
                Some(0.25),
                Some(0.5),
                Some(1.5),
                Some(2.5),
            ]))],
        )
        .unwrap();
        let path = write_parquet_with(
            &[batch],
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(2))
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
                .set_bloom_filter_enabled(true)
                .build(),
        );
        let pred = Expr::Binary(
            mizuchi_core::expr::BinaryOp::Eq,
            Box::new(Expr::column("price")),
            Box::new(Expr::Literal(Arc::new(arrow::array::Float32Array::from(
                vec![1.5],
            )))),
        );
        let filters = [pred];
        let (out, stats) = read_parquet_reported(
            &path,
            ParquetReadOptions {
                filters: Some(&filters),
                projection: Some(&["price".to_string()]),
                ..ParquetReadOptions::default()
            },
        )
        .unwrap();
        assert_eq!(stats.row_groups_total, 2);
        assert_eq!(stats.row_groups_kept, 1);
        let values: Vec<f32> = out
            .iter()
            .flat_map(|b| {
                let col = b
                    .column_by_name("price")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow::array::Float32Array>()
                    .unwrap();
                (0..col.len()).map(|i| col.value(i))
            })
            .collect();
        assert_eq!(values, vec![1.5]);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn parquet_clustered_q6_predicate_skips_groups() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_shipdate", DataType::Int64, true),
            Field::new("l_discount", DataType::Float64, true),
            Field::new("l_quantity", DataType::Float64, true),
            Field::new("l_extendedprice", DataType::Float64, true),
        ]));
        let mut dates = Vec::new();
        let mut disc = Vec::new();
        let mut qty = Vec::new();
        let mut price = Vec::new();
        // Four clustered groups of 4 rows: 1993, 1994, 1996, 1998.
        for (base, n) in [(100i64, 4), (800, 4), (1500, 4), (2400, 4)] {
            for i in 0..n {
                dates.push(Some(base + i));
                disc.push(Some(0.06));
                qty.push(Some(10.0));
                price.push(Some(1000.0));
            }
        }
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(dates)),
                Arc::new(arrow::array::Float64Array::from(disc)),
                Arc::new(arrow::array::Float64Array::from(qty)),
                Arc::new(arrow::array::Float64Array::from(price)),
            ],
        )
        .unwrap();
        let path = write_parquet(&[batch], 4);
        let pred = Expr::Binary(
            mizuchi_core::expr::BinaryOp::And,
            Box::new(Expr::Binary(
                mizuchi_core::expr::BinaryOp::GtEq,
                Box::new(Expr::column("l_shipdate")),
                Box::new(Expr::lit_i64(731)),
            )),
            Box::new(Expr::Binary(
                mizuchi_core::expr::BinaryOp::Lt,
                Box::new(Expr::column("l_shipdate")),
                Box::new(Expr::lit_i64(1096)),
            )),
        );
        let filters = [pred];
        let names = [
            "l_shipdate".to_string(),
            "l_discount".to_string(),
            "l_quantity".to_string(),
            "l_extendedprice".to_string(),
        ];
        let (out, stats) = read_parquet_reported(
            &path,
            ParquetReadOptions {
                filters: Some(&filters),
                projection: Some(&names),
                ..ParquetReadOptions::default()
            },
        )
        .unwrap();
        assert_eq!(stats.row_groups_total, 4);
        assert_eq!(stats.row_groups_kept, 1);
        assert!(stats.compressed_bytes_kept < stats.compressed_bytes_total);
        assert_eq!(
            col_i64(&out, "l_shipdate"),
            vec![Some(800), Some(801), Some(802), Some(803)]
        );
        std::fs::remove_file(path).ok();
    }
}
