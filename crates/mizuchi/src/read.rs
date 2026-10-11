//! Parquet and CSV readers that yield Arrow record batches.
//!
//! Callers pass the path. Parquet projection uses the file column mask so
//! unread columns are not decoded. Row-group skip is a hook the optimizer can
//! fill from footer statistics. CSV schema inference is optional; without it
//! every column is Utf8.

use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::Path;

use arrow::array::{Array, RecordBatch};
use arrow::csv::ReaderBuilder;
use arrow::csv::reader::Format;
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::batch::{Batch, BatchPolicy, DEFAULT_MAX_ROWS};
use crate::error::{Error, Result};
use crate::expr::{BinaryOp, Expr};
use crate::schema::from_arrow_schema;
use parquet::file::statistics::Statistics;

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

/// Footer facts for one Parquet row group, used to skip groups before decode.
#[derive(Clone, Debug)]
pub struct RowGroupStats {
    /// Zero-based index in the file.
    pub index: usize,
    /// Rows in this group.
    pub num_rows: i64,
    /// Root column names in file order.
    pub column_names: Vec<String>,
    /// Int64 column ranges from footer statistics.
    pub i64_ranges: Vec<ColumnRange>,
}

/// Options for [`read_parquet`].
#[derive(Default)]
pub struct ParquetReadOptions<'a> {
    /// Column names to decode. `None` reads every column.
    pub projection: Option<&'a [String]>,
    /// Return true to skip this row group. `None` keeps every group.
    pub prune_row_group: Option<&'a dyn Fn(&RowGroupStats) -> bool>,
    /// Comparison predicates used to skip row groups from footer min/max.
    pub filters: Option<&'a [Expr]>,
    /// Morsel size for decoded batches.
    pub policy: BatchPolicy,
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

/// Read a Parquet file into engine batches.
pub fn read_parquet(path: impl AsRef<Path>, opts: ParquetReadOptions<'_>) -> Result<Vec<Batch>> {
    let path = path.as_ref();
    let file = File::open(path).map_err(|e| io_err(path, e))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::Io(format!("{}: {e}", path.display())))?;
    read_parquet_from_builder(builder, &opts, &path.display().to_string())
}

/// Decode a Parquet builder (file or object bytes) with the same prune rules.
pub(crate) fn read_parquet_from_builder<R>(
    builder: ParquetRecordBatchReaderBuilder<R>,
    opts: &ParquetReadOptions<'_>,
    label: &str,
) -> Result<Vec<Batch>>
where
    R: parquet::file::reader::ChunkReader + 'static,
{
    let parquet_schema = builder.parquet_schema().clone();
    let arrow_schema = builder.schema().clone();
    from_arrow_schema(arrow_schema.clone())?;

    let column_names: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();

    let mut keep_groups = Vec::new();
    for (index, rg) in builder.metadata().row_groups().iter().enumerate() {
        let mut i64_ranges = Vec::new();
        // Leaf chunks line up with Arrow root fields only on flat schemas.
        // Nested files leave ranges empty so prune stays conservative.
        let cols = rg.columns();
        if cols.len() == column_names.len() {
            for (ci, col) in cols.iter().enumerate() {
                let name = column_names[ci].clone();
                if let Some(Statistics::Int64(s)) = col.statistics() {
                    i64_ranges.push(ColumnRange {
                        name,
                        min_i64: s.min_opt().copied(),
                        max_i64: s.max_opt().copied(),
                    });
                }
            }
        }
        let stats = RowGroupStats {
            index,
            num_rows: rg.num_rows(),
            column_names: column_names.clone(),
            i64_ranges,
        };
        let skip_hook = opts
            .prune_row_group
            .map(|prune| prune(&stats))
            .unwrap_or(false);
        let skip_stats = opts
            .filters
            .map(|preds| preds.iter().any(|p| row_group_cannot_match(&stats, p)))
            .unwrap_or(false);
        if !skip_hook && !skip_stats {
            keep_groups.push(index);
        }
    }

    let batch_size = opts.policy.max_rows().unwrap_or(DEFAULT_MAX_ROWS);
    let mut builder = builder
        .with_batch_size(batch_size)
        .with_row_groups(keep_groups);

    if let Some(names) = opts.projection {
        let mask = projection_mask(&parquet_schema, &arrow_schema, names)?;
        builder = builder.with_projection(mask);
    }

    let reader = builder
        .build()
        .map_err(|e| Error::Io(format!("{label}: {e}")))?;
    collect_reader(reader, &opts.policy)
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
        Expr::Binary(op, l, r) => {
            let (name, lit, swapped) = match (col_name(l), i64_lit(r), col_name(r), i64_lit(l)) {
                (Some(n), Some(v), _, _) => (n, v, false),
                (_, _, Some(n), Some(v)) => (n, v, true),
                _ => return false,
            };
            let Some(range) = stats.i64_ranges.iter().find(|c| c.name == name) else {
                return false;
            };
            let op = if swapped { swap_cmp(*op) } else { *op };
            match op {
                BinaryOp::Gt => range.max_i64.is_some_and(|m| m <= lit),
                BinaryOp::GtEq => range.max_i64.is_some_and(|m| m < lit),
                BinaryOp::Lt => range.min_i64.is_some_and(|m| m >= lit),
                BinaryOp::LtEq => range.min_i64.is_some_and(|m| m > lit),
                BinaryOp::Eq => {
                    range.max_i64.is_some_and(|m| m < lit) || range.min_i64.is_some_and(|m| m > lit)
                }
                _ => false,
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

fn col_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(n) => Some(n.clone()),
        _ => None,
    }
}

fn i64_lit(expr: &Expr) -> Option<i64> {
    let Expr::Literal(arr) = expr else {
        return None;
    };
    let a = arr.as_any().downcast_ref::<arrow::array::Int64Array>()?;
    if a.len() != 1 || a.is_null(0) {
        return None;
    }
    Some(a.value(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    fn write_parquet(batches: &[RecordBatch], max_row_group: usize) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "mizuchi-read-{}-{}.parquet",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let file = File::create(&path).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(max_row_group))
            .build();
        let mut writer = ArrowWriter::try_new(file, batches[0].schema(), Some(props)).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();
        path
    }

    fn write_csv(body: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "mizuchi-read-{}-{}.csv",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
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
            crate::expr::BinaryOp::Gt,
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
            crate::expr::BinaryOp::Gt,
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
}
