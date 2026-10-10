//! Parquet and CSV readers that yield Arrow record batches.
//!
//! Callers pass the path. Parquet projection uses the file column mask so
//! unread columns are not decoded. Row-group skip is a hook the optimizer can
//! fill from footer statistics. CSV schema inference is optional; without it
//! every column is Utf8.

use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::Path;

use arrow::array::RecordBatch;
use arrow::csv::ReaderBuilder;
use arrow::csv::reader::Format;
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::batch::{Batch, BatchPolicy, DEFAULT_MAX_ROWS};
use crate::error::{Error, Result};
use crate::schema::from_arrow_schema;

/// Footer facts for one Parquet row group, used to skip groups before decode.
#[derive(Clone, Debug)]
pub struct RowGroupStats {
    /// Zero-based index in the file.
    pub index: usize,
    /// Rows in this group.
    pub num_rows: i64,
    /// Root column names in file order.
    pub column_names: Vec<String>,
}

/// Options for [`read_parquet`].
#[derive(Default)]
pub struct ParquetReadOptions<'a> {
    /// Column names to decode. `None` reads every column.
    pub projection: Option<&'a [String]>,
    /// Return true to skip this row group. `None` keeps every group.
    pub prune_row_group: Option<&'a dyn Fn(&RowGroupStats) -> bool>,
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
        let stats = RowGroupStats {
            index,
            num_rows: rg.num_rows(),
            column_names: column_names.clone(),
        };
        let skip = opts
            .prune_row_group
            .map(|prune| prune(&stats))
            .unwrap_or(false);
        if !skip {
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
        .map_err(|e| Error::Io(format!("{}: {e}", path.display())))?;
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
}
