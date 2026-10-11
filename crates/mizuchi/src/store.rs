//! Object-store datasets. The embedding runner supplies the [`ObjectStore`];
//! the engine never opens a network connection or reads credentials.
//!
//! Hive-style `col=value` path segments are pruned from [`Expr`] filters
//! before any Parquet object is fetched. Footer statistics then prune row
//! groups inside a fetched file (same rules as [`crate::read::read_parquet`]).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::Array;
use futures::TryStreamExt;
use futures::executor::block_on;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::expr::{BinaryOp, Expr};
use crate::read::{ParquetReadOptions, read_parquet_from_builder};
use crate::write::{
    ParquetWriteOptions, WriteMode, split_partitions, take_rows, validate_partitions,
};

static JOB: AtomicU64 = AtomicU64::new(1);

/// Read Parquet objects under `prefix`. Partition directories that cannot
/// match `opts.filters` are not fetched.
pub fn read_parquet_store(
    store: &dyn ObjectStore,
    prefix: &str,
    opts: ParquetReadOptions<'_>,
) -> Result<Vec<Batch>> {
    Ok(read_parquet_store_counted(store, prefix, opts)?.0)
}

fn read_parquet_store_counted(
    store: &dyn ObjectStore,
    prefix: &str,
    opts: ParquetReadOptions<'_>,
) -> Result<(Vec<Batch>, usize)> {
    let root = object_path(prefix)?;
    let objects = list_parquet(store, &root)?;
    let mut kept = Vec::new();
    for loc in objects {
        let rel = relative_to(&loc, prefix);
        if hive_path_cannot_match(&rel, opts.filters) {
            continue;
        }
        kept.push(loc);
    }
    let fetched = kept.len();
    let mut out = Vec::new();
    for loc in kept {
        let get = block_on(store.get(&loc)).map_err(store_err)?;
        let bytes = block_on(get.bytes()).map_err(store_err)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| Error::Io(format!("{loc}: {e}")))?;
        out.extend(read_parquet_from_builder(builder, &opts, loc.as_ref())?);
    }
    Ok((out, fetched))
}

/// Write `batches` as Parquet objects under `prefix`. Staging objects are
/// published by rename; a failed job deletes staging and leaves committed
/// objects unchanged.
pub fn write_parquet_store(
    store: &dyn ObjectStore,
    prefix: &str,
    batches: &[Batch],
    opts: ParquetWriteOptions<'_>,
) -> Result<()> {
    if let Some(batch) = batches.first() {
        validate_partitions(batch, opts.partition_by)?;
        for other in &batches[1..] {
            if other.schema() != batch.schema() {
                return Err(Error::InvalidBatch(
                    "write batches must share one schema".into(),
                ));
            }
        }
    } else if opts.mode == WriteMode::Append {
        return Ok(());
    }

    let root = object_path(prefix)?;
    let id = JOB.fetch_add(1, Ordering::Relaxed);
    let staging_prefix = join_path(&root, &format!(".mizuchi-staging-{id}"))?;

    let run = (|| {
        let staged = stage_store(store, &staging_prefix, batches, opts.partition_by)?;
        publish_store(store, &root, &staging_prefix, &staged, opts.mode, id)?;
        let success = join_path(&root, "_SUCCESS")?;
        block_on(store.put(&success, PutPayload::from(Vec::<u8>::new()))).map_err(store_err)?;
        Ok(())
    })();

    let _ = delete_prefix(store, &staging_prefix);
    run
}

fn stage_store(
    store: &dyn ObjectStore,
    staging: &ObjectPath,
    batches: &[Batch],
    partition_by: &[String],
) -> Result<Vec<ObjectPath>> {
    let mut parts: BTreeMap<String, Vec<arrow::array::RecordBatch>> = BTreeMap::new();
    let mut schema = None;
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        schema = Some(batch.schema());
        let groups = split_partitions(batch, partition_by)?;
        for (key, rows) in groups {
            parts.entry(key).or_default().push(take_rows(batch, &rows)?);
        }
    }
    let Some(schema) = schema else {
        return Ok(Vec::new());
    };
    let mut staged = Vec::new();
    for (key, recs) in parts {
        let rel = if key.is_empty() {
            "part-00000.parquet".to_string()
        } else {
            format!("{key}/part-00000.parquet")
        };
        let loc = join_path(staging, &rel)?;
        let bytes = encode_parquet(&schema, &recs)?;
        block_on(store.put(&loc, PutPayload::from(bytes))).map_err(store_err)?;
        staged.push(loc);
    }
    Ok(staged)
}

fn publish_store(
    store: &dyn ObjectStore,
    root: &ObjectPath,
    staging: &ObjectPath,
    staged: &[ObjectPath],
    mode: WriteMode,
    id: u64,
) -> Result<()> {
    let old_prefix = join_path(root, &format!(".mizuchi-old-{id}"))?;
    if mode == WriteMode::Overwrite {
        // Relocate committed objects first so a failed publish still has the
        // previous dataset under the hidden prefix instead of deleting it.
        for loc in list_committed(store, root)? {
            let rel = relative_to(&loc, root.as_ref());
            let dest = join_path(&old_prefix, &rel)?;
            block_on(store.rename(&loc, &dest)).map_err(store_err)?;
        }
    }
    for src in staged {
        let rel = src
            .as_ref()
            .strip_prefix(staging.as_ref())
            .unwrap_or(src.as_ref())
            .trim_start_matches('/');
        let dest = if mode == WriteMode::Append {
            let parent_rel = match rel.rsplit_once('/') {
                Some((dir, _)) => dir,
                None => "",
            };
            let dest_dir = if parent_rel.is_empty() {
                root.clone()
            } else {
                join_path(root, parent_rel)?
            };
            let n = next_part_index(store, &dest_dir)?;
            if parent_rel.is_empty() {
                join_path(root, &format!("part-{n:05}.parquet"))?
            } else {
                join_path(root, &format!("{parent_rel}/part-{n:05}.parquet"))?
            }
        } else {
            join_path(root, rel)?
        };
        block_on(store.rename(src, &dest)).map_err(store_err)?;
    }
    if mode == WriteMode::Overwrite {
        let _ = delete_prefix(store, &old_prefix);
    }
    Ok(())
}

fn encode_parquet(
    schema: &arrow::datatypes::SchemaRef,
    batches: &[arrow::array::RecordBatch],
) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None)
            .map_err(|e| Error::Io(e.to_string()))?;
        for batch in batches {
            writer.write(batch).map_err(|e| Error::Io(e.to_string()))?;
        }
        writer.close().map_err(|e| Error::Io(e.to_string()))?;
    }
    Ok(buf)
}

fn list_parquet(store: &dyn ObjectStore, prefix: &ObjectPath) -> Result<Vec<ObjectPath>> {
    list_objects(store, prefix, |s| s.ends_with(".parquet"))
}

fn list_committed(store: &dyn ObjectStore, prefix: &ObjectPath) -> Result<Vec<ObjectPath>> {
    list_objects(store, prefix, |s| {
        s.ends_with(".parquet") || s.rsplit('/').next() == Some("_SUCCESS")
    })
}

fn list_objects(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    keep: impl Fn(&str) -> bool,
) -> Result<Vec<ObjectPath>> {
    let metas: Vec<object_store::ObjectMeta> =
        block_on(store.list(Some(prefix)).try_collect()).map_err(store_err)?;
    let mut out: Vec<ObjectPath> = metas
        .into_iter()
        .map(|m| m.location)
        .filter(|p| {
            let s = p.as_ref();
            keep(s) && !segment_hidden(s)
        })
        .collect();
    out.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
    Ok(out)
}

fn segment_hidden(path: &str) -> bool {
    path.split('/').any(|s| s.starts_with(".mizuchi-"))
}

fn delete_prefix(store: &dyn ObjectStore, prefix: &ObjectPath) -> Result<()> {
    let metas: Vec<object_store::ObjectMeta> =
        block_on(store.list(Some(prefix)).try_collect()).map_err(store_err)?;
    for m in metas {
        let _ = block_on(store.delete(&m.location));
    }
    Ok(())
}

fn next_part_index(store: &dyn ObjectStore, dir: &ObjectPath) -> Result<u32> {
    let mut max: Option<u32> = None;
    for loc in list_parquet(store, dir)? {
        let name = loc.as_ref().rsplit('/').next().unwrap_or("");
        let Some(num) = name
            .strip_prefix("part-")
            .and_then(|s| s.strip_suffix(".parquet"))
            .and_then(|s| s.parse::<u32>().ok())
        else {
            continue;
        };
        max = Some(max.map_or(num, |m| m.max(num)));
    }
    Ok(max.map_or(0, |m| m + 1))
}

fn object_path(s: &str) -> Result<ObjectPath> {
    ObjectPath::parse(s.trim_matches('/')).map_err(|e| Error::Io(e.to_string()))
}

fn join_path(base: &ObjectPath, rel: &str) -> Result<ObjectPath> {
    let rel = rel.trim_matches('/');
    if rel.is_empty() {
        return Ok(base.clone());
    }
    let joined = if base.as_ref().is_empty() {
        rel.to_string()
    } else {
        format!("{}/{rel}", base.as_ref())
    };
    object_path(&joined)
}

fn relative_to(loc: &ObjectPath, prefix: &str) -> String {
    let p = loc.as_ref();
    let prefix = prefix.trim_matches('/');
    p.strip_prefix(prefix)
        .map(|s| s.trim_start_matches('/').to_string())
        .unwrap_or_else(|| p.to_string())
}

/// True when Hive path segments prove `filters` cannot hold for this object.
fn hive_path_cannot_match(rel: &str, filters: Option<&[Expr]>) -> bool {
    let Some(preds) = filters else {
        return false;
    };
    let parts = parse_hive(rel);
    if parts.is_empty() {
        return false;
    }
    preds.iter().any(|p| hive_pred_cannot_match(&parts, p))
}

fn parse_hive(rel: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for seg in rel.split('/') {
        if let Some((k, v)) = seg.split_once('=') {
            out.insert(k.to_string(), v.to_string());
        }
    }
    out
}

fn hive_pred_cannot_match(parts: &BTreeMap<String, String>, pred: &Expr) -> bool {
    match pred {
        Expr::Binary(BinaryOp::And, l, r) => {
            hive_pred_cannot_match(parts, l) || hive_pred_cannot_match(parts, r)
        }
        Expr::Binary(op, l, r) => {
            let (name, lit, swapped) = match (col_name(l), i64_lit(r), col_name(r), i64_lit(l)) {
                (Some(n), Some(v), _, _) => (n, v, false),
                (_, _, Some(n), Some(v)) => (n, v, true),
                _ => return false,
            };
            let Some(raw) = parts.get(&name) else {
                return false;
            };
            if raw == "__HIVE_DEFAULT_PARTITION__" {
                return matches!(
                    if swapped { swap_cmp(*op) } else { *op },
                    BinaryOp::Eq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq
                );
            }
            let Ok(part) = raw.parse::<i64>() else {
                return false;
            };
            let op = if swapped { swap_cmp(*op) } else { *op };
            match op {
                BinaryOp::Eq => part != lit,
                BinaryOp::NotEq => part == lit,
                BinaryOp::Lt => part >= lit,
                BinaryOp::LtEq => part > lit,
                BinaryOp::Gt => part <= lit,
                BinaryOp::GtEq => part < lit,
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

fn store_err(e: object_store::Error) -> Error {
    Error::Io(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use object_store::memory::InMemory;
    use std::sync::Arc;

    fn sample() -> Batch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("year", DataType::Int64, true),
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        Batch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(2020), Some(2020), Some(2021)])),
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            ],
        )
        .unwrap()
    }

    fn write_year(store: &dyn ObjectStore, prefix: &str, batches: &[Batch], mode: WriteMode) {
        let cols = [String::from("year")];
        write_parquet_store(
            store,
            prefix,
            batches,
            ParquetWriteOptions {
                partition_by: &cols,
                mode,
            },
        )
        .unwrap();
    }

    fn ids(batches: &[Batch]) -> Vec<i64> {
        let mut out = Vec::new();
        for b in batches {
            let col = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..col.len() {
                if !col.is_null(i) {
                    out.push(col.value(i));
                }
            }
        }
        out.sort();
        out
    }

    #[test]
    fn store_round_trip_partitioned() {
        let store = InMemory::new();
        write_year(&store, "ds", &[sample()], WriteMode::Overwrite);
        let out = read_parquet_store(&store, "ds", ParquetReadOptions::default()).unwrap();
        assert_eq!(ids(&out), vec![1, 2, 3]);
    }

    #[test]
    fn pruned_partitions_are_not_fetched() {
        let store = InMemory::new();
        write_year(&store, "ds", &[sample()], WriteMode::Overwrite);
        let pred = Expr::Binary(
            BinaryOp::Eq,
            Box::new(Expr::column("year")),
            Box::new(Expr::lit_i64(2020)),
        );
        let opts = ParquetReadOptions {
            filters: Some(std::slice::from_ref(&pred)),
            ..ParquetReadOptions::default()
        };
        let (out, fetched) = read_parquet_store_counted(&store, "ds", opts).unwrap();
        assert_eq!(ids(&out), vec![1, 2]);
        assert_eq!(fetched, 1, "year=2021 object must not be fetched");
    }

    #[test]
    fn store_append_and_overwrite() {
        let store = InMemory::new();
        write_year(&store, "ds", &[sample()], WriteMode::Overwrite);
        write_year(&store, "ds", &[sample()], WriteMode::Append);
        let out = read_parquet_store(&store, "ds", ParquetReadOptions::default()).unwrap();
        assert_eq!(ids(&out), vec![1, 1, 2, 2, 3, 3]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("year", DataType::Int64, true),
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let only = Batch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(2020)])),
                Arc::new(Int64Array::from(vec![Some(9)])),
                Arc::new(StringArray::from(vec![Some("z")])),
            ],
        )
        .unwrap();
        write_year(&store, "ds", &[only], WriteMode::Overwrite);
        let out = read_parquet_store(&store, "ds", ParquetReadOptions::default()).unwrap();
        assert_eq!(ids(&out), vec![9]);
    }
}
