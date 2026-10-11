//! Partitioned Parquet writer with append/overwrite and staging commit.
//!
//! Files land under a hidden staging directory, then publish by rename so the
//! dataset directory never shows a partial write. A failed or cancelled job
//! removes staging and leaves committed files as they were. Partition layout
//! is Hive-style `col=value` directories. Partition columns stay in the file
//! schema so [`crate::read::read_parquet`] round-trips a part file.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{Array, BooleanArray, Int32Array, Int64Array, StringArray, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::DataType;
use parquet::arrow::ArrowWriter;

use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::exec::CancelToken;

/// How a write updates an existing dataset directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteMode {
    /// Add new part files next to existing ones.
    Append,
    /// Replace the dataset with this write's output.
    Overwrite,
}

/// Options for [`write_parquet`].
#[derive(Clone, Copy, Debug)]
pub struct ParquetWriteOptions<'a> {
    /// Column names that form Hive-style partition directories. Empty writes
    /// part files in the dataset root.
    pub partition_by: &'a [String],
    /// Append or replace committed files.
    pub mode: WriteMode,
}

impl Default for ParquetWriteOptions<'static> {
    fn default() -> Self {
        Self {
            partition_by: &[],
            mode: WriteMode::Overwrite,
        }
    }
}

static JOB: AtomicU64 = AtomicU64::new(1);

/// Write `batches` as Parquet under `dest`.
pub fn write_parquet(
    dest: impl AsRef<Path>,
    batches: &[Batch],
    opts: ParquetWriteOptions<'_>,
) -> Result<()> {
    write_parquet_cancel(dest, batches, opts, &CancelToken::new())
}

/// [`write_parquet`] that stops at the next partition boundary when `cancel`
/// is set. Staging is removed; committed files stay as they were.
pub fn write_parquet_cancel(
    dest: impl AsRef<Path>,
    batches: &[Batch],
    opts: ParquetWriteOptions<'_>,
    cancel: &CancelToken,
) -> Result<()> {
    write_parquet_stats(dest, batches, opts, cancel).map(|_| ())
}

/// Stats for a committed write: partitions, rows, bytes, and a stable commit id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteStats {
    /// Dataset directory.
    pub path: String,
    /// Hive-style partition directories that received files.
    pub partitions: Vec<String>,
    /// `append` or `overwrite`.
    pub mode: String,
    /// Rows in `batches`.
    pub rows: u64,
    /// Bytes of committed Parquet files.
    pub bytes: u64,
    /// Deterministic id (`mode:rows:bytes:partitions`).
    pub commit_id: String,
}

/// [`write_parquet_cancel`] that also returns [`WriteStats`].
pub fn write_parquet_stats(
    dest: impl AsRef<Path>,
    batches: &[Batch],
    opts: ParquetWriteOptions<'_>,
    cancel: &CancelToken,
) -> Result<WriteStats> {
    write_inner(dest.as_ref(), batches, opts, cancel, CommitCtrl::default())
}

#[derive(Clone, Copy, Default)]
struct CommitCtrl {
    fail_before_commit: bool,
    leave_staging: bool,
    cancel_after_groups: Option<usize>,
}

fn write_inner(
    dest: &Path,
    batches: &[Batch],
    opts: ParquetWriteOptions<'_>,
    cancel: &CancelToken,
    ctrl: CommitCtrl,
) -> Result<WriteStats> {
    cancel.check()?;
    if let Some(batch) = batches.first() {
        validate_partitions(batch, opts.partition_by)?;
        for other in &batches[1..] {
            if other.schema() != batch.schema() {
                return Err(Error::InvalidBatch(
                    "write batches must share one schema".into(),
                ));
            }
        }
    } else if !opts.partition_by.is_empty() && opts.mode == WriteMode::Append {
        // Nothing to add; keep the dataset.
        return dataset_write_stats(dest, batches, opts);
    }

    fs::create_dir_all(dest).map_err(|e| io_err(dest, e))?;
    let id = JOB.fetch_add(1, Ordering::Relaxed);
    let staging = dest.join(format!(".mizuchi-staging-{id}"));
    fs::create_dir_all(&staging).map_err(|e| io_err(&staging, e))?;

    let run = (|| {
        cancel.check()?;
        stage_files(
            &staging,
            batches,
            opts.partition_by,
            cancel,
            ctrl.cancel_after_groups,
        )?;
        cancel.check()?;
        if ctrl.fail_before_commit {
            return Err(Error::Io("injected commit failure".into()));
        }
        publish(dest, &staging, opts.mode, id)?;
        write_success(dest)?;
        dataset_write_stats(dest, batches, opts)
    })();

    match run {
        Ok(stats) => {
            let _ = fs::remove_dir_all(&staging);
            Ok(stats)
        }
        Err(e) => {
            if !ctrl.leave_staging {
                let _ = fs::remove_dir_all(&staging);
            }
            Err(e)
        }
    }
}

fn dataset_write_stats(
    dest: &Path,
    batches: &[Batch],
    opts: ParquetWriteOptions<'_>,
) -> Result<WriteStats> {
    let files = list_parquets(dest).unwrap_or_default();
    let bytes: u64 = files
        .iter()
        .map(|f| std::fs::metadata(f).map(|m| m.len()).unwrap_or(0))
        .sum();
    let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    let mut partitions: Vec<String> = files
        .iter()
        .filter_map(|f| {
            let rel = f.strip_prefix(dest).ok()?;
            let parent = rel.parent()?;
            if parent.as_os_str().is_empty() {
                None
            } else {
                Some(parent.to_string_lossy().replace('\\', "/"))
            }
        })
        .collect();
    partitions.sort();
    partitions.dedup();
    let mode = match opts.mode {
        WriteMode::Append => "append",
        WriteMode::Overwrite => "overwrite",
    };
    Ok(WriteStats {
        path: dest.display().to_string(),
        partitions: partitions.clone(),
        mode: mode.to_string(),
        rows,
        bytes,
        commit_id: format!("{mode}:{rows}:{bytes}:{}", partitions.join("/")),
    })
}

pub(crate) fn validate_partitions(batch: &Batch, partition_by: &[String]) -> Result<()> {
    let mut seen = Vec::new();
    for name in partition_by {
        if seen.iter().any(|s: &String| s == name) {
            return Err(Error::Plan(format!("duplicate partition column {name}")));
        }
        seen.push(name.clone());
        let col = batch
            .column_by_name(name)
            .ok_or_else(|| Error::Plan(format!("partition column {name} is not in the batch")))?;
        match col.data_type() {
            DataType::Int64 | DataType::Int32 | DataType::Utf8 | DataType::Boolean => {}
            other => {
                return Err(Error::UnsupportedType(format!(
                    "partition column {name}: {other}"
                )));
            }
        }
    }
    Ok(())
}

fn stage_files(
    staging: &Path,
    batches: &[Batch],
    partition_by: &[String],
    cancel: &CancelToken,
    cancel_after_groups: Option<usize>,
) -> Result<()> {
    let mut writers: BTreeMap<String, ArrowWriter<File>> = BTreeMap::new();
    let mut tmp_paths: BTreeMap<String, PathBuf> = BTreeMap::new();
    let mut groups_seen = 0usize;
    for batch in batches {
        cancel.check()?;
        if batch.num_rows() == 0 {
            continue;
        }
        let groups = split_partitions(batch, partition_by)?;
        for (key, rows) in groups {
            groups_seen += 1;
            if cancel_after_groups == Some(groups_seen) {
                return Err(Error::Cancelled);
            }
            cancel.check()?;
            let taken = take_rows(batch, &rows)?;
            if !tmp_paths.contains_key(&key) {
                let rel = if key.is_empty() {
                    PathBuf::from("part-00000.parquet")
                } else {
                    PathBuf::from(&key).join("part-00000.parquet")
                };
                let final_path = staging.join(&rel);
                if let Some(parent) = final_path.parent() {
                    fs::create_dir_all(parent).map_err(|e| io_err(parent, e))?;
                }
                let tmp = tmp_path(&final_path);
                let file = File::create(&tmp).map_err(|e| io_err(&tmp, e))?;
                let writer = ArrowWriter::try_new(file, batch.schema(), None)
                    .map_err(|e| Error::Io(e.to_string()))?;
                tmp_paths.insert(key.clone(), tmp);
                writers.insert(key.clone(), writer);
            }
            let writer = writers
                .get_mut(&key)
                .ok_or_else(|| Error::Io("missing partition writer".into()))?;
            writer.write(&taken).map_err(|e| Error::Io(e.to_string()))?;
        }
    }
    for (key, writer) in writers {
        writer.close().map_err(|e| Error::Io(e.to_string()))?;
        let tmp = tmp_paths
            .get(&key)
            .ok_or_else(|| Error::Io("missing staging temp file".into()))?;
        let published = parquet_from_tmp(tmp);
        fs::rename(tmp, &published).map_err(|e| io_err(&published, e))?;
    }
    Ok(())
}

pub(crate) fn take_rows(batch: &Batch, rows: &[u32]) -> Result<arrow::array::RecordBatch> {
    let indices = UInt32Array::from(rows.to_vec());
    let cols: Result<Vec<_>> = batch
        .as_arrow()
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &indices, None).map_err(|e| Error::InvalidBatch(e.to_string())))
        .collect();
    arrow::array::RecordBatch::try_new(batch.schema(), cols?)
        .map_err(|e| Error::InvalidBatch(e.to_string()))
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut os = path.as_os_str().to_os_string();
    os.push(".tmp");
    PathBuf::from(os)
}

fn parquet_from_tmp(tmp: &Path) -> PathBuf {
    let s = tmp.to_string_lossy();
    match s.strip_suffix(".tmp") {
        Some(stripped) => PathBuf::from(stripped),
        None => tmp.to_path_buf(),
    }
}

pub(crate) fn split_partitions(
    batch: &Batch,
    partition_by: &[String],
) -> Result<BTreeMap<String, Vec<u32>>> {
    let mut groups: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    if partition_by.is_empty() {
        groups.insert(String::new(), (0..batch.num_rows() as u32).collect());
        return Ok(groups);
    }
    let cols: Vec<_> = partition_by
        .iter()
        .map(|n| batch.column_by_name(n).expect("validated"))
        .collect();
    for row in 0..batch.num_rows() {
        let mut parts = Vec::with_capacity(partition_by.len());
        for (name, col) in partition_by.iter().zip(&cols) {
            parts.push(format!("{name}={}", cell_key(col.as_ref(), row)?));
        }
        groups.entry(parts.join("/")).or_default().push(row as u32);
    }
    Ok(groups)
}

fn cell_key(arr: &dyn Array, row: usize) -> Result<String> {
    if arr.is_null(row) {
        return Ok("__HIVE_DEFAULT_PARTITION__".into());
    }
    match arr.data_type() {
        DataType::Int64 => {
            let a = arr.as_any().downcast_ref::<Int64Array>().expect("int64");
            Ok(a.value(row).to_string())
        }
        DataType::Int32 => {
            let a = arr.as_any().downcast_ref::<Int32Array>().expect("int32");
            Ok(a.value(row).to_string())
        }
        DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().expect("utf8");
            Ok(escape_segment(a.value(row)))
        }
        DataType::Boolean => {
            let a = arr.as_any().downcast_ref::<BooleanArray>().expect("bool");
            Ok(if a.value(row) {
                "true".into()
            } else {
                "false".into()
            })
        }
        other => Err(Error::UnsupportedType(other.to_string())),
    }
}

fn escape_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '/' => out.push_str("%2F"),
            '=' => out.push_str("%3D"),
            '%' => out.push_str("%25"),
            '\n' => out.push_str("%0A"),
            '\r' => out.push_str("%0D"),
            _ => out.push(ch),
        }
    }
    out
}

fn publish(dest: &Path, staging: &Path, mode: WriteMode, id: u64) -> Result<()> {
    match mode {
        WriteMode::Overwrite => publish_overwrite(dest, staging, id),
        WriteMode::Append => publish_append(dest, staging),
    }
}

fn publish_overwrite(dest: &Path, staging: &Path, id: u64) -> Result<()> {
    let old = dest.join(format!(".mizuchi-old-{id}"));
    fs::create_dir_all(&old).map_err(|e| io_err(&old, e))?;
    let children = fs::read_dir(dest).map_err(|e| io_err(dest, e))?;
    for ent in children {
        let ent = ent.map_err(|e| io_err(dest, e))?;
        let name = ent.file_name();
        let name_s = name.to_string_lossy();
        if name_s.starts_with(".mizuchi-") {
            continue;
        }
        let from = dest.join(&name);
        fs::rename(&from, old.join(&name)).map_err(|e| io_err(&from, e))?;
    }
    move_tree(staging, dest)?;
    let _ = fs::remove_dir_all(&old);
    Ok(())
}

fn publish_append(dest: &Path, staging: &Path) -> Result<()> {
    for file in list_parquets(staging)? {
        let rel = file.strip_prefix(staging).unwrap_or(&file);
        let parent_rel = rel.parent().unwrap_or(Path::new(""));
        let dest_dir = dest.join(parent_rel);
        fs::create_dir_all(&dest_dir).map_err(|e| io_err(&dest_dir, e))?;
        let n = next_part_index(&dest_dir)?;
        let dest_file = dest_dir.join(format!("part-{n:05}.parquet"));
        fs::rename(&file, &dest_file).map_err(|e| io_err(&dest_file, e))?;
    }
    Ok(())
}

fn move_tree(from: &Path, to: &Path) -> Result<()> {
    for file in list_parquets(from)? {
        let rel = file.strip_prefix(from).unwrap_or(&file);
        let dest_file = to.join(rel);
        if let Some(parent) = dest_file.parent() {
            fs::create_dir_all(parent).map_err(|e| io_err(parent, e))?;
        }
        fs::rename(&file, &dest_file).map_err(|e| io_err(&dest_file, e))?;
    }
    Ok(())
}

fn next_part_index(dir: &Path) -> Result<u32> {
    if !dir.exists() {
        return Ok(0);
    }
    let mut max: Option<u32> = None;
    let ents = fs::read_dir(dir).map_err(|e| io_err(dir, e))?;
    for ent in ents {
        let ent = ent.map_err(|e| io_err(dir, e))?;
        let name = ent.file_name();
        let name = name.to_string_lossy();
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

fn list_parquets(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    walk_parquets(root, &mut out)?;
    out.sort();
    Ok(out)
}

fn walk_parquets(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    let ents = fs::read_dir(dir).map_err(|e| io_err(dir, e))?;
    for ent in ents {
        let ent = ent.map_err(|e| io_err(dir, e))?;
        let name = ent.file_name();
        let name_s = name.to_string_lossy();
        if name_s.starts_with('.') {
            continue;
        }
        let path = ent.path();
        if path.is_dir() {
            walk_parquets(&path, out)?;
        } else if name_s.ends_with(".parquet") {
            out.push(path);
        }
    }
    Ok(())
}

fn write_success(dest: &Path) -> Result<()> {
    let path = dest.join("_SUCCESS");
    fs::write(&path, b"").map_err(|e| io_err(&path, e))
}

fn io_err(path: &Path, err: std::io::Error) -> Error {
    Error::Io(format!("{}: {err}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read::{ParquetReadOptions, read_parquet};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    static DEST_SEQ: AtomicU64 = AtomicU64::new(1);

    fn tmp_dest() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "mizuchi-write-{}-{}",
            std::process::id(),
            DEST_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn sample_batch() -> Batch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("year", DataType::Int64, true),
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        Batch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(2020),
                    Some(2020),
                    Some(2021),
                    None,
                ])),
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4)])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("c"),
                    Some("d"),
                ])),
            ],
        )
        .unwrap()
    }

    fn write_year(dest: &Path, batches: &[Batch], mode: WriteMode) -> Result<()> {
        let cols = [String::from("year")];
        write_parquet(
            dest,
            batches,
            ParquetWriteOptions {
                partition_by: &cols,
                mode,
            },
        )
    }

    fn write_year_inner(
        dest: &Path,
        batches: &[Batch],
        mode: WriteMode,
        ctrl: CommitCtrl,
    ) -> Result<()> {
        let cols = [String::from("year")];
        write_inner(
            dest,
            batches,
            ParquetWriteOptions {
                partition_by: &cols,
                mode,
            },
            &CancelToken::new(),
            ctrl,
        )
        .map(|_| ())
    }

    fn committed_parts(dest: &Path) -> Vec<PathBuf> {
        list_parquets(dest).unwrap()
    }

    fn read_ids(dest: &Path) -> Vec<i64> {
        let mut ids = Vec::new();
        for part in committed_parts(dest) {
            let batches = read_parquet(&part, ParquetReadOptions::default()).unwrap();
            for b in batches {
                let col = b
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..col.len() {
                    if !col.is_null(i) {
                        ids.push(col.value(i));
                    }
                }
            }
        }
        ids.sort();
        ids
    }

    #[test]
    fn partitioned_layout_round_trips() {
        let dest = tmp_dest();
        write_year(&dest, &[sample_batch()], WriteMode::Overwrite).unwrap();
        let parts = committed_parts(&dest);
        let names: Vec<String> = parts
            .iter()
            .map(|p| {
                p.strip_prefix(&dest)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert!(
            names
                .iter()
                .any(|n| n.contains("year=2020") && n.ends_with(".parquet")),
            "{names:?}"
        );
        assert!(
            names
                .iter()
                .any(|n| n.contains("year=2021") && n.ends_with(".parquet")),
            "{names:?}"
        );
        assert!(
            names
                .iter()
                .any(|n| n.contains("year=__HIVE_DEFAULT_PARTITION__")),
            "{names:?}"
        );
        assert!(dest.join("_SUCCESS").is_file());
        assert_eq!(read_ids(&dest), vec![1, 2, 3, 4]);
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn append_adds_part_files() {
        let dest = tmp_dest();
        write_year(&dest, &[sample_batch()], WriteMode::Overwrite).unwrap();
        let first = committed_parts(&dest).len();
        write_year(&dest, &[sample_batch()], WriteMode::Append).unwrap();
        let parts = committed_parts(&dest);
        assert_eq!(parts.len(), first * 2, "{parts:?}");
        assert_eq!(read_ids(&dest), vec![1, 1, 2, 2, 3, 3, 4, 4]);
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn overwrite_replaces_dataset() {
        let dest = tmp_dest();
        write_year(&dest, &[sample_batch()], WriteMode::Overwrite).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("year", DataType::Int64, true),
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let only_2020 = Batch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(2020)])),
                Arc::new(Int64Array::from(vec![Some(9)])),
                Arc::new(StringArray::from(vec![Some("z")])),
            ],
        )
        .unwrap();
        write_year(&dest, &[only_2020], WriteMode::Overwrite).unwrap();
        let names: Vec<String> = committed_parts(&dest)
            .iter()
            .map(|p| {
                p.strip_prefix(&dest)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert!(names.iter().any(|n| n.contains("year=2020")), "{names:?}");
        assert!(names.iter().all(|n| !n.contains("year=2021")), "{names:?}");
        assert_eq!(read_ids(&dest), vec![9]);
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn cancel_leaves_dataset_unchanged() {
        let dest = tmp_dest();
        write_year(&dest, &[sample_batch()], WriteMode::Overwrite).unwrap();
        let before = read_ids(&dest);
        let token = CancelToken::new();
        token.cancel();
        let cols = [String::from("year")];
        let err = write_parquet_cancel(
            &dest,
            &[sample_batch()],
            ParquetWriteOptions {
                partition_by: &cols,
                mode: WriteMode::Append,
            },
            &token,
        )
        .unwrap_err();
        assert!(matches!(err, Error::Cancelled), "{err:?}");
        assert_eq!(read_ids(&dest), before);
        let leftover: Vec<_> = fs::read_dir(&dest)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with(".mizuchi-staging-")
            })
            .collect();
        assert!(leftover.is_empty(), "{leftover:?}");
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn crash_before_commit_leaves_dataset_unchanged() {
        let dest = tmp_dest();
        write_year(&dest, &[sample_batch()], WriteMode::Overwrite).unwrap();
        let before = read_ids(&dest);
        let err = write_year_inner(
            &dest,
            &[sample_batch()],
            WriteMode::Append,
            CommitCtrl {
                fail_before_commit: true,
                leave_staging: true,
                ..CommitCtrl::default()
            },
        )
        .unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err:?}");
        assert_eq!(read_ids(&dest), before);
        let staging: Vec<_> = fs::read_dir(&dest)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with(".mizuchi-staging-")
            })
            .collect();
        assert_eq!(
            staging.len(),
            1,
            "crash leaves staging, not committed files"
        );
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn injected_failure_cleans_staging() {
        let dest = tmp_dest();
        write_year(&dest, &[sample_batch()], WriteMode::Overwrite).unwrap();
        let before = read_ids(&dest);
        let err = write_year_inner(
            &dest,
            &[sample_batch()],
            WriteMode::Append,
            CommitCtrl {
                fail_before_commit: true,
                leave_staging: false,
                ..CommitCtrl::default()
            },
        )
        .unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err:?}");
        assert_eq!(read_ids(&dest), before);
        let leftover: Vec<_> = fs::read_dir(&dest)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with(".mizuchi-staging-")
            })
            .collect();
        assert!(leftover.is_empty());
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn unpartitioned_write() {
        let dest = tmp_dest();
        write_parquet(
            &dest,
            &[sample_batch()],
            ParquetWriteOptions {
                partition_by: &[],
                mode: WriteMode::Overwrite,
            },
        )
        .unwrap();
        let parts = committed_parts(&dest);
        assert_eq!(parts.len(), 1);
        assert!(parts[0].ends_with("part-00000.parquet"));
        assert_eq!(read_ids(&dest), vec![1, 2, 3, 4]);
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn cancel_mid_write_leaves_dataset_unchanged() {
        let dest = tmp_dest();
        write_year(&dest, &[sample_batch()], WriteMode::Overwrite).unwrap();
        let before = read_ids(&dest);
        let err = write_year_inner(
            &dest,
            &[sample_batch()],
            WriteMode::Append,
            CommitCtrl {
                cancel_after_groups: Some(1),
                ..CommitCtrl::default()
            },
        )
        .unwrap_err();
        assert!(matches!(err, Error::Cancelled), "{err:?}");
        assert_eq!(read_ids(&dest), before);
        let leftover: Vec<_> = fs::read_dir(&dest)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with(".mizuchi-staging-")
            })
            .collect();
        assert!(leftover.is_empty(), "{leftover:?}");
        let _ = fs::remove_dir_all(&dest);
    }
}
