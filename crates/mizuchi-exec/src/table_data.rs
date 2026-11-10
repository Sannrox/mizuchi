//! Rows behind a registered table: resident batches, or Parquet files that a
//! scan decodes on demand.
//!
//! A Parquet registration reads footers only. It stamps each file (see
//! [`FileStamp`]) and derives table stats from footer totals, so a
//! registered table costs no decoded memory. Each scan reopens the files,
//! prunes row groups with its filters, decodes only the columns it reads, and
//! yields one morsel at a time. A file that changed after registration fails
//! the scan rather than mixing two versions into one result.

use std::collections::BTreeSet;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use arrow::datatypes::SchemaRef;
use mizuchi_core::batch::{Batch, BatchPolicy};
use mizuchi_core::error::{Error, Result};
use mizuchi_core::expr::Expr;
use mizuchi_io::{EncryptionSession, ParquetBatches, ParquetReadOptions};
use mizuchi_plan::{ColumnStats, TableStats};

/// Rows behind one registered table.
#[derive(Clone, Debug)]
pub(crate) enum TableData {
    /// Decoded batches held by the session.
    Resident(Vec<Batch>),
    /// Parquet files decoded at scan time.
    Parquet(ParquetFiles),
}

/// Parquet files of one table plus the read options fixed at registration.
#[derive(Clone)]
pub(crate) struct ParquetFiles {
    files: Vec<FileStamp>,
    schema: SchemaRef,
    /// Columns the table exposes, when registration projected the files.
    projection: Option<Vec<String>>,
    /// Registration filters, applied by every scan.
    filters: Vec<Expr>,
    encryption: Option<EncryptionSession>,
}

impl std::fmt::Debug for ParquetFiles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetFiles")
            .field("files", &self.files.len())
            .field("projection", &self.projection)
            .field("filters", &self.filters.len())
            .finish_non_exhaustive()
    }
}

/// Identity of a file when it was registered: length and modification time,
/// plus device, inode, and change time on Unix. Change time cannot be set by
/// an unprivileged writer, so a replacement that keeps size and mtime still
/// differs.
#[derive(Clone, PartialEq)]
struct FileStamp {
    path: PathBuf,
    len: u64,
    modified: Option<SystemTime>,
    identity: Option<(u64, u64, i64, i64)>,
}

impl FileStamp {
    fn take(path: &Path) -> Result<Self> {
        let meta =
            std::fs::metadata(path).map_err(|e| Error::Io(format!("{}: {e}", path.display())))?;
        Ok(Self::from_meta(path, &meta))
    }

    fn from_meta(path: &Path, meta: &std::fs::Metadata) -> Self {
        #[cfg(unix)]
        let identity = {
            use std::os::unix::fs::MetadataExt;
            Some((meta.dev(), meta.ino(), meta.ctime(), meta.ctime_nsec()))
        };
        #[cfg(not(unix))]
        let identity = None;
        Self {
            path: path.to_path_buf(),
            len: meta.len(),
            modified: meta.modified().ok(),
            identity,
        }
    }

    /// Fail when the file at the path is no longer the one registered.
    fn check(&self) -> Result<()> {
        self.expect(&Self::take(&self.path)?)
    }

    /// Fail when the open handle `file` is no longer the one registered.
    fn check_open(&self, file: &File) -> Result<()> {
        let meta = file
            .metadata()
            .map_err(|e| Error::Io(format!("{}: {e}", self.path.display())))?;
        self.expect(&Self::from_meta(&self.path, &meta))
    }

    fn expect(&self, now: &Self) -> Result<()> {
        if now != self {
            return Err(Error::Io(format!(
                "{}: file changed since it was registered; register it again",
                self.path.display()
            )));
        }
        Ok(())
    }
}

impl ParquetFiles {
    /// Stamp `files`, check that they decode to one schema, and sum their
    /// footers into table stats. Decodes no rows. A row-group prune hook
    /// cannot outlive this call, so registration refuses one.
    pub(crate) fn open(
        files: &[PathBuf],
        opts: &ParquetReadOptions<'_>,
    ) -> Result<(Self, SchemaRef, TableStats)> {
        if opts.prune_row_group.is_some() {
            return Err(Error::Plan(
                "a registered Parquet table cannot keep a row-group prune hook; pass filters"
                    .into(),
            ));
        }
        let mut schema: Option<SchemaRef> = None;
        let mut stats = TableStats::default();
        let mut stamps = Vec::with_capacity(files.len());
        for file in files {
            let stamp = FileStamp::take(file)?;
            let (batches, _) = mizuchi_io::stream_parquet(file, opts)?;
            let file_schema = mizuchi_core::schema::from_arrow_schema(batches.schema())?;
            match &schema {
                None => schema = Some(file_schema.clone()),
                Some(first) if first.as_ref() != file_schema.as_ref() => {
                    return Err(Error::InvalidBatch(format!(
                        "{}: schema differs from the other files of this table",
                        file.display()
                    )));
                }
                Some(_) => {}
            }
            add_footer(
                &mut stats,
                &file_schema,
                &mizuchi_io::parquet_footer(file, opts.encryption)?,
                stamps.is_empty(),
            );
            stamps.push(stamp);
        }
        let schema = schema.ok_or_else(|| Error::Io("no parquet files to register".into()))?;
        let files = Self {
            files: stamps,
            schema: schema.clone(),
            projection: opts.projection.map(<[String]>::to_vec),
            filters: opts.filters.map(<[Expr]>::to_vec).unwrap_or_default(),
            encryption: opts.encryption.cloned(),
        };
        Ok((files, schema, stats))
    }

    /// Fail when any file changed since registration.
    pub(crate) fn check(&self) -> Result<()> {
        self.files.iter().try_for_each(FileStamp::check)
    }

    /// Decode every file on demand. `columns` narrows the read to what the
    /// scan uses (`None` reads the table schema); `filters` are scan preds
    /// on table columns, used to prune row groups and pages.
    pub(crate) fn morsels(
        &self,
        columns: Option<Vec<String>>,
        filters: &[Expr],
        policy: &BatchPolicy,
    ) -> FileMorsels {
        let mut all = self.filters.clone();
        all.extend(
            filters
                .iter()
                .filter(|f| by_name(f) && self.prunes_safely(f))
                .cloned(),
        );
        FileMorsels {
            files: self.files.clone().into_iter(),
            current: None,
            columns: columns.or_else(|| self.projection.clone()),
            filters: all,
            encryption: self.encryption.clone(),
            policy: *policy,
        }
    }
}

impl ParquetFiles {
    /// True when every column `pred` names has a type whose Parquet footer
    /// and page statistics compare like the SQL value. Decimal, date, and
    /// time columns store scaled or encoded integers, so their preds stay in
    /// the engine filter and never prune.
    fn prunes_safely(&self, pred: &Expr) -> bool {
        column_names(pred).iter().all(|name| {
            self.schema
                .field_with_name(name)
                .is_ok_and(|f| plain_stats(f.data_type()))
        })
    }
}

fn plain_stats(ty: &arrow::datatypes::DataType) -> bool {
    use arrow::datatypes::DataType as T;
    matches!(
        ty,
        T::Int8
            | T::Int16
            | T::Int32
            | T::Int64
            | T::UInt8
            | T::UInt16
            | T::UInt32
            | T::UInt64
            | T::Float32
            | T::Float64
            | T::Utf8
            | T::LargeUtf8
            | T::Boolean
    )
}

/// Columns a scan must decode: `projection` plus the columns its filters
/// name. `None` (read every column) when a filter refers to a column by
/// ordinal, which only holds against the full table schema.
pub(crate) fn scan_columns(projection: Option<&[String]>, filters: &[Expr]) -> Option<Vec<String>> {
    let names = projection?;
    if !filters.iter().all(by_name) {
        return None;
    }
    let mut read: Vec<String> = names.to_vec();
    for name in filters.iter().flat_map(column_names) {
        if !read.contains(&name) {
            read.push(name);
        }
    }
    Some(read)
}

/// True when `expr` names its columns rather than using ordinals, so it
/// evaluates the same against a batch holding only some table columns.
fn by_name(expr: &Expr) -> bool {
    crate::filters_on_projected(std::slice::from_ref(expr), &column_names(expr))
}

fn column_names(expr: &Expr) -> Vec<String> {
    let mut refs = BTreeSet::new();
    crate::markings::collect_column_names(expr, &arrow::datatypes::Schema::empty(), &mut refs);
    refs.into_iter().collect()
}

fn add_footer(
    stats: &mut TableStats,
    schema: &arrow::datatypes::Schema,
    footer: &mizuchi_io::ParquetFooter,
    first: bool,
) {
    stats.row_count += footer.rows;
    for field in schema.fields() {
        let name = field.name();
        let col = footer.columns.iter().find(|c| c.name == *name);
        let int64 = field.data_type() == &arrow::datatypes::DataType::Int64;
        let (min, max) = match col {
            Some(c) if int64 => (c.min_i64, c.max_i64),
            _ => (None, None),
        };
        let nulls = col.and_then(|c| c.null_count).unwrap_or(0);
        let entry = stats
            .columns
            .entry(name.clone())
            .or_insert_with(|| ColumnStats {
                ndv: None,
                min_i64: min,
                max_i64: max,
                null_count: 0,
            });
        if !first {
            entry.min_i64 = entry.min_i64.zip(min).map(|(a, b)| a.min(b));
            entry.max_i64 = entry.max_i64.zip(max).map(|(a, b)| a.max(b));
        }
        entry.null_count += nulls;
    }
}

/// Morsels of a table in registration order.
pub(crate) enum Morsels {
    Resident(std::vec::IntoIter<Batch>),
    Files(Box<FileMorsels>),
}

impl Iterator for Morsels {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Result<Batch>> {
        match self {
            Self::Resident(batches) => batches.next().map(Ok),
            Self::Files(files) => files.next(),
        }
    }
}

/// Decodes Parquet files one at a time, one morsel per `next`. Each file is
/// checked against its stamp through the open handle when the read starts,
/// before each morsel, and at its end, so a rewrite during the scan fails it.
pub(crate) struct FileMorsels {
    files: std::vec::IntoIter<FileStamp>,
    current: Option<(ParquetBatches, File, FileStamp)>,
    columns: Option<Vec<String>>,
    filters: Vec<Expr>,
    encryption: Option<EncryptionSession>,
    policy: BatchPolicy,
}

impl FileMorsels {
    fn open(&self, stamp: FileStamp) -> Result<(ParquetBatches, File, FileStamp)> {
        let file = File::open(&stamp.path)
            .map_err(|e| Error::Io(format!("{}: {e}", stamp.path.display())))?;
        stamp.check_open(&file)?;
        let reader = file
            .try_clone()
            .map_err(|e| Error::Io(format!("{}: {e}", stamp.path.display())))?;
        let opts = ParquetReadOptions {
            projection: self.columns.as_deref(),
            prune_row_group: None,
            filters: (!self.filters.is_empty()).then_some(self.filters.as_slice()),
            policy: self.policy,
            encryption: self.encryption.as_ref(),
        };
        let label = stamp.path.display().to_string();
        let batches = mizuchi_io::stream_parquet_file(reader, &label, &opts)?.0;
        Ok((batches, file, stamp))
    }

    /// Stop after an error: later files must not follow a failed one.
    fn halt(&mut self) {
        self.files = Vec::new().into_iter();
        self.current = None;
    }
}

impl Iterator for FileMorsels {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Result<Batch>> {
        loop {
            if let Some((batches, file, stamp)) = &mut self.current {
                // Check the handle before yielding, so a scan that stops
                // early (LIMIT) still rejects a file rewritten under it.
                let step = match batches.next() {
                    Some(batch) => Some(stamp.check_open(file).and(batch)),
                    None => stamp.check_open(file).err().map(Err),
                };
                match step {
                    Some(Ok(batch)) => return Some(Ok(batch)),
                    Some(Err(err)) => {
                        self.halt();
                        return Some(Err(err));
                    }
                    None => self.current = None,
                }
            }
            let stamp = self.files.next()?;
            match self.open(stamp) {
                Ok(open) => self.current = Some(open),
                Err(err) => {
                    self.halt();
                    return Some(Err(err));
                }
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn replacement_keeping_size_and_mtime_fails_the_stamp() {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "mizuchi-stamp-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("t.parquet");
        std::fs::write(&path, b"aaaa").unwrap();
        let stamp = FileStamp::take(&path).unwrap();
        stamp.check().unwrap();

        let swap = dir.join("swap");
        std::fs::write(&swap, b"bbbb").unwrap();
        let mtime = std::fs::metadata(&path).unwrap().modified().unwrap();
        File::options()
            .write(true)
            .open(&swap)
            .unwrap()
            .set_modified(mtime)
            .unwrap();
        std::fs::rename(&swap, &path).unwrap();
        let err = stamp.check().unwrap_err().to_string();
        assert!(err.contains("changed since it was registered"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
