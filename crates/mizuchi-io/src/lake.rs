//! Delta Lake and Apache Iceberg table snapshots.
//!
//! Writes use the published log formats: Delta `_delta_log` JSON commits and
//! Iceberg `metadata/vN.metadata.json` snapshots. Data files are Parquet.
//! The next log/metadata file is created exclusively (`create_new` on a
//! path, [`object_store::PutMode::Create`] on an object store); if it already
//! exists the writer returns [`Error::Io`] instead of replacing a commit.
//! Time travel replays the log to a version or timestamp. Schema evolution
//! records add, rename, and widen in the format metadata and projects old
//! files onto the current schema on read.
//!
//! Local paths and caller-supplied [`ObjectStore`] handles share one FileIO
//! seam. The engine never constructs an object store or opens credentials.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, new_null_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use futures::TryStreamExt;
use futures::executor::block_on;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use serde_json::{Value, json};

use crate::crypto::{EncryptionSession, parquet_writer_properties};
use crate::read::{
    parquet_reader_options_with, require_encrypted_footer, require_encrypted_footer_file,
};
use crate::write::WriteMode;
use mizuchi_core::batch::Batch;
use mizuchi_core::error::{Error, Result};

/// Snapshot identity returned after a commit or read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LakeSnapshot {
    /// `delta` or `iceberg`.
    pub format: String,
    /// Delta log version, or Iceberg metadata version.
    pub version: i64,
    /// Iceberg snapshot id (Delta uses the same value as `version`).
    pub snapshot_id: i64,
    /// Commit timestamp in milliseconds since the Unix epoch.
    pub timestamp_ms: i64,
    /// Data file paths relative to the table root.
    pub files: Vec<String>,
}

/// Which snapshot a read should reconstruct.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeTravel {
    /// Current snapshot.
    Latest,
    /// Delta log version or Iceberg metadata version.
    Version(i64),
    /// Latest snapshot with `timestamp_ms` less than or equal to this value.
    TimestampMs(i64),
}

/// One schema change recorded as a metadata commit.
#[derive(Clone, Debug)]
pub enum SchemaEvolution {
    /// Append a nullable column. Existing files read as null.
    AddColumn {
        /// New field name.
        name: String,
        /// Arrow type stored in the table schema.
        data_type: DataType,
    },
    /// Change a field's name. Physical Parquet names stay stable.
    RenameColumn {
        /// Current name.
        from: String,
        /// New name.
        to: String,
    },
    /// Promote a column to a wider Arrow type (`Int32`→`Int64`, `Float32`→`Float64`).
    WidenColumn {
        /// Column to promote.
        name: String,
        /// Wider type.
        data_type: DataType,
    },
}

static FILE_SEQ: AtomicU64 = AtomicU64::new(1);

/// FileIO for lake commits: local paths or a caller-supplied object store.
trait LakeIo {
    fn label(&self) -> String;
    fn put_create(&self, rel: &str, bytes: &[u8]) -> Result<()>;
    fn put(&self, rel: &str, bytes: &[u8]) -> Result<()>;
    fn get(&self, rel: &str) -> Result<Vec<u8>>;
    fn list(&self, prefix: &str) -> Result<Vec<String>>;
    fn encryption(&self) -> Option<&EncryptionSession> {
        None
    }
    fn write_parquet(
        &self,
        rel: &str,
        batches: &[Batch],
        schema: &Schema,
        physical: &BTreeMap<String, String>,
    ) -> Result<u64> {
        let file_schema = physical_schema(schema, physical);
        let mut buf = Vec::new();
        {
            let props = lake_writer_properties(self.encryption(), file_schema.as_ref(), physical)?;
            let mut writer = ArrowWriter::try_new(&mut buf, file_schema.clone(), props)
                .map_err(|e| Error::Io(e.to_string()))?;
            for batch in batches {
                let projected = to_physical_batch(batch, schema, physical)?;
                if projected.num_rows() > 0 {
                    writer
                        .write(projected.as_arrow())
                        .map_err(|e| Error::Io(e.to_string()))?;
                }
            }
            writer.close().map_err(|e| Error::Io(e.to_string()))?;
        }
        let size = buf.len() as u64;
        self.put_create(rel, &buf)?;
        Ok(size)
    }
    fn read_parquet(&self, rel: &str) -> Result<Vec<Batch>> {
        read_parquet_bytes(rel, &self.get(rel)?, self.encryption())
    }
}

struct PathIo {
    root: PathBuf,
    encryption: Option<EncryptionSession>,
}

impl PathIo {
    fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            encryption: None,
        }
    }

    fn with_keys(mut self, keys: EncryptionSession) -> Self {
        self.encryption = Some(keys);
        self
    }
}

impl LakeIo for PathIo {
    fn label(&self) -> String {
        self.root.display().to_string()
    }

    fn encryption(&self) -> Option<&EncryptionSession> {
        self.encryption.as_ref()
    }

    fn put_create(&self, rel: &str, bytes: &[u8]) -> Result<()> {
        exclusive_create(&confined(&self.root, rel)?, bytes)
    }

    fn put(&self, rel: &str, bytes: &[u8]) -> Result<()> {
        let path = confined(&self.root, rel)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| io_err(parent, e))?;
        }
        fs::write(&path, bytes).map_err(|e| io_err(&path, e))
    }

    fn get(&self, rel: &str) -> Result<Vec<u8>> {
        let path = confined(&self.root, rel)?;
        fs::read(&path).map_err(|e| io_err(&path, e))
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let base = if prefix.is_empty() {
            self.root.clone()
        } else {
            confined(&self.root, prefix)?
        };
        walk_files(&base, &self.root, &mut out)?;
        out.sort();
        Ok(out)
    }

    fn write_parquet(
        &self,
        rel: &str,
        batches: &[Batch],
        schema: &Schema,
        physical: &BTreeMap<String, String>,
    ) -> Result<u64> {
        let path = confined(&self.root, rel)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| io_err(parent, e))?;
        }
        let file_schema = physical_schema(schema, physical);
        let mut opts = OpenOptions::new();
        opts.write(true).create_new(true);
        let file = match opts.open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(Error::Io(format!(
                    "concurrent writer conflict: {}",
                    path.display()
                )));
            }
            Err(e) => return Err(io_err(&path, e)),
        };
        let props = lake_writer_properties(self.encryption(), file_schema.as_ref(), physical)?;
        let mut writer = ArrowWriter::try_new(file, file_schema.clone(), props)
            .map_err(|e| Error::Io(e.to_string()))?;
        for batch in batches {
            let projected = to_physical_batch(batch, schema, physical)?;
            if projected.num_rows() > 0 {
                writer
                    .write(projected.as_arrow())
                    .map_err(|e| Error::Io(e.to_string()))?;
            }
        }
        writer.close().map_err(|e| Error::Io(e.to_string()))?;
        Ok(fs::metadata(&path).map(|m| m.len()).unwrap_or(0))
    }

    fn read_parquet(&self, rel: &str) -> Result<Vec<Batch>> {
        let path = confined(&self.root, rel)?;
        let file = File::open(&path).map_err(|e| io_err(&path, e))?;
        require_encrypted_footer_file(&file, self.encryption())?;
        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
            file,
            parquet_reader_options_with(self.encryption())?,
        )
        .map_err(|e| Error::Io(format!("{}: {e}", path.display())))?;
        let reader = builder
            .with_batch_size(mizuchi_core::batch::DEFAULT_MAX_ROWS)
            .build()
            .map_err(|e| Error::Io(format!("{}: {e}", path.display())))?;
        let mut out = Vec::new();
        for item in reader {
            let batch = item.map_err(|e| Error::Io(e.to_string()))?;
            out.push(Batch::from_arrow(batch)?);
        }
        Ok(out)
    }
}

struct StoreIo<'a> {
    store: &'a dyn ObjectStore,
    prefix: String,
    encryption: Option<EncryptionSession>,
}

impl<'a> StoreIo<'a> {
    fn new(store: &'a dyn ObjectStore, prefix: &str) -> Self {
        Self {
            store,
            prefix: prefix.trim_matches('/').to_string(),
            encryption: None,
        }
    }

    fn with_keys(mut self, keys: EncryptionSession) -> Self {
        self.encryption = Some(keys);
        self
    }

    fn loc(&self, rel: &str) -> Result<ObjectPath> {
        check_rel(rel)?;
        let root = crate::store::object_path(&self.prefix)?;
        crate::store::join_path(&root, rel)
    }
}

impl LakeIo for StoreIo<'_> {
    fn label(&self) -> String {
        self.prefix.clone()
    }

    fn encryption(&self) -> Option<&EncryptionSession> {
        self.encryption.as_ref()
    }

    fn put_create(&self, rel: &str, bytes: &[u8]) -> Result<()> {
        let loc = self.loc(rel)?;
        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match block_on(
            self.store
                .put_opts(&loc, PutPayload::from(bytes.to_vec()), opts),
        ) {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { path, .. }) => {
                Err(Error::Io(format!("concurrent writer conflict: {path}")))
            }
            Err(e) => Err(crate::store::store_err(e)),
        }
    }

    fn put(&self, rel: &str, bytes: &[u8]) -> Result<()> {
        let loc = self.loc(rel)?;
        block_on(self.store.put(&loc, PutPayload::from(bytes.to_vec())))
            .map(|_| ())
            .map_err(crate::store::store_err)
    }

    fn get(&self, rel: &str) -> Result<Vec<u8>> {
        let loc = self.loc(rel)?;
        let get = block_on(self.store.get(&loc)).map_err(crate::store::store_err)?;
        let bytes = block_on(get.bytes()).map_err(crate::store::store_err)?;
        Ok(bytes.to_vec())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        check_rel(prefix)?;
        let loc = self.loc(prefix)?;
        let metas: Vec<object_store::ObjectMeta> =
            block_on(self.store.list(Some(&loc)).try_collect()).map_err(crate::store::store_err)?;
        let mut out: Vec<String> = metas
            .into_iter()
            .map(|m| crate::store::relative_to(&m.location, &self.prefix))
            .collect();
        out.sort();
        Ok(out)
    }
}

fn walk_files(dir: &Path, root: &Path, out: &mut Vec<String>) -> Result<()> {
    let ents = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(io_err(dir, e)),
    };
    for ent in ents {
        let ent = ent.map_err(|e| io_err(dir, e))?;
        let path = ent.path();
        if path.is_dir() {
            walk_files(&path, root, out)?;
        } else {
            let rel = path.strip_prefix(root).unwrap_or(&path);
            out.push(rel.to_string_lossy().replace('\\', "/"));
        }
    }
    Ok(())
}

fn check_rel(rel: &str) -> Result<()> {
    if rel.is_empty() {
        return Ok(());
    }
    let rel_path = Path::new(rel);
    if rel_path.is_absolute() {
        return Err(Error::Io(format!("path escapes table root: {rel}")));
    }
    for c in rel_path.components() {
        match c {
            Component::Normal(_) | Component::CurDir => {}
            _ => return Err(Error::Io(format!("path escapes table root: {rel}"))),
        }
    }
    Ok(())
}

/// Write `batches` as a Delta table at `dest`. An existing table gets a new
/// commit: [`WriteMode::Overwrite`] removes prior data files, [`WriteMode::Append`]
/// adds files beside them.
pub fn write_delta(
    dest: impl AsRef<Path>,
    batches: &[Batch],
    mode: WriteMode,
) -> Result<LakeSnapshot> {
    write_delta_at(dest.as_ref(), batches, mode, None)
}

/// [`write_delta`] with Parquet modular encryption.
pub fn write_delta_keys(
    dest: impl AsRef<Path>,
    batches: &[Batch],
    mode: WriteMode,
    keys: &EncryptionSession,
) -> Result<LakeSnapshot> {
    write_delta_io(
        &PathIo::new(dest.as_ref()).with_keys(keys.clone()),
        batches,
        mode,
        None,
    )
}

/// [`write_delta`] that fails if the current version is not `expected`.
pub fn write_delta_at(
    dest: impl AsRef<Path>,
    batches: &[Batch],
    mode: WriteMode,
    expected: Option<i64>,
) -> Result<LakeSnapshot> {
    write_delta_io(&PathIo::new(dest.as_ref()), batches, mode, expected)
}

/// [`write_delta_at`] with Parquet modular encryption.
pub fn write_delta_at_keys(
    dest: impl AsRef<Path>,
    batches: &[Batch],
    mode: WriteMode,
    expected: Option<i64>,
    keys: &EncryptionSession,
) -> Result<LakeSnapshot> {
    write_delta_io(
        &PathIo::new(dest.as_ref()).with_keys(keys.clone()),
        batches,
        mode,
        expected,
    )
}

/// Read a Delta table, optionally at a past version or timestamp.
pub fn read_delta(dest: impl AsRef<Path>, travel: TimeTravel) -> Result<Vec<Batch>> {
    let io = PathIo::new(dest.as_ref());
    let snap = delta_replay(&io, travel)?;
    read_files(&io, &snap.files, &snap.schema, &snap.physical)
}

/// [`read_delta`] that unwraps encrypted data files.
pub fn read_delta_keys(
    dest: impl AsRef<Path>,
    travel: TimeTravel,
    keys: &EncryptionSession,
) -> Result<Vec<Batch>> {
    let io = PathIo::new(dest.as_ref()).with_keys(keys.clone());
    let snap = delta_replay(&io, travel)?;
    read_files(&io, &snap.files, &snap.schema, &snap.physical)
}

/// Current Delta snapshot metadata.
pub fn delta_head(dest: impl AsRef<Path>) -> Result<LakeSnapshot> {
    Ok(delta_replay(&PathIo::new(dest.as_ref()), TimeTravel::Latest)?.meta)
}

/// Commit a schema change to a Delta table.
pub fn evolve_delta(dest: impl AsRef<Path>, change: SchemaEvolution) -> Result<LakeSnapshot> {
    evolve_delta_io(&PathIo::new(dest.as_ref()), change)
}

/// Write `batches` as an Iceberg table at `dest`.
pub fn write_iceberg(
    dest: impl AsRef<Path>,
    batches: &[Batch],
    mode: WriteMode,
) -> Result<LakeSnapshot> {
    write_iceberg_at(dest.as_ref(), batches, mode, None)
}

/// [`write_iceberg`] with Parquet modular encryption.
pub fn write_iceberg_keys(
    dest: impl AsRef<Path>,
    batches: &[Batch],
    mode: WriteMode,
    keys: &EncryptionSession,
) -> Result<LakeSnapshot> {
    write_iceberg_io(
        &PathIo::new(dest.as_ref()).with_keys(keys.clone()),
        batches,
        mode,
        None,
    )
}

/// [`write_iceberg`] that fails if the current version is not `expected`.
pub fn write_iceberg_at(
    dest: impl AsRef<Path>,
    batches: &[Batch],
    mode: WriteMode,
    expected: Option<i64>,
) -> Result<LakeSnapshot> {
    write_iceberg_io(&PathIo::new(dest.as_ref()), batches, mode, expected)
}

/// [`write_iceberg_at`] with Parquet modular encryption.
pub fn write_iceberg_at_keys(
    dest: impl AsRef<Path>,
    batches: &[Batch],
    mode: WriteMode,
    expected: Option<i64>,
    keys: &EncryptionSession,
) -> Result<LakeSnapshot> {
    write_iceberg_io(
        &PathIo::new(dest.as_ref()).with_keys(keys.clone()),
        batches,
        mode,
        expected,
    )
}

/// Read an Iceberg table, optionally at a past snapshot.
pub fn read_iceberg(dest: impl AsRef<Path>, travel: TimeTravel) -> Result<Vec<Batch>> {
    let io = PathIo::new(dest.as_ref());
    let snap = iceberg_replay(&io, travel)?;
    read_files(&io, &snap.files, &snap.schema, &snap.physical)
}

/// [`read_iceberg`] that unwraps encrypted data files.
pub fn read_iceberg_keys(
    dest: impl AsRef<Path>,
    travel: TimeTravel,
    keys: &EncryptionSession,
) -> Result<Vec<Batch>> {
    let io = PathIo::new(dest.as_ref()).with_keys(keys.clone());
    let snap = iceberg_replay(&io, travel)?;
    read_files(&io, &snap.files, &snap.schema, &snap.physical)
}

/// Current Iceberg snapshot metadata.
pub fn iceberg_head(dest: impl AsRef<Path>) -> Result<LakeSnapshot> {
    Ok(iceberg_replay(&PathIo::new(dest.as_ref()), TimeTravel::Latest)?.meta)
}

/// Commit a schema change to an Iceberg table.
pub fn evolve_iceberg(dest: impl AsRef<Path>, change: SchemaEvolution) -> Result<LakeSnapshot> {
    evolve_iceberg_io(&PathIo::new(dest.as_ref()), change)
}

/// [`write_delta`] against a caller-supplied object store prefix.
pub fn write_delta_store(
    store: &dyn ObjectStore,
    prefix: &str,
    batches: &[Batch],
    mode: WriteMode,
) -> Result<LakeSnapshot> {
    write_delta_store_at(store, prefix, batches, mode, None)
}

/// [`write_delta_store`] with Parquet modular encryption.
pub fn write_delta_store_keys(
    store: &dyn ObjectStore,
    prefix: &str,
    batches: &[Batch],
    mode: WriteMode,
    keys: &EncryptionSession,
) -> Result<LakeSnapshot> {
    write_delta_io(
        &StoreIo::new(store, prefix).with_keys(keys.clone()),
        batches,
        mode,
        None,
    )
}

/// [`write_delta_store_at`] with Parquet modular encryption.
pub fn write_delta_store_at_keys(
    store: &dyn ObjectStore,
    prefix: &str,
    batches: &[Batch],
    mode: WriteMode,
    expected: Option<i64>,
    keys: &EncryptionSession,
) -> Result<LakeSnapshot> {
    write_delta_io(
        &StoreIo::new(store, prefix).with_keys(keys.clone()),
        batches,
        mode,
        expected,
    )
}

/// [`write_delta_store`] that fails if the current version is not `expected`.
pub fn write_delta_store_at(
    store: &dyn ObjectStore,
    prefix: &str,
    batches: &[Batch],
    mode: WriteMode,
    expected: Option<i64>,
) -> Result<LakeSnapshot> {
    write_delta_io(&StoreIo::new(store, prefix), batches, mode, expected)
}

/// Read a Delta table from an object store prefix.
pub fn read_delta_store(
    store: &dyn ObjectStore,
    prefix: &str,
    travel: TimeTravel,
) -> Result<Vec<Batch>> {
    let io = StoreIo::new(store, prefix);
    let snap = delta_replay(&io, travel)?;
    read_files(&io, &snap.files, &snap.schema, &snap.physical)
}

/// [`read_delta_store`] that unwraps encrypted data files.
pub fn read_delta_store_keys(
    store: &dyn ObjectStore,
    prefix: &str,
    travel: TimeTravel,
    keys: &EncryptionSession,
) -> Result<Vec<Batch>> {
    let io = StoreIo::new(store, prefix).with_keys(keys.clone());
    let snap = delta_replay(&io, travel)?;
    read_files(&io, &snap.files, &snap.schema, &snap.physical)
}

/// Current Delta snapshot on an object store prefix.
pub fn delta_head_store(store: &dyn ObjectStore, prefix: &str) -> Result<LakeSnapshot> {
    Ok(delta_replay(&StoreIo::new(store, prefix), TimeTravel::Latest)?.meta)
}

/// Commit a schema change to a Delta table on an object store prefix.
pub fn evolve_delta_store(
    store: &dyn ObjectStore,
    prefix: &str,
    change: SchemaEvolution,
) -> Result<LakeSnapshot> {
    evolve_delta_io(&StoreIo::new(store, prefix), change)
}

/// [`write_iceberg`] against a caller-supplied object store prefix.
pub fn write_iceberg_store(
    store: &dyn ObjectStore,
    prefix: &str,
    batches: &[Batch],
    mode: WriteMode,
) -> Result<LakeSnapshot> {
    write_iceberg_store_at(store, prefix, batches, mode, None)
}

/// [`write_iceberg_store`] with Parquet modular encryption.
pub fn write_iceberg_store_keys(
    store: &dyn ObjectStore,
    prefix: &str,
    batches: &[Batch],
    mode: WriteMode,
    keys: &EncryptionSession,
) -> Result<LakeSnapshot> {
    write_iceberg_io(
        &StoreIo::new(store, prefix).with_keys(keys.clone()),
        batches,
        mode,
        None,
    )
}

/// [`write_iceberg_store_at`] with Parquet modular encryption.
pub fn write_iceberg_store_at_keys(
    store: &dyn ObjectStore,
    prefix: &str,
    batches: &[Batch],
    mode: WriteMode,
    expected: Option<i64>,
    keys: &EncryptionSession,
) -> Result<LakeSnapshot> {
    write_iceberg_io(
        &StoreIo::new(store, prefix).with_keys(keys.clone()),
        batches,
        mode,
        expected,
    )
}

/// [`write_iceberg_store`] that fails if the current version is not `expected`.
pub fn write_iceberg_store_at(
    store: &dyn ObjectStore,
    prefix: &str,
    batches: &[Batch],
    mode: WriteMode,
    expected: Option<i64>,
) -> Result<LakeSnapshot> {
    write_iceberg_io(&StoreIo::new(store, prefix), batches, mode, expected)
}

/// Read an Iceberg table from an object store prefix.
pub fn read_iceberg_store(
    store: &dyn ObjectStore,
    prefix: &str,
    travel: TimeTravel,
) -> Result<Vec<Batch>> {
    let io = StoreIo::new(store, prefix);
    let snap = iceberg_replay(&io, travel)?;
    read_files(&io, &snap.files, &snap.schema, &snap.physical)
}

/// [`read_iceberg_store`] that unwraps encrypted data files.
pub fn read_iceberg_store_keys(
    store: &dyn ObjectStore,
    prefix: &str,
    travel: TimeTravel,
    keys: &EncryptionSession,
) -> Result<Vec<Batch>> {
    let io = StoreIo::new(store, prefix).with_keys(keys.clone());
    let snap = iceberg_replay(&io, travel)?;
    read_files(&io, &snap.files, &snap.schema, &snap.physical)
}

/// Current Iceberg snapshot on an object store prefix.
pub fn iceberg_head_store(store: &dyn ObjectStore, prefix: &str) -> Result<LakeSnapshot> {
    Ok(iceberg_replay(&StoreIo::new(store, prefix), TimeTravel::Latest)?.meta)
}

/// Commit a schema change to an Iceberg table on an object store prefix.
pub fn evolve_iceberg_store(
    store: &dyn ObjectStore,
    prefix: &str,
    change: SchemaEvolution,
) -> Result<LakeSnapshot> {
    evolve_iceberg_io(&StoreIo::new(store, prefix), change)
}

struct Resolved {
    meta: LakeSnapshot,
    schema: SchemaRef,
    files: Vec<String>,
    physical: BTreeMap<String, String>,
    /// `mizuchi.encryption` from the latest metadata commit, if any.
    encryption: Option<Value>,
}

fn write_delta_io(
    io: &dyn LakeIo,
    batches: &[Batch],
    mode: WriteMode,
    expected: Option<i64>,
) -> Result<LakeSnapshot> {
    let schema = schema_of(batches)?;
    let now = now_ms();
    let current = existing_delta(io)?;
    if let Some(exp) = expected {
        let got = current.as_ref().map(|c| c.meta.version);
        if got != Some(exp) {
            return Err(Error::Io(format!(
                "concurrent writer conflict: expected version {exp}, found {got:?}"
            )));
        }
    }
    refuse_lake_encryption_mismatch(io, current.as_ref(), mode)?;
    let next_ver = current.as_ref().map(|c| c.meta.version + 1).unwrap_or(0);
    let physical = match &current {
        Some(c) if mode == WriteMode::Append => c.physical.clone(),
        _ => default_physical(&schema),
    };
    let schema = match &current {
        Some(c) if mode == WriteMode::Append => c.schema.clone(),
        _ => schema,
    };
    let data_rel = unique_data_rel(next_ver, "");
    let size = io.write_parquet(&data_rel, batches, &schema, &physical)?;

    let mut actions = Vec::new();
    actions.push(json!({"commitInfo": {"timestamp": now, "operation": "WRITE"}}));
    if next_ver == 0 || mode == WriteMode::Overwrite {
        let schema_string = delta_schema_string(&schema, &physical)?;
        if next_ver == 0 {
            actions.push(json!({
                "protocol": {"minReaderVersion": 1, "minWriterVersion": 2}
            }));
        }
        actions.push(json!({
            "metaData": {
                "id": table_id(&io.label(), now),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_string,
                "partitionColumns": [],
                "configuration": delta_configuration(io, None),
                "createdTime": now
            }
        }));
        if next_ver != 0
            && let Some(cur) = &current
        {
            for file in &cur.files {
                actions.push(json!({
                    "remove": {
                        "path": file,
                        "deletionTimestamp": now,
                        "dataChange": true
                    }
                }));
            }
        }
    }
    actions.push(json!({
        "add": {
            "path": data_rel,
            "partitionValues": {},
            "size": size,
            "modificationTime": now,
            "dataChange": true
        }
    }));
    commit_delta_log(io, next_ver, &actions)?;
    let mut files = match (mode, current) {
        (WriteMode::Append, Some(cur)) => cur.files,
        _ => Vec::new(),
    };
    files.push(data_rel);
    Ok(LakeSnapshot {
        format: "delta".into(),
        version: next_ver,
        snapshot_id: next_ver,
        timestamp_ms: now,
        files,
    })
}

fn evolve_delta_io(io: &dyn LakeIo, change: SchemaEvolution) -> Result<LakeSnapshot> {
    let cur = delta_replay(io, TimeTravel::Latest)?;
    let (schema, physical) = apply_evolution(cur.schema, cur.physical, change)?;
    let now = now_ms();
    let next_ver = cur.meta.version + 1;
    let schema_string = delta_schema_string(&schema, &physical)?;
    let actions = vec![
        json!({"commitInfo": {"timestamp": now, "operation": "CHANGE COLUMN"}}),
        json!({
            "metaData": {
                "id": table_id(&io.label(), now),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_string,
                "partitionColumns": [],
                "configuration": delta_configuration(io, cur.encryption.as_ref()),
                "createdTime": now
            }
        }),
    ];
    commit_delta_log(io, next_ver, &actions)?;
    Ok(LakeSnapshot {
        format: "delta".into(),
        version: next_ver,
        snapshot_id: next_ver,
        timestamp_ms: now,
        files: cur.files,
    })
}

fn delta_replay(io: &dyn LakeIo, travel: TimeTravel) -> Result<Resolved> {
    let mut versions = list_delta_versions(io)?;
    if versions.is_empty() {
        return Err(Error::Io(format!("{} is not a Delta table", io.label())));
    }
    versions.sort_unstable();
    let mut files: Vec<String> = Vec::new();
    let mut schema = Arc::new(Schema::empty());
    let mut physical = BTreeMap::new();
    let mut encryption = None;
    let mut last_ts = 0i64;
    let mut last_ver = -1i64;
    if let TimeTravel::Version(v) = travel
        && !versions.contains(&v)
    {
        return Err(Error::Io(format!("Delta version {v} does not exist")));
    }
    let mut applied_any = false;
    for ver in &versions {
        let rel = format!("_delta_log/{ver:020}.json");
        let text =
            String::from_utf8(io.get(&rel)?).map_err(|e| Error::Io(format!("delta log: {e}")))?;
        let mut ts = last_ts;
        let mut actions: Vec<Value> = Vec::new();
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let v: Value =
                serde_json::from_str(line).map_err(|e| Error::Io(format!("delta log: {e}")))?;
            if let Some(info) = v.get("commitInfo") {
                ts = info.get("timestamp").and_then(Value::as_i64).unwrap_or(ts);
            }
            actions.push(v);
        }
        if let TimeTravel::TimestampMs(want) = travel
            && ts > want
        {
            break;
        }
        for v in actions {
            if let Some(md) = v.get("metaData") {
                let (s, p) = parse_delta_schema(md)?;
                schema = s;
                physical = p;
                encryption = md
                    .get("configuration")
                    .and_then(|c| c.get("mizuchi.encryption"))
                    .cloned();
            }
            if let Some(add) = v.get("add")
                && let Some(p) = add.get("path").and_then(Value::as_str)
                && !files.iter().any(|f| f == p)
            {
                files.push(p.to_string());
            }
            if let Some(rm) = v.get("remove")
                && let Some(p) = rm.get("path").and_then(Value::as_str)
            {
                files.retain(|f| f != p);
            }
        }
        last_ts = ts;
        last_ver = *ver;
        applied_any = true;
        if let TimeTravel::Version(v) = travel
            && last_ver >= v
        {
            break;
        }
    }
    if let TimeTravel::TimestampMs(want) = travel
        && !applied_any
    {
        return Err(Error::Io(format!("no Delta snapshot at timestamp {want}")));
    }
    Ok(Resolved {
        meta: LakeSnapshot {
            format: "delta".into(),
            version: last_ver,
            snapshot_id: last_ver,
            timestamp_ms: last_ts,
            files: files.clone(),
        },
        schema,
        files,
        physical,
        encryption,
    })
}

fn list_delta_versions(io: &dyn LakeIo) -> Result<Vec<i64>> {
    let mut out = Vec::new();
    for name in io.list("_delta_log")? {
        let file = name.rsplit('/').next().unwrap_or(&name);
        if let Some(stem) = file.strip_suffix(".json")
            && let Ok(v) = stem.parse::<i64>()
        {
            out.push(v);
        }
    }
    Ok(out)
}

fn commit_delta_log(io: &dyn LakeIo, version: i64, actions: &[Value]) -> Result<()> {
    let mut body = String::new();
    for action in actions {
        body.push_str(&serde_json::to_string(action).map_err(|e| Error::Io(e.to_string()))?);
        body.push('\n');
    }
    io.put_create(&format!("_delta_log/{version:020}.json"), body.as_bytes())
}

fn write_iceberg_io(
    io: &dyn LakeIo,
    batches: &[Batch],
    mode: WriteMode,
    expected: Option<i64>,
) -> Result<LakeSnapshot> {
    let schema = schema_of(batches)?;
    let now = now_ms();
    let current = existing_iceberg(io)?;
    if let Some(exp) = expected {
        let got = current.as_ref().map(|c| c.meta.version);
        if got != Some(exp) {
            return Err(Error::Io(format!(
                "concurrent writer conflict: expected version {exp}, found {got:?}"
            )));
        }
    }
    refuse_lake_encryption_mismatch(io, current.as_ref(), mode)?;
    let next_ver = current.as_ref().map(|c| c.meta.version + 1).unwrap_or(1);
    let snapshot_id = now ^ (next_ver << 8);
    let physical = match &current {
        Some(c) if mode == WriteMode::Append => c.physical.clone(),
        _ => default_physical(&schema),
    };
    let schema = match &current {
        Some(c) if mode == WriteMode::Append => c.schema.clone(),
        _ => schema,
    };
    // Reject unsupported types before writing anything, so a refused write
    // leaves no manifest for the next write's snapshot id to collide with.
    for f in schema.fields() {
        iceberg_type(f.data_type())?;
    }
    let data_rel = unique_data_rel(next_ver, "data/");
    let size = io.write_parquet(&data_rel, batches, &schema, &physical)?;
    let mut files = match (mode, &current) {
        (WriteMode::Append, Some(cur)) => cur.files.clone(),
        _ => Vec::new(),
    };
    files.push(data_rel.clone());
    let manifest_rel = format!("metadata/snap-{snapshot_id}.json");
    let manifest = json!({
        "format": "iceberg-json-manifest",
        "files": files.iter().map(|f| json!({
            "file-path": f,
            "file-format": "PARQUET",
            "file-size-in-bytes": if f == &data_rel { size } else { 0 },
        })).collect::<Vec<_>>(),
    });
    io.put_create(
        &manifest_rel,
        serde_json::to_string_pretty(&manifest)
            .map_err(|e| Error::Io(e.to_string()))?
            .as_bytes(),
    )?;
    let field_ids = field_ids_of(&schema, current.as_ref().map(|c| c.schema.as_ref()));
    let metadata = iceberg_metadata(
        io,
        next_ver,
        snapshot_id,
        now,
        &schema,
        &physical,
        &field_ids,
        &manifest_rel,
        current.as_ref(),
        None,
    )?;
    io.put_create(
        &format!("metadata/v{next_ver}.metadata.json"),
        serde_json::to_string_pretty(&metadata)
            .map_err(|e| Error::Io(e.to_string()))?
            .as_bytes(),
    )?;
    io.put(
        "metadata/version-hint.text",
        format!("{next_ver}").as_bytes(),
    )?;
    Ok(LakeSnapshot {
        format: "iceberg".into(),
        version: next_ver,
        snapshot_id,
        timestamp_ms: now,
        files,
    })
}

fn evolve_iceberg_io(io: &dyn LakeIo, change: SchemaEvolution) -> Result<LakeSnapshot> {
    let cur = iceberg_replay(io, TimeTravel::Latest)?;
    let (schema, physical) = apply_evolution(cur.schema.clone(), cur.physical.clone(), change)?;
    let now = now_ms();
    let next_ver = cur.meta.version + 1;
    let snapshot_id = cur.meta.snapshot_id;
    let manifest_rel = format!("metadata/snap-{snapshot_id}.json");
    let field_ids = field_ids_of(&schema, Some(cur.schema.as_ref()));
    let metadata = iceberg_metadata(
        io,
        next_ver,
        snapshot_id,
        now,
        &schema,
        &physical,
        &field_ids,
        &manifest_rel,
        Some(&cur),
        cur.encryption.as_ref(),
    )?;
    io.put_create(
        &format!("metadata/v{next_ver}.metadata.json"),
        serde_json::to_string_pretty(&metadata)
            .map_err(|e| Error::Io(e.to_string()))?
            .as_bytes(),
    )?;
    io.put(
        "metadata/version-hint.text",
        format!("{next_ver}").as_bytes(),
    )?;
    Ok(LakeSnapshot {
        format: "iceberg".into(),
        version: next_ver,
        snapshot_id,
        timestamp_ms: now,
        files: cur.files,
    })
}

fn iceberg_replay(io: &dyn LakeIo, travel: TimeTravel) -> Result<Resolved> {
    let latest = iceberg_latest_version(io)?
        .ok_or_else(|| Error::Io(format!("{} is not an Iceberg table", io.label())))?;
    let chosen = match travel {
        TimeTravel::Latest => latest,
        TimeTravel::Version(v) => {
            if v < 1 || v > latest {
                return Err(Error::Io(format!("Iceberg version {v} does not exist")));
            }
            v
        }
        TimeTravel::TimestampMs(want) => {
            let mut pick = None;
            for v in 1..=latest {
                let md = read_json_io(io, &format!("metadata/v{v}.metadata.json"))?;
                let ts = md
                    .get("last-updated-ms")
                    .and_then(Value::as_i64)
                    .unwrap_or(0);
                if ts <= want {
                    pick = Some(v);
                }
            }
            pick.ok_or_else(|| Error::Io(format!("no Iceberg snapshot at timestamp {want}")))?
        }
    };
    let md = read_json_io(io, &format!("metadata/v{chosen}.metadata.json"))?;
    let schema = parse_iceberg_schema(&md)?;
    let physical = iceberg_physical(&md);
    let snap_id = md
        .get("current-snapshot-id")
        .and_then(Value::as_i64)
        .unwrap_or(chosen);
    let ts = md
        .get("last-updated-ms")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let files = iceberg_files(io, &md)?;
    let encryption = md
        .get("properties")
        .and_then(|p| p.get("mizuchi.encryption"))
        .cloned();
    Ok(Resolved {
        meta: LakeSnapshot {
            format: "iceberg".into(),
            version: chosen,
            snapshot_id: snap_id,
            timestamp_ms: ts,
            files: files.clone(),
        },
        schema,
        files,
        physical,
        encryption,
    })
}

fn iceberg_files(io: &dyn LakeIo, md: &Value) -> Result<Vec<String>> {
    let snaps = md
        .get("snapshots")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::Io("Iceberg metadata missing snapshots".into()))?;
    let Some(current) = md.get("current-snapshot-id").and_then(Value::as_i64) else {
        if snaps.is_empty() {
            return Ok(Vec::new());
        }
        return Err(Error::Io(
            "Iceberg metadata missing current-snapshot-id".into(),
        ));
    };
    let snap = snaps
        .iter()
        .find(|s| s.get("snapshot-id").and_then(Value::as_i64) == Some(current))
        .ok_or_else(|| Error::Io(format!("Iceberg snapshot {current} not in metadata")))?;
    let list = snap
        .get("manifest-list")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Io("Iceberg snapshot missing manifest-list".into()))?;
    check_rel(list)?;
    let man = read_json_io(io, list)
        .map_err(|e| Error::Io(format!("Iceberg manifest is missing: {list} ({e})")))?;
    let files = man
        .get("files")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::Io("Iceberg manifest missing files".into()))?;
    let mut out = Vec::new();
    for f in files {
        let rel = f
            .get("file-path")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Io("Iceberg manifest missing file-path".into()))?;
        check_rel(rel)?;
        out.push(rel.to_string());
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn iceberg_metadata(
    io: &dyn LakeIo,
    version: i64,
    snapshot_id: i64,
    now: i64,
    schema: &Schema,
    physical: &BTreeMap<String, String>,
    field_ids: &BTreeMap<String, i64>,
    manifest_rel: &str,
    previous: Option<&Resolved>,
    keep_encryption: Option<&Value>,
) -> Result<Value> {
    let mut snapshots = previous
        .and_then(|p| {
            read_json_io(io, &format!("metadata/v{}.metadata.json", p.meta.version))
                .ok()
                .and_then(|m| m.get("snapshots").cloned())
        })
        .unwrap_or(json!([]));
    let snap_entry = json!({
        "snapshot-id": snapshot_id,
        "timestamp-ms": now,
        "manifest-list": manifest_rel,
        "summary": {"operation": "append"}
    });
    if let Some(arr) = snapshots.as_array_mut() {
        if let Some(pos) = arr
            .iter()
            .position(|s| s.get("snapshot-id").and_then(Value::as_i64) == Some(snapshot_id))
        {
            arr[pos] = snap_entry;
        } else {
            arr.push(snap_entry);
        }
    }
    let mut fields = Vec::with_capacity(schema.fields().len());
    for f in schema.fields() {
        let ty = iceberg_type(f.data_type())?;
        fields.push(json!({
            "id": field_ids.get(f.name()).copied().unwrap_or(0),
            "name": f.name(),
            "required": false,
            "type": ty,
            "physical-name": physical.get(f.name()).cloned().unwrap_or_else(|| f.name().clone()),
        }));
    }
    Ok(json!({
        "format-version": 2,
        "table-uuid": table_id(&io.label(), now),
        "location": io.label(),
        "last-sequence-number": version,
        "last-updated-ms": now,
        "last-column-id": field_ids.values().copied().max().unwrap_or(0),
        "current-schema-id": version,
        "schemas": [{
            "type": "struct",
            "schema-id": version,
            "fields": fields
        }],
        "current-snapshot-id": snapshot_id,
        "snapshots": snapshots,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "default-spec-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "default-sort-order-id": 0,
        "refs": {"main": {"snapshot-id": snapshot_id, "type": "branch"}},
        "properties": iceberg_properties(io, keep_encryption)
    }))
}

fn refuse_lake_encryption_mismatch(
    io: &dyn LakeIo,
    current: Option<&Resolved>,
    mode: WriteMode,
) -> Result<()> {
    let table_encrypted = current.and_then(|c| c.encryption.as_ref()).is_some();
    let write_encrypted = io.encryption().is_some();
    if table_encrypted && !write_encrypted {
        return Err(Error::Io(
            "encrypted lake table requires keys for writes".into(),
        ));
    }
    if !table_encrypted && write_encrypted && current.is_some() && mode == WriteMode::Append {
        return Err(Error::Io(
            "cannot append encrypted files to an unencrypted lake table".into(),
        ));
    }
    if table_encrypted && write_encrypted && mode == WriteMode::Append {
        let record = current.and_then(|c| c.encryption.as_ref());
        let table_tenant = record
            .and_then(|enc| lake_record_field(enc, "tenant_id"))
            .ok_or_else(|| Error::Io("encrypted lake table is missing tenant id".into()))?;
        let table_kek = record
            .and_then(|enc| lake_record_field(enc, "kek_id"))
            .ok_or_else(|| Error::Io("encrypted lake table is missing kek id".into()))?;
        let Some(keys) = io.encryption() else {
            return Ok(());
        };
        if table_tenant != keys.tenant().as_str() {
            return Err(Error::Io(
                "encrypted lake table tenant does not match write keys".into(),
            ));
        }
        if table_kek != keys.kek_id().as_str() {
            return Err(Error::Io(
                "encrypted lake table kek does not match write keys".into(),
            ));
        }
    }
    Ok(())
}

fn lake_record_field(enc: &Value, name: &str) -> Option<String> {
    let text = enc.as_str()?;
    let v: Value = serde_json::from_str(text).ok()?;
    v.get(name)?.as_str().map(str::to_string)
}

fn delta_configuration(io: &dyn LakeIo, keep: Option<&Value>) -> Value {
    let mut cfg = json!({"delta.columnMapping.mode": "name"});
    if let Some(keys) = io.encryption() {
        cfg["mizuchi.encryption"] = serde_json::Value::String(keys.lake_key_record());
    } else if let Some(prev) = keep {
        cfg["mizuchi.encryption"] = prev.clone();
    }
    cfg
}

fn iceberg_properties(io: &dyn LakeIo, keep: Option<&Value>) -> Value {
    if let Some(keys) = io.encryption() {
        json!({"mizuchi.encryption": keys.lake_key_record()})
    } else if let Some(prev) = keep {
        json!({"mizuchi.encryption": prev})
    } else {
        json!({})
    }
}

fn parse_iceberg_schema(md: &Value) -> Result<SchemaRef> {
    let schemas = md
        .get("schemas")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::Io("Iceberg metadata missing schemas".into()))?;
    let current = md.get("current-schema-id").and_then(Value::as_i64);
    let schema = schemas
        .iter()
        .find(|s| s.get("schema-id").and_then(Value::as_i64) == current)
        .or(schemas.last())
        .ok_or_else(|| Error::Io("Iceberg schema not found".into()))?;
    let fields = schema
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::Io("Iceberg schema has no fields".into()))?;
    let mut out = Vec::new();
    for f in fields {
        let name = f
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Io("Iceberg field missing name".into()))?;
        let ty = f
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Io("Iceberg field missing type".into()))?;
        out.push(Field::new(name, from_iceberg_type(ty)?, true));
    }
    Ok(Arc::new(Schema::new(out)))
}

fn iceberg_physical(md: &Value) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    let Some(schemas) = md.get("schemas").and_then(Value::as_array) else {
        return map;
    };
    let current = md.get("current-schema-id").and_then(Value::as_i64);
    let schema = schemas
        .iter()
        .find(|s| s.get("schema-id").and_then(Value::as_i64) == current)
        .or(schemas.last());
    let Some(fields) = schema.and_then(|s| s.get("fields").and_then(Value::as_array)) else {
        return map;
    };
    for f in fields {
        if let (Some(name), Some(phys)) = (
            f.get("name").and_then(Value::as_str),
            f.get("physical-name").and_then(Value::as_str),
        ) {
            map.insert(name.to_string(), phys.to_string());
        } else if let Some(name) = f.get("name").and_then(Value::as_str) {
            map.insert(name.to_string(), name.to_string());
        }
    }
    map
}

fn field_ids_of(schema: &Schema, previous: Option<&Schema>) -> BTreeMap<String, i64> {
    let mut ids = BTreeMap::new();
    let mut next = 1i64;
    if let Some(prev) = previous {
        for (i, f) in prev.fields().iter().enumerate() {
            ids.insert(f.name().clone(), (i as i64) + 1);
            next = next.max((i as i64) + 2);
        }
    }
    for f in schema.fields() {
        if !ids.contains_key(f.name()) {
            // renamed: try to keep id by matching later; assign next
            ids.insert(f.name().clone(), next);
            next += 1;
        }
    }
    ids
}

fn schema_of(batches: &[Batch]) -> Result<SchemaRef> {
    let Some(first) = batches.first() else {
        return Err(Error::InvalidBatch(
            "lake write needs at least one batch".into(),
        ));
    };
    for other in &batches[1..] {
        if other.schema() != first.schema() {
            return Err(Error::InvalidBatch(
                "lake write batches must share one schema".into(),
            ));
        }
    }
    Ok(first.schema())
}

fn default_physical(schema: &Schema) -> BTreeMap<String, String> {
    schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.name().clone()))
        .collect()
}

fn unique_physical(physical: &BTreeMap<String, String>, name: &str) -> String {
    let taken: std::collections::HashSet<&str> = physical.values().map(String::as_str).collect();
    if !taken.contains(name) {
        return name.to_string();
    }
    let mut n = 0u32;
    loop {
        let cand = format!("{name}__{n}");
        if !taken.contains(cand.as_str()) {
            return cand;
        }
        n += 1;
    }
}

fn apply_evolution(
    schema: SchemaRef,
    mut physical: BTreeMap<String, String>,
    change: SchemaEvolution,
) -> Result<(SchemaRef, BTreeMap<String, String>)> {
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    match change {
        SchemaEvolution::AddColumn { name, data_type } => {
            if fields.iter().any(|f| f.name() == &name) {
                return Err(Error::Plan(format!("column {name} already exists")));
            }
            let phys = unique_physical(&physical, &name);
            fields.push(Field::new(&name, data_type, true));
            physical.insert(name, phys);
        }
        SchemaEvolution::RenameColumn { from, to } => {
            let idx = fields
                .iter()
                .position(|f| f.name() == &from)
                .ok_or_else(|| Error::Plan(format!("column {from} not found")))?;
            if fields.iter().any(|f| f.name() == &to) {
                return Err(Error::Plan(format!("column {to} already exists")));
            }
            let phys = physical.remove(&from).unwrap_or_else(|| from.clone());
            let dt = fields[idx].data_type().clone();
            fields[idx] = Field::new(&to, dt, true);
            physical.insert(to, phys);
        }
        SchemaEvolution::WidenColumn { name, data_type } => {
            let idx = fields
                .iter()
                .position(|f| f.name() == &name)
                .ok_or_else(|| Error::Plan(format!("column {name} not found")))?;
            let from = fields[idx].data_type();
            if !is_widen(from, &data_type) {
                return Err(Error::Plan(format!("cannot widen {from} to {data_type}")));
            }
            fields[idx] = Field::new(&name, data_type, true);
        }
    }
    Ok((Arc::new(Schema::new(fields)), physical))
}

fn is_widen(from: &DataType, to: &DataType) -> bool {
    matches!(
        (from, to),
        (
            DataType::Int8 | DataType::Int16 | DataType::Int32,
            DataType::Int64
        ) | (DataType::Int8 | DataType::Int16, DataType::Int32)
            | (DataType::Float32, DataType::Float64)
    )
}

fn lake_writer_properties(
    enc: Option<&EncryptionSession>,
    file_schema: &Schema,
    physical: &BTreeMap<String, String>,
) -> Result<Option<WriterProperties>> {
    match enc {
        None => parquet_writer_properties(None, file_schema),
        Some(keys) => {
            let mapped = keys.with_physical_names(physical);
            parquet_writer_properties(Some(&mapped), file_schema)
        }
    }
}

fn physical_schema(schema: &Schema, physical: &BTreeMap<String, String>) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            let name = physical
                .get(f.name())
                .cloned()
                .unwrap_or_else(|| f.name().clone());
            Field::new(name, f.data_type().clone(), true)
        })
        .collect();
    Arc::new(Schema::new(fields))
}

fn to_physical_batch(
    batch: &Batch,
    schema: &Schema,
    physical: &BTreeMap<String, String>,
) -> Result<Batch> {
    let file_schema = physical_schema(schema, physical);
    let mut cols = Vec::new();
    for field in schema.fields() {
        let dt = field.data_type();
        let arr = match batch.column_by_name(field.name()) {
            Some(c) => cast_to(c.clone(), dt)?,
            None => new_null_array(dt, batch.num_rows()),
        };
        cols.push(arr);
    }
    Batch::try_new(file_schema, cols)
}

fn read_files(
    io: &dyn LakeIo,
    files: &[String],
    schema: &SchemaRef,
    physical: &BTreeMap<String, String>,
) -> Result<Vec<Batch>> {
    if files.is_empty() {
        return Ok(vec![Batch::empty(schema.clone())?]);
    }
    let mut out = Vec::new();
    for rel in files {
        check_rel(rel)?;
        let batches = io.read_parquet(rel)?;
        for batch in batches {
            out.push(project_logical(&batch, schema, physical)?);
        }
    }
    if out.is_empty() {
        out.push(Batch::empty(schema.clone())?);
    }
    Ok(out)
}

fn read_parquet_bytes(
    rel: &str,
    bytes: &[u8],
    enc: Option<&EncryptionSession>,
) -> Result<Vec<Batch>> {
    require_encrypted_footer(bytes, enc)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
        Bytes::copy_from_slice(bytes),
        parquet_reader_options_with(enc)?,
    )
    .map_err(|e| Error::Io(format!("{rel}: {e}")))?;
    let reader = builder
        .with_batch_size(mizuchi_core::batch::DEFAULT_MAX_ROWS)
        .build()
        .map_err(|e| Error::Io(format!("{rel}: {e}")))?;
    let mut out = Vec::new();
    for item in reader {
        let batch = item.map_err(|e| Error::Io(e.to_string()))?;
        out.push(Batch::from_arrow(batch)?);
    }
    Ok(out)
}

fn project_logical(
    batch: &Batch,
    schema: &SchemaRef,
    physical: &BTreeMap<String, String>,
) -> Result<Batch> {
    let mut cols = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let arr = match physical.get(field.name()) {
            Some(phys) => batch.column_by_name(phys),
            None => batch.column_by_name(field.name()),
        };
        let col = match arr {
            Some(a) => cast_to(a.clone(), field.data_type())?,
            None => new_null_array(field.data_type(), batch.num_rows()),
        };
        cols.push(col);
    }
    Batch::try_new(schema.clone(), cols)
}

fn cast_to(arr: ArrayRef, dt: &DataType) -> Result<ArrayRef> {
    if arr.data_type() == dt {
        Ok(arr)
    } else {
        cast(arr.as_ref(), dt).map_err(|e| Error::Eval(e.to_string()))
    }
}

fn delta_schema_string(schema: &Schema, physical: &BTreeMap<String, String>) -> Result<String> {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for (i, f) in schema.fields().iter().enumerate() {
        let ty = delta_type(f.data_type())?;
        fields.push(json!({
            "name": f.name(),
            "type": ty,
            "nullable": true,
            "metadata": {
                "delta.columnMapping.id": i + 1,
                "delta.columnMapping.physicalName": physical.get(f.name()).cloned().unwrap_or_else(|| f.name().clone())
            }
        }));
    }
    serde_json::to_string(&json!({"type": "struct", "fields": fields}))
        .map_err(|e| Error::Io(format!("schemaString: {e}")))
}

fn parse_delta_schema(md: &Value) -> Result<(SchemaRef, BTreeMap<String, String>)> {
    let raw = md
        .get("schemaString")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Io("Delta metaData missing schemaString".into()))?;
    let v: Value =
        serde_json::from_str(raw).map_err(|e| Error::Io(format!("schemaString: {e}")))?;
    let fields = v
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::Io("Delta schemaString has no fields".into()))?;
    let mut out = Vec::new();
    let mut physical = BTreeMap::new();
    for f in fields {
        let name = f
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Io("Delta field missing name".into()))?;
        let ty = f
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Io("Delta field missing type".into()))?;
        out.push(Field::new(name, from_delta_type(ty)?, true));
        let phys = f
            .get("metadata")
            .and_then(|m| m.get("delta.columnMapping.physicalName"))
            .and_then(Value::as_str)
            .unwrap_or(name);
        physical.insert(name.to_string(), phys.to_string());
    }
    Ok((Arc::new(Schema::new(out)), physical))
}

fn delta_type(dt: &DataType) -> Result<&'static str> {
    Ok(match dt {
        DataType::Boolean => "boolean",
        DataType::Int8 => "byte",
        DataType::Int16 => "short",
        DataType::Int32 => "integer",
        DataType::Int64 => "long",
        DataType::Float32 => "float",
        DataType::Float64 => "double",
        DataType::Utf8 | DataType::LargeUtf8 => "string",
        DataType::Binary | DataType::LargeBinary => "binary",
        DataType::Date32 => "date",
        other => {
            return Err(Error::UnsupportedType(format!("Delta type {other}")));
        }
    })
}

fn from_delta_type(ty: &str) -> Result<DataType> {
    Ok(match ty {
        "boolean" => DataType::Boolean,
        "byte" => DataType::Int8,
        "short" => DataType::Int16,
        "integer" | "int" => DataType::Int32,
        "long" => DataType::Int64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "string" => DataType::Utf8,
        "binary" => DataType::Binary,
        "date" => DataType::Date32,
        other => return Err(Error::UnsupportedType(format!("Delta type {other}"))),
    })
}

fn iceberg_type(dt: &DataType) -> Result<&'static str> {
    Ok(match dt {
        DataType::Boolean => "boolean",
        DataType::Int32 => "int",
        DataType::Int64 => "long",
        DataType::Float32 => "float",
        DataType::Float64 => "double",
        DataType::Utf8 | DataType::LargeUtf8 => "string",
        DataType::Binary | DataType::LargeBinary => "binary",
        DataType::Date32 => "date",
        other => {
            return Err(Error::UnsupportedType(format!("Iceberg type {other}")));
        }
    })
}

fn from_iceberg_type(ty: &str) -> Result<DataType> {
    Ok(match ty {
        "boolean" => DataType::Boolean,
        "int" => DataType::Int32,
        "long" => DataType::Int64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "string" => DataType::Utf8,
        "binary" => DataType::Binary,
        "date" => DataType::Date32,
        other => return Err(Error::UnsupportedType(format!("Iceberg type {other}"))),
    })
}

fn existing_delta(io: &dyn LakeIo) -> Result<Option<Resolved>> {
    if list_delta_versions(io)?.is_empty() {
        return Ok(None);
    }
    Ok(Some(delta_replay(io, TimeTravel::Latest)?))
}

fn existing_iceberg(io: &dyn LakeIo) -> Result<Option<Resolved>> {
    if iceberg_latest_version(io)?.is_none() {
        return Ok(None);
    }
    Ok(Some(iceberg_replay(io, TimeTravel::Latest)?))
}

fn iceberg_latest_version(io: &dyn LakeIo) -> Result<Option<i64>> {
    let mut max = None;
    for name in io.list("metadata")? {
        let file = name.rsplit('/').next().unwrap_or(&name);
        if let Some(stem) = file.strip_prefix('v')
            && let Some(num) = stem.strip_suffix(".metadata.json")
            && let Ok(v) = num.parse::<i64>()
        {
            max = Some(max.map_or(v, |m: i64| m.max(v)));
        }
    }
    Ok(max)
}

fn unique_data_rel(version: i64, prefix: &str) -> String {
    let seq = FILE_SEQ.fetch_add(1, Ordering::Relaxed);
    format!(
        "{prefix}part-{version:05}-{pid:x}-{now:x}-{seq:x}.parquet",
        pid = std::process::id(),
        now = now_ms() as u64,
    )
}

fn confined(root: &Path, rel: &str) -> Result<PathBuf> {
    let rel_path = Path::new(rel);
    if rel_path.is_absolute() {
        return Err(Error::Io(format!("path escapes table root: {rel}")));
    }
    for c in rel_path.components() {
        match c {
            Component::Normal(_) | Component::CurDir => {}
            _ => return Err(Error::Io(format!("path escapes table root: {rel}"))),
        }
    }
    let joined = root.join(rel_path);
    let root_canon = match fs::canonicalize(root) {
        Ok(p) => p,
        Err(_) => return Ok(joined),
    };
    if joined.symlink_metadata().is_ok() {
        let canon = fs::canonicalize(&joined)
            .map_err(|_| Error::Io(format!("path escapes table root: {rel}")))?;
        if !canon.starts_with(&root_canon) {
            return Err(Error::Io(format!("path escapes table root: {rel}")));
        }
        return Ok(canon);
    }
    Ok(joined)
}

fn exclusive_create(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| io_err(parent, e))?;
    }
    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    match opts.open(path) {
        Ok(mut file) => {
            file.write_all(bytes).map_err(|e| io_err(path, e))?;
            file.sync_all().map_err(|e| io_err(path, e))?;
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(Error::Io(format!(
            "concurrent writer conflict: {}",
            path.display()
        ))),
        Err(e) => Err(io_err(path, e)),
    }
}

fn read_json_io(io: &dyn LakeIo, rel: &str) -> Result<Value> {
    let text = String::from_utf8(io.get(rel)?).map_err(|e| Error::Io(format!("{rel}: {e}")))?;
    serde_json::from_str(&text).map_err(|e| Error::Io(format!("{rel}: {e}")))
}

fn table_id(label: &str, now: i64) -> String {
    format!("{:016x}{:08x}", now, label.len() as u32)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn io_err(path: &Path, err: std::io::Error) -> Error {
    Error::Io(format!("{}: {err}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use std::path::PathBuf;

    static SEQ: AtomicU64 = AtomicU64::new(1);

    fn tmp() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "mizuchi-lake-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&p);
        p
    }

    fn batch_i64() -> Batch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        Batch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            ],
        )
        .unwrap()
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
    fn delta_round_trip_and_time_travel() {
        let dest = tmp();
        let first = write_delta(&dest, &[batch_i64()], WriteMode::Overwrite).unwrap();
        assert_eq!(first.version, 0);
        assert!(
            dest.join("_delta_log")
                .join("00000000000000000000.json")
                .is_file()
        );
        let got = read_delta(&dest, TimeTravel::Latest).unwrap();
        assert_eq!(ids(&got), vec![1, 2]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let more = Batch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(3)])),
                Arc::new(StringArray::from(vec![Some("c")])),
            ],
        )
        .unwrap();
        let second = write_delta(&dest, &[more], WriteMode::Append).unwrap();
        assert_eq!(second.version, 1);
        assert_eq!(
            ids(&read_delta(&dest, TimeTravel::Latest).unwrap()),
            vec![1, 2, 3]
        );
        assert_eq!(
            ids(&read_delta(&dest, TimeTravel::Version(0)).unwrap()),
            vec![1, 2]
        );
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn delta_conflict_when_commit_exists() {
        let dest = tmp();
        let first = write_delta(&dest, &[batch_i64()], WriteMode::Overwrite).unwrap();
        write_delta_at(
            &dest,
            &[batch_i64()],
            WriteMode::Append,
            Some(first.version),
        )
        .unwrap();
        let err = write_delta_at(
            &dest,
            &[batch_i64()],
            WriteMode::Append,
            Some(first.version),
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Io(ref s) if s.contains("conflict")),
            "{err:?}"
        );
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn delta_corrupt_log_is_not_a_new_table() {
        let dest = tmp();
        write_delta(&dest, &[batch_i64()], WriteMode::Overwrite).unwrap();
        fs::write(
            dest.join("_delta_log").join("00000000000000000001.json"),
            b"not-json",
        )
        .unwrap();
        let err = write_delta(&dest, &[batch_i64()], WriteMode::Append).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("delta log") || msg.contains("not-json") || msg.contains("expected"),
            "{msg}"
        );
        assert!(!msg.contains("expected version"), "{msg}");
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn delta_rejects_escaped_data_path() {
        let dest = tmp();
        write_delta(&dest, &[batch_i64()], WriteMode::Overwrite).unwrap();
        let log = dest.join("_delta_log").join("00000000000000000000.json");
        let body = fs::read_to_string(&log).unwrap();
        let tampered = body.replace("\"path\":\"part-", "\"path\":\"../part-");
        // If the original path had no quoted part- prefix match, inject an add.
        if tampered == body {
            fs::write(
                &log,
                format!("{body}{{\"add\":{{\"path\":\"../secret.parquet\",\"partitionValues\":{{}},\"size\":1,\"modificationTime\":1,\"dataChange\":true}}}}\n"),
            )
            .unwrap();
        } else {
            fs::write(&log, tampered).unwrap();
        }
        let err = read_delta(&dest, TimeTravel::Latest).unwrap_err();
        assert!(err.to_string().contains("escapes table root"), "{err}");
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn add_after_rename_keeps_physical_names_distinct() {
        let dest = tmp();
        write_delta(&dest, &[batch_i64()], WriteMode::Overwrite).unwrap();
        evolve_delta(
            &dest,
            SchemaEvolution::RenameColumn {
                from: "id".into(),
                to: "key".into(),
            },
        )
        .unwrap();
        evolve_delta(
            &dest,
            SchemaEvolution::AddColumn {
                name: "id".into(),
                data_type: DataType::Int64,
            },
        )
        .unwrap();
        let got = read_delta(&dest, TimeTravel::Latest).unwrap();
        let key = got[0]
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let id = got[0].column_by_name("id").unwrap();
        assert_eq!(key.value(0), 1);
        assert_eq!(key.value(1), 2);
        assert!(
            id.is_null(0),
            "added id must not reuse key's parquet column"
        );
        assert!(id.is_null(1));
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn unsupported_arrow_type_is_rejected() {
        let dest = tmp();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "flag",
            DataType::UInt64,
            true,
        )]));
        let batch = Batch::try_new(
            schema,
            vec![Arc::new(arrow::array::UInt64Array::from(vec![Some(1)]))],
        )
        .unwrap();
        let err =
            write_delta(&dest, std::slice::from_ref(&batch), WriteMode::Overwrite).unwrap_err();
        assert!(matches!(err, Error::UnsupportedType(_)), "{err:?}");
        let err = write_iceberg(&dest, &[batch], WriteMode::Overwrite).unwrap_err();
        assert!(matches!(err, Error::UnsupportedType(_)), "{err:?}");
        assert!(
            !dest.join("metadata").exists() && !dest.join("data").exists(),
            "a rejected Iceberg write must not leave files"
        );
        let i8_schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int8, true)]));
        let i8_batch = Batch::try_new(
            i8_schema,
            vec![Arc::new(arrow::array::Int8Array::from(vec![Some(1)]))],
        )
        .unwrap();
        let err = write_iceberg(&dest, std::slice::from_ref(&i8_batch), WriteMode::Overwrite)
            .unwrap_err();
        assert!(matches!(err, Error::UnsupportedType(_)), "{err:?}");
        write_delta(&dest, &[i8_batch], WriteMode::Overwrite).unwrap();
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn lake_read_rejects_symlink_outside_table_root() {
        let dest = tmp();
        write_delta(&dest, &[batch_i64()], WriteMode::Overwrite).unwrap();
        let outside = tmp();
        fs::create_dir_all(&outside).unwrap();
        let secret = outside.join("secret.parquet");
        fs::write(&secret, b"not parquet").unwrap();
        let data = fs::read_dir(&dest)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|ext| ext == "parquet"))
            .expect("data parquet");
        fs::remove_file(&data).unwrap();
        std::os::unix::fs::symlink(&secret, &data).unwrap();
        let err = read_delta(&dest, TimeTravel::Latest).unwrap_err();
        assert!(err.to_string().contains("escapes table root"), "{err}");
        let _ = fs::remove_dir_all(&dest);
        let _ = fs::remove_dir_all(&outside);
    }

    #[test]
    fn iceberg_missing_manifest_is_not_empty_table() {
        let dest = tmp();
        write_iceberg(&dest, &[batch_i64()], WriteMode::Overwrite).unwrap();
        let meta = dest.join("metadata").join("v1.metadata.json");
        let md: Value = serde_json::from_str(&fs::read_to_string(&meta).unwrap()).unwrap();
        let list = md
            .get("snapshots")
            .and_then(Value::as_array)
            .and_then(|a| a.first())
            .and_then(|s| s.get("manifest-list"))
            .and_then(Value::as_str)
            .expect("manifest-list");
        fs::remove_file(dest.join(list)).unwrap();
        let err = read_iceberg(&dest, TimeTravel::Latest).unwrap_err();
        assert!(err.to_string().contains("manifest"), "{err}");
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn iceberg_round_trip_and_schema_add() {
        let dest = tmp();
        write_iceberg(&dest, &[batch_i64()], WriteMode::Overwrite).unwrap();
        assert!(dest.join("metadata").join("version-hint.text").is_file());
        assert_eq!(
            ids(&read_iceberg(&dest, TimeTravel::Latest).unwrap()),
            vec![1, 2]
        );
        evolve_iceberg(
            &dest,
            SchemaEvolution::AddColumn {
                name: "extra".into(),
                data_type: DataType::Int64,
            },
        )
        .unwrap();
        let got = read_iceberg(&dest, TimeTravel::Latest).unwrap();
        assert!(got[0].schema().field_with_name("extra").is_ok());
        let extra = got[0].column_by_name("extra").unwrap();
        assert!(extra.is_null(0));
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn store_delta_and_iceberg_round_trip() {
        let store = object_store::memory::InMemory::new();
        let first =
            write_delta_store(&store, "delta/t", &[batch_i64()], WriteMode::Overwrite).unwrap();
        assert_eq!(first.version, 0);
        assert_eq!(
            ids(&read_delta_store(&store, "delta/t", TimeTravel::Latest).unwrap()),
            vec![1, 2]
        );
        let more = Batch::try_new(
            batch_i64().schema(),
            vec![
                Arc::new(Int64Array::from(vec![Some(3)])),
                Arc::new(StringArray::from(vec![Some("c")])),
            ],
        )
        .unwrap();
        write_delta_store(&store, "delta/t", &[more], WriteMode::Append).unwrap();
        assert_eq!(
            ids(&read_delta_store(&store, "delta/t", TimeTravel::Latest).unwrap()),
            vec![1, 2, 3]
        );
        assert_eq!(
            ids(&read_delta_store(&store, "delta/t", TimeTravel::Version(0)).unwrap()),
            vec![1, 2]
        );
        let err = write_delta_store_at(
            &store,
            "delta/t",
            &[batch_i64()],
            WriteMode::Append,
            Some(0),
        )
        .unwrap_err();
        assert!(err.to_string().contains("concurrent writer conflict"));

        write_iceberg_store(&store, "ice/t", &[batch_i64()], WriteMode::Overwrite).unwrap();
        assert_eq!(
            ids(&read_iceberg_store(&store, "ice/t", TimeTravel::Latest).unwrap()),
            vec![1, 2]
        );
        evolve_iceberg_store(
            &store,
            "ice/t",
            SchemaEvolution::AddColumn {
                name: "extra".into(),
                data_type: DataType::Int64,
            },
        )
        .unwrap();
        let got = read_iceberg_store(&store, "ice/t", TimeTravel::Latest).unwrap();
        assert!(got[0].schema().field_with_name("extra").is_ok());
    }

    fn lake_keys() -> crate::crypto::EncryptionSession {
        lake_keys_for(Vec::new())
    }

    fn lake_keys_for(columns: Vec<String>) -> crate::crypto::EncryptionSession {
        use crate::crypto::{
            EncryptionSession, KeyPurpose, MemoryKeyProvider, RuntimeProfile, TenantId,
            UnwrapGrant, unix_now,
        };
        use std::sync::Arc;
        let provider = Arc::new(MemoryKeyProvider::new(RuntimeProfile::Development).unwrap());
        let tenant = TenantId::new("acme").unwrap();
        let grant = UnwrapGrant {
            tenant_id: tenant.clone(),
            purpose: KeyPurpose::Store,
            expires_unix_secs: unix_now() + 3_600,
        };
        EncryptionSession::new(provider, tenant, grant, columns)
    }

    fn lake_keys_on(
        provider: std::sync::Arc<dyn crate::crypto::KeyProvider + Send + Sync>,
        columns: Vec<String>,
    ) -> crate::crypto::EncryptionSession {
        use crate::crypto::{EncryptionSession, KeyPurpose, TenantId, UnwrapGrant, unix_now};
        let tenant = TenantId::new("acme").unwrap();
        let grant = UnwrapGrant {
            tenant_id: tenant.clone(),
            purpose: KeyPurpose::Store,
            expires_unix_secs: unix_now() + 3_600,
        };
        EncryptionSession::new(provider, tenant, grant, columns)
    }

    #[test]
    fn lake_column_keys_follow_rename_to_physical_path() {
        use crate::crypto::{KeyProvider, MemoryKeyProvider, RuntimeProfile};
        let dest = tmp();
        let provider: std::sync::Arc<dyn KeyProvider + Send + Sync> =
            std::sync::Arc::new(MemoryKeyProvider::new(RuntimeProfile::Development).unwrap());
        let schema = Arc::new(Schema::new(vec![
            Field::new("secret", DataType::Utf8, true),
            Field::new("note", DataType::Utf8, true),
        ]));
        let first = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("employee_ssn=111-11-1111")])),
                Arc::new(StringArray::from(vec![Some("note-one")])),
            ],
        )
        .unwrap();
        let keys = lake_keys_on(provider.clone(), vec!["secret".into()]);
        write_delta_keys(&dest, &[first], WriteMode::Overwrite, &keys).unwrap();
        evolve_delta(
            &dest,
            SchemaEvolution::RenameColumn {
                from: "secret".into(),
                to: "sensitive".into(),
            },
        )
        .unwrap();
        let renamed = Arc::new(Schema::new(vec![
            Field::new("sensitive", DataType::Utf8, true),
            Field::new("note", DataType::Utf8, true),
        ]));
        let second = Batch::try_new(
            renamed,
            vec![
                Arc::new(StringArray::from(vec![Some("employee_ssn=222-22-2222")])),
                Arc::new(StringArray::from(vec![Some("note-two")])),
            ],
        )
        .unwrap();
        let keys_after = lake_keys_on(provider, vec!["sensitive".into()]);
        write_delta_keys(&dest, &[second], WriteMode::Append, &keys_after).unwrap();
        let mut leaked = false;
        for rel in walk_parquet_files(&dest) {
            let bytes = fs::read(&rel).unwrap();
            for needle in [
                b"employee_ssn=111-11-1111".as_slice(),
                b"employee_ssn=222-22-2222".as_slice(),
            ] {
                if bytes.windows(needle.len()).any(|w| w == needle) {
                    leaked = true;
                }
            }
        }
        assert!(
            !leaked,
            "renamed column leaked plaintext after dedicated key"
        );
        let got = read_delta_keys(&dest, TimeTravel::Latest, &keys_after).unwrap();
        assert!(got.iter().any(|b| b.num_rows() > 0));
        let _ = fs::remove_dir_all(&dest);
    }

    fn walk_parquet_files(root: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
            let Ok(entries) = fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, out);
                } else if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                    out.push(path);
                }
            }
        }
        walk(root, &mut out);
        out
    }

    #[test]
    fn encrypted_delta_round_trip_needs_key() {
        let dest = tmp();
        let keys = lake_keys();
        write_delta_keys(&dest, &[batch_i64()], WriteMode::Overwrite, &keys).unwrap();
        let err = read_delta(&dest, TimeTravel::Latest).unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err:?}");
        assert_eq!(
            ids(&read_delta_keys(&dest, TimeTravel::Latest, &keys).unwrap()),
            vec![1, 2]
        );
        let log = fs::read_dir(dest.join("_delta_log"))
            .unwrap()
            .next()
            .unwrap();
        let text = fs::read_to_string(log.unwrap().path()).unwrap();
        assert!(text.contains("mizuchi.encryption"), "{text}");
        assert!(!text.contains("employee_ssn"));
        let mut saw_string = false;
        for line in text.lines() {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            if let Some(enc) = v.pointer("/metaData/configuration/mizuchi.encryption") {
                assert!(enc.is_string(), "{enc}");
                saw_string = true;
            }
        }
        assert!(saw_string, "{text}");
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn encrypted_delta_refuses_substituted_plaintext_file() {
        let dest = tmp();
        let keys = lake_keys();
        write_delta_keys(&dest, &[batch_i64()], WriteMode::Overwrite, &keys).unwrap();
        let part = fs::read_dir(&dest)
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.extension().and_then(|s| s.to_str()) == Some("parquet"))
            .expect("data file");
        let batch = batch_i64();
        crate::write::write_parquet_file(&part, std::slice::from_ref(&batch), batch.schema())
            .unwrap();
        let err = read_delta_keys(&dest, TimeTravel::Latest, &keys).unwrap_err();
        assert!(
            err.to_string()
                .contains("encrypted read requires encrypted parquet"),
            "{err}"
        );
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn encrypted_iceberg_round_trip_needs_key() {
        let dest = tmp();
        let keys = lake_keys();
        write_iceberg_keys(&dest, &[batch_i64()], WriteMode::Overwrite, &keys).unwrap();
        let err = read_iceberg(&dest, TimeTravel::Latest).unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err:?}");
        assert_eq!(
            ids(&read_iceberg_keys(&dest, TimeTravel::Latest, &keys).unwrap()),
            vec![1, 2]
        );
        let hint = fs::read_to_string(dest.join("metadata/version-hint.text")).unwrap();
        let md = fs::read_to_string(dest.join(format!("metadata/v{}.metadata.json", hint.trim())))
            .unwrap();
        let v: serde_json::Value = serde_json::from_str(&md).unwrap();
        let enc = &v["properties"]["mizuchi.encryption"];
        assert!(enc.is_string(), "{enc}");
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn encrypted_delta_store_round_trip_needs_key() {
        let store = object_store::memory::InMemory::new();
        let keys = lake_keys();
        write_delta_store_keys(
            &store,
            "delta/t",
            &[batch_i64()],
            WriteMode::Overwrite,
            &keys,
        )
        .unwrap();
        let err = read_delta_store(&store, "delta/t", TimeTravel::Latest).unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err:?}");
        assert_eq!(
            ids(&read_delta_store_keys(&store, "delta/t", TimeTravel::Latest, &keys).unwrap()),
            vec![1, 2]
        );
    }

    #[test]
    fn encrypted_iceberg_store_round_trip_needs_key() {
        let store = object_store::memory::InMemory::new();
        let keys = lake_keys();
        write_iceberg_store_keys(&store, "ice/t", &[batch_i64()], WriteMode::Overwrite, &keys)
            .unwrap();
        let err = read_iceberg_store(&store, "ice/t", TimeTravel::Latest).unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err:?}");
        assert_eq!(
            ids(&read_iceberg_store_keys(&store, "ice/t", TimeTravel::Latest, &keys).unwrap()),
            vec![1, 2]
        );
    }

    #[test]
    fn schema_evolution_keeps_lake_encryption_property() {
        let keys = lake_keys();
        let dest = tmp();
        write_delta_keys(&dest, &[batch_i64()], WriteMode::Overwrite, &keys).unwrap();
        evolve_delta(
            &dest,
            SchemaEvolution::AddColumn {
                name: "extra".into(),
                data_type: DataType::Int64,
            },
        )
        .unwrap();
        let mut logs: Vec<_> = fs::read_dir(dest.join("_delta_log"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        logs.sort();
        let text = fs::read_to_string(logs.last().unwrap()).unwrap();
        let mut saw = false;
        for line in text.lines() {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            if let Some(enc) = v.pointer("/metaData/configuration/mizuchi.encryption") {
                assert!(enc.is_string(), "{enc}");
                saw = true;
            }
        }
        assert!(saw, "{text}");
        assert_eq!(
            ids(&read_delta_keys(&dest, TimeTravel::Latest, &keys).unwrap()),
            vec![1, 2]
        );
        let _ = fs::remove_dir_all(&dest);

        let dest = tmp();
        write_iceberg_keys(&dest, &[batch_i64()], WriteMode::Overwrite, &keys).unwrap();
        evolve_iceberg(
            &dest,
            SchemaEvolution::AddColumn {
                name: "extra".into(),
                data_type: DataType::Int64,
            },
        )
        .unwrap();
        let hint = fs::read_to_string(dest.join("metadata/version-hint.text")).unwrap();
        let md = fs::read_to_string(dest.join(format!("metadata/v{}.metadata.json", hint.trim())))
            .unwrap();
        let v: serde_json::Value = serde_json::from_str(&md).unwrap();
        assert!(v["properties"]["mizuchi.encryption"].is_string(), "{md}");
        assert_eq!(
            ids(&read_iceberg_keys(&dest, TimeTravel::Latest, &keys).unwrap()),
            vec![1, 2]
        );
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn unkeyed_write_to_encrypted_lake_is_refused() {
        let keys = lake_keys();
        let dest = tmp();
        write_delta_keys(&dest, &[batch_i64()], WriteMode::Overwrite, &keys).unwrap();
        let err = write_delta(&dest, &[batch_i64()], WriteMode::Append).unwrap_err();
        assert!(
            err.to_string()
                .contains("encrypted lake table requires keys"),
            "{err}"
        );
        let err = write_delta(&dest, &[batch_i64()], WriteMode::Overwrite).unwrap_err();
        assert!(
            err.to_string()
                .contains("encrypted lake table requires keys"),
            "{err}"
        );
        write_delta_keys(&dest, &[batch_i64()], WriteMode::Append, &keys).unwrap();
        assert_eq!(
            ids(&read_delta_keys(&dest, TimeTravel::Latest, &keys).unwrap()).len(),
            4
        );
        let _ = fs::remove_dir_all(&dest);

        let dest = tmp();
        write_iceberg_keys(&dest, &[batch_i64()], WriteMode::Overwrite, &keys).unwrap();
        let err = write_iceberg(&dest, &[batch_i64()], WriteMode::Append).unwrap_err();
        assert!(
            err.to_string()
                .contains("encrypted lake table requires keys"),
            "{err}"
        );
        write_iceberg_keys(&dest, &[batch_i64()], WriteMode::Append, &keys).unwrap();
        assert_eq!(
            ids(&read_iceberg_keys(&dest, TimeTravel::Latest, &keys).unwrap()).len(),
            4
        );
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn encrypted_append_to_plaintext_lake_is_refused() {
        let keys = lake_keys();
        let dest = tmp();
        write_delta(&dest, &[batch_i64()], WriteMode::Overwrite).unwrap();
        let err = write_delta_keys(&dest, &[batch_i64()], WriteMode::Append, &keys).unwrap_err();
        assert!(
            err.to_string()
                .contains("cannot append encrypted files to an unencrypted lake table"),
            "{err}"
        );
        write_delta_keys(&dest, &[batch_i64()], WriteMode::Overwrite, &keys).unwrap();
        let err = write_delta(&dest, &[batch_i64()], WriteMode::Append).unwrap_err();
        assert!(
            err.to_string()
                .contains("encrypted lake table requires keys"),
            "{err}"
        );
        let _ = fs::remove_dir_all(&dest);

        let dest = tmp();
        write_iceberg(&dest, &[batch_i64()], WriteMode::Overwrite).unwrap();
        let err = write_iceberg_keys(&dest, &[batch_i64()], WriteMode::Append, &keys).unwrap_err();
        assert!(
            err.to_string()
                .contains("cannot append encrypted files to an unencrypted lake table"),
            "{err}"
        );
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn encrypted_append_wrong_tenant_is_refused() {
        use crate::crypto::{
            EncryptionSession, KeyPurpose, MemoryKeyProvider, RuntimeProfile, TenantId,
            UnwrapGrant, unix_now,
        };
        let dest = tmp();
        let keys_a = lake_keys();
        write_delta_keys(&dest, &[batch_i64()], WriteMode::Overwrite, &keys_a).unwrap();
        let provider =
            std::sync::Arc::new(MemoryKeyProvider::new(RuntimeProfile::Development).unwrap());
        let tenant = TenantId::new("beta").unwrap();
        let grant = UnwrapGrant {
            tenant_id: tenant.clone(),
            purpose: KeyPurpose::Store,
            expires_unix_secs: unix_now() + 3_600,
        };
        let keys_b = EncryptionSession::new(provider, tenant, grant, Vec::new());
        let err = write_delta_keys(&dest, &[batch_i64()], WriteMode::Append, &keys_b).unwrap_err();
        assert!(
            err.to_string()
                .contains("encrypted lake table tenant does not match write keys"),
            "{err}"
        );
        write_delta_keys(&dest, &[batch_i64()], WriteMode::Append, &keys_a).unwrap();
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn encrypted_append_wrong_kek_is_refused() {
        use crate::crypto::{KeyProvider, MemoryKeyProvider, RuntimeProfile};
        let dest = tmp();
        let provider_a: std::sync::Arc<dyn KeyProvider + Send + Sync> =
            std::sync::Arc::new(MemoryKeyProvider::new(RuntimeProfile::Development).unwrap());
        let provider_b: std::sync::Arc<dyn KeyProvider + Send + Sync> =
            std::sync::Arc::new(MemoryKeyProvider::new(RuntimeProfile::Development).unwrap());
        let keys_a = lake_keys_on(provider_a, Vec::new());
        let keys_b = lake_keys_on(provider_b, Vec::new());
        write_delta_keys(&dest, &[batch_i64()], WriteMode::Overwrite, &keys_a).unwrap();
        let err = write_delta_keys(&dest, &[batch_i64()], WriteMode::Append, &keys_b).unwrap_err();
        assert!(
            err.to_string()
                .contains("encrypted lake table kek does not match write keys"),
            "{err}"
        );
        write_delta_keys(&dest, &[batch_i64()], WriteMode::Append, &keys_a).unwrap();
        let _ = fs::remove_dir_all(&dest);
    }
}
