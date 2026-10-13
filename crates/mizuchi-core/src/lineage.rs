//! Engine-neutral scan and write records for query reports.

/// One scan after projection and partition prune.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputScan {
    /// Catalog table name, or the dataset path when there is no table.
    pub dataset: String,
    /// Filesystem or object prefix, when known.
    pub path: Option<String>,
    /// Hive-style partition directories that were scanned.
    pub partitions: Vec<String>,
    /// Columns read after projection pushdown.
    pub columns: Vec<String>,
    /// File mtime in milliseconds since epoch, when the OS provides it.
    pub mtime_ms: Option<u64>,
    /// Object etag, when the store provides it.
    pub etag: Option<String>,
}

/// One write after staging commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputWrite {
    /// Dataset directory or object prefix.
    pub path: String,
    /// Hive-style partition directories that received files.
    pub partitions: Vec<String>,
    /// `append` or `overwrite`.
    pub mode: String,
    /// Rows in the written batches.
    pub rows: u64,
    /// Bytes of committed Parquet objects.
    pub bytes: u64,
    /// Deterministic id for this write (`mode:rows:bytes:partitions`).
    pub commit_id: String,
}
