//! Per-query read/write report for embedding runners.

use crate::plan::LogicalPlan;

/// Batches plus the lineage report for one execute.
#[derive(Clone, Debug)]
pub struct QueryResult {
    /// Arrow morsels produced by the query.
    pub batches: Vec<crate::batch::Batch>,
    /// What the query read and wrote.
    pub report: QueryReport,
}

/// Engine-neutral record of one query's inputs, outputs, version, and plan.
///
/// The same SQL and files produce the same report: partitions and columns
/// are sorted, and `commit_id` is derived from path, mode, rows, and bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryReport {
    /// [`crate::VERSION`].
    pub engine_version: String,
    /// Executed logical and physical plan text.
    pub plan: String,
    /// Tables and files actually scanned after pruning.
    pub inputs: Vec<InputScan>,
    /// Datasets this call wrote.
    pub outputs: Vec<OutputWrite>,
}

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

impl QueryReport {
    pub(super) fn new(plan: String, inputs: Vec<InputScan>) -> Self {
        let mut inputs = inputs;
        inputs.sort_by(|a, b| a.dataset.cmp(&b.dataset));
        Self {
            engine_version: crate::VERSION.to_string(),
            plan,
            inputs,
            outputs: Vec::new(),
        }
    }

    pub(super) fn from_write(output: OutputWrite) -> Self {
        Self {
            engine_version: crate::VERSION.to_string(),
            plan: String::new(),
            inputs: Vec::new(),
            outputs: vec![output],
        }
    }
}

pub(super) fn plan_text(plan: &LogicalPlan) -> String {
    format!(
        "== Logical Plan ==\n{}== Physical Plan ==\n{}",
        plan.explain(),
        super::metrics::physical_shape(plan).format(false)
    )
}
