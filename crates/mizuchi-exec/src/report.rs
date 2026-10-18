//! Per-query read/write report for embedding runners.

use mizuchi_core::VERSION;
use mizuchi_plan::LogicalPlan;

pub use mizuchi_core::{InputScan, OutputWrite};

/// Output column with the opaque labels that reached it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnLabels {
    /// Result or written column name.
    pub name: String,
    /// Sorted unique labels. Empty means unmarked.
    pub labels: Vec<String>,
}

/// Batches plus the lineage report for one execute.
#[derive(Clone, Debug)]
pub struct QueryResult {
    /// Arrow morsels produced by the query.
    pub batches: Vec<mizuchi_core::batch::Batch>,
    /// What the query read and wrote.
    pub report: QueryReport,
    /// Memory-pool high water for this execute.
    pub peak_bytes: u64,
    /// Spill file bytes written during this execute.
    pub spilled_bytes: u64,
}

/// Engine-neutral record of one query's inputs, outputs, version, and plan.
///
/// The same SQL and files produce the same report: partitions and columns
/// are sorted, and `commit_id` is derived from path, mode, rows, and bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryReport {
    /// [`mizuchi_core::VERSION`].
    pub engine_version: String,
    /// Executed logical and physical plan text.
    pub plan: String,
    /// Tables and files actually scanned after pruning.
    pub inputs: Vec<InputScan>,
    /// Datasets this call wrote.
    pub outputs: Vec<OutputWrite>,
    /// Principal from the query policy, when set.
    pub principal: Option<String>,
    /// Actor from the query policy, when set.
    pub actor: Option<String>,
    /// Output columns with the labels that reached them.
    pub column_markings: Vec<ColumnLabels>,
}

impl QueryReport {
    pub(super) fn new(plan: String, inputs: Vec<InputScan>) -> Self {
        let mut inputs = inputs;
        inputs.sort_by(|a, b| a.dataset.cmp(&b.dataset));
        Self {
            engine_version: VERSION.to_string(),
            plan,
            inputs,
            outputs: Vec::new(),
            principal: None,
            actor: None,
            column_markings: Vec::new(),
        }
    }

    pub(super) fn from_write(output: OutputWrite) -> Self {
        Self {
            engine_version: VERSION.to_string(),
            plan: String::new(),
            inputs: Vec::new(),
            outputs: vec![output],
            principal: None,
            actor: None,
            column_markings: Vec::new(),
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
