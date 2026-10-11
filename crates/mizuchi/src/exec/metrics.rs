//! Per-operator rows, batches, elapsed time, and peak pool bytes.
//!
//! Wrappers are installed only for `EXPLAIN ANALYZE`. Ordinary `execute_sql`
//! builds the same operator tree with no timing.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use super::CancelToken;
use super::memory::MemoryPool;
use super::op::Operator;
use crate::batch::Batch;
use crate::error::Result;
use crate::plan::LogicalPlan;

/// Snapshot of one physical operator after a run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperatorMetrics {
    /// Physical operator name (`Scan`, `Filter`, `HashAggregate`, ...).
    pub name: String,
    /// Output rows.
    pub rows: u64,
    /// Output batches.
    pub batches: u64,
    /// Inclusive time in this operator's `next_batch` (and blocking build).
    pub elapsed_ns: u64,
    /// High-water of the query memory pool observed here.
    pub peak_bytes: u64,
    /// Child operators, left to right.
    pub children: Vec<OperatorMetrics>,
}

impl OperatorMetrics {
    /// JSON object for runners and the Python API. No extra crate.
    pub fn to_json(&self) -> String {
        let mut out = String::new();
        self.write_json(&mut out);
        out
    }

    fn write_json(&self, out: &mut String) {
        out.push_str("{\"name\":\"");
        out.push_str(&self.name);
        out.push_str("\",\"rows\":");
        out.push_str(&self.rows.to_string());
        out.push_str(",\"batches\":");
        out.push_str(&self.batches.to_string());
        out.push_str(",\"elapsed_ns\":");
        out.push_str(&self.elapsed_ns.to_string());
        out.push_str(",\"peak_bytes\":");
        out.push_str(&self.peak_bytes.to_string());
        out.push_str(",\"children\":[");
        for (i, child) in self.children.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            child.write_json(out);
        }
        out.push_str("]}");
    }
}

#[derive(Default)]
pub(super) struct Counters {
    rows: AtomicU64,
    batches: AtomicU64,
    elapsed_ns: AtomicU64,
    peak_bytes: AtomicU64,
}

pub(super) struct PhysNode {
    pub name: &'static str,
    pub metrics: Arc<Counters>,
    pub children: Vec<PhysNode>,
}

impl PhysNode {
    pub fn snapshot(&self) -> OperatorMetrics {
        OperatorMetrics {
            name: self.name.to_string(),
            rows: self.metrics.rows.load(Ordering::Relaxed),
            batches: self.metrics.batches.load(Ordering::Relaxed),
            elapsed_ns: self.metrics.elapsed_ns.load(Ordering::Relaxed),
            peak_bytes: self.metrics.peak_bytes.load(Ordering::Relaxed),
            children: self.children.iter().map(PhysNode::snapshot).collect(),
        }
    }

    pub fn format(&self, analyze: bool) -> String {
        let mut out = String::new();
        self.format_into(analyze, 0, &mut out);
        out
    }

    fn format_into(&self, analyze: bool, indent: usize, out: &mut String) {
        let pad = "  ".repeat(indent);
        if analyze {
            let snap = self.snapshot();
            out.push_str(&format!(
                "{pad}{} rows={} batches={} elapsed_ns={} peak_bytes={}\n",
                snap.name, snap.rows, snap.batches, snap.elapsed_ns, snap.peak_bytes
            ));
        } else {
            out.push_str(&format!("{pad}{}\n", self.name));
        }
        for child in &self.children {
            child.format_into(analyze, indent + 1, out);
        }
    }
}

/// Physical tree names matching [`super::physical`], without building operators.
pub(super) fn physical_shape(plan: &LogicalPlan) -> PhysNode {
    match plan {
        LogicalPlan::Empty { .. } => leaf("Empty"),
        LogicalPlan::Scan { .. } => leaf("Scan"),
        LogicalPlan::Filter { input, .. } => node("Filter", vec![physical_shape(input)]),
        LogicalPlan::Project { input, .. } => node("Project", vec![physical_shape(input)]),
        LogicalPlan::Aggregate { input, .. } => node("HashAggregate", vec![physical_shape(input)]),
        LogicalPlan::Join { left, right, .. } => node(
            "HashJoin",
            vec![physical_shape(left), physical_shape(right)],
        ),
        LogicalPlan::Sort { input, .. } => node("Sort", vec![physical_shape(input)]),
        LogicalPlan::Limit { input, .. } => match input.as_ref() {
            LogicalPlan::Sort { input: sorted, .. } => node("Sort", vec![physical_shape(sorted)]),
            _ => node("Limit", vec![physical_shape(input)]),
        },
        LogicalPlan::Union { inputs, .. } => {
            node("Union", inputs.iter().map(physical_shape).collect())
        }
    }
}

pub(super) fn leaf(name: &'static str) -> PhysNode {
    PhysNode {
        name,
        metrics: Arc::new(Counters::default()),
        children: Vec::new(),
    }
}

fn node(name: &'static str, children: Vec<PhysNode>) -> PhysNode {
    PhysNode {
        name,
        metrics: Arc::new(Counters::default()),
        children,
    }
}

pub(super) fn finish(
    name: &'static str,
    op: Box<dyn Operator>,
    children: Vec<PhysNode>,
    analyze: bool,
    pool: &MemoryPool,
) -> (Box<dyn Operator>, PhysNode) {
    let metrics = Arc::new(Counters::default());
    let node = PhysNode {
        name,
        metrics: metrics.clone(),
        children,
    };
    if analyze {
        (
            Box::new(Metered {
                inner: op,
                metrics,
                used: pool.used_handle(),
            }),
            node,
        )
    } else {
        (op, node)
    }
}

pub(super) fn add_blocking(node: &PhysNode, elapsed_ns: u64, peak_bytes: u64) {
    node.metrics
        .elapsed_ns
        .fetch_add(elapsed_ns, Ordering::Relaxed);
    node.metrics
        .peak_bytes
        .fetch_max(peak_bytes, Ordering::Relaxed);
}

struct Metered {
    inner: Box<dyn Operator>,
    metrics: Arc<Counters>,
    used: Arc<AtomicU64>,
}

impl Operator for Metered {
    fn next_batch(&mut self, cancel: &CancelToken) -> Result<Option<Batch>> {
        let t0 = Instant::now();
        let out = self.inner.next_batch(cancel)?;
        let ns = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.metrics.elapsed_ns.fetch_add(ns, Ordering::Relaxed);
        if let Some(batch) = &out {
            self.metrics.batches.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .rows
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        }
        self.metrics
            .peak_bytes
            .fetch_max(self.used.load(Ordering::Relaxed), Ordering::Relaxed);
        Ok(out)
    }
}
