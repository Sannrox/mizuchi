//! Per-operator rows, batches, elapsed time, peak pool bytes, and copied bytes.
//!
//! Wrappers are installed only for `EXPLAIN ANALYZE`. Ordinary `execute_sql`
//! builds the same operator tree with no timing.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use super::CancelToken;
use super::memory::MemoryPool;
use super::op::Operator;
use mizuchi_core::batch::Batch;
use mizuchi_core::error::Result;
use mizuchi_plan::LogicalPlan;

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
    /// Bytes copied by concat/take under this operator (zero-copy paths add 0).
    pub copied_bytes: u64,
    /// True when `elapsed_ns` is a blocking build recorded after children
    /// already ran (join, aggregate, sort). Those child times are disjoint.
    pub blocking: bool,
    /// Child operators, left to right.
    pub children: Vec<OperatorMetrics>,
}

impl OperatorMetrics {
    /// Time spent in this operator excluding nested pulls.
    ///
    /// Pipelined children (`Scan`, `Filter`, `Project`) run inside this
    /// node's `elapsed_ns`. Blocking children (join/aggregate/sort) already
    /// finished during `build_physical`; subtracting them would undercount
    /// nested joins. Exclusive time is the CPU profile of the plan.
    pub fn exclusive_ns(&self) -> u64 {
        let nested: u64 = self
            .children
            .iter()
            .filter(|c| !c.blocking)
            .map(|c| c.elapsed_ns)
            .sum();
        self.elapsed_ns.saturating_sub(nested)
    }

    /// Depth-first visit of this node and every descendant.
    pub fn walk<'a>(&'a self, out: &mut Vec<&'a OperatorMetrics>) {
        out.push(self);
        for child in &self.children {
            child.walk(out);
        }
    }

    /// Every operator, highest exclusive time first.
    pub fn ranked_exclusive(&self) -> Vec<&OperatorMetrics> {
        let mut nodes = Vec::new();
        self.walk(&mut nodes);
        nodes.sort_by_key(|b| std::cmp::Reverse(b.exclusive_ns()));
        nodes
    }

    /// Ranked exclusive-time rows as a JSON array (no nested children).
    pub fn ranked_exclusive_json(&self) -> String {
        let ranked = self.ranked_exclusive();
        let mut out = String::from("[");
        for (i, n) in ranked.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str("{\"name\":\"");
            out.push_str(&n.name);
            out.push_str("\",\"exclusive_ns\":");
            out.push_str(&n.exclusive_ns().to_string());
            out.push_str(",\"elapsed_ns\":");
            out.push_str(&n.elapsed_ns.to_string());
            out.push_str(",\"peak_bytes\":");
            out.push_str(&n.peak_bytes.to_string());
            out.push_str(",\"copied_bytes\":");
            out.push_str(&n.copied_bytes.to_string());
            out.push_str(",\"rows\":");
            out.push_str(&n.rows.to_string());
            out.push_str(",\"batches\":");
            out.push_str(&n.batches.to_string());
            out.push('}');
        }
        out.push(']');
        out
    }

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
        out.push_str(",\"exclusive_ns\":");
        out.push_str(&self.exclusive_ns().to_string());
        out.push_str(",\"blocking\":");
        out.push_str(if self.blocking { "true" } else { "false" });
        out.push_str(",\"peak_bytes\":");
        out.push_str(&self.peak_bytes.to_string());
        out.push_str(",\"copied_bytes\":");
        out.push_str(&self.copied_bytes.to_string());
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
pub(crate) struct Counters {
    rows: AtomicU64,
    batches: AtomicU64,
    elapsed_ns: AtomicU64,
    peak_bytes: AtomicU64,
    copied_bytes: AtomicU64,
    blocking: AtomicBool,
}

pub(crate) struct PhysNode {
    pub name: &'static str,
    /// Extra label after the name (`runtime_filter=[...]` on a Scan).
    pub detail: String,
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
            copied_bytes: self.metrics.copied_bytes.load(Ordering::Relaxed),
            blocking: self.metrics.blocking.load(Ordering::Relaxed),
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
        let label = if self.detail.is_empty() {
            self.name.to_string()
        } else {
            format!("{} {}", self.name, self.detail)
        };
        if analyze {
            let snap = self.snapshot();
            out.push_str(&format!(
                "{pad}{label} rows={} batches={} elapsed_ns={} exclusive_ns={} peak_bytes={} copied_bytes={}\n",
                snap.rows,
                snap.batches,
                snap.elapsed_ns,
                snap.exclusive_ns(),
                snap.peak_bytes,
                snap.copied_bytes
            ));
        } else {
            out.push_str(&format!("{pad}{label}\n"));
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
        LogicalPlan::Intersect { left, right, .. } => node(
            "Intersect",
            vec![physical_shape(left), physical_shape(right)],
        ),
        LogicalPlan::Except { left, right, .. } => {
            node("Except", vec![physical_shape(left), physical_shape(right)])
        }
        LogicalPlan::Window { input, .. } => node("Window", vec![physical_shape(input)]),
        LogicalPlan::Recursive { anchors, step, .. } => {
            let mut children: Vec<PhysNode> = anchors.iter().map(physical_shape).collect();
            children.push(physical_shape(step));
            node("Recursive", children)
        }
        LogicalPlan::WorkScan { .. } => leaf("WorkScan"),
        LogicalPlan::Scalar { input, .. } => node("Scalar", vec![physical_shape(input)]),
    }
}

pub(super) fn leaf(name: &'static str) -> PhysNode {
    PhysNode {
        name,
        detail: String::new(),
        metrics: Arc::new(Counters::default()),
        children: Vec::new(),
    }
}

fn node(name: &'static str, children: Vec<PhysNode>) -> PhysNode {
    PhysNode {
        name,
        detail: String::new(),
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
        detail: String::new(),
        metrics: metrics.clone(),
        children,
    };
    if analyze {
        (
            Box::new(Metered {
                inner: op,
                metrics,
                used: pool.used_handle(),
                copied: pool.copied_handle(),
                last_copied: pool.copied(),
            }),
            node,
        )
    } else {
        (op, node)
    }
}

pub(super) fn add_blocking(node: &PhysNode, elapsed_ns: u64, peak_bytes: u64, copied_bytes: u64) {
    node.metrics
        .elapsed_ns
        .fetch_add(elapsed_ns, Ordering::Relaxed);
    node.metrics
        .peak_bytes
        .fetch_max(peak_bytes, Ordering::Relaxed);
    node.metrics
        .copied_bytes
        .fetch_add(copied_bytes, Ordering::Relaxed);
    node.metrics.blocking.store(true, Ordering::Relaxed);
}

struct Metered {
    inner: Box<dyn Operator>,
    metrics: Arc<Counters>,
    used: Arc<AtomicU64>,
    copied: Arc<AtomicU64>,
    last_copied: u64,
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
        let now = self.copied.load(Ordering::Relaxed);
        self.metrics
            .copied_bytes
            .fetch_add(now.saturating_sub(self.last_copied), Ordering::Relaxed);
        self.last_copied = now;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::OperatorMetrics;

    fn node(
        name: &str,
        elapsed_ns: u64,
        peak_bytes: u64,
        blocking: bool,
        children: Vec<OperatorMetrics>,
    ) -> OperatorMetrics {
        OperatorMetrics {
            name: name.into(),
            rows: 1,
            batches: 1,
            elapsed_ns,
            peak_bytes,
            copied_bytes: 0,
            blocking,
            children,
        }
    }

    #[test]
    fn ranked_exclusive_orders_by_self_time() {
        let scan_hot = node("Scan", 80, 100, false, Vec::new());
        let scan_cold = node("Scan", 10, 50, false, Vec::new());
        let join = node("HashJoin", 120, 200, false, vec![scan_hot, scan_cold]);
        assert_eq!(join.exclusive_ns(), 30);
        let ranked = join.ranked_exclusive();
        assert_eq!(ranked.len(), 3);
        assert_eq!(ranked[0].name, "Scan");
        assert_eq!(ranked[0].exclusive_ns(), 80);
        assert_eq!(ranked[1].name, "HashJoin");
        assert_eq!(ranked[1].exclusive_ns(), 30);
        assert_eq!(ranked[2].name, "Scan");
        assert_eq!(ranked[2].exclusive_ns(), 10);
        let json = join.ranked_exclusive_json();
        assert!(
            json.starts_with("[{\"name\":\"Scan\",\"exclusive_ns\":80"),
            "{json}"
        );
        assert!(
            json.contains("\"name\":\"HashJoin\",\"exclusive_ns\":30"),
            "{json}"
        );
        assert!(json.contains("\"copied_bytes\":0"), "{json}");
    }

    #[test]
    fn exclusive_skips_already_finished_blocking_children() {
        let scan = node("Scan", 100, 50, false, Vec::new());
        let inner = node("HashJoin", 336_000, 200, true, vec![scan.clone()]);
        let outer = node("HashJoin", 1_274_000, 400, true, vec![inner, scan]);
        // Inner build is disjoint; only the pipelined Scan is nested in outer.
        assert_eq!(outer.exclusive_ns(), 1_274_000 - 100);
        assert_eq!(outer.children[0].exclusive_ns(), 336_000 - 100);
        let json = outer.to_json();
        assert!(json.contains("\"blocking\":true"), "{json}");
        assert!(json.contains("\"blocking\":false"), "{json}");
    }
}
