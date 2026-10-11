//! Stage planning: hash-partition exchanges, shuffle-map stages, local workers.
//!
//! Equi-joins and grouped aggregates require a hash clustering of their inputs.
//! Those requirements become exchange operators. The plan is then cut at each
//! exchange: the child is a shuffle-map stage that writes partitioned batches
//! into a shuffle, and the parent reads them as `ShuffleRead`.
//! A result stage produces the query output. Tasks of one stage are assigned
//! round-robin to a bounded worker pool. A failed task is retried on any
//! remaining worker up to four attempts; committed shuffle blocks are kept
//! and reused. Partial writes from a failed attempt are discarded.
//! After shuffle-map stages finish, join partitions that exceed both five
//! times the median size and a byte threshold are split by grouping map
//! outputs; the matching other-side partition is replicated for each split.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;

use super::join::{self, JoinSpec};
use super::key;
use super::memory::batch_bytes;
use super::{Operator, Session, collect, physical};
use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::expr::Expr;
use crate::plan::{
    AggCall, JoinType, LogicalPlan, NamedExpr, SortExpr, WindowCall, WindowFrame, fmt_expr,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StageKind {
    ShuffleMap,
    Result,
}

#[derive(Clone, Debug)]
struct Stage {
    id: usize,
    kind: StageKind,
    plan: PhysicalPlan,
    write: Option<ShuffleWrite>,
    partitions: usize,
}

#[derive(Clone, Debug)]
struct ShuffleWrite {
    shuffle_id: usize,
    keys: Vec<Expr>,
    partitions: usize,
}

#[derive(Clone, Debug)]
enum PhysicalPlan {
    Empty {
        schema: SchemaRef,
    },
    Scan {
        table: String,
        schema: SchemaRef,
        projection: Option<Vec<String>>,
        filters: Vec<Expr>,
    },
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<PhysicalPlan>,
        exprs: Vec<NamedExpr>,
        schema: SchemaRef,
    },
    Aggregate {
        input: Box<PhysicalPlan>,
        group: Vec<NamedExpr>,
        aggs: Vec<(String, AggCall)>,
        schema: SchemaRef,
    },
    Join {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        join_type: JoinType,
        on: Option<Expr>,
        schema: SchemaRef,
    },
    Sort {
        input: Box<PhysicalPlan>,
        order: Vec<SortExpr>,
    },
    Limit {
        input: Box<PhysicalPlan>,
        skip: u64,
        fetch: Option<u64>,
    },
    Union {
        inputs: Vec<PhysicalPlan>,
        schema: SchemaRef,
        all: bool,
    },
    Window {
        input: Box<PhysicalPlan>,
        partition: Vec<Expr>,
        order: Vec<SortExpr>,
        frame: WindowFrame,
        fns: Vec<WindowCall>,
        schema: SchemaRef,
    },
    Exchange {
        input: Box<PhysicalPlan>,
        keys: Vec<Expr>,
        partitions: usize,
        shuffle_id: usize,
        schema: SchemaRef,
    },
    ShuffleRead {
        shuffle_id: usize,
        partitions: usize,
        schema: SchemaRef,
    },
}

impl PhysicalPlan {
    fn schema(&self) -> &SchemaRef {
        match self {
            Self::Empty { schema }
            | Self::Scan { schema, .. }
            | Self::Project { schema, .. }
            | Self::Aggregate { schema, .. }
            | Self::Join { schema, .. }
            | Self::Union { schema, .. }
            | Self::Window { schema, .. }
            | Self::Exchange { schema, .. }
            | Self::ShuffleRead { schema, .. } => schema,
            Self::Filter { input, .. } | Self::Sort { input, .. } | Self::Limit { input, .. } => {
                input.schema()
            }
        }
    }

    fn output_partitions(&self) -> usize {
        match self {
            Self::Exchange { partitions, .. } | Self::ShuffleRead { partitions, .. } => *partitions,
            Self::Filter { input, .. }
            | Self::Project { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Sort { input, .. }
            | Self::Limit { input, .. }
            | Self::Window { input, .. } => input.output_partitions(),
            Self::Join { left, .. } => left.output_partitions(),
            Self::Empty { .. } | Self::Scan { .. } | Self::Union { .. } => 1,
        }
    }
}

fn from_logical(plan: &LogicalPlan) -> PhysicalPlan {
    match plan {
        LogicalPlan::Empty { schema } => PhysicalPlan::Empty {
            schema: schema.clone(),
        },
        LogicalPlan::Scan {
            table,
            schema,
            projection,
            filters,
            ..
        } => PhysicalPlan::Scan {
            table: table.clone(),
            schema: schema.clone(),
            projection: projection.clone(),
            filters: filters.clone(),
        },
        LogicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(from_logical(input)),
            predicate: predicate.clone(),
        },
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => PhysicalPlan::Project {
            input: Box::new(from_logical(input)),
            exprs: exprs.clone(),
            schema: schema.clone(),
        },
        LogicalPlan::Aggregate {
            input,
            group,
            aggs,
            schema,
        } => PhysicalPlan::Aggregate {
            input: Box::new(from_logical(input)),
            group: group.clone(),
            aggs: aggs.clone(),
            schema: schema.clone(),
        },
        LogicalPlan::Join {
            left,
            right,
            join_type,
            on,
            schema,
        } => PhysicalPlan::Join {
            left: Box::new(from_logical(left)),
            right: Box::new(from_logical(right)),
            join_type: *join_type,
            on: on.clone(),
            schema: schema.clone(),
        },
        LogicalPlan::Sort { input, order } => PhysicalPlan::Sort {
            input: Box::new(from_logical(input)),
            order: order.clone(),
        },
        LogicalPlan::Limit { input, skip, fetch } => PhysicalPlan::Limit {
            input: Box::new(from_logical(input)),
            skip: *skip,
            fetch: *fetch,
        },
        LogicalPlan::Union {
            inputs,
            schema,
            all,
        } => PhysicalPlan::Union {
            inputs: inputs.iter().map(from_logical).collect(),
            schema: schema.clone(),
            all: *all,
        },
        LogicalPlan::Window {
            input,
            partition,
            order,
            frame,
            fns,
            schema,
        } => PhysicalPlan::Window {
            input: Box::new(from_logical(input)),
            partition: partition.clone(),
            order: order.clone(),
            frame: *frame,
            fns: fns.clone(),
            schema: schema.clone(),
        },
    }
}

fn exchange(
    input: PhysicalPlan,
    keys: Vec<Expr>,
    partitions: usize,
    next_id: &mut usize,
) -> PhysicalPlan {
    let schema = input.schema().clone();
    let shuffle_id = *next_id;
    *next_id += 1;
    PhysicalPlan::Exchange {
        input: Box::new(input),
        keys,
        partitions,
        shuffle_id,
        schema,
    }
}

fn ensure_distribution(plan: PhysicalPlan, n: usize, next_id: &mut usize) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Join {
            left,
            right,
            join_type,
            on,
            schema,
        } => {
            let left = ensure_distribution(*left, n, next_id);
            let right = ensure_distribution(*right, n, next_id);
            let keys = on
                .as_ref()
                .and_then(|pred| join::equi_key_pairs(pred, left.schema().fields().len()).ok())
                .unwrap_or_default();
            if keys.is_empty() {
                PhysicalPlan::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type,
                    on,
                    schema,
                }
            } else {
                let (lk, rk): (Vec<_>, Vec<_>) = keys.into_iter().unzip();
                PhysicalPlan::Join {
                    left: Box::new(exchange(left, lk, n, next_id)),
                    right: Box::new(exchange(right, rk, n, next_id)),
                    join_type,
                    on,
                    schema,
                }
            }
        }
        PhysicalPlan::Aggregate {
            input,
            group,
            aggs,
            schema,
        } => {
            let input = ensure_distribution(*input, n, next_id);
            if group.is_empty() {
                PhysicalPlan::Aggregate {
                    input: Box::new(input),
                    group,
                    aggs,
                    schema,
                }
            } else {
                let keys = group.iter().map(|g| g.expr.clone()).collect();
                PhysicalPlan::Aggregate {
                    input: Box::new(exchange(input, keys, n, next_id)),
                    group,
                    aggs,
                    schema,
                }
            }
        }
        PhysicalPlan::Sort { input, order } => {
            let input = ensure_distribution(*input, n, next_id);
            let input = if input.output_partitions() == 1 {
                input
            } else {
                exchange(input, Vec::new(), 1, next_id)
            };
            PhysicalPlan::Sort {
                input: Box::new(input),
                order,
            }
        }
        PhysicalPlan::Window {
            input,
            partition,
            order,
            frame,
            fns,
            schema,
        } => {
            let input = ensure_distribution(*input, n, next_id);
            let input = if input.output_partitions() == 1 {
                input
            } else {
                exchange(input, Vec::new(), 1, next_id)
            };
            PhysicalPlan::Window {
                input: Box::new(input),
                partition,
                order,
                frame,
                fns,
                schema,
            }
        }
        PhysicalPlan::Limit { input, skip, fetch } => {
            let input = ensure_distribution(*input, n, next_id);
            let input = if input.output_partitions() == 1 {
                input
            } else {
                exchange(input, Vec::new(), 1, next_id)
            };
            PhysicalPlan::Limit {
                input: Box::new(input),
                skip,
                fetch,
            }
        }
        PhysicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(ensure_distribution(*input, n, next_id)),
            predicate,
        },
        PhysicalPlan::Project {
            input,
            exprs,
            schema,
        } => PhysicalPlan::Project {
            input: Box::new(ensure_distribution(*input, n, next_id)),
            exprs,
            schema,
        },
        PhysicalPlan::Union {
            inputs,
            schema,
            all,
        } => PhysicalPlan::Union {
            inputs: inputs
                .into_iter()
                .map(|p| ensure_distribution(p, n, next_id))
                .collect(),
            schema,
            all,
        },
        other => other,
    }
}

fn fragment(plan: PhysicalPlan, stages: &mut Vec<Stage>) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Exchange {
            input,
            keys,
            partitions,
            shuffle_id,
            schema,
        } => {
            let input = fragment(*input, stages);
            stages.push(Stage {
                id: stages.len(),
                kind: StageKind::ShuffleMap,
                plan: input,
                write: Some(ShuffleWrite {
                    shuffle_id,
                    keys,
                    partitions,
                }),
                partitions,
            });
            PhysicalPlan::ShuffleRead {
                shuffle_id,
                partitions,
                schema,
            }
        }
        PhysicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(fragment(*input, stages)),
            predicate,
        },
        PhysicalPlan::Project {
            input,
            exprs,
            schema,
        } => PhysicalPlan::Project {
            input: Box::new(fragment(*input, stages)),
            exprs,
            schema,
        },
        PhysicalPlan::Aggregate {
            input,
            group,
            aggs,
            schema,
        } => PhysicalPlan::Aggregate {
            input: Box::new(fragment(*input, stages)),
            group,
            aggs,
            schema,
        },
        PhysicalPlan::Join {
            left,
            right,
            join_type,
            on,
            schema,
        } => PhysicalPlan::Join {
            left: Box::new(fragment(*left, stages)),
            right: Box::new(fragment(*right, stages)),
            join_type,
            on,
            schema,
        },
        PhysicalPlan::Sort { input, order } => PhysicalPlan::Sort {
            input: Box::new(fragment(*input, stages)),
            order,
        },
        PhysicalPlan::Window {
            input,
            partition,
            order,
            frame,
            fns,
            schema,
        } => PhysicalPlan::Window {
            input: Box::new(fragment(*input, stages)),
            partition,
            order,
            frame,
            fns,
            schema,
        },
        PhysicalPlan::Limit { input, skip, fetch } => PhysicalPlan::Limit {
            input: Box::new(fragment(*input, stages)),
            skip,
            fetch,
        },
        PhysicalPlan::Union {
            inputs,
            schema,
            all,
        } => PhysicalPlan::Union {
            inputs: inputs.into_iter().map(|p| fragment(p, stages)).collect(),
            schema,
            all,
        },
        other => other,
    }
}

fn plan_stages(logical: &LogicalPlan, workers: usize) -> Vec<Stage> {
    let n = workers.max(1);
    let mut next_id = 0usize;
    let physical = ensure_distribution(from_logical(logical), n, &mut next_id);
    let mut stages = Vec::new();
    let root = fragment(physical, &mut stages);
    let partitions = root.output_partitions().max(1);
    stages.push(Stage {
        id: stages.len(),
        kind: StageKind::Result,
        plan: root,
        write: None,
        partitions,
    });
    stages
}

pub(super) fn explain_stages(logical: &LogicalPlan, workers: usize) -> Result<String> {
    let stages = plan_stages(logical, workers);
    let mut out = String::new();
    for stage in &stages {
        let kind = match stage.kind {
            StageKind::ShuffleMap => "shuffle-map",
            StageKind::Result => "result",
        };
        out.push_str(&format!(
            "Stage {} {kind} partitions={}\n",
            stage.id, stage.partitions
        ));
        explain_physical(&stage.plan, 1, &mut out);
    }
    Ok(out)
}

fn explain_physical(plan: &PhysicalPlan, indent: usize, out: &mut String) {
    let pad = "  ".repeat(indent);
    match plan {
        PhysicalPlan::Empty { .. } => out.push_str(&format!("{pad}Empty\n")),
        PhysicalPlan::Scan { table, schema, .. } => {
            out.push_str(&format!("{pad}Scan {table} {}\n", schema));
        }
        PhysicalPlan::Filter { input, predicate } => {
            out.push_str(&format!(
                "{pad}Filter {}\n",
                fmt_expr(predicate, input.schema().as_ref())
            ));
            explain_physical(input, indent + 1, out);
        }
        PhysicalPlan::Project { input, schema, .. } => {
            out.push_str(&format!("{pad}Project {}\n", schema));
            explain_physical(input, indent + 1, out);
        }
        PhysicalPlan::Aggregate { input, schema, .. } => {
            out.push_str(&format!("{pad}Aggregate {}\n", schema));
            explain_physical(input, indent + 1, out);
        }
        PhysicalPlan::Join {
            left,
            right,
            join_type,
            on,
            ..
        } => {
            let on = on
                .as_ref()
                .map(|e| fmt_expr(e, &join_schema(left.schema(), right.schema())))
                .unwrap_or_else(|| "true".to_string());
            out.push_str(&format!("{pad}Join {join_type} ({on})\n"));
            explain_physical(left, indent + 1, out);
            explain_physical(right, indent + 1, out);
        }
        PhysicalPlan::Sort { input, .. } => {
            out.push_str(&format!("{pad}Sort\n"));
            explain_physical(input, indent + 1, out);
        }
        PhysicalPlan::Window { input, schema, .. } => {
            out.push_str(&format!("{pad}Window {schema}\n"));
            explain_physical(input, indent + 1, out);
        }
        PhysicalPlan::Limit { input, skip, fetch } => {
            out.push_str(&format!("{pad}Limit skip={skip} fetch={fetch:?}\n"));
            explain_physical(input, indent + 1, out);
        }
        PhysicalPlan::Union { inputs, all, .. } => {
            out.push_str(&format!("{pad}Union all={all}\n"));
            for input in inputs {
                explain_physical(input, indent + 1, out);
            }
        }
        PhysicalPlan::Exchange {
            input,
            partitions,
            shuffle_id,
            ..
        } => {
            out.push_str(&format!(
                "{pad}Exchange hash id={shuffle_id} partitions={partitions}\n"
            ));
            explain_physical(input, indent + 1, out);
        }
        PhysicalPlan::ShuffleRead {
            shuffle_id,
            partitions,
            ..
        } => out.push_str(&format!(
            "{pad}ShuffleRead {shuffle_id} partitions={partitions}\n"
        )),
    }
}

fn join_schema(left: &SchemaRef, right: &SchemaRef) -> arrow::datatypes::Schema {
    let mut fields = left.fields().iter().cloned().collect::<Vec<_>>();
    fields.extend(right.fields().iter().cloned());
    arrow::datatypes::Schema::new(fields)
}

static JOB_SEQ: AtomicU64 = AtomicU64::new(1);

/// Failed tasks retry this many times, matching the usual default of four
/// individual task failures before the stage is aborted.
const MAX_TASK_ATTEMPTS: u32 = 4;

/// A partition is skewed when it is larger than this times the median size
/// (and also larger than the session byte threshold).
const SKEW_FACTOR: u64 = 5;

#[cfg(test)]
static INJECT_TASK_FAILURES: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static INJECT_RESULT_ONLY: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static MAP_COMMITS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static INJECT_STRAGGLER: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static SPECULATIVE_DISCARDS: AtomicU64 = AtomicU64::new(0);

struct ShuffleStore {
    /// In-memory blocks keyed by `(shuffle_id, map_partition, reduce_partition)`.
    memory: Mutex<HashMap<(usize, usize, usize), Vec<Batch>>>,
    /// Committed block sizes for adaptive splits, same key as `memory`.
    sizes: Mutex<HashMap<(usize, usize, usize), u64>>,
    dir: Option<PathBuf>,
}

#[derive(Clone, Debug)]
struct TaskSpec {
    partition: usize,
    /// Map id used when this task writes a shuffle. Unique per split so
    /// parallel join tasks do not overwrite each other's blocks.
    output_map: usize,
    /// shuffle_id -> map ids to read. Empty means every map.
    maps: HashMap<usize, Vec<usize>>,
}

#[derive(Clone)]
struct PendingTask {
    spec: TaskSpec,
    attempts: u32,
    speculative: bool,
}

struct ShuffleJob {
    store: Arc<ShuffleStore>,
    dir: Option<PathBuf>,
}

impl Drop for ShuffleJob {
    fn drop(&mut self) {
        if let Some(dir) = &self.dir {
            let _ = fs::remove_dir_all(dir);
        }
    }
}

impl ShuffleStore {
    fn memory() -> Self {
        Self {
            memory: Mutex::new(HashMap::new()),
            sizes: Mutex::new(HashMap::new()),
            dir: None,
        }
    }

    fn files(dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&dir).map_err(|e| Error::Io(e.to_string()))?;
        Ok(Self {
            memory: Mutex::new(HashMap::new()),
            sizes: Mutex::new(HashMap::new()),
            dir: Some(dir),
        })
    }

    fn block_path(&self, shuffle_id: usize, map: usize, reduce: usize) -> Option<PathBuf> {
        self.dir.as_ref().map(|dir| {
            dir.join(format!(
                "shuffle-{shuffle_id}-map-{map}-reduce-{reduce}.arrow"
            ))
        })
    }

    fn write(
        &self,
        shuffle_id: usize,
        map: usize,
        reduce: usize,
        schema: SchemaRef,
        batches: Vec<Batch>,
    ) -> Result<()> {
        let bytes = batches.iter().map(batch_bytes).sum();
        self.sizes
            .lock()
            .map_err(|_| Error::Eval("shuffle size lock poisoned".into()))?
            .insert((shuffle_id, map, reduce), bytes);
        if let Some(path) = self.block_path(shuffle_id, map, reduce) {
            return write_ipc(&path, schema, &batches);
        }
        let mut g = self
            .memory
            .lock()
            .map_err(|_| Error::Eval("shuffle lock poisoned".into()))?;
        g.insert((shuffle_id, map, reduce), batches);
        Ok(())
    }

    fn read(
        &self,
        shuffle_id: usize,
        reduce: usize,
        schema: SchemaRef,
        maps: Option<&[usize]>,
    ) -> Result<Vec<Batch>> {
        if let Some(dir) = &self.dir {
            return read_ipc_reduce(dir, shuffle_id, reduce, schema, maps);
        }
        let g = self
            .memory
            .lock()
            .map_err(|_| Error::Eval("shuffle lock poisoned".into()))?;
        let mut blocks: Vec<(usize, Vec<Batch>)> = g
            .iter()
            .filter_map(|((sid, map, red), batches)| {
                if *sid == shuffle_id && *red == reduce && maps.is_none_or(|m| m.contains(map)) {
                    Some((*map, batches.clone()))
                } else {
                    None
                }
            })
            .collect();
        blocks.sort_by_key(|(map, _)| *map);
        let mut out = Vec::new();
        for (_, batches) in blocks {
            out.extend(batches);
        }
        if out.is_empty() {
            out.push(Batch::empty(schema)?);
        }
        Ok(out)
    }

    fn map_sizes(&self, shuffle_id: usize, reduce: usize) -> Result<Vec<(usize, u64)>> {
        let g = self
            .sizes
            .lock()
            .map_err(|_| Error::Eval("shuffle size lock poisoned".into()))?;
        let mut out: Vec<(usize, u64)> = g
            .iter()
            .filter_map(|((sid, map, red), bytes)| {
                if *sid == shuffle_id && *red == reduce {
                    Some((*map, *bytes))
                } else {
                    None
                }
            })
            .collect();
        out.sort_by_key(|(map, _)| *map);
        Ok(out)
    }

    fn reduce_bytes(&self, shuffle_id: usize, n_reduce: usize) -> Result<Vec<u64>> {
        let g = self
            .sizes
            .lock()
            .map_err(|_| Error::Eval("shuffle size lock poisoned".into()))?;
        let mut out = vec![0u64; n_reduce.max(1)];
        for ((sid, _map, red), bytes) in g.iter() {
            if *sid == shuffle_id && *red < out.len() {
                out[*red] = out[*red].saturating_add(*bytes);
            }
        }
        Ok(out)
    }

    /// Drop a failed map attempt so a retry does not mix with partial output.
    fn abort_map(&self, shuffle_id: usize, map: usize, reduces: usize) -> Result<()> {
        {
            let mut sizes = self
                .sizes
                .lock()
                .map_err(|_| Error::Eval("shuffle size lock poisoned".into()))?;
            for reduce in 0..reduces {
                sizes.remove(&(shuffle_id, map, reduce));
            }
        }
        if self.dir.is_some() {
            for reduce in 0..reduces {
                if let Some(path) = self.block_path(shuffle_id, map, reduce) {
                    let _ = fs::remove_file(&path);
                    let _ = fs::remove_file(tmp_path(&path));
                }
            }
            return Ok(());
        }
        let mut g = self
            .memory
            .lock()
            .map_err(|_| Error::Eval("shuffle lock poisoned".into()))?;
        for reduce in 0..reduces {
            g.remove(&(shuffle_id, map, reduce));
        }
        Ok(())
    }
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut os = path.as_os_str().to_os_string();
    os.push(".tmp");
    PathBuf::from(os)
}

fn write_ipc(path: &Path, schema: SchemaRef, batches: &[Batch]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| Error::Io(e.to_string()))?;
    }
    let tmp = tmp_path(path);
    let write_tmp = || -> Result<()> {
        let file = File::create(&tmp).map_err(|e| Error::Io(e.to_string()))?;
        let mut writer =
            FileWriter::try_new(file, schema.as_ref()).map_err(|e| Error::Io(e.to_string()))?;
        for batch in batches {
            if batch.num_rows() > 0 {
                writer
                    .write(batch.as_arrow())
                    .map_err(|e| Error::Io(e.to_string()))?;
            }
        }
        writer.finish().map_err(|e| Error::Io(e.to_string()))?;
        Ok(())
    };
    match write_tmp() {
        Ok(()) => fs::rename(&tmp, path).map_err(|e| Error::Io(e.to_string())),
        Err(e) => {
            let _ = fs::remove_file(&tmp);
            Err(e)
        }
    }
}

fn read_ipc_reduce(
    dir: &Path,
    shuffle_id: usize,
    reduce: usize,
    schema: SchemaRef,
    maps: Option<&[usize]>,
) -> Result<Vec<Batch>> {
    let prefix = format!("shuffle-{shuffle_id}-map-");
    let suffix = format!("-reduce-{reduce}.arrow");
    let mut paths = Vec::new();
    if dir.exists() {
        for ent in fs::read_dir(dir).map_err(|e| Error::Io(e.to_string()))? {
            let ent = ent.map_err(|e| Error::Io(e.to_string()))?;
            let name = ent.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with(&prefix) || !name.ends_with(&suffix) {
                continue;
            }
            if let Some(maps) = maps {
                let mid = &name[prefix.len()..name.len() - suffix.len()];
                let Ok(map) = mid.parse::<usize>() else {
                    continue;
                };
                if !maps.contains(&map) {
                    continue;
                }
            }
            paths.push(ent.path());
        }
    }
    paths.sort();
    let mut out = Vec::new();
    for path in paths {
        let file = File::open(&path).map_err(|e| Error::Io(e.to_string()))?;
        let reader = FileReader::try_new(file, None).map_err(|e| Error::Io(e.to_string()))?;
        for batch in reader {
            let batch = batch.map_err(|e| Error::Io(e.to_string()))?;
            if batch.num_rows() > 0 {
                out.push(Batch::from_arrow(batch)?);
            }
        }
    }
    if out.is_empty() {
        out.push(Batch::empty(schema)?);
    }
    Ok(out)
}

struct TaskCtx {
    session: Session,
    partition: usize,
    partitions: usize,
    shuffle: Arc<ShuffleStore>,
    pool: Arc<super::memory::MemoryPool>,
    map_filter: HashMap<usize, Vec<usize>>,
}

pub(super) fn execute_staged(logical: &LogicalPlan, session: &Session) -> Result<Vec<Batch>> {
    let mut notes = Vec::new();
    execute_staged_inner(logical, session, &mut notes)
}

pub(super) fn explain_analyze(logical: &LogicalPlan, session: &Session) -> Result<String> {
    let mut notes = Vec::new();
    let batches = execute_staged_inner(logical, session, &mut notes)?;
    let mut text = explain_stages(logical, session.workers.max(1))?;
    if notes.is_empty() {
        text.push_str("Adaptive: no partition splits\n");
    } else {
        for note in &notes {
            text.push_str(note);
            if !note.ends_with('\n') {
                text.push('\n');
            }
        }
    }
    let rows: usize = batches.iter().map(Batch::num_rows).sum();
    text.push_str(&format!("rows={rows}\n"));
    Ok(text)
}

fn execute_staged_inner(
    logical: &LogicalPlan,
    session: &Session,
    notes: &mut Vec<String>,
) -> Result<Vec<Batch>> {
    let workers = session.workers.max(1);
    let stages = plan_stages(logical, workers);
    let job = match &session.shuffle_dir {
        Some(root) => {
            let id = JOB_SEQ.fetch_add(1, Ordering::Relaxed);
            let dir = root.join(format!("job-{id}"));
            ShuffleJob {
                store: Arc::new(ShuffleStore::files(dir.clone())?),
                dir: Some(dir),
            }
        }
        None => ShuffleJob {
            store: Arc::new(ShuffleStore::memory()),
            dir: None,
        },
    };
    let pool = Arc::new(session.memory_pool()?);
    let mut result = Vec::new();
    for stage in &stages {
        let tasks = if join_shuffle_reads(&stage.plan).is_some() {
            adaptive_tasks(stage, &job.store, session, notes)?
        } else {
            default_tasks(stage.partitions)
        };
        let out = run_stage(stage, session, &job.store, workers, pool.clone(), tasks)?;
        if stage.kind == StageKind::Result {
            result = out;
        }
    }
    Ok(result)
}

fn default_tasks(partitions: usize) -> Vec<TaskSpec> {
    (0..partitions.max(1))
        .map(|p| TaskSpec {
            partition: p,
            output_map: p,
            maps: HashMap::new(),
        })
        .collect()
}

fn adaptive_tasks(
    stage: &Stage,
    store: &ShuffleStore,
    session: &Session,
    notes: &mut Vec<String>,
) -> Result<Vec<TaskSpec>> {
    let Some((left_sid, right_sid, n_reduce, join_type)) = join_shuffle_reads(&stage.plan) else {
        return Ok(default_tasks(stage.partitions));
    };
    let left_bytes = store.reduce_bytes(left_sid, n_reduce)?;
    let right_bytes = store.reduce_bytes(right_sid, n_reduce)?;
    let left_med = median_u64(&left_bytes);
    let right_med = median_u64(&right_bytes);
    let left_thr = skew_threshold(left_med, session.skew_threshold);
    let right_thr = skew_threshold(right_med, session.skew_threshold);
    let left_target = target_size(&left_bytes, left_thr, session.advisory_partition_size);
    let right_target = target_size(&right_bytes, right_thr, session.advisory_partition_size);
    let mut tasks = Vec::new();
    let mut splits = 0usize;
    for p in 0..n_reduce.max(1) {
        let left_maps = store.map_sizes(left_sid, p)?;
        let right_maps = store.map_sizes(right_sid, p)?;
        let left_sz = left_bytes.get(p).copied().unwrap_or(0);
        let right_sz = right_bytes.get(p).copied().unwrap_or(0);
        let left_ids: Vec<usize> = left_maps.iter().map(|(m, _)| *m).collect();
        let right_ids: Vec<usize> = right_maps.iter().map(|(m, _)| *m).collect();
        let can_split_left = matches!(
            join_type,
            JoinType::Inner | JoinType::Left | JoinType::Semi | JoinType::Anti
        );
        let can_split_right = matches!(join_type, JoinType::Inner | JoinType::Right);
        // Split the preserved side (or the larger side for inner). Replicating a
        // preserved side would emit unmatched rows once per split task.
        let groups = if can_split_left
            && left_sz > left_thr
            && (left_sz >= right_sz || !can_split_right || right_sz <= right_thr)
        {
            let groups = group_maps(&left_maps, left_target);
            if groups.len() > 1 {
                notes.push(format!(
                    "Adaptive skew: shuffle {left_sid} partition {p} bytes={left_sz} median={left_med} split into {}\n",
                    groups.len()
                ));
                splits += groups.len();
            }
            groups
                .into_iter()
                .map(|g| (g, right_ids.clone()))
                .collect::<Vec<_>>()
        } else if can_split_right && right_sz > right_thr {
            let groups = group_maps(&right_maps, right_target);
            if groups.len() > 1 {
                notes.push(format!(
                    "Adaptive skew: shuffle {right_sid} partition {p} bytes={right_sz} median={right_med} split into {}\n",
                    groups.len()
                ));
                splits += groups.len();
            }
            groups
                .into_iter()
                .map(|g| (left_ids.clone(), g))
                .collect::<Vec<_>>()
        } else {
            vec![(left_ids, right_ids)]
        };
        for (lm, rm) in groups {
            let mut maps = HashMap::new();
            maps.insert(left_sid, lm);
            maps.insert(right_sid, rm);
            tasks.push(TaskSpec {
                partition: p,
                output_map: tasks.len(),
                maps,
            });
        }
    }
    if splits > 0 {
        notes.push(format!(
            "Adaptive skew: join tasks {} -> {}\n",
            n_reduce.max(1),
            tasks.len()
        ));
    }
    if tasks.is_empty() {
        Ok(default_tasks(stage.partitions))
    } else {
        Ok(tasks)
    }
}

fn join_shuffle_reads(plan: &PhysicalPlan) -> Option<(usize, usize, usize, JoinType)> {
    match plan {
        PhysicalPlan::Join {
            left,
            right,
            join_type,
            ..
        } => match (left.as_ref(), right.as_ref()) {
            (
                PhysicalPlan::ShuffleRead {
                    shuffle_id: l,
                    partitions: lp,
                    ..
                },
                PhysicalPlan::ShuffleRead {
                    shuffle_id: r,
                    partitions: rp,
                    ..
                },
            ) => Some((*l, *r, (*lp).max(*rp), *join_type)),
            _ => None,
        },
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Window { input, .. } => join_shuffle_reads(input),
        _ => None,
    }
}

fn skew_threshold(median: u64, floor: u64) -> u64 {
    floor.max(median.saturating_mul(SKEW_FACTOR))
}

fn median_u64(values: &[u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut v = values.to_vec();
    v.sort_unstable();
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        v[n / 2 - 1] / 2 + v[n / 2] / 2
    }
}

fn target_size(sizes: &[u64], skew_thr: u64, advisory: u64) -> u64 {
    let non: Vec<u64> = sizes.iter().copied().filter(|s| *s <= skew_thr).collect();
    let avg = if non.is_empty() {
        0
    } else {
        non.iter().sum::<u64>() / non.len() as u64
    };
    advisory.max(avg).max(1)
}

fn group_maps(maps: &[(usize, u64)], target: u64) -> Vec<Vec<usize>> {
    let mut out = Vec::new();
    let mut cur = Vec::new();
    let mut acc = 0u64;
    for (map, bytes) in maps {
        if !cur.is_empty() && acc.saturating_add(*bytes) > target {
            out.push(std::mem::take(&mut cur));
            acc = 0;
        }
        cur.push(*map);
        acc = acc.saturating_add(*bytes);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    if out.is_empty() {
        out.push(Vec::new());
    }
    out
}

fn run_stage(
    stage: &Stage,
    session: &Session,
    shuffle: &Arc<ShuffleStore>,
    workers: usize,
    pool: Arc<super::memory::MemoryPool>,
    tasks: Vec<TaskSpec>,
) -> Result<Vec<Batch>> {
    let n = stage.partitions.max(1);
    let mut queued = Vec::new();
    for t in tasks {
        if session.speculate {
            queued.push(PendingTask {
                spec: t.clone(),
                attempts: 0,
                speculative: true,
            });
        }
        queued.push(PendingTask {
            spec: t,
            attempts: 0,
            speculative: false,
        });
    }
    let workers = workers.min(queued.len().max(1)).max(1);
    let pending: Arc<Mutex<Vec<PendingTask>>> = Arc::new(Mutex::new(queued));
    let committed: Arc<Mutex<HashSet<(usize, usize)>>> = Arc::new(Mutex::new(HashSet::new()));
    let abort: Arc<Mutex<Option<Error>>> = Arc::new(Mutex::new(None));
    let mut out = Vec::new();
    loop {
        if let Some(err) = abort
            .lock()
            .map_err(|_| Error::Eval("abort lock poisoned".into()))?
            .take()
        {
            return Err(err);
        }
        let remaining = pending
            .lock()
            .map_err(|_| Error::Eval("task queue lock poisoned".into()))?
            .len();
        if remaining == 0 {
            break;
        }
        let wave = workers.min(remaining).max(1);
        let in_flight = Arc::new(Mutex::new(vec![None; wave]));
        let mut handles = Vec::with_capacity(wave);
        for i in 0..wave {
            let pending = pending.clone();
            let abort = abort.clone();
            let in_flight = in_flight.clone();
            let shuffle = shuffle.clone();
            let session = session.clone();
            let plan = stage.plan.clone();
            let write = stage.write.clone();
            let pool = pool.clone();
            let kind = stage.kind;
            let committed = committed.clone();
            handles.push(thread::spawn(move || -> Result<Vec<Batch>> {
                let mut local = Vec::new();
                loop {
                    if abort
                        .lock()
                        .map_err(|_| Error::Eval("abort lock poisoned".into()))?
                        .is_some()
                    {
                        break;
                    }
                    session.cancel.check()?;
                    let job = {
                        let mut g = pending
                            .lock()
                            .map_err(|_| Error::Eval("task queue lock poisoned".into()))?;
                        g.pop()
                    };
                    let Some(job) = job else {
                        break;
                    };
                    {
                        let mut g = in_flight
                            .lock()
                            .map_err(|_| Error::Eval("in-flight lock poisoned".into()))?;
                        g[i] = Some(job.clone());
                    }
                    let ran = panic::catch_unwind(AssertUnwindSafe(|| {
                        if !job.speculative {
                            inject_straggler();
                        }
                        inject_failure(kind);
                        run_one_task(&plan, &write, &session, &job.spec, n, &shuffle, &pool)
                    }));
                    {
                        let mut g = in_flight
                            .lock()
                            .map_err(|_| Error::Eval("in-flight lock poisoned".into()))?;
                        g[i] = None;
                    }
                    match ran {
                        Ok(Ok(batches)) => {
                            let mut g = committed
                                .lock()
                                .map_err(|_| Error::Eval("commit lock poisoned".into()))?;
                            if g.insert((job.spec.partition, job.spec.output_map)) {
                                local.extend(batches);
                            } else {
                                #[cfg(test)]
                                SPECULATIVE_DISCARDS.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Ok(Err(e)) => {
                            requeue_or_abort(&pending, &abort, job, e, &write, &shuffle)?;
                        }
                        Err(_) => {
                            requeue_or_abort(
                                &pending,
                                &abort,
                                job,
                                Error::Eval("worker panicked".into()),
                                &write,
                                &shuffle,
                            )?;
                        }
                    }
                }
                Ok(local)
            }));
        }
        for (i, h) in handles.into_iter().enumerate() {
            match h.join() {
                Ok(Ok(part)) => out.extend(part),
                Ok(Err(e)) => {
                    let mut g = abort
                        .lock()
                        .map_err(|_| Error::Eval("abort lock poisoned".into()))?;
                    if g.is_none() {
                        *g = Some(e);
                    }
                }
                Err(_) => {
                    let job = {
                        let mut g = in_flight
                            .lock()
                            .map_err(|_| Error::Eval("in-flight lock poisoned".into()))?;
                        g[i].take()
                    };
                    if let Some(job) = job {
                        requeue_or_abort(
                            &pending,
                            &abort,
                            job,
                            Error::Eval("worker panicked".into()),
                            &stage.write,
                            shuffle,
                        )?;
                    }
                }
            }
        }
    }
    if let Some(err) = abort
        .lock()
        .map_err(|_| Error::Eval("abort lock poisoned".into()))?
        .take()
    {
        return Err(err);
    }
    Ok(out)
}

fn inject_straggler() {
    #[cfg(test)]
    {
        if INJECT_STRAGGLER.swap(false, Ordering::SeqCst) {
            thread::sleep(std::time::Duration::from_millis(150));
        }
    }
}

fn inject_failure(_kind: StageKind) {
    #[cfg(test)]
    {
        if INJECT_RESULT_ONLY.load(Ordering::SeqCst) && _kind != StageKind::Result {
            return;
        }
        let mut cur = INJECT_TASK_FAILURES.load(Ordering::SeqCst);
        loop {
            if cur == 0 {
                return;
            }
            match INJECT_TASK_FAILURES.compare_exchange(
                cur,
                cur - 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => panic!("injected worker failure"),
                Err(seen) => cur = seen,
            }
        }
    }
}

fn run_one_task(
    plan: &PhysicalPlan,
    write: &Option<ShuffleWrite>,
    session: &Session,
    spec: &TaskSpec,
    partitions: usize,
    shuffle: &Arc<ShuffleStore>,
    pool: &Arc<super::memory::MemoryPool>,
) -> Result<Vec<Batch>> {
    let ctx = TaskCtx {
        session: session.clone(),
        partition: spec.partition,
        partitions,
        shuffle: shuffle.clone(),
        pool: pool.clone(),
        map_filter: spec.maps.clone(),
    };
    let batches = collect(execute_physical(plan, &ctx)?, &session.cancel)?;
    if let Some(write) = write {
        let parts = partition_batches(&batches, &write.keys, write.partitions)?;
        let schema = plan.schema().clone();
        for (r, part) in parts.into_iter().enumerate() {
            shuffle.write(write.shuffle_id, spec.output_map, r, schema.clone(), part)?;
        }
        #[cfg(test)]
        MAP_COMMITS.fetch_add(1, Ordering::Relaxed);
        Ok(Vec::new())
    } else {
        Ok(batches)
    }
}

fn requeue_or_abort(
    pending: &Mutex<Vec<PendingTask>>,
    abort: &Mutex<Option<Error>>,
    job: PendingTask,
    cause: Error,
    write: &Option<ShuffleWrite>,
    shuffle: &ShuffleStore,
) -> Result<()> {
    if matches!(cause, Error::Cancelled) {
        let mut g = abort
            .lock()
            .map_err(|_| Error::Eval("abort lock poisoned".into()))?;
        if g.is_none() {
            *g = Some(cause);
        }
        return Ok(());
    }
    if let Some(write) = write {
        shuffle.abort_map(write.shuffle_id, job.spec.output_map, write.partitions)?;
    }
    let next = job.attempts.saturating_add(1);
    if next >= MAX_TASK_ATTEMPTS {
        let mut g = abort
            .lock()
            .map_err(|_| Error::Eval("abort lock poisoned".into()))?;
        if g.is_none() {
            *g = Some(Error::Eval(format!(
                "task partition {} failed after {MAX_TASK_ATTEMPTS} attempts: {cause}",
                job.spec.partition
            )));
        }
        return Ok(());
    }
    pending
        .lock()
        .map_err(|_| Error::Eval("task queue lock poisoned".into()))?
        .push(PendingTask {
            spec: job.spec,
            attempts: next,
            speculative: job.speculative,
        });
    Ok(())
}

fn execute_physical(plan: &PhysicalPlan, ctx: &TaskCtx) -> Result<Box<dyn Operator>> {
    let lowering = ctx.session.lowering.0.as_ref();
    match plan {
        PhysicalPlan::Empty { schema } => lowering.empty(schema.clone()),
        PhysicalPlan::Scan {
            table,
            projection,
            filters,
            ..
        } => {
            let logical = LogicalPlan::Scan {
                table: table.clone(),
                schema: plan.schema().clone(),
                alias: None,
                projection: projection.clone(),
                filters: filters.clone(),
                stats: None,
            };
            let batches = collect(
                physical(&logical, &ctx.session, ctx.pool.as_ref())?,
                &ctx.session.cancel,
            )?;
            lowering.scan(slice_partition(
                batches,
                plan.schema().clone(),
                ctx.partition,
                ctx.partitions,
            )?)
        }
        PhysicalPlan::ShuffleRead {
            shuffle_id, schema, ..
        } => {
            let maps = ctx.map_filter.get(shuffle_id).map(|v| v.as_slice());
            lowering.scan(
                ctx.shuffle
                    .read(*shuffle_id, ctx.partition, schema.clone(), maps)?,
            )
        }
        PhysicalPlan::Filter { input, predicate } => {
            lowering.filter(execute_physical(input, ctx)?, predicate.clone())
        }
        PhysicalPlan::Project {
            input,
            exprs,
            schema,
        } => lowering.project(execute_physical(input, ctx)?, exprs.clone(), schema.clone()),
        PhysicalPlan::Aggregate {
            input,
            group,
            aggs,
            schema,
        } => {
            let child = execute_physical(input, ctx)?;
            let batches = super::agg::run(
                child,
                group,
                aggs,
                schema.clone(),
                &ctx.session.policy,
                ctx.pool.as_ref(),
                &ctx.session.cancel,
            )?;
            lowering.scan(batches)
        }
        PhysicalPlan::Join {
            left,
            right,
            join_type,
            on,
            schema,
        } => {
            let batches = join::run(
                execute_physical(left, ctx)?,
                execute_physical(right, ctx)?,
                JoinSpec {
                    join_type: *join_type,
                    on: on.as_ref(),
                    left_schema: left.schema().clone(),
                    right_schema: right.schema().clone(),
                    schema: schema.clone(),
                },
                &ctx.session.policy,
                ctx.pool.as_ref(),
                &ctx.session.cancel,
            )?;
            lowering.scan(batches)
        }
        PhysicalPlan::Sort { input, order } => {
            let batches = super::sort::ordered(
                execute_physical(input, ctx)?,
                input.schema().clone(),
                order,
                0,
                None,
                &ctx.session.policy,
                ctx.pool.as_ref(),
                &ctx.session.cancel,
            )?;
            lowering.scan(batches)
        }
        PhysicalPlan::Limit { input, skip, fetch } => match input.as_ref() {
            PhysicalPlan::Sort {
                input: sorted,
                order,
            } => {
                let batches = super::sort::ordered(
                    execute_physical(sorted, ctx)?,
                    sorted.schema().clone(),
                    order,
                    *skip,
                    *fetch,
                    &ctx.session.policy,
                    ctx.pool.as_ref(),
                    &ctx.session.cancel,
                )?;
                lowering.scan(batches)
            }
            _ => lowering.limit(execute_physical(input, ctx)?, *skip, *fetch),
        },
        PhysicalPlan::Union { .. } => Err(Error::NotImplemented("physical union")),
        PhysicalPlan::Window {
            input,
            partition,
            order,
            frame,
            fns,
            schema,
        } => {
            let child = execute_physical(input, ctx)?;
            let batches = super::window::run(
                child,
                input.schema().clone(),
                partition,
                order,
                *frame,
                fns,
                schema.clone(),
                &ctx.session.policy,
                ctx.pool.as_ref(),
                &ctx.session.cancel,
            )?;
            lowering.scan(batches)
        }
        PhysicalPlan::Exchange { .. } => {
            Err(Error::Plan("exchange survived stage fragmentation".into()))
        }
    }
}

fn slice_partition(
    batches: Vec<Batch>,
    schema: SchemaRef,
    partition: usize,
    n: usize,
) -> Result<Vec<Batch>> {
    if n <= 1 {
        return Ok(batches);
    }
    let total: usize = batches.iter().map(Batch::num_rows).sum();
    let start = total * partition / n;
    let end = total * (partition + 1) / n;
    if start >= end {
        return Ok(vec![Batch::empty(schema)?]);
    }
    let mut rows = 0usize;
    let mut out = Vec::new();
    for batch in batches {
        let n_rows = batch.num_rows();
        let b0 = rows;
        let b1 = rows + n_rows;
        rows = b1;
        let lo = start.max(b0);
        let hi = end.min(b1);
        if lo < hi {
            out.push(batch.slice(lo - b0, hi - lo)?);
        }
    }
    if out.is_empty() {
        out.push(Batch::empty(schema)?);
    }
    Ok(out)
}

fn partition_batches(batches: &[Batch], keys: &[Expr], n: usize) -> Result<Vec<Vec<Batch>>> {
    let mut out = vec![Vec::new(); n];
    if n == 1 {
        out[0].extend(batches.iter().cloned());
        return Ok(out);
    }
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let ids = partition_ids(batch, keys, n)?;
        for (p, bucket) in out.iter_mut().enumerate() {
            let mask: Vec<bool> = ids.iter().map(|id| *id == p as u32).collect();
            let bools = BooleanArray::from(mask);
            let filtered = filter_record_batch(batch.as_arrow(), &bools)
                .map_err(|e| Error::Eval(e.to_string()))?;
            if filtered.num_rows() > 0 {
                bucket.push(Batch::from_arrow(filtered)?);
            }
        }
    }
    Ok(out)
}

fn partition_ids(batch: &Batch, keys: &[Expr], n: usize) -> Result<Vec<u32>> {
    let mut ids = Vec::with_capacity(batch.num_rows());
    if keys.is_empty() {
        for i in 0..batch.num_rows() {
            ids.push((i % n) as u32);
        }
        return Ok(ids);
    }
    let cols = key::eval_all(keys, batch)?;
    for i in 0..batch.num_rows() {
        let k = key::extract_key(&cols, i)?;
        let mut h = std::collections::hash_map::DefaultHasher::new();
        k.hash(&mut h);
        ids.push((h.finish() % n as u64) as u32);
    }
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::{Arc, Mutex};

    fn join_session() -> Session {
        let t_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let t = Batch::try_new(
            t_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(2),
                Some(3),
                None,
            ]))],
        )
        .unwrap();
        let u_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("x", DataType::Int64, true),
        ]));
        let u = Batch::try_new(
            u_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(2), Some(2), Some(4), None])),
                Arc::new(Int64Array::from(vec![
                    Some(10),
                    Some(30),
                    Some(40),
                    Some(99),
                ])),
            ],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", t_schema, vec![t]).unwrap();
        s.register("u", u_schema, vec![u]).unwrap();
        s
    }

    fn sorted_pairs(batches: &[Batch], left: &str, right: &str) -> Vec<(Option<i64>, Option<i64>)> {
        let mut rows = Vec::new();
        for batch in batches {
            let l = batch
                .column_by_name(left)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let r = batch
                .column_by_name(right)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..l.len() {
                rows.push((
                    if l.is_null(i) { None } else { Some(l.value(i)) },
                    if r.is_null(i) { None } else { Some(r.value(i)) },
                ));
            }
        }
        rows.sort_by_key(|(a, b)| (a.unwrap_or(i64::MIN), b.unwrap_or(i64::MIN)));
        rows
    }

    #[test]
    fn join_explain_shows_shuffle_map_and_result_stages() {
        let _g = lock_stage_tests();
        let s = join_session().with_workers(2).unwrap();
        let text = s
            .explain_stages("SELECT t.a, u.id FROM t JOIN u ON t.a = u.id")
            .unwrap();
        assert!(text.contains("Stage 0 shuffle-map"), "{text}");
        assert!(text.contains("Stage 1 shuffle-map"), "{text}");
        assert!(text.contains("result"), "{text}");
        assert!(text.contains("ShuffleRead"), "{text}");
        assert!(text.contains("Join Inner"), "{text}");
    }

    #[test]
    fn two_worker_join_matches_pull_path() {
        let _g = lock_stage_tests();
        let s = join_session().with_workers(2).unwrap();
        let sql = "SELECT t.a, u.id FROM t JOIN u ON t.a = u.id";
        let pull = s.execute_sql(sql).unwrap();
        let staged = s.execute_staged(sql).unwrap();
        assert_eq!(
            sorted_pairs(&pull, "a", "id"),
            sorted_pairs(&staged, "a", "id")
        );
    }

    #[test]
    fn scan_only_is_a_single_result_stage() {
        let _g = lock_stage_tests();
        let s = join_session().with_workers(2).unwrap();
        let text = s.explain_stages("SELECT a FROM t").unwrap();
        assert!(text.contains("Stage 0 result"), "{text}");
        assert!(!text.contains("shuffle-map"), "{text}");
        let pull = s.execute_sql("SELECT a FROM t").unwrap();
        let staged = s.execute_staged("SELECT a FROM t").unwrap();
        assert_eq!(
            sorted_pairs(&pull, "a", "a"),
            sorted_pairs(&staged, "a", "a")
        );
    }

    #[test]
    fn ipc_shuffle_dir_matches_pull_and_cleans_up() {
        let _g = lock_stage_tests();
        let root = std::env::temp_dir().join(format!(
            "mizuchi-shuffle-{}-{}",
            std::process::id(),
            JOB_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        let s = join_session()
            .with_workers(2)
            .unwrap()
            .with_shuffle_dir(root.clone());
        let sql = "SELECT t.a, u.id FROM t JOIN u ON t.a = u.id";
        let pull = s.execute_sql(sql).unwrap();
        let staged = s.execute_staged(sql).unwrap();
        assert_eq!(
            sorted_pairs(&pull, "a", "id"),
            sorted_pairs(&staged, "a", "id")
        );
        let leftover: Vec<_> = fs::read_dir(&root)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        let _ = fs::remove_dir_all(&root);
        assert!(leftover.is_empty(), "{leftover:?}");
    }

    #[test]
    fn shuffle_files_are_arrow_ipc() {
        let _g = lock_stage_tests();
        let root = std::env::temp_dir().join(format!(
            "mizuchi-shuffle-ipc-{}-{}",
            std::process::id(),
            JOB_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        let store = ShuffleStore::files(root.clone()).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(1), Some(2)]))],
        )
        .unwrap();
        store.write(0, 0, 1, schema.clone(), vec![batch]).unwrap();
        let path = root.join("shuffle-0-map-0-reduce-1.arrow");
        let file = File::open(&path).unwrap();
        let reader = FileReader::try_new(file, None).unwrap();
        let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(rows, 2);
        let got = store.read(0, 1, schema, None).unwrap();
        assert_eq!(got.iter().map(Batch::num_rows).sum::<usize>(), 2);
        let _ = fs::remove_dir_all(&root);
    }

    static STAGE_GATE: Mutex<()> = Mutex::new(());

    fn lock_stage_tests() -> std::sync::MutexGuard<'static, ()> {
        STAGE_GATE.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn reset_inject() {
        INJECT_TASK_FAILURES.store(0, Ordering::SeqCst);
        INJECT_RESULT_ONLY.store(false, Ordering::SeqCst);
        MAP_COMMITS.store(0, Ordering::SeqCst);
        INJECT_STRAGGLER.store(false, Ordering::SeqCst);
        SPECULATIVE_DISCARDS.store(0, Ordering::SeqCst);
    }

    #[test]
    fn killed_worker_retries_and_matches_pull() {
        let _g = lock_stage_tests();
        reset_inject();
        INJECT_TASK_FAILURES.store(1, Ordering::SeqCst);
        let s = join_session().with_workers(2).unwrap();
        let sql = "SELECT t.a, u.id FROM t JOIN u ON t.a = u.id";
        let pull = s.execute_sql(sql).unwrap();
        let staged = s.execute_staged(sql).unwrap();
        assert_eq!(
            sorted_pairs(&pull, "a", "id"),
            sorted_pairs(&staged, "a", "id")
        );
        reset_inject();
    }

    #[test]
    fn speculative_straggler_keeps_one_commit() {
        let _g = lock_stage_tests();
        reset_inject();
        INJECT_STRAGGLER.store(true, Ordering::SeqCst);
        let s = join_session().with_workers(2).unwrap().with_speculate(true);
        let sql = "SELECT t.a, u.id FROM t JOIN u ON t.a = u.id";
        let pull = s.execute_sql(sql).unwrap();
        let staged = s.execute_staged(sql).unwrap();
        assert_eq!(
            sorted_pairs(&pull, "a", "id"),
            sorted_pairs(&staged, "a", "id")
        );
        assert!(
            SPECULATIVE_DISCARDS.load(Ordering::SeqCst) >= 1,
            "expected a discarded speculative copy"
        );
        reset_inject();
    }

    #[test]
    fn result_retry_reuses_committed_shuffle_maps() {
        let _g = lock_stage_tests();
        reset_inject();
        INJECT_RESULT_ONLY.store(true, Ordering::SeqCst);
        INJECT_TASK_FAILURES.store(1, Ordering::SeqCst);
        let s = join_session().with_workers(2).unwrap();
        let sql = "SELECT t.a, u.id FROM t JOIN u ON t.a = u.id";
        let pull = s.execute_sql(sql).unwrap();
        let staged = s.execute_staged(sql).unwrap();
        assert_eq!(
            sorted_pairs(&pull, "a", "id"),
            sorted_pairs(&staged, "a", "id")
        );
        // Two shuffle-map stages × two partitions; result-stage retry must
        // not re-run maps.
        assert_eq!(MAP_COMMITS.load(Ordering::SeqCst), 4);
        reset_inject();
    }

    #[test]
    fn too_many_task_failures_fail_closed() {
        let _g = lock_stage_tests();
        reset_inject();
        INJECT_TASK_FAILURES.store(MAX_TASK_ATTEMPTS as u64, Ordering::SeqCst);
        let s = join_session().with_workers(1).unwrap();
        let err = s.execute_staged("SELECT a FROM t").unwrap_err();
        assert!(
            matches!(err, Error::Eval(ref m) if m.contains("failed after")),
            "{err:?}"
        );
        reset_inject();
        INJECT_TASK_FAILURES.store((MAX_TASK_ATTEMPTS - 1) as u64, Ordering::SeqCst);
        let out = s.execute_staged("SELECT a FROM t").unwrap();
        assert_eq!(sorted_pairs(&out, "a", "a").len(), 5);
        reset_inject();
    }

    #[test]
    fn ipc_shuffle_survives_killed_worker() {
        let _g = lock_stage_tests();
        reset_inject();
        INJECT_RESULT_ONLY.store(true, Ordering::SeqCst);
        INJECT_TASK_FAILURES.store(1, Ordering::SeqCst);
        let root = std::env::temp_dir().join(format!(
            "mizuchi-shuffle-retry-{}-{}",
            std::process::id(),
            JOB_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        let s = join_session()
            .with_workers(2)
            .unwrap()
            .with_shuffle_dir(root.clone());
        let sql = "SELECT t.a, u.id FROM t JOIN u ON t.a = u.id";
        let pull = s.execute_sql(sql).unwrap();
        let staged = s.execute_staged(sql).unwrap();
        assert_eq!(
            sorted_pairs(&pull, "a", "id"),
            sorted_pairs(&staged, "a", "id")
        );
        assert_eq!(MAP_COMMITS.load(Ordering::SeqCst), 4);
        let leftover: Vec<_> = fs::read_dir(&root)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        let _ = fs::remove_dir_all(&root);
        assert!(leftover.is_empty(), "{leftover:?}");
        reset_inject();
    }

    fn skewed_join_session() -> Session {
        let t_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let u_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("x", DataType::Int64, true),
        ]));
        let mut t_vals = vec![Some(1i64); 800];
        t_vals.extend([Some(2), Some(3), Some(4), Some(5), Some(99)]);
        let t = Batch::try_new(t_schema.clone(), vec![Arc::new(Int64Array::from(t_vals))]).unwrap();
        let mut u_ids = vec![Some(1i64); 40];
        u_ids.extend([Some(2), Some(3), Some(4), Some(5), Some(77)]);
        let u_x = vec![Some(0i64); u_ids.len()];
        let u = Batch::try_new(
            u_schema.clone(),
            vec![
                Arc::new(Int64Array::from(u_ids)),
                Arc::new(Int64Array::from(u_x)),
            ],
        )
        .unwrap();
        let mut s = Session::new();
        s.register("t", t_schema, vec![t]).unwrap();
        s.register("u", u_schema, vec![u]).unwrap();
        s.with_workers(4)
            .unwrap()
            .with_skew_threshold(1)
            .with_advisory_partition_size(1)
    }

    #[test]
    fn skewed_join_matches_pull_and_splits() {
        let _g = lock_stage_tests();
        let s = skewed_join_session();
        let sql = "SELECT t.a, u.id FROM t JOIN u ON t.a = u.id";
        let pull = s.execute_sql(sql).unwrap();
        let staged = s.execute_staged(sql).unwrap();
        assert_eq!(
            sorted_pairs(&pull, "a", "id"),
            sorted_pairs(&staged, "a", "id")
        );
        let text = s.explain_analyze(sql).unwrap();
        assert!(text.contains("Adaptive skew"), "{text}");
        assert!(text.contains("split into"), "{text}");
        assert!(text.contains("join tasks"), "{text}");
        let pull_rows = sorted_pairs(&pull, "a", "id").len();
        assert!(text.contains(&format!("rows={pull_rows}")), "{text}");

        let left_sql = "SELECT t.a, u.id FROM t LEFT JOIN u ON t.a = u.id";
        let left_pull = s.execute_sql(left_sql).unwrap();
        let left_staged = s.execute_staged(left_sql).unwrap();
        assert_eq!(
            sorted_pairs(&left_pull, "a", "id"),
            sorted_pairs(&left_staged, "a", "id")
        );
        let right_sql = "SELECT t.a, u.id FROM t RIGHT JOIN u ON t.a = u.id";
        let right_pull = s.execute_sql(right_sql).unwrap();
        let right_staged = s.execute_staged(right_sql).unwrap();
        assert_eq!(
            sorted_pairs(&right_pull, "a", "id"),
            sorted_pairs(&right_staged, "a", "id")
        );
    }

    #[test]
    fn default_threshold_does_not_split_tiny_joins() {
        let _g = lock_stage_tests();
        let s = join_session().with_workers(2).unwrap();
        let text = s
            .explain_analyze("SELECT t.a, u.id FROM t JOIN u ON t.a = u.id")
            .unwrap();
        assert!(text.contains("Adaptive: no partition splits"), "{text}");
    }
}
