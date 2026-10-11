//! Stage planning: hash-partition exchanges, shuffle-map stages, local workers.
//!
//! Equi-joins and grouped aggregates require a hash clustering of their inputs.
//! Those requirements become exchange operators. The plan is then cut at each
//! exchange: the child is a shuffle-map stage that writes partitioned batches
//! into an in-memory shuffle, and the parent reads them as `ShuffleRead`.
//! A result stage produces the query output. Tasks of one stage are assigned
//! round-robin to a bounded worker pool.

use std::collections::HashMap;
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
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
use super::{Exec, Session, collect, physical};
use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::expr::Expr;
use crate::plan::{AggCall, JoinType, LogicalPlan, NamedExpr, SortExpr, fmt_expr};

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
            | Self::Limit { input, .. } => input.output_partitions(),
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

struct ShuffleStore {
    memory: Mutex<HashMap<(usize, usize), Vec<Batch>>>,
    dir: Option<PathBuf>,
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
            dir: None,
        }
    }

    fn files(dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&dir).map_err(|e| Error::Io(e.to_string()))?;
        Ok(Self {
            memory: Mutex::new(HashMap::new()),
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
        mut batches: Vec<Batch>,
    ) -> Result<()> {
        if let Some(path) = self.block_path(shuffle_id, map, reduce) {
            return write_ipc(&path, schema, &batches);
        }
        let mut g = self
            .memory
            .lock()
            .map_err(|_| Error::Eval("shuffle lock poisoned".into()))?;
        g.entry((shuffle_id, reduce))
            .or_default()
            .append(&mut batches);
        Ok(())
    }

    fn read(&self, shuffle_id: usize, reduce: usize, schema: SchemaRef) -> Result<Vec<Batch>> {
        if let Some(dir) = &self.dir {
            return read_ipc_reduce(dir, shuffle_id, reduce, schema);
        }
        let g = self
            .memory
            .lock()
            .map_err(|_| Error::Eval("shuffle lock poisoned".into()))?;
        match g.get(&(shuffle_id, reduce)) {
            Some(batches) => Ok(batches.clone()),
            None => Ok(vec![Batch::empty(schema)?]),
        }
    }
}

fn write_ipc(path: &Path, schema: SchemaRef, batches: &[Batch]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| Error::Io(e.to_string()))?;
    }
    let file = File::create(path).map_err(|e| Error::Io(e.to_string()))?;
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
}

fn read_ipc_reduce(
    dir: &Path,
    shuffle_id: usize,
    reduce: usize,
    schema: SchemaRef,
) -> Result<Vec<Batch>> {
    let prefix = format!("shuffle-{shuffle_id}-map-");
    let suffix = format!("-reduce-{reduce}.arrow");
    let mut paths = Vec::new();
    if dir.exists() {
        for ent in fs::read_dir(dir).map_err(|e| Error::Io(e.to_string()))? {
            let ent = ent.map_err(|e| Error::Io(e.to_string()))?;
            let name = ent.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(&prefix) && name.ends_with(&suffix) {
                paths.push(ent.path());
            }
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
}

pub(super) fn execute_staged(logical: &LogicalPlan, session: &Session) -> Result<Vec<Batch>> {
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
    let mut result = Vec::new();
    for stage in &stages {
        let out = run_stage(stage, session, &job.store, workers)?;
        if stage.kind == StageKind::Result {
            result = out;
        }
    }
    Ok(result)
}

fn run_stage(
    stage: &Stage,
    session: &Session,
    shuffle: &Arc<ShuffleStore>,
    workers: usize,
) -> Result<Vec<Batch>> {
    let n = stage.partitions.max(1);
    let workers = workers.min(n).max(1);
    let jobs: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new((0..n).collect()));
    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let jobs = jobs.clone();
        let shuffle = shuffle.clone();
        let session = session.clone();
        let plan = stage.plan.clone();
        let write = stage.write.clone();
        handles.push(thread::spawn(move || -> Result<Vec<Batch>> {
            let mut local = Vec::new();
            loop {
                let p = {
                    let mut g = jobs
                        .lock()
                        .map_err(|_| Error::Eval("task queue lock poisoned".into()))?;
                    g.pop()
                };
                let Some(p) = p else {
                    break;
                };
                let ctx = TaskCtx {
                    session: session.clone(),
                    partition: p,
                    partitions: n,
                    shuffle: shuffle.clone(),
                };
                let batches = collect(execute_physical(&plan, &ctx)?)?;
                if let Some(write) = &write {
                    let parts = partition_batches(&batches, &write.keys, write.partitions)?;
                    let schema = plan.schema().clone();
                    for (r, part) in parts.into_iter().enumerate() {
                        shuffle.write(write.shuffle_id, p, r, schema.clone(), part)?;
                    }
                } else {
                    local.extend(batches);
                }
            }
            Ok(local)
        }));
    }
    let mut out = Vec::new();
    for h in handles {
        let part = h
            .join()
            .map_err(|_| Error::Eval("worker panicked".into()))??;
        out.extend(part);
    }
    Ok(out)
}

fn execute_physical(plan: &PhysicalPlan, ctx: &TaskCtx) -> Result<Exec> {
    match plan {
        PhysicalPlan::Empty { schema } => Ok(Exec::Empty {
            schema: schema.clone(),
            done: false,
        }),
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
            let batches = collect(physical(&logical, &ctx.session)?)?;
            Ok(Exec::Scan {
                batches: slice_partition(
                    batches,
                    plan.schema().clone(),
                    ctx.partition,
                    ctx.partitions,
                )?,
                index: 0,
            })
        }
        PhysicalPlan::ShuffleRead {
            shuffle_id, schema, ..
        } => Ok(Exec::Scan {
            batches: ctx
                .shuffle
                .read(*shuffle_id, ctx.partition, schema.clone())?,
            index: 0,
        }),
        PhysicalPlan::Filter { input, predicate } => Ok(Exec::Filter {
            input: Box::new(execute_physical(input, ctx)?),
            predicate: predicate.clone(),
        }),
        PhysicalPlan::Project {
            input,
            exprs,
            schema,
        } => Ok(Exec::Project {
            input: Box::new(execute_physical(input, ctx)?),
            exprs: exprs.clone(),
            schema: schema.clone(),
        }),
        PhysicalPlan::Aggregate {
            input,
            group,
            aggs,
            schema,
        } => {
            let child = execute_physical(input, ctx)?;
            let batches = super::agg::run(child, group, aggs, schema.clone(), &ctx.session.policy)?;
            Ok(Exec::Scan { batches, index: 0 })
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
            )?;
            Ok(Exec::Scan { batches, index: 0 })
        }
        PhysicalPlan::Sort { input, order } => {
            let batches = super::sort::ordered(
                execute_physical(input, ctx)?,
                input.schema().clone(),
                order,
                0,
                None,
                &ctx.session.policy,
            )?;
            Ok(Exec::Scan { batches, index: 0 })
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
                )?;
                Ok(Exec::Scan { batches, index: 0 })
            }
            _ => Ok(Exec::Limit {
                input: Box::new(execute_physical(input, ctx)?),
                skip: *skip,
                fetch: *fetch,
            }),
        },
        PhysicalPlan::Union { .. } => Err(Error::NotImplemented("physical union")),
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
    use std::sync::Arc;

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
        let got = store.read(0, 1, schema).unwrap();
        assert_eq!(got.iter().map(Batch::num_rows).sum::<usize>(), 2);
        let _ = fs::remove_dir_all(&root);
    }
}
