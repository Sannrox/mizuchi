//! Continuous queries as micro-batches (ADR 0005, ADR 0009).
//!
//! Each trigger takes the batches ingested since the last commit and runs
//! them as one finite aggregate. Event-time watermarks drop late rows.
//! Tumbling, sliding, and session windows assign a `[start, end)` range
//! per row. A caller-provided checkpoint directory holds an offset log, a
//! commit log, and Arrow IPC state. The sink writes one Parquet file per
//! micro-batch id so a replay of the same id replaces the same file.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use serde_json::{Value, json};

use super::CancelToken;
use mizuchi_core::batch::Batch;
use mizuchi_core::error::{Error, Result};

/// Event-time window assigned to each input row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventWindow {
    /// Adjacent, non-overlapping windows of `size`.
    Tumbling {
        /// Window width.
        size: Duration,
    },
    /// Overlapping windows of `size` that advance by `slide`.
    Sliding {
        /// Window width.
        size: Duration,
        /// How far consecutive windows advance.
        slide: Duration,
    },
    /// Merge events that fall within `gap` of the current session end.
    Session {
        /// Inactivity gap that closes a session.
        gap: Duration,
    },
}

/// Which groups a trigger writes to the sink.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamOutput {
    /// Every group accumulated so far.
    Complete,
    /// Groups whose window end is at or behind the watermark.
    Append,
}

/// Checkpoint, sink, watermark, and window for a [`StreamQuery`].
#[derive(Clone, Debug)]
pub struct StreamQueryConfig {
    /// Offset log, commit log, and state files.
    pub checkpoint: PathBuf,
    /// Directory of `batch-{id}.parquet` files.
    pub sink: PathBuf,
    /// Event-time column (`Timestamp` or `Int64` microseconds).
    pub event_time: String,
    /// Rows with event time behind `max_event - delay` are late.
    pub delay: Duration,
    /// Window assignment.
    pub window: EventWindow,
    /// Extra grouping columns besides the window.
    pub group: Vec<String>,
    /// Optional `Int64` column summed per group. `COUNT(*)` always runs.
    pub value: Option<String>,
    /// Sink contents after each committed trigger.
    pub output: StreamOutput,
}

/// One committed (or attempted) micro-batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamTrigger {
    /// Monotonic id starting at 0.
    pub batch_id: u64,
    /// Input rows in this trigger, including late rows.
    pub rows_in: u64,
    /// Rows written to the sink file (0 when the file is omitted).
    pub rows_out: u64,
    /// Watermark after this trigger, in microseconds.
    pub watermark_us: i64,
}

/// Running continuous query over an unbounded in-memory source.
pub struct StreamQuery {
    schema: SchemaRef,
    cfg: StreamQueryConfig,
    cancel: CancelToken,
    pending: Vec<Batch>,
    watermark_us: i64,
    max_event_us: i64,
    next_batch_id: u64,
    incomplete: Option<u64>,
    state: WindowState,
    out_schema: SchemaRef,
    size_us: i64,
    slide_us: i64,
    gap_us: i64,
    delay_us: i64,
}

#[derive(Clone)]
enum WindowState {
    Fixed(BTreeMap<GroupKey, Slot>),
    Sessions(BTreeMap<Vec<GroupDim>, Vec<SessionSlot>>),
}

#[derive(Clone, Copy)]
struct Slot {
    count: i64,
    sum: i64,
    seen_sum: bool,
    max_event: i64,
}

#[derive(Clone, Copy)]
struct SessionSlot {
    start: i64,
    end: i64,
    slot: Slot,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct GroupKey {
    start: i64,
    end: i64,
    dims: Vec<GroupDim>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum GroupDim {
    Null,
    Utf8(String),
}

impl StreamQuery {
    /// Create a new query. Fails if `checkpoint` already has metadata.
    pub fn try_new(schema: SchemaRef, cfg: StreamQueryConfig) -> Result<Self> {
        Self::open_or_new(schema, cfg, CancelToken::new())
    }

    /// Resume from `checkpoint`. The in-memory source starts empty; the
    /// caller re-ingests rows that were not part of a committed trigger.
    pub fn open(checkpoint: impl AsRef<Path>) -> Result<Self> {
        recover(checkpoint.as_ref(), CancelToken::new())
    }

    pub(super) fn open_or_new(
        schema: SchemaRef,
        cfg: StreamQueryConfig,
        cancel: CancelToken,
    ) -> Result<Self> {
        let meta = cfg.checkpoint.join("metadata.json");
        if meta.exists() {
            let q = recover(&cfg.checkpoint, cancel)?;
            if q.schema.as_ref() != schema.as_ref() {
                return Err(Error::Plan(
                    "stream checkpoint schema does not match".into(),
                ));
            }
            return Ok(q);
        }
        init_new(schema, cfg, cancel)
    }

    /// Append a micro-batch. Rows are processed on the next [`Self::trigger`].
    pub fn ingest(&mut self, batch: Batch) -> Result<()> {
        self.cancel.check()?;
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(Error::InvalidBatch(
                "stream batch schema does not match the query".into(),
            ));
        }
        self.pending.push(batch);
        Ok(())
    }

    /// Run one micro-batch: filter late rows, update window state, write the
    /// sink file, persist state, then append a commit marker.
    pub fn trigger(&mut self) -> Result<StreamTrigger> {
        self.run_trigger(true)
    }

    /// Event-time watermark in microseconds after the last committed trigger.
    pub fn watermark_us(&self) -> i64 {
        self.watermark_us
    }

    /// Concatenate committed sink files (append) or the latest snapshot
    /// (complete). Uncommitted files are ignored.
    pub fn committed_output(&self) -> Result<Vec<Batch>> {
        let ids = committed_ids(&self.cfg.checkpoint)?;
        match self.cfg.output {
            StreamOutput::Complete => match ids.last() {
                None => Ok(Vec::new()),
                Some(&id) => read_sink_file(&self.cfg.sink.join(format!("batch-{id}.parquet"))),
            },
            StreamOutput::Append => {
                let mut out = Vec::new();
                for id in ids {
                    let path = self.cfg.sink.join(format!("batch-{id}.parquet"));
                    if path.exists() {
                        out.extend(read_sink_file(&path)?);
                    }
                }
                Ok(out)
            }
        }
    }

    fn run_trigger(&mut self, commit: bool) -> Result<StreamTrigger> {
        self.cancel.check()?;
        if self.pending.is_empty() {
            return Ok(StreamTrigger {
                batch_id: self.next_batch_id,
                rows_in: 0,
                rows_out: 0,
                watermark_us: self.watermark_us,
            });
        }
        let batch_id = self.incomplete.unwrap_or(self.next_batch_id);
        let inputs = std::mem::take(&mut self.pending);
        let rows_in: u64 = inputs.iter().map(|b| b.num_rows() as u64).sum();

        let snapshot = self.state.clone();
        let old_wm = self.watermark_us;
        let old_max = self.max_event_us;

        let result = (|| {
            self.apply_batches(&inputs, old_wm)?;
            self.cancel.check()?;
            let emitted = self.emit(old_wm)?;
            let rows_out: u64 = emitted.iter().map(|b| b.num_rows() as u64).sum();
            let offset = json!({
                "watermark_us": self.watermark_us,
                "max_event_us": self.max_event_us,
            });
            write_offset(
                &self.cfg.checkpoint,
                batch_id,
                &offset,
                self.incomplete.is_some(),
            )?;
            let sink_path = self.cfg.sink.join(format!("batch-{batch_id}.parquet"));
            if rows_out > 0 {
                write_parquet_file(&sink_path, &emitted, self.out_schema.clone())?;
            } else if sink_path.exists() {
                fs::remove_file(&sink_path).map_err(|e| io_err(&sink_path, e))?;
            }
            write_state(&self.cfg.checkpoint, batch_id, self)?;
            if commit {
                write_commit(
                    &self.cfg.checkpoint,
                    batch_id,
                    json!({
                        "rows": rows_out,
                        "file": if rows_out > 0 {
                            Some(format!("batch-{batch_id}.parquet"))
                        } else {
                            None
                        },
                    }),
                )?;
            }
            Ok(StreamTrigger {
                batch_id,
                rows_in,
                rows_out,
                watermark_us: self.watermark_us,
            })
        })();

        match result {
            Ok(t) if commit => {
                self.incomplete = None;
                self.next_batch_id = batch_id.saturating_add(1);
                Ok(t)
            }
            Ok(t) => {
                // Offset, sink, and state exist; commit marker does not.
                self.state = snapshot;
                self.watermark_us = old_wm;
                self.max_event_us = old_max;
                self.pending = inputs;
                self.incomplete = Some(batch_id);
                self.next_batch_id = batch_id;
                Ok(t)
            }
            Err(e) => {
                self.state = snapshot;
                self.watermark_us = old_wm;
                self.max_event_us = old_max;
                self.pending = inputs;
                Err(e)
            }
        }
    }

    fn apply_batches(&mut self, inputs: &[Batch], watermark: i64) -> Result<()> {
        let mut batch_max = self.max_event_us;
        for batch in inputs {
            self.cancel.check()?;
            let ts = batch
                .column_by_name(&self.cfg.event_time)
                .ok_or_else(|| Error::Plan(format!("missing {}", self.cfg.event_time)))?;
            let value = match &self.cfg.value {
                Some(name) => Some(
                    batch
                        .column_by_name(name)
                        .ok_or_else(|| Error::Plan(format!("missing {name}")))?,
                ),
                None => None,
            };
            let groups = group_cols(batch, &self.cfg.group)?;
            for row in 0..batch.num_rows() {
                let Some(event) = event_us(ts.as_ref(), row)? else {
                    continue;
                };
                batch_max = batch_max.max(event);
                if event < watermark {
                    continue;
                }
                let val = match value {
                    Some(col) if !col.is_null(row) => Some(i64_at(col.as_ref(), row)?),
                    _ => None,
                };
                let dims = dims_at(&groups, row)?;
                self.add_event(event, dims, val)?;
            }
        }
        self.max_event_us = batch_max;
        let candidate = batch_max.saturating_sub(self.delay_us);
        if candidate > self.watermark_us {
            self.watermark_us = candidate;
        }
        Ok(())
    }

    fn add_event(&mut self, event: i64, dims: Vec<GroupDim>, val: Option<i64>) -> Result<()> {
        match (&mut self.state, self.cfg.window) {
            (WindowState::Fixed(map), EventWindow::Tumbling { .. }) => {
                let start = event.div_euclid(self.size_us) * self.size_us;
                let end = start.saturating_add(self.size_us);
                bump(
                    map.entry(GroupKey { start, end, dims })
                        .or_insert(Slot::new()),
                    event,
                    val,
                );
            }
            (WindowState::Fixed(map), EventWindow::Sliding { .. }) => {
                for start in sliding_starts(event, self.size_us, self.slide_us) {
                    let end = start.saturating_add(self.size_us);
                    bump(
                        map.entry(GroupKey {
                            start,
                            end,
                            dims: dims.clone(),
                        })
                        .or_insert(Slot::new()),
                        event,
                        val,
                    );
                }
            }
            (WindowState::Sessions(map), EventWindow::Session { .. }) => {
                let sessions = map.entry(dims).or_default();
                merge_session(sessions, event, self.gap_us, val);
            }
            _ => return Err(Error::Plan("window state kind mismatch".into())),
        }
        Ok(())
    }

    fn emit(&mut self, prev_watermark: i64) -> Result<Vec<Batch>> {
        match self.cfg.output {
            StreamOutput::Complete => self.snapshot_all(),
            StreamOutput::Append => self.take_finalized(prev_watermark),
        }
    }

    fn snapshot_all(&self) -> Result<Vec<Batch>> {
        let rows = match &self.state {
            WindowState::Fixed(map) => map
                .iter()
                .map(|(k, s)| emit_row(k.start, k.end, &k.dims, s))
                .collect(),
            WindowState::Sessions(map) => map
                .iter()
                .flat_map(|(dims, sessions)| {
                    sessions.iter().map(|s| {
                        emit_row(s.start, s.end.saturating_add(self.gap_us), dims, &s.slot)
                    })
                })
                .collect(),
        };
        rows_to_batch(self.out_schema.clone(), rows, self.cfg.value.is_some())
    }

    fn take_finalized(&mut self, _prev_watermark: i64) -> Result<Vec<Batch>> {
        let wm = self.watermark_us;
        let rows = match &mut self.state {
            WindowState::Fixed(map) => {
                let keys: Vec<GroupKey> = map.keys().filter(|k| k.end <= wm).cloned().collect();
                let mut rows = Vec::new();
                for k in keys {
                    if let Some(s) = map.remove(&k) {
                        rows.push(emit_row(k.start, k.end, &k.dims, &s));
                    }
                }
                rows
            }
            WindowState::Sessions(map) => {
                let gap = self.gap_us;
                let mut rows = Vec::new();
                let keys: Vec<Vec<GroupDim>> = map.keys().cloned().collect();
                for dims in keys {
                    let Some(sessions) = map.get_mut(&dims) else {
                        continue;
                    };
                    let mut keep = Vec::new();
                    for s in sessions.drain(..) {
                        if s.end.saturating_add(gap) <= wm {
                            rows.push(emit_row(s.start, s.end.saturating_add(gap), &dims, &s.slot));
                        } else {
                            keep.push(s);
                        }
                    }
                    if keep.is_empty() {
                        map.remove(&dims);
                    } else {
                        *sessions = keep;
                    }
                }
                rows
            }
        };
        rows_to_batch(self.out_schema.clone(), rows, self.cfg.value.is_some())
    }
}

impl Slot {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0,
            seen_sum: false,
            max_event: i64::MIN,
        }
    }
}

fn bump(slot: &mut Slot, event: i64, val: Option<i64>) {
    slot.count += 1;
    slot.max_event = slot.max_event.max(event);
    if let Some(v) = val {
        slot.sum = slot.sum.saturating_add(v);
        slot.seen_sum = true;
    }
}

fn merge_session(sessions: &mut Vec<SessionSlot>, event: i64, gap: i64, val: Option<i64>) {
    let mut merged = SessionSlot {
        start: event,
        end: event,
        slot: Slot::new(),
    };
    bump(&mut merged.slot, event, val);
    let mut rest = Vec::new();
    for s in sessions.drain(..) {
        let left = s.start.max(merged.start);
        let right = s
            .end
            .saturating_add(gap)
            .min(merged.end.saturating_add(gap));
        if left <= right {
            merged.start = merged.start.min(s.start);
            merged.end = merged.end.max(s.end);
            merged.slot.count += s.slot.count;
            merged.slot.sum = merged.slot.sum.saturating_add(s.slot.sum);
            merged.slot.seen_sum |= s.slot.seen_sum;
            merged.slot.max_event = merged.slot.max_event.max(s.slot.max_event);
        } else {
            rest.push(s);
        }
    }
    rest.push(merged);
    rest.sort_by_key(|s| s.start);
    *sessions = rest;
}

fn sliding_starts(ts: i64, size: i64, slide: i64) -> Vec<i64> {
    if size <= 0 || slide <= 0 {
        return Vec::new();
    }
    let first = (ts - size + slide).div_euclid(slide) * slide;
    let mut starts = Vec::new();
    let mut s = first;
    while s <= ts {
        if ts < s.saturating_add(size) {
            starts.push(s);
        }
        match s.checked_add(slide) {
            Some(n) => s = n,
            None => break,
        }
        if starts.len() > 4096 {
            break;
        }
    }
    starts
}

fn init_new(schema: SchemaRef, cfg: StreamQueryConfig, cancel: CancelToken) -> Result<StreamQuery> {
    validate_cfg(&schema, &cfg)?;
    fs::create_dir_all(&cfg.checkpoint).map_err(|e| io_err(&cfg.checkpoint, e))?;
    fs::create_dir_all(&cfg.sink).map_err(|e| io_err(&cfg.sink, e))?;
    let (size_us, slide_us, gap_us) = window_us(&cfg.window)?;
    let delay_us = micros(cfg.delay)?;
    let out_schema = output_schema(&cfg)?;
    let state = match cfg.window {
        EventWindow::Session { .. } => WindowState::Sessions(BTreeMap::new()),
        _ => WindowState::Fixed(BTreeMap::new()),
    };
    write_schema_file(&cfg.checkpoint.join("schema.arrow"), schema.clone())?;
    let meta = json!({
        "event_time": cfg.event_time,
        "delay_us": delay_us,
        "window": window_json(&cfg.window)?,
        "group": cfg.group,
        "value": cfg.value,
        "output": match cfg.output {
            StreamOutput::Complete => "complete",
            StreamOutput::Append => "append",
        },
        "sink": cfg.sink.display().to_string(),
    });
    create_exclusive(
        &cfg.checkpoint.join("metadata.json"),
        serde_json::to_vec_pretty(&meta).map_err(|e| Error::Io(e.to_string()))?,
    )?;
    Ok(StreamQuery {
        schema,
        cfg,
        cancel,
        pending: Vec::new(),
        watermark_us: i64::MIN,
        max_event_us: i64::MIN,
        next_batch_id: 0,
        incomplete: None,
        state,
        out_schema,
        size_us,
        slide_us,
        gap_us,
        delay_us,
    })
}

fn recover(checkpoint: &Path, cancel: CancelToken) -> Result<StreamQuery> {
    let text = fs::read_to_string(checkpoint.join("metadata.json"))
        .map_err(|e| io_err(&checkpoint.join("metadata.json"), e))?;
    let meta: Value =
        serde_json::from_str(&text).map_err(|e| Error::Io(format!("stream metadata: {e}")))?;
    let schema = read_schema_file(&checkpoint.join("schema.arrow"))?;
    let cfg = cfg_from_meta(checkpoint, &meta)?;
    validate_cfg(&schema, &cfg)?;
    let (size_us, slide_us, gap_us) = window_us(&cfg.window)?;
    let delay_us = micros(cfg.delay)?;
    let out_schema = output_schema(&cfg)?;
    let committed = committed_ids(checkpoint)?;
    let last = committed.last().copied();
    let mut state = match cfg.window {
        EventWindow::Session { .. } => WindowState::Sessions(BTreeMap::new()),
        _ => WindowState::Fixed(BTreeMap::new()),
    };
    let mut watermark_us = i64::MIN;
    let mut max_event_us = i64::MIN;
    if let Some(id) = last {
        if let Some(v) = read_offset(checkpoint, id)? {
            watermark_us = v["watermark_us"].as_i64().unwrap_or(i64::MIN);
            max_event_us = v["max_event_us"].as_i64().unwrap_or(i64::MIN);
        }
        state = load_state(checkpoint, id, &cfg)?;
    }
    let next = last.map(|id| id.saturating_add(1)).unwrap_or(0);
    let incomplete = if offset_exists(checkpoint, next) && !commit_exists(checkpoint, next) {
        Some(next)
    } else {
        None
    };
    let next_batch_id = incomplete.unwrap_or(next);
    Ok(StreamQuery {
        schema,
        cfg,
        cancel,
        pending: Vec::new(),
        watermark_us,
        max_event_us,
        next_batch_id,
        incomplete,
        state,
        out_schema,
        size_us,
        slide_us,
        gap_us,
        delay_us,
    })
}

fn cfg_from_meta(checkpoint: &Path, meta: &Value) -> Result<StreamQueryConfig> {
    let event_time = meta["event_time"]
        .as_str()
        .ok_or_else(|| Error::Io("stream metadata missing event_time".into()))?
        .to_string();
    let delay_us = meta["delay_us"]
        .as_i64()
        .ok_or_else(|| Error::Io("stream metadata missing delay_us".into()))?;
    let delay = Duration::from_micros(u64::try_from(delay_us.max(0)).unwrap_or(0));
    let window = window_from_json(&meta["window"])?;
    let group = meta["group"]
        .as_array()
        .ok_or_else(|| Error::Io("stream metadata missing group".into()))?
        .iter()
        .map(|v| {
            v.as_str()
                .map(str::to_string)
                .ok_or_else(|| Error::Io("stream group name".into()))
        })
        .collect::<Result<Vec<_>>>()?;
    let value = meta["value"].as_str().map(str::to_string);
    let output = match meta["output"].as_str() {
        Some("complete") => StreamOutput::Complete,
        Some("append") => StreamOutput::Append,
        _ => return Err(Error::Io("stream metadata output".into())),
    };
    let sink = PathBuf::from(
        meta["sink"]
            .as_str()
            .ok_or_else(|| Error::Io("stream metadata missing sink".into()))?,
    );
    Ok(StreamQueryConfig {
        checkpoint: checkpoint.to_path_buf(),
        sink,
        event_time,
        delay,
        window,
        group,
        value,
        output,
    })
}

fn validate_cfg(schema: &Schema, cfg: &StreamQueryConfig) -> Result<()> {
    require_field(schema, &cfg.event_time)?;
    for g in &cfg.group {
        require_field(schema, g)?;
    }
    if let Some(v) = &cfg.value {
        let f = require_field(schema, v)?;
        if f.data_type() != &DataType::Int64 {
            return Err(Error::UnsupportedType(format!(
                "stream value {}: {}",
                v,
                f.data_type()
            )));
        }
    }
    let (size, slide, gap) = window_us(&cfg.window)?;
    if matches!(
        cfg.window,
        EventWindow::Tumbling { .. } | EventWindow::Sliding { .. }
    ) && size <= 0
    {
        return Err(Error::Plan("window size must be greater than zero".into()));
    }
    if matches!(cfg.window, EventWindow::Sliding { .. }) && slide <= 0 {
        return Err(Error::Plan("window slide must be greater than zero".into()));
    }
    if matches!(cfg.window, EventWindow::Session { .. }) && gap <= 0 {
        return Err(Error::Plan("session gap must be greater than zero".into()));
    }
    Ok(())
}

fn require_field<'a>(schema: &'a Schema, name: &str) -> Result<&'a Field> {
    schema
        .field_with_name(name)
        .map_err(|_| Error::Plan(format!("stream column {name} is not in the schema")))
}

fn window_us(window: &EventWindow) -> Result<(i64, i64, i64)> {
    Ok(match window {
        EventWindow::Tumbling { size } => (micros(*size)?, 0, 0),
        EventWindow::Sliding { size, slide } => (micros(*size)?, micros(*slide)?, 0),
        EventWindow::Session { gap } => (0, 0, micros(*gap)?),
    })
}

fn micros(d: Duration) -> Result<i64> {
    i64::try_from(d.as_micros())
        .map_err(|_| Error::Plan("duration exceeds i64 microseconds".into()))
}

fn window_json(window: &EventWindow) -> Result<Value> {
    Ok(match window {
        EventWindow::Tumbling { size } => json!({"kind": "tumbling", "size_us": micros(*size)?}),
        EventWindow::Sliding { size, slide } => json!({
            "kind": "sliding",
            "size_us": micros(*size)?,
            "slide_us": micros(*slide)?,
        }),
        EventWindow::Session { gap } => json!({"kind": "session", "gap_us": micros(*gap)?}),
    })
}

fn window_from_json(v: &Value) -> Result<EventWindow> {
    match v["kind"].as_str() {
        Some("tumbling") => Ok(EventWindow::Tumbling {
            size: Duration::from_micros(as_u64_us(&v["size_us"])?),
        }),
        Some("sliding") => Ok(EventWindow::Sliding {
            size: Duration::from_micros(as_u64_us(&v["size_us"])?),
            slide: Duration::from_micros(as_u64_us(&v["slide_us"])?),
        }),
        Some("session") => Ok(EventWindow::Session {
            gap: Duration::from_micros(as_u64_us(&v["gap_us"])?),
        }),
        _ => Err(Error::Io("stream metadata window kind".into())),
    }
}

fn as_u64_us(v: &Value) -> Result<u64> {
    u64::try_from(v.as_i64().unwrap_or(-1)).map_err(|_| Error::Io("stream duration".into()))
}

fn output_schema(cfg: &StreamQueryConfig) -> Result<SchemaRef> {
    let mut fields = vec![
        Field::new(
            "window_start",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new(
            "window_end",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ];
    // Group columns keep their input names; types are filled when emitting
    // from the first event. Utf8 and Int64 are stored in GroupDim.
    for name in &cfg.group {
        fields.push(Field::new(name, DataType::Utf8, true));
    }
    fields.push(Field::new("count", DataType::Int64, false));
    if cfg.value.is_some() {
        fields.push(Field::new("sum", DataType::Int64, true));
    }
    Ok(Arc::new(Schema::new(fields)))
}

struct EmitRow {
    start: i64,
    end: i64,
    dims: Vec<GroupDim>,
    count: i64,
    sum: Option<i64>,
}

fn emit_row(start: i64, end: i64, dims: &[GroupDim], slot: &Slot) -> EmitRow {
    EmitRow {
        start,
        end,
        dims: dims.to_vec(),
        count: slot.count,
        sum: if slot.seen_sum { Some(slot.sum) } else { None },
    }
}

fn rows_to_batch(schema: SchemaRef, mut rows: Vec<EmitRow>, has_sum: bool) -> Result<Vec<Batch>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    rows.sort_by(|a, b| a.start.cmp(&b.start).then_with(|| a.dims.cmp(&b.dims)));
    let starts: Vec<i64> = rows.iter().map(|r| r.start).collect();
    let ends: Vec<i64> = rows.iter().map(|r| r.end).collect();
    let mut cols: Vec<ArrayRef> = vec![
        Arc::new(TimestampMicrosecondArray::from(starts)),
        Arc::new(TimestampMicrosecondArray::from(ends)),
    ];
    let n_group = schema.fields().len() - 2 - 1 - usize::from(has_sum);
    for g in 0..n_group {
        let values: Vec<Option<String>> = rows
            .iter()
            .map(|r| match r.dims.get(g) {
                Some(GroupDim::Utf8(s)) => Some(s.clone()),
                _ => None,
            })
            .collect();
        cols.push(Arc::new(StringArray::from(values)));
    }
    let counts: Vec<i64> = rows.iter().map(|r| r.count).collect();
    cols.push(Arc::new(Int64Array::from(counts)));
    if has_sum {
        let sums: Vec<Option<i64>> = rows.iter().map(|r| r.sum).collect();
        cols.push(Arc::new(Int64Array::from(sums)));
    }
    Ok(vec![Batch::try_new(schema, cols)?])
}

fn group_cols<'a>(batch: &'a Batch, names: &[String]) -> Result<Vec<&'a ArrayRef>> {
    names
        .iter()
        .map(|n| {
            batch
                .column_by_name(n)
                .ok_or_else(|| Error::Plan(format!("missing {n}")))
        })
        .collect()
}

fn dims_at(cols: &[&ArrayRef], row: usize) -> Result<Vec<GroupDim>> {
    cols.iter().map(|c| dim_at(c.as_ref(), row)).collect()
}

fn dim_at(array: &dyn Array, row: usize) -> Result<GroupDim> {
    if array.is_null(row) {
        return Ok(GroupDim::Null);
    }
    match array.data_type() {
        DataType::Int64 => Ok(GroupDim::Utf8(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| Error::Eval("int64 group".into()))?
                .value(row)
                .to_string(),
        )),
        DataType::Utf8 => Ok(GroupDim::Utf8(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Eval("utf8 group".into()))?
                .value(row)
                .to_string(),
        )),
        other => Err(Error::UnsupportedType(format!("stream group {other}"))),
    }
}

fn event_us(array: &dyn Array, row: usize) -> Result<Option<i64>> {
    if array.is_null(row) {
        return Ok(None);
    }
    Ok(Some(match array.data_type() {
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::Eval("int64 event time".into()))?
            .value(row),
        DataType::Timestamp(TimeUnit::Second, _) => array
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .ok_or_else(|| Error::Eval("timestamp event time".into()))?
            .value(row)
            .checked_mul(1_000_000)
            .ok_or_else(|| Error::Eval("timestamp overflow".into()))?,
        DataType::Timestamp(TimeUnit::Millisecond, _) => array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| Error::Eval("timestamp event time".into()))?
            .value(row)
            .checked_mul(1_000)
            .ok_or_else(|| Error::Eval("timestamp overflow".into()))?,
        DataType::Timestamp(TimeUnit::Microsecond, _) => array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| Error::Eval("timestamp event time".into()))?
            .value(row),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| Error::Eval("timestamp event time".into()))?
                .value(row)
                / 1_000
        }
        other => {
            return Err(Error::UnsupportedType(format!("event time {other}")));
        }
    }))
}

fn i64_at(array: &dyn Array, row: usize) -> Result<i64> {
    array
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::Eval("int64 value".into()))
        .map(|a| a.value(row))
}

fn write_offset(checkpoint: &Path, id: u64, body: &Value, replace: bool) -> Result<()> {
    let path = checkpoint.join("offsets").join(id.to_string());
    let bytes = serde_json::to_vec_pretty(body).map_err(|e| Error::Io(e.to_string()))?;
    if replace {
        write_complete(&path, &bytes)
    } else if path.exists() {
        Ok(())
    } else {
        create_exclusive(&path, bytes)
    }
}

fn write_commit(checkpoint: &Path, id: u64, body: Value) -> Result<()> {
    let path = checkpoint.join("commits").join(id.to_string());
    let bytes = serde_json::to_vec_pretty(&body).map_err(|e| Error::Io(e.to_string()))?;
    create_exclusive(&path, bytes)
}

fn write_state(checkpoint: &Path, id: u64, q: &StreamQuery) -> Result<()> {
    let path = checkpoint.join("state").join(format!("{id}.arrow"));
    let rows = match &q.state {
        WindowState::Fixed(map) => map
            .iter()
            .map(|(k, s)| state_row(k.start, k.end, &k.dims, s))
            .collect(),
        WindowState::Sessions(map) => map
            .iter()
            .flat_map(|(dims, sessions)| {
                sessions
                    .iter()
                    .map(|s| state_row(s.start, s.end, dims, &s.slot))
            })
            .collect(),
    };
    let schema = state_schema(q.cfg.group.len(), q.cfg.value.is_some());
    let batches = state_to_batch(
        schema.clone(),
        rows,
        q.cfg.group.len(),
        q.cfg.value.is_some(),
    )?;
    let (tmp, file) = create_tmp_file(&path)?;
    let result = (|| {
        let mut writer =
            FileWriter::try_new(file, schema.as_ref()).map_err(|e| Error::Io(e.to_string()))?;
        for batch in &batches {
            if batch.num_rows() > 0 {
                writer
                    .write(batch.as_arrow())
                    .map_err(|e| Error::Io(e.to_string()))?;
            }
        }
        writer.finish().map_err(|e| Error::Io(e.to_string()))?;
        publish_replace(&tmp, &path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

struct StateRow {
    start: i64,
    end: i64,
    dims: Vec<GroupDim>,
    count: i64,
    sum: i64,
    seen_sum: bool,
    max_event: i64,
}

fn state_row(start: i64, end: i64, dims: &[GroupDim], slot: &Slot) -> StateRow {
    StateRow {
        start,
        end,
        dims: dims.to_vec(),
        count: slot.count,
        sum: slot.sum,
        seen_sum: slot.seen_sum,
        max_event: slot.max_event,
    }
}

fn state_schema(n_group: usize, has_sum: bool) -> SchemaRef {
    let mut fields = vec![
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
    ];
    for i in 0..n_group {
        fields.push(Field::new(format!("g{i}"), DataType::Utf8, true));
    }
    fields.push(Field::new("count", DataType::Int64, false));
    if has_sum {
        fields.push(Field::new("sum", DataType::Int64, false));
        fields.push(Field::new("seen_sum", DataType::Int64, false));
    }
    fields.push(Field::new("max_event", DataType::Int64, false));
    Arc::new(Schema::new(fields))
}

fn state_to_batch(
    schema: SchemaRef,
    rows: Vec<StateRow>,
    n_group: usize,
    has_sum: bool,
) -> Result<Vec<Batch>> {
    if rows.is_empty() {
        return Ok(vec![Batch::empty(schema)?]);
    }
    let starts: Vec<i64> = rows.iter().map(|r| r.start).collect();
    let ends: Vec<i64> = rows.iter().map(|r| r.end).collect();
    let mut cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(starts)),
        Arc::new(Int64Array::from(ends)),
    ];
    for g in 0..n_group {
        let values: Vec<Option<String>> = rows
            .iter()
            .map(|r| match r.dims.get(g) {
                Some(GroupDim::Utf8(s)) => Some(s.clone()),
                _ => None,
            })
            .collect();
        cols.push(Arc::new(StringArray::from(values)));
    }
    cols.push(Arc::new(Int64Array::from(
        rows.iter().map(|r| r.count).collect::<Vec<_>>(),
    )));
    if has_sum {
        cols.push(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.sum).collect::<Vec<_>>(),
        )));
        cols.push(Arc::new(Int64Array::from(
            rows.iter()
                .map(|r| i64::from(r.seen_sum))
                .collect::<Vec<_>>(),
        )));
    }
    cols.push(Arc::new(Int64Array::from(
        rows.iter().map(|r| r.max_event).collect::<Vec<_>>(),
    )));
    Ok(vec![Batch::try_new(schema, cols)?])
}

fn load_state(checkpoint: &Path, id: u64, cfg: &StreamQueryConfig) -> Result<WindowState> {
    let path = checkpoint.join("state").join(format!("{id}.arrow"));
    if !path.exists() {
        return Ok(match cfg.window {
            EventWindow::Session { .. } => WindowState::Sessions(BTreeMap::new()),
            _ => WindowState::Fixed(BTreeMap::new()),
        });
    }
    let file = File::open(&path).map_err(|e| io_err(&path, e))?;
    let reader = FileReader::try_new(file, None).map_err(|e| Error::Io(e.to_string()))?;
    let n_group = cfg.group.len();
    let has_sum = cfg.value.is_some();
    let mut fixed = BTreeMap::new();
    let mut sessions: BTreeMap<Vec<GroupDim>, Vec<SessionSlot>> = BTreeMap::new();
    for item in reader {
        let batch = Batch::from_arrow(item.map_err(|e| Error::Io(e.to_string()))?)?;
        let start = col_i64(&batch, "start")?;
        let end = col_i64(&batch, "end")?;
        let count = col_i64(&batch, "count")?;
        let max_event = col_i64(&batch, "max_event")?;
        let sum = if has_sum {
            Some(col_i64(&batch, "sum")?)
        } else {
            None
        };
        let seen = if has_sum {
            Some(col_i64(&batch, "seen_sum")?)
        } else {
            None
        };
        let groups: Vec<ArrayRef> = (0..n_group).map(|i| batch.column(2 + i).clone()).collect();
        for row in 0..batch.num_rows() {
            let dims = (0..n_group)
                .map(|i| dim_at(groups[i].as_ref(), row))
                .collect::<Result<Vec<_>>>()?;
            let slot = Slot {
                count: start_val(&count, row),
                sum: sum.as_ref().map(|a| start_val(a, row)).unwrap_or(0),
                seen_sum: seen.as_ref().is_some_and(|a| start_val(a, row) != 0),
                max_event: start_val(&max_event, row),
            };
            let s = start_val(&start, row);
            let e = start_val(&end, row);
            match cfg.window {
                EventWindow::Session { .. } => {
                    sessions.entry(dims).or_default().push(SessionSlot {
                        start: s,
                        end: e,
                        slot,
                    });
                }
                _ => {
                    fixed.insert(
                        GroupKey {
                            start: s,
                            end: e,
                            dims,
                        },
                        slot,
                    );
                }
            }
        }
    }
    Ok(match cfg.window {
        EventWindow::Session { .. } => WindowState::Sessions(sessions),
        _ => WindowState::Fixed(fixed),
    })
}

fn col_i64(batch: &Batch, name: &str) -> Result<Int64Array> {
    batch
        .column_by_name(name)
        .ok_or_else(|| Error::Io(format!("state missing {name}")))?
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| Error::Io(format!("state {name} is not int64")))
}

fn start_val(a: &Int64Array, row: usize) -> i64 {
    a.value(row)
}

fn committed_ids(checkpoint: &Path) -> Result<Vec<u64>> {
    list_numeric_names(&checkpoint.join("commits"))
}

fn offset_exists(checkpoint: &Path, id: u64) -> bool {
    checkpoint.join("offsets").join(id.to_string()).exists()
}

fn commit_exists(checkpoint: &Path, id: u64) -> bool {
    checkpoint.join("commits").join(id.to_string()).exists()
}

fn read_offset(checkpoint: &Path, id: u64) -> Result<Option<Value>> {
    let path = checkpoint.join("offsets").join(id.to_string());
    if !path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(&path).map_err(|e| io_err(&path, e))?;
    serde_json::from_str(&text)
        .map(Some)
        .map_err(|e| Error::Io(format!("stream offset: {e}")))
}

fn list_numeric_names(dir: &Path) -> Result<Vec<u64>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut ids = Vec::new();
    let ents = fs::read_dir(dir).map_err(|e| io_err(dir, e))?;
    for ent in ents {
        let ent = ent.map_err(|e| io_err(dir, e))?;
        if let Ok(id) = ent.file_name().to_string_lossy().parse::<u64>() {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

fn write_schema_file(path: &Path, schema: SchemaRef) -> Result<()> {
    let (tmp, file) = create_tmp_file(path)?;
    let result = (|| {
        let mut writer =
            FileWriter::try_new(file, schema.as_ref()).map_err(|e| Error::Io(e.to_string()))?;
        writer.finish().map_err(|e| Error::Io(e.to_string()))?;
        publish_replace(&tmp, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

fn read_schema_file(path: &Path) -> Result<SchemaRef> {
    let file = File::open(path).map_err(|e| io_err(path, e))?;
    let reader = FileReader::try_new(file, None).map_err(|e| Error::Io(e.to_string()))?;
    Ok(reader.schema())
}

fn write_parquet_file(path: &Path, batches: &[Batch], schema: SchemaRef) -> Result<()> {
    mizuchi_io::write_parquet_file(path, batches, schema)
}

fn read_sink_file(path: &Path) -> Result<Vec<Batch>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    mizuchi_io::read_parquet(path, mizuchi_io::ParquetReadOptions::default())
}

fn tmp_sibling(path: &Path) -> PathBuf {
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    path.with_file_name(format!("{name}.tmp-{}", std::process::id()))
}

fn create_tmp_file(path: &Path) -> Result<(PathBuf, File)> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| io_err(parent, e))?;
    }
    let tmp = tmp_sibling(path);
    let file = File::create(&tmp).map_err(|e| io_err(&tmp, e))?;
    Ok((tmp, file))
}

fn write_tmp(path: &Path, bytes: &[u8]) -> Result<PathBuf> {
    let (tmp, mut file) = create_tmp_file(path)?;
    file.write_all(bytes).map_err(|e| io_err(&tmp, e))?;
    file.sync_all().map_err(|e| io_err(&tmp, e))?;
    Ok(tmp)
}

/// Dest appears only after `tmp` is durable. Replaces an existing dest.
fn publish_replace(tmp: &Path, dest: &Path) -> Result<()> {
    {
        let file = File::open(tmp).map_err(|e| io_err(tmp, e))?;
        file.sync_all().map_err(|e| io_err(tmp, e))?;
    }
    fs::rename(tmp, dest).map_err(|e| {
        let _ = fs::remove_file(tmp);
        io_err(dest, e)
    })
}

/// Dest appears only after the bytes are durable, so a kill cannot leave a
/// truncated offset or commit that `recover` would try to parse.
fn write_complete(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = write_tmp(path, bytes)?;
    fs::rename(&tmp, path).map_err(|e| {
        let _ = fs::remove_file(&tmp);
        io_err(path, e)
    })
}

fn create_exclusive(path: &Path, bytes: Vec<u8>) -> Result<()> {
    let tmp = write_tmp(path, &bytes)?;
    let publish = exclusive_publish(&tmp, path);
    if publish.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    publish
}

fn exclusive_publish(tmp: &Path, dest: &Path) -> Result<()> {
    match fs::hard_link(tmp, dest) {
        Ok(()) => {
            let _ = fs::remove_file(tmp);
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(Error::Io(format!(
            "concurrent writer conflict: {}",
            dest.display()
        ))),
        Err(e) => Err(io_err(dest, e)),
    }
}

fn io_err(path: &Path, err: std::io::Error) -> Error {
    Error::Io(format!("{}: {err}", path.display()))
}

#[cfg(test)]
impl StreamQuery {
    /// Write offset, sink, and state, then skip the commit marker.
    pub(crate) fn trigger_without_commit(&mut self) -> Result<StreamTrigger> {
        self.run_trigger(false)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::Session;

    static SEQ: AtomicU64 = AtomicU64::new(1);

    fn tmp(name: &str) -> PathBuf {
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let p =
            std::env::temp_dir().join(format!("mizuchi-stream-{name}-{n}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, true),
            Field::new("k", DataType::Utf8, true),
            Field::new("v", DataType::Int64, true),
        ]))
    }

    fn batch(ts: &[i64], k: &[&str], v: &[i64]) -> Batch {
        Batch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(ts.to_vec())),
                Arc::new(StringArray::from(
                    k.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(v.to_vec())),
            ],
        )
        .unwrap()
    }

    fn cfg(
        root: &Path,
        window: EventWindow,
        output: StreamOutput,
        delay: Duration,
    ) -> StreamQueryConfig {
        StreamQueryConfig {
            checkpoint: root.join("chk"),
            sink: root.join("sink"),
            event_time: "ts".into(),
            delay,
            window,
            group: vec!["k".into()],
            value: Some("v".into()),
            output,
        }
    }

    fn rows_of(batches: &[Batch]) -> Vec<(i64, i64, String, i64, i64)> {
        let mut out = Vec::new();
        for b in batches {
            let s = b
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let e = b
                .column(1)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let k = b.column(2).as_any().downcast_ref::<StringArray>().unwrap();
            let c = b.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
            let v = b.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                out.push((
                    s.value(i),
                    e.value(i),
                    k.value(i).to_string(),
                    c.value(i),
                    v.value(i),
                ));
            }
        }
        out.sort();
        out
    }

    fn batch_equiv(ts: &[i64], k: &[&str], v: &[i64], size: i64) -> Vec<(i64, String, i64, i64)> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("wstart", DataType::Int64, true),
            Field::new("k", DataType::Utf8, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let mut wstarts = Vec::new();
        let mut keys = Vec::new();
        let mut vals = Vec::new();
        for i in 0..ts.len() {
            wstarts.push(ts[i].div_euclid(size) * size);
            keys.push(Some(k[i].to_string()));
            vals.push(v[i]);
        }
        let b = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(wstarts)),
                Arc::new(StringArray::from(keys)),
                Arc::new(Int64Array::from(vals)),
            ],
        )
        .unwrap();
        let mut session = Session::new();
        session.register("t", schema, vec![b]).unwrap();
        let got = session
            .execute_sql("SELECT wstart, k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY wstart, k")
            .unwrap();
        let mut out = Vec::new();
        for batch in &got {
            let w = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let key = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let c = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let s = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                out.push((w.value(i), key.value(i).to_string(), c.value(i), s.value(i)));
            }
        }
        out.sort();
        out
    }

    #[test]
    fn int64_group_survives_checkpoint_round_trip() {
        let root = tmp("i64-group");
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, true),
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let mk = |ts: i64, k: i64, v: i64| {
            Batch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![ts])),
                    Arc::new(Int64Array::from(vec![k])),
                    Arc::new(Int64Array::from(vec![v])),
                ],
            )
            .unwrap()
        };
        let mut q = StreamQuery::try_new(
            schema.clone(),
            StreamQueryConfig {
                checkpoint: root.join("chk"),
                sink: root.join("sink"),
                event_time: "ts".into(),
                delay: Duration::from_secs(0),
                window: EventWindow::Tumbling {
                    size: Duration::from_secs(10),
                },
                group: vec!["k".into()],
                value: Some("v".into()),
                output: StreamOutput::Complete,
            },
        )
        .unwrap();
        q.ingest(mk(1_000_000, 7, 1)).unwrap();
        q.trigger().unwrap();
        drop(q);
        let mut q = StreamQuery::open(root.join("chk")).unwrap();
        q.ingest(mk(2_000_000, 7, 3)).unwrap();
        q.trigger().unwrap();
        let got = rows_of(&q.committed_output().unwrap());
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(got[0].2, "7");
        assert_eq!(got[0].3, 2);
        assert_eq!(got[0].4, 4);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn session_stream_query_writes_checkpoint() {
        let root = tmp("session-api");
        let session = Session::new();
        let mut q = session
            .stream_query(
                schema(),
                cfg(
                    &root,
                    EventWindow::Tumbling {
                        size: Duration::from_secs(10),
                    },
                    StreamOutput::Complete,
                    Duration::from_secs(0),
                ),
            )
            .unwrap();
        q.ingest(batch(&[1_000_000], &["a"], &[1])).unwrap();
        q.trigger().unwrap();
        assert_eq!(rows_of(&q.committed_output().unwrap()).len(), 1);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn governed_session_refuses_stream_query() {
        let root = tmp("governed-stream");
        let session = Session::new().with_governed(true);
        let err = match session.stream_query(
            schema(),
            cfg(
                &root,
                EventWindow::Tumbling {
                    size: Duration::from_secs(10),
                },
                StreamOutput::Complete,
                Duration::from_secs(0),
            ),
        ) {
            Ok(_) => panic!("governed stream_query should fail"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("destination table"), "{err}");
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn tumbling_complete_matches_batch_groupby() {
        let root = tmp("tumbling");
        let size = Duration::from_secs(10);
        let mut q = StreamQuery::try_new(
            schema(),
            cfg(
                &root,
                EventWindow::Tumbling { size },
                StreamOutput::Complete,
                Duration::from_secs(0),
            ),
        )
        .unwrap();
        let ts = [1_000_000, 2_000_000, 11_000_000, 12_000_000, 21_000_000];
        let k = ["a", "a", "a", "b", "a"];
        let v = [1, 2, 3, 4, 5];
        q.ingest(batch(&ts, &k, &v)).unwrap();
        q.trigger().unwrap();
        let got: Vec<(i64, String, i64, i64)> = rows_of(&q.committed_output().unwrap())
            .into_iter()
            .map(|(start, _, key, c, s)| (start, key, c, s))
            .collect();
        let want = batch_equiv(&ts, &k, &v, 10_000_000);
        assert_eq!(got, want);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn sliding_windows_cover_each_row() {
        let root = tmp("sliding");
        let mut q = StreamQuery::try_new(
            schema(),
            cfg(
                &root,
                EventWindow::Sliding {
                    size: Duration::from_secs(10),
                    slide: Duration::from_secs(5),
                },
                StreamOutput::Complete,
                Duration::from_secs(0),
            ),
        )
        .unwrap();
        q.ingest(batch(&[6_000_000], &["a"], &[1])).unwrap();
        q.trigger().unwrap();
        let got = rows_of(&q.committed_output().unwrap());
        assert_eq!(got.len(), 2, "{got:?}");
        assert_eq!(got[0].0, 0);
        assert_eq!(got[0].1, 10_000_000);
        assert_eq!(got[1].0, 5_000_000);
        assert_eq!(got[1].1, 15_000_000);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn session_merges_within_gap() {
        let root = tmp("session");
        let mut q = StreamQuery::try_new(
            schema(),
            cfg(
                &root,
                EventWindow::Session {
                    gap: Duration::from_secs(10),
                },
                StreamOutput::Complete,
                Duration::from_secs(0),
            ),
        )
        .unwrap();
        q.ingest(batch(
            &[0, 3_000_000, 20_000_000],
            &["a", "a", "a"],
            &[1, 1, 1],
        ))
        .unwrap();
        q.trigger().unwrap();
        let got = rows_of(&q.committed_output().unwrap());
        assert_eq!(got.len(), 2, "{got:?}");
        assert_eq!(got[0].0, 0);
        assert_eq!(got[0].1, 13_000_000);
        assert_eq!(got[0].3, 2);
        assert_eq!(got[1].0, 20_000_000);
        assert_eq!(got[1].3, 1);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn out_of_order_rows_before_watermark_are_dropped() {
        let root = tmp("late");
        let mut q = StreamQuery::try_new(
            schema(),
            cfg(
                &root,
                EventWindow::Tumbling {
                    size: Duration::from_secs(10),
                },
                StreamOutput::Complete,
                Duration::from_secs(10),
            ),
        )
        .unwrap();
        q.ingest(batch(&[100_000_000, 105_000_000], &["a", "a"], &[1, 1]))
            .unwrap();
        q.trigger().unwrap();
        assert_eq!(q.watermark_us(), 95_000_000);
        q.ingest(batch(&[90_000_000, 200_000_000], &["a", "a"], &[9, 1]))
            .unwrap();
        q.trigger().unwrap();
        let got = rows_of(&q.committed_output().unwrap());
        let w100 = got.iter().find(|r| r.0 == 100_000_000).unwrap();
        assert_eq!(
            w100.3, 2,
            "late 90s row must not join the 100s window: {got:?}"
        );
        let w200 = got.iter().find(|r| r.0 == 200_000_000).unwrap();
        assert_eq!(w200.3, 1);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn kill_and_restart_sink_is_exactly_once() {
        let root = tmp("eos");
        let window = EventWindow::Tumbling {
            size: Duration::from_secs(10),
        };
        let mut q = StreamQuery::try_new(
            schema(),
            cfg(
                &root,
                window,
                StreamOutput::Complete,
                Duration::from_secs(0),
            ),
        )
        .unwrap();
        q.ingest(batch(&[1_000_000], &["a"], &[1])).unwrap();
        q.trigger().unwrap();
        q.ingest(batch(&[11_000_000], &["a"], &[2])).unwrap();
        q.trigger_without_commit().unwrap();
        drop(q);

        let mut q = StreamQuery::open(root.join("chk")).unwrap();
        q.ingest(batch(&[11_000_000], &["a"], &[2])).unwrap();
        q.trigger().unwrap();
        let got = rows_of(&q.committed_output().unwrap());
        assert_eq!(got.len(), 2, "{got:?}");
        assert_eq!(got[0].3, 1);
        assert_eq!(got[0].4, 1);
        assert_eq!(got[1].3, 1);
        assert_eq!(got[1].4, 2);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn append_emits_windows_behind_watermark() {
        let root = tmp("append");
        let mut q = StreamQuery::try_new(
            schema(),
            cfg(
                &root,
                EventWindow::Tumbling {
                    size: Duration::from_secs(10),
                },
                StreamOutput::Append,
                Duration::from_secs(0),
            ),
        )
        .unwrap();
        q.ingest(batch(&[1_000_000], &["a"], &[1])).unwrap();
        q.trigger().unwrap();
        assert!(q.committed_output().unwrap().is_empty());
        q.ingest(batch(&[11_000_000], &["a"], &[2])).unwrap();
        q.trigger().unwrap();
        let got = rows_of(&q.committed_output().unwrap());
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(got[0].0, 0);
        assert_eq!(got[0].1, 10_000_000);
        assert_eq!(got[0].4, 1);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn cancelled_trigger_leaves_committed_sink_unchanged() {
        let root = tmp("cancel");
        let cancel = CancelToken::new();
        let mut q = StreamQuery::open_or_new(
            schema(),
            cfg(
                &root,
                EventWindow::Tumbling {
                    size: Duration::from_secs(10),
                },
                StreamOutput::Complete,
                Duration::from_secs(0),
            ),
            cancel.clone(),
        )
        .unwrap();
        q.ingest(batch(&[1_000_000], &["a"], &[1])).unwrap();
        q.trigger().unwrap();
        q.ingest(batch(&[11_000_000], &["a"], &[2])).unwrap();
        cancel.cancel();
        let err = q.trigger().unwrap_err();
        assert!(matches!(err, Error::Cancelled), "{err:?}");
        drop(q);
        let q = StreamQuery::open(root.join("chk")).unwrap();
        let got = rows_of(&q.committed_output().unwrap());
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].4, 1);
        let _ = fs::remove_dir_all(&root);
    }
}
