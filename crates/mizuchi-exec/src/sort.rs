//! Sort, top-k, and limit over Arrow batches.
//!
//! A limit without a sort walks the child and slices rows in input order. A
//! sort materializes the child, evaluates the keys, and orders them with Arrow
//! lexicographic sort. LIMIT after ORDER BY asks that sort for only the first
//! `skip + fetch` keys, then drops the offset. Under a memory limit the sort
//! spills sorted runs and streams a k-way merge of them, holding one reserved
//! chunk per run.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{Array, RecordBatch, UInt32Array};
use arrow::compute::{
    SortColumn, SortOptions, concat_batches, interleave_record_batch, lexsort_to_indices, take,
};
use arrow::datatypes::SchemaRef;
use arrow::row::{RowConverter, Rows, SortField};

use super::CancelToken;
use super::memory::{
    self, MemoryPool, OwnedReservation, Reserver, SpillIter, SpillReader, batch_bytes,
};
use super::{Exec, Lowering, Operator};
use mizuchi_core::batch::{Batch, BatchPolicy};
use mizuchi_core::error::{Error, Result};
use mizuchi_plan::SortExpr;

/// Sorted rows: already in memory, or a merge that streams them from
/// spilled runs.
pub(super) enum Sorted {
    Batches(Vec<Batch>),
    Stream(Box<dyn Operator>),
}

impl Sorted {
    /// Operator over the sorted rows built with `lowering`.
    pub(super) fn lower(
        self,
        lowering: &dyn Lowering,
        cancel: &CancelToken,
    ) -> Result<Box<dyn Operator>> {
        match self {
            Self::Batches(batches) => lowering.scan(batches),
            Self::Stream(op) => lowering.scan_stream(op, cancel),
        }
    }
}

/// [`sorted`], collected. For callers that need every sorted row at once.
#[allow(clippy::too_many_arguments)]
pub(super) fn ordered(
    child: Box<dyn Operator>,
    schema: SchemaRef,
    order: &[SortExpr],
    skip: u64,
    fetch: Option<u64>,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    match sorted(child, schema, order, skip, fetch, policy, pool, cancel)? {
        Sorted::Batches(batches) => Ok(batches),
        Sorted::Stream(op) => super::collect(op, cancel),
    }
}

/// Sort `child` by `order`, then apply `skip` / `fetch` (top-k when `fetch` is set).
#[allow(clippy::too_many_arguments)]
pub(super) fn sorted(
    mut child: Box<dyn Operator>,
    schema: SchemaRef,
    order: &[SortExpr],
    skip: u64,
    fetch: Option<u64>,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Sorted> {
    if matches!(fetch, Some(0)) {
        return Ok(Sorted::Batches(Vec::new()));
    }
    if !pool.has_limit() {
        return sort_concat(&mut child, schema, order, skip, fetch, policy, pool, cancel)
            .map(Sorted::Batches);
    }
    if let Some(fetch) = fetch
        && skip.saturating_add(fetch) <= TOP_K_MAX_ROWS
    {
        return top_k(child, schema, order, skip, fetch, policy, pool, cancel);
    }
    external_sort(child, schema, order, skip, fetch, policy, pool, cancel)
}

/// Most runs one merge reads at once. More runs are merged in passes.
const MERGE_FAN_IN: usize = 8;

/// Spilled runs are cut into chunks of `limit / CHUNKS_PER_LIMIT` bytes. A
/// merge holds one chunk plus its encoded keys per run, so a full fan-in
/// reserves at most about half the limit; the merge's output batch is the
/// same size again and is not reserved once it is handed downstream.
const CHUNKS_PER_LIMIT: u64 = 32;

/// Memory-limited sort: reserve each batch, spill sorted runs when the next
/// batch does not fit, then stream a k-way merge of the runs.
#[allow(clippy::too_many_arguments)]
fn external_sort(
    mut child: Box<dyn Operator>,
    schema: SchemaRef,
    order: &[SortExpr],
    skip: u64,
    fetch: Option<u64>,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Sorted> {
    let chunk_bytes = (pool.limit().unwrap_or(u64::MAX) / CHUNKS_PER_LIMIT).max(1);
    let mut run: Vec<Batch> = Vec::new();
    let mut held: Vec<memory::Reservation<'_>> = Vec::new();
    let mut spills: VecDeque<RunReader> = VecDeque::new();
    // Widest row seen in any run, which sizes the merge's output batches.
    let mut row_bytes = 1;
    while let Some(batch) = child.next_batch(cancel)? {
        if batch.num_rows() == 0 {
            continue;
        }
        let sz = batch_bytes(&batch);
        match pool.try_reserve(sz) {
            Ok(r) => {
                run.push(batch);
                held.push(r);
            }
            Err(e) => {
                if !pool.can_spill() {
                    return Err(e);
                }
                if run.is_empty() {
                    return Err(Error::Memory(
                        "a single sort batch exceeds the memory limit".into(),
                    ));
                }
                let (spilled, width) =
                    spill_sorted_run(&run, schema.clone(), order, chunk_bytes, pool, cancel)?;
                spills.push_back(RunReader::new(spilled));
                row_bytes = row_bytes.max(width);
                held.clear();
                run.clear();
                let r = pool.try_reserve(sz).map_err(|_| {
                    Error::Memory("a single sort batch exceeds the memory limit".into())
                })?;
                run.push(batch);
                held.push(r);
            }
        }
    }
    if spills.is_empty() {
        drop(held);
        let mut concat = batches_to_exec(run, schema.clone());
        return sort_concat(
            &mut concat,
            schema,
            order,
            skip,
            fetch,
            policy,
            pool,
            cancel,
        )
        .map(Sorted::Batches);
    }
    if !run.is_empty() {
        let (spilled, width) =
            spill_sorted_run(&run, schema.clone(), order, chunk_bytes, pool, cancel)?;
        spills.push_back(RunReader::new(spilled));
        row_bytes = row_bytes.max(width);
        held.clear();
        run.clear();
    }
    let chunk_rows = usize::try_from(chunk_bytes / row_bytes)
        .unwrap_or(usize::MAX)
        .max(1);
    // Merge as many runs as fit into one, until the last merge takes them
    // all; that one streams out.
    loop {
        let mut merge = Merge::new(schema.clone(), order, chunk_rows, pool)?;
        merge.take_runs(&mut spills)?;
        if spills.is_empty() {
            merge.out_rows = policy.max_rows().map_or(chunk_rows, |m| m.min(chunk_rows));
            merge.skip = usize::try_from(skip).unwrap_or(usize::MAX);
            merge.fetch = fetch.map(|f| usize::try_from(f).unwrap_or(usize::MAX));
            return Ok(Sorted::Stream(Box::new(merge)));
        }
        let mut merged = Run::new();
        while let Some(batch) = merge.next_batch(cancel)? {
            merged.push_back(pool.write_spill_file(schema.clone(), &[batch])?);
        }
        spills.push_back(RunReader::new(merged));
    }
}

/// Largest `skip + fetch` a memory-limited sort keeps as a running top-k.
const TOP_K_MAX_ROWS: u64 = 65_536;

/// Memory-limited ORDER BY ... LIMIT: fold each input batch into the best
/// `skip + fetch` rows seen so far. The kept rows are copied out of their
/// input, so the reservation covers exactly what is retained and an input
/// batch (often a slice of a much larger parent) is charged only while it is
/// merged. When a reservation fails and spilling is available, the kept rows
/// and the rest of the input go to [`external_sort`]; without spilling the
/// pool error is returned, as the external sort would.
#[allow(clippy::too_many_arguments)]
fn top_k(
    mut child: Box<dyn Operator>,
    schema: SchemaRef,
    order: &[SortExpr],
    skip: u64,
    fetch: u64,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Sorted> {
    let k = skip.saturating_add(fetch);
    let unlimited = BatchPolicy::unlimited();
    let mut kept: Option<Batch> = None;
    let mut kept_held: Option<memory::Reservation<'_>> = None;
    // Batches still owned by this sort when a reservation fails.
    let pending: Vec<Batch> = loop {
        let Some(batch) = child.next_batch(cancel)? else {
            return match kept {
                Some(kept) => sort_batch(&kept, order, skip, Some(fetch), policy, pool),
                None => empty_out(schema, policy),
            }
            .map(Sorted::Batches);
        };
        if batch.num_rows() == 0 {
            continue;
        }
        let incoming = match pool.try_reserve(batch_bytes(&batch)) {
            Ok(r) => r,
            Err(e) if !pool.can_spill() => return Err(e),
            Err(_) => break kept.take().into_iter().chain([batch]).collect(),
        };
        let mut candidates = batches_to_exec(
            kept.take().into_iter().chain([batch]).collect(),
            schema.clone(),
        );
        let best = sort_concat(
            &mut *candidates,
            schema.clone(),
            order,
            0,
            Some(k),
            &unlimited,
            pool,
            cancel,
        )?;
        let best = compact(best, schema.clone(), pool)?;
        drop(candidates);
        // The consumed input and the old kept rows are released before the
        // new kept rows are reserved: the charge moves, it is never doubled.
        drop(incoming);
        drop(kept_held.take());
        match pool.try_reserve(batch_bytes(&best)) {
            Ok(r) => kept_held = Some(r),
            Err(e) if !pool.can_spill() => return Err(e),
            Err(_) => break vec![best],
        }
        kept = Some(best);
    };
    drop(kept_held);
    let rest = Box::new(Chain {
        front: pending.into_iter(),
        rest: child,
    });
    external_sort(rest, schema, order, skip, Some(fetch), policy, pool, cancel)
}

/// Yields `front`, then every batch of `rest`.
struct Chain {
    front: std::vec::IntoIter<Batch>,
    rest: Box<dyn Operator>,
}

impl Operator for Chain {
    fn next_batch(&mut self, cancel: &CancelToken) -> Result<Option<Batch>> {
        match self.front.next() {
            Some(batch) => Ok(Some(batch)),
            None => self.rest.next_batch(cancel),
        }
    }
}

/// One freshly allocated batch holding `batches`' rows, so nothing it keeps
/// shares a buffer with a larger parent.
fn compact(batches: Vec<Batch>, schema: SchemaRef, pool: &MemoryPool) -> Result<Batch> {
    let arrows: Vec<RecordBatch> = batches.into_iter().map(Batch::into_arrow).collect();
    let joined = Batch::from_arrow(
        concat_batches(&schema, &arrows).map_err(|e| Error::Eval(e.to_string()))?,
    )?;
    let n = u32::try_from(joined.num_rows())
        .map_err(|_| Error::Eval("top-k exceeds u32 row indices".into()))?;
    take_all_columns(&joined, &UInt32Array::from_iter_values(0..n), pool)
}

#[allow(clippy::too_many_arguments)]
fn sort_concat(
    child: &mut dyn Operator,
    schema: SchemaRef,
    order: &[SortExpr],
    skip: u64,
    fetch: Option<u64>,
    policy: &BatchPolicy,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Vec<Batch>> {
    let batch = concat_exec(child, schema.clone(), pool, cancel)?;
    sort_batch(&batch, order, skip, fetch, policy, pool)
}

fn sort_batch(
    batch: &Batch,
    order: &[SortExpr],
    skip: u64,
    fetch: Option<u64>,
    policy: &BatchPolicy,
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    let n = batch.num_rows();
    let skip = clamp_rows(skip, n);
    if skip >= n || n == 0 {
        return empty_out(batch.schema(), policy);
    }
    if n > u32::MAX as usize {
        return Err(Error::Eval("sort input exceeds u32 row indices".into()));
    }
    let mut columns = Vec::with_capacity(order.len());
    for key in order {
        let values = key.expr.eval(batch)?;
        columns.push(SortColumn {
            values,
            options: Some(sort_options(key)),
        });
    }
    let k = fetch.map(|f| skip.saturating_add(clamp_rows(f, n)).min(n));
    let indices = lexsort_to_indices(&columns, k).map_err(|e| Error::Eval(e.to_string()))?;
    let start = skip.min(indices.len());
    let len = indices.len() - start;
    take_rows(batch, &indices.slice(start, len), policy, pool)
}

/// Sort `run` and spill it as one file of chunks of about `chunk_bytes`.
/// Returns the run and its bytes per row.
fn spill_sorted_run(
    run: &[Batch],
    schema: SchemaRef,
    order: &[SortExpr],
    chunk_bytes: u64,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<(Run, u64)> {
    let mut child = batches_to_exec(run.to_vec(), schema.clone());
    let sorted = sort_concat(
        &mut child,
        schema.clone(),
        order,
        0,
        None,
        &BatchPolicy::unlimited(),
        pool,
        cancel,
    )?;
    let rows: u64 = sorted.iter().map(|b| b.num_rows() as u64).sum();
    let bytes: u64 = sorted.iter().map(batch_bytes).sum();
    let row_bytes = bytes.div_ceil(rows.max(1)).max(1);
    let chunk_rows = usize::try_from(chunk_bytes / row_bytes)
        .unwrap_or(usize::MAX)
        .max(1);
    let chunk = BatchPolicy::new(chunk_rows)?;
    let chunks: Vec<Batch> = sorted
        .iter()
        .flat_map(|b| b.split_by_policy(&chunk))
        .collect();
    let path = pool.write_spill_file(schema, &chunks)?;
    Ok((Run::from([path]), row_bytes))
}

fn batches_to_exec(batches: Vec<Batch>, _schema: SchemaRef) -> Box<dyn Operator> {
    Box::new(Exec::Scan { batches, index: 0 })
}

/// Sorted run on disk: files read in order, each a sequence of sorted chunks.
type Run = VecDeque<PathBuf>;

/// Reads a [`Run`] one chunk at a time. Each file is removed once read; the
/// rest are removed on drop, so a cancelled or failed merge leaves nothing.
struct RunReader {
    files: Run,
    current: Option<SpillIter>,
    /// Chunk read but not yet merged because the pool had no room for it.
    pending: Option<Batch>,
}

impl RunReader {
    fn new(files: Run) -> Self {
        Self {
            files,
            current: None,
            pending: None,
        }
    }

    fn next_chunk(&mut self, reader: &SpillReader) -> Result<Option<Batch>> {
        if let Some(batch) = self.pending.take() {
            return Ok(Some(batch));
        }
        loop {
            if let Some(iter) = &mut self.current {
                match iter.next() {
                    Some(batch) => return batch.map(Some),
                    None => {
                        self.current = None;
                        if let Some(done) = self.files.pop_front() {
                            let _ = std::fs::remove_file(done);
                        }
                    }
                }
            }
            let Some(path) = self.files.front() else {
                return Ok(None);
            };
            self.current = Some(reader.open(path)?);
        }
    }
}

impl Drop for RunReader {
    fn drop(&mut self) {
        self.current = None;
        for path in self.files.drain(..) {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// The chunk a run is currently merging from, with its encoded sort keys and
/// the reservation that covers both.
struct Head {
    batch: Batch,
    keys: Rows,
    pos: usize,
    _held: OwnedReservation,
}

struct Cursor {
    run: RunReader,
    head: Head,
}

/// Outcome of moving a run to its next chunk.
enum Start {
    Ready(Cursor),
    Spent,
    /// The chunk did not fit in the pool; it waits in the returned run.
    NoRoom(RunReader, Error),
}

/// Streaming k-way merge of sorted runs. Holds one chunk per run, reserved
/// in the pool, and yields batches of at most `out_rows` rows after dropping
/// `skip` rows and stopping after `fetch`.
struct Merge {
    cursors: Vec<Cursor>,
    order: Vec<SortExpr>,
    converter: RowConverter,
    reserver: Reserver,
    reader: SpillReader,
    /// The pool's copied-bytes counter.
    copied: Arc<AtomicU64>,
    out_rows: usize,
    skip: usize,
    fetch: Option<usize>,
}

impl Merge {
    /// Empty merge yielding batches of `out_rows`; [`Self::take_runs`] adds runs.
    fn new(
        schema: SchemaRef,
        order: &[SortExpr],
        out_rows: usize,
        pool: &MemoryPool,
    ) -> Result<Self> {
        let empty = Batch::empty(schema)?;
        let mut fields = Vec::with_capacity(order.len());
        for key in order {
            fields.push(SortField::new_with_options(
                key.expr.eval(&empty)?.data_type().clone(),
                sort_options(key),
            ));
        }
        Ok(Self {
            cursors: Vec::new(),
            order: order.to_vec(),
            converter: RowConverter::new(fields).map_err(|e| Error::Eval(e.to_string()))?,
            reserver: pool.reserver(),
            reader: pool.spill_reader(),
            copied: pool.copied_handle(),
            out_rows: out_rows.max(1),
            skip: 0,
            fetch: None,
        })
    }

    /// Moves runs from the front of `runs` into the merge while their first
    /// chunks fit in the pool, up to [`MERGE_FAN_IN`]. Fails closed when
    /// fewer than two fit and runs are left over, since no merge can then
    /// make progress.
    fn take_runs(&mut self, runs: &mut VecDeque<RunReader>) -> Result<()> {
        while self.cursors.len() < MERGE_FAN_IN
            && let Some(run) = runs.pop_front()
        {
            match self.start(run)? {
                Start::Ready(cursor) => self.cursors.push(cursor),
                Start::Spent => {}
                Start::NoRoom(_, e) if self.cursors.len() < 2 => return Err(e),
                Start::NoRoom(run, _) => {
                    runs.push_front(run);
                    break;
                }
            }
        }
        Ok(())
    }

    /// Cursor at the next chunk of `run`, with the chunk reserved.
    fn start(&self, mut run: RunReader) -> Result<Start> {
        let Some(batch) = run.next_chunk(&self.reader)? else {
            return Ok(Start::Spent);
        };
        // A decoded chunk's columns all share one IPC body buffer, which
        // `batch_bytes` would count once per column; a copy is charged true.
        let rows = u32::try_from(batch.num_rows())
            .map_err(|_| Error::Eval("sort chunk exceeds u32 row indices".into()))?;
        let batch = take_columns(&batch, &UInt32Array::from_iter_values(0..rows))?;
        self.copied
            .fetch_add(batch_bytes(&batch), Ordering::Relaxed);
        let mut columns = Vec::with_capacity(self.order.len());
        for key in &self.order {
            columns.push(key.expr.eval(&batch)?);
        }
        let keys = self
            .converter
            .convert_columns(&columns)
            .map_err(|e| Error::Eval(e.to_string()))?;
        match self
            .reserver
            .try_reserve(batch_bytes(&batch) + keys.size() as u64)
        {
            Ok(held) => Ok(Start::Ready(Cursor {
                run,
                head: Head {
                    batch,
                    keys,
                    pos: 0,
                    _held: held,
                },
            })),
            Err(e) => {
                run.pending = Some(batch);
                Ok(Start::NoRoom(run, e))
            }
        }
    }

    /// Next merged batch before skip / fetch. It ends early when a run's
    /// chunk runs out, because the chunk is replaced before the next pick.
    fn merge_some(&mut self) -> Result<Option<Batch>> {
        if self.cursors.is_empty() {
            return Ok(None);
        }
        let mut picks: Vec<(usize, usize)> = Vec::new();
        loop {
            let mut best = 0;
            for i in 1..self.cursors.len() {
                let (a, b) = (&self.cursors[i].head, &self.cursors[best].head);
                if a.keys.row(a.pos) < b.keys.row(b.pos) {
                    best = i;
                }
            }
            let head = &mut self.cursors[best].head;
            picks.push((best, head.pos));
            head.pos += 1;
            if head.pos == head.batch.num_rows() || picks.len() == self.out_rows {
                break;
            }
        }
        let sources: Vec<&RecordBatch> = self
            .cursors
            .iter()
            .map(|c| c.head.batch.as_arrow())
            .collect();
        let out = Batch::from_arrow(
            interleave_record_batch(&sources, &picks).map_err(|e| Error::Eval(e.to_string()))?,
        )?;
        let mut i = self.cursors.len();
        while i > 0 {
            i -= 1;
            let head = &self.cursors[i].head;
            if head.pos < head.batch.num_rows() {
                continue;
            }
            // The spent chunk and its reservation go before the next is read.
            let Cursor { run, head } = self.cursors.remove(i);
            drop(head);
            match self.start(run)? {
                Start::Ready(next) => self.cursors.insert(i, next),
                Start::Spent => {}
                Start::NoRoom(_, e) => return Err(e),
            }
        }
        Ok(Some(out))
    }
}

impl Operator for Merge {
    fn next_batch(&mut self, cancel: &CancelToken) -> Result<Option<Batch>> {
        loop {
            cancel.check()?;
            if self.fetch == Some(0) {
                // Done early: release the chunks and remove the run files.
                self.cursors.clear();
                return Ok(None);
            }
            let Some(mut batch) = self.merge_some()? else {
                return Ok(None);
            };
            let n = batch.num_rows();
            if self.skip >= n {
                self.skip -= n;
                continue;
            }
            let take_n = self.fetch.map_or(n - self.skip, |f| f.min(n - self.skip));
            if self.skip > 0 || take_n < n {
                batch = batch.slice(self.skip, take_n)?;
            }
            self.skip = 0;
            if let Some(f) = &mut self.fetch {
                *f -= take_n;
            }
            return Ok(Some(batch));
        }
    }
}

fn sort_options(key: &SortExpr) -> SortOptions {
    SortOptions {
        descending: !key.asc,
        nulls_first: key.nulls_first,
    }
}

fn concat_exec(
    exec: &mut dyn Operator,
    schema: SchemaRef,
    pool: &MemoryPool,
    cancel: &CancelToken,
) -> Result<Batch> {
    let mut batches = Vec::new();
    while let Some(batch) = exec.next_batch(cancel)? {
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }
    match batches.len() {
        0 => Batch::empty(schema),
        1 => Ok(batches.pop().expect("len is 1")),
        _ => {
            let arrows: Vec<RecordBatch> = batches.into_iter().map(Batch::into_arrow).collect();
            let inner = concat_batches(&schema, &arrows).map_err(|e| Error::Eval(e.to_string()))?;
            let out = Batch::from_arrow(inner)?;
            pool.record_copy(batch_bytes(&out));
            Ok(out)
        }
    }
}

fn take_rows(
    batch: &Batch,
    indices: &dyn Array,
    policy: &BatchPolicy,
    pool: &MemoryPool,
) -> Result<Vec<Batch>> {
    if indices.is_empty() {
        return empty_out(batch.schema(), policy);
    }
    let out = if let Some(idx) = indices.as_any().downcast_ref::<UInt32Array>() {
        if idx.null_count() == 0 {
            let values = idx.values();
            let gathered = batch.gather(values)?;
            if mizuchi_core::batch::contiguous_indices(values).is_none() {
                pool.record_copy(batch_bytes(&gathered));
            }
            gathered
        } else {
            take_all_columns(batch, indices, pool)?
        }
    } else {
        take_all_columns(batch, indices, pool)?
    };
    Ok(out
        .split_by_policy(policy)
        .into_iter()
        .filter(|b| b.num_rows() > 0)
        .collect())
}

fn take_all_columns(batch: &Batch, indices: &dyn Array, pool: &MemoryPool) -> Result<Batch> {
    let out = take_columns(batch, indices)?;
    pool.record_copy(batch_bytes(&out));
    Ok(out)
}

fn take_columns(batch: &Batch, indices: &dyn Array) -> Result<Batch> {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for i in 0..batch.num_columns() {
        columns.push(
            take(batch.column(i).as_ref(), indices, None)
                .map_err(|e| Error::Eval(e.to_string()))?,
        );
    }
    Batch::try_new(batch.schema(), columns)
}

fn empty_out(schema: SchemaRef, policy: &BatchPolicy) -> Result<Vec<Batch>> {
    Ok(Batch::empty(schema)?
        .split_by_policy(policy)
        .into_iter()
        .filter(|b| b.num_rows() > 0)
        .collect())
}

fn clamp_rows(n: u64, len: usize) -> usize {
    (n as usize).min(len)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{AsArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use mizuchi_core::expr::Expr;

    use super::*;

    #[test]
    fn limited_top_k_charges_kept_rows_not_each_slice_parent() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let parent = Batch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(0..65_536))],
        )
        .unwrap();
        // Sixteen zero-copy slices, each charged for the whole parent.
        let slices = parent.split_by_policy(&BatchPolicy::default());
        assert_eq!(slices.len(), 16);
        let cap = 3 * batch_bytes(&parent);
        let pool = MemoryPool::new(Some(cap), None).unwrap();
        let order = [SortExpr {
            expr: Expr::nth(0),
            asc: false,
            nulls_first: false,
        }];
        let out = ordered(
            batches_to_exec(slices, schema.clone()),
            schema,
            &order,
            2,
            Some(3),
            &BatchPolicy::default(),
            &pool,
            &CancelToken::new(),
        )
        .unwrap();
        let values: Vec<i64> = out
            .iter()
            .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
            .collect();
        assert_eq!(values, [65_533, 65_532, 65_531]);
        // One slice in flight plus the kept rows, never all sixteen parents.
        assert!(pool.peak() <= cap, "peak {} over cap {cap}", pool.peak());
    }

    /// Eight separately allocated 4,096-row batches of `v` in ascending order.
    fn wide_batches(schema: &SchemaRef) -> Vec<Batch> {
        (0..8)
            .map(|i| {
                Batch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int64Array::from_iter_values(
                        i * 4096..(i + 1) * 4096,
                    ))],
                )
                .unwrap()
            })
            .collect()
    }

    fn top_desc(
        batches: Vec<Batch>,
        schema: SchemaRef,
        fetch: u64,
        pool: &MemoryPool,
    ) -> Result<Vec<i64>> {
        let order = [SortExpr {
            expr: Expr::nth(0),
            asc: false,
            nulls_first: false,
        }];
        let out = ordered(
            batches_to_exec(batches, schema.clone()),
            schema,
            &order,
            0,
            Some(fetch),
            &BatchPolicy::default(),
            pool,
            &CancelToken::new(),
        )?;
        Ok(out
            .iter()
            .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
            .collect())
    }

    #[test]
    fn limited_top_k_falls_back_to_spilling_when_kept_rows_and_input_overflow() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batches = wide_batches(&schema);
        // 6,000 kept rows plus one more batch do not fit; two batches do.
        let cap = batch_bytes(&batches[0]) * 22 / 10;
        let root = std::env::temp_dir().join(format!(
            "mizuchi-topk-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let pool = MemoryPool::new(Some(cap), Some(root.clone())).unwrap();
        let values = top_desc(batches.clone(), schema.clone(), 6_000, &pool);
        let spilled = pool.spilled();
        drop(pool);
        let _ = std::fs::remove_dir_all(&root);
        let expected: Vec<i64> = (8 * 4096 - 6_000..8 * 4096).rev().collect();
        assert_eq!(values.unwrap(), expected);
        assert!(spilled > 0, "expected the external sort to spill");
        // Without a spill directory the pool error is kept.
        let pool = MemoryPool::new(Some(cap), None).unwrap();
        assert!(matches!(
            top_desc(batches, schema, 6_000, &pool),
            Err(Error::Memory(_))
        ));
    }

    /// Unique spill root for one test, removed by the caller.
    fn spill_root(tag: &str) -> std::path::PathBuf {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "mizuchi-{tag}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    fn files_under(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|e| {
                let path = e.unwrap().path();
                if path.is_dir() { files_under(&path) } else { 1 }
            })
            .sum()
    }

    #[test]
    fn external_sort_streams_a_bounded_merge_of_input_many_times_the_limit() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        const N: i64 = 64 * 4096;
        // A permutation of 0..N in 64 separately allocated batches.
        let batches: Vec<Batch> = (0..64)
            .map(|b| {
                let keys: Vec<i64> = (b * 4096..(b + 1) * 4096).map(|i| (i * 7919) % N).collect();
                let vals: Vec<i64> = keys.iter().map(|k| k * 2).collect();
                Batch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(keys)),
                        Arc::new(Int64Array::from(vals)),
                    ],
                )
                .unwrap()
            })
            .collect();
        // About a sixteenth of the input: more runs than one merge reads.
        let cap = 4 * batch_bytes(&batches[0]);
        let root = spill_root("extsort");
        let pool = MemoryPool::new(Some(cap), Some(root.clone())).unwrap();
        let order = [SortExpr {
            expr: Expr::nth(0),
            asc: true,
            nulls_first: false,
        }];
        let cancel = CancelToken::new();
        let Sorted::Stream(mut merge) = sorted(
            batches_to_exec(batches, schema.clone()),
            schema,
            &order,
            100,
            None,
            &BatchPolicy::default(),
            &pool,
            &cancel,
        )
        .unwrap() else {
            panic!("a spilled sort must stream its merge");
        };
        let mut keys = Vec::new();
        while let Some(batch) = merge.next_batch(&cancel).unwrap() {
            // Output batches are merge chunks, never whole runs.
            assert!(batch_bytes(&batch) <= cap / CHUNKS_PER_LIMIT * 2);
            let k = batch.column(0).as_primitive::<Int64Type>().values();
            let v = batch.column(1).as_primitive::<Int64Type>().values();
            assert!(k.iter().zip(v.iter()).all(|(k, v)| v == &(k * 2)));
            keys.extend_from_slice(k);
        }
        let leftover = files_under(&root);
        let (peak, spilled) = (pool.peak(), pool.spilled());
        drop(merge);
        drop(pool);
        let _ = std::fs::remove_dir_all(&root);
        assert_eq!(keys, (100..N).collect::<Vec<_>>());
        assert!(spilled > 0);
        // Every reservation, merge chunks included, fits under the limit.
        assert!(peak <= cap, "peak {peak} over cap {cap}");
        assert_eq!(leftover, 0, "a drained merge removes its run files");
    }

    #[test]
    fn external_sort_fails_closed_when_two_merge_chunks_do_not_fit() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batches: Vec<Batch> = (0..12)
            .map(|i| {
                Batch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int64Array::from_iter_values(
                        (0..8).map(|r| r * 12 + i),
                    ))],
                )
                .unwrap()
            })
            .collect();
        // Room for runs of one small batch, not for two merge chunks: each
        // chunk carries fixed Arrow and key-encoding overhead.
        let cap = batch_bytes(&batches[0]) * 3 / 2;
        let root = spill_root("extsort-tight");
        let pool = MemoryPool::new(Some(cap), Some(root.clone())).unwrap();
        let order = [SortExpr {
            expr: Expr::nth(0),
            asc: true,
            nulls_first: false,
        }];
        let out = ordered(
            batches_to_exec(batches, schema.clone()),
            schema,
            &order,
            0,
            None,
            &BatchPolicy::default(),
            &pool,
            &CancelToken::new(),
        );
        let (peak, spilled, leftover) = (pool.peak(), pool.spilled(), files_under(&root));
        drop(pool);
        let _ = std::fs::remove_dir_all(&root);
        assert!(matches!(out, Err(Error::Memory(_))), "{out:?}");
        assert!(spilled > 0, "runs spill before the merge fails");
        assert!(peak <= cap);
        assert_eq!(leftover, 0, "a failed merge removes its run files");
    }

    #[test]
    fn limited_top_k_accepts_a_batch_that_uses_the_whole_budget() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = wide_batches(&schema).remove(0);
        let pool = MemoryPool::new(Some(batch_bytes(&batch)), None).unwrap();
        assert_eq!(
            top_desc(vec![batch], schema, 3, &pool).unwrap(),
            [4095, 4094, 4093]
        );
    }
}
