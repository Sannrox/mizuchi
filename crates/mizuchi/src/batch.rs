//! Owned and borrowed Arrow record batches used by every operator.
//!
//! A batch is a standard [`RecordBatch`]. Cloning an owned batch is cheap:
//! arrays share Arrow buffers through `Arc`.

use arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::SchemaRef;

use crate::error::{Error, Result};
use crate::schema::{from_arrow_schema, validate_schema};

/// Maximum rows in one operator batch unless the caller sets a policy.
///
/// Sized for vectorized kernels: large enough to amortize dispatch, small
/// enough to stay in cache and bound a single allocation.
pub const DEFAULT_MAX_ROWS: usize = 4096;

/// How operators slice input into record batches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchPolicy {
    max_rows: Option<usize>,
}

impl Default for BatchPolicy {
    fn default() -> Self {
        Self {
            max_rows: Some(DEFAULT_MAX_ROWS),
        }
    }
}

impl BatchPolicy {
    /// Finite row cap. `max_rows` must be greater than zero.
    pub fn new(max_rows: usize) -> Result<Self> {
        if max_rows == 0 {
            return Err(Error::InvalidPolicy(
                "batch max_rows must be greater than zero",
            ));
        }
        Ok(Self {
            max_rows: Some(max_rows),
        })
    }

    /// No row cap. Callers still split by other resource limits.
    pub fn unlimited() -> Self {
        Self { max_rows: None }
    }

    /// Configured maximum rows, or `None` when unlimited.
    pub fn max_rows(&self) -> Option<usize> {
        self.max_rows
    }

    /// Row counts of successive batches that cover `total_rows`.
    pub fn chunk_row_counts(&self, total_rows: usize) -> impl Iterator<Item = usize> {
        let cap = self.max_rows.unwrap_or(usize::MAX);
        let mut remaining = total_rows;
        std::iter::from_fn(move || {
            if remaining == 0 {
                return None;
            }
            let n = remaining.min(cap);
            remaining -= n;
            Some(n)
        })
    }
}

/// Owned record batch. The canonical unit of work for operators.
#[derive(Clone, Debug, PartialEq)]
pub struct Batch {
    inner: RecordBatch,
}

impl Batch {
    /// Build a batch from Arrow arrays. Buffers are taken as-is; they are not copied.
    pub fn try_new(schema: SchemaRef, columns: Vec<ArrayRef>) -> Result<Self> {
        let schema = from_arrow_schema(schema)?;
        let inner = RecordBatch::try_new(schema, columns)
            .map_err(|err| Error::InvalidBatch(err.to_string()))?;
        Ok(Self { inner })
    }

    /// Wrap an Arrow record batch after checking the schema is supported.
    pub fn from_arrow(batch: RecordBatch) -> Result<Self> {
        validate_schema(batch.schema().as_ref())?;
        Ok(Self { inner: batch })
    }

    /// Zero-row batch with the given schema.
    pub fn empty(schema: SchemaRef) -> Result<Self> {
        let schema = from_arrow_schema(schema)?;
        Ok(Self {
            inner: RecordBatch::new_empty(schema),
        })
    }

    /// Standard Arrow record batch. There is no private memory format.
    pub fn as_arrow(&self) -> &RecordBatch {
        &self.inner
    }

    /// Drop the wrapper, yielding the Arrow record batch.
    pub fn into_arrow(self) -> RecordBatch {
        self.inner
    }

    /// Schema of this batch.
    pub fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    /// Number of rows.
    pub fn num_rows(&self) -> usize {
        self.inner.num_rows()
    }

    /// Number of columns.
    pub fn num_columns(&self) -> usize {
        self.inner.num_columns()
    }

    /// Column at `index`.
    pub fn column(&self, index: usize) -> &ArrayRef {
        self.inner.column(index)
    }

    /// Column named `name`, if present.
    pub fn column_by_name(&self, name: &str) -> Option<&ArrayRef> {
        self.inner.column_by_name(name)
    }

    /// Select rows by index. A contiguous run is a zero-copy slice.
    pub fn gather(&self, indices: &[u32]) -> Result<Self> {
        if indices.is_empty() {
            return Self::empty(self.schema());
        }
        if let Some((offset, length)) = contiguous_indices(indices) {
            return self.slice(offset, length);
        }
        let idx = UInt32Array::from(indices.to_vec());
        let mut columns = Vec::with_capacity(self.num_columns());
        for col in self.as_arrow().columns() {
            columns.push(
                take(col.as_ref(), &idx, None)
                    .map_err(|err| Error::InvalidBatch(err.to_string()))?,
            );
        }
        Self::try_new(self.schema(), columns)
    }

    /// Zero-copy window of `length` rows starting at `offset`.
    pub fn slice(&self, offset: usize, length: usize) -> Result<Self> {
        let rows = self.num_rows();
        if offset
            .checked_add(length)
            .is_none_or(|end| offset > rows || end > rows)
        {
            return Err(Error::InvalidBatch(format!(
                "slice offset {offset} length {length} exceeds {rows} rows"
            )));
        }
        Ok(Self {
            inner: self.inner.slice(offset, length),
        })
    }

    /// Project a subset of columns by index, sharing the selected arrays.
    pub fn project(&self, indices: &[usize]) -> Result<Self> {
        let inner = self
            .inner
            .project(indices)
            .map_err(|err| Error::InvalidBatch(err.to_string()))?;
        Ok(Self { inner })
    }

    /// Split into policy-sized batches. Empty input yields one empty batch.
    pub fn split_by_policy(&self, policy: &BatchPolicy) -> Vec<Self> {
        if self.num_rows() == 0 {
            return vec![self.clone()];
        }
        let mut offset = 0;
        let mut out = Vec::new();
        for length in policy.chunk_row_counts(self.num_rows()) {
            out.push(Self {
                inner: self.inner.slice(offset, length),
            });
            offset += length;
        }
        out
    }
}

/// Contiguous increasing indices `[start, start+1, …]` with no null sentinels.
pub(crate) fn contiguous_indices(indices: &[u32]) -> Option<(usize, usize)> {
    let first = *indices.first()?;
    if first == u32::MAX {
        return None;
    }
    for (i, &idx) in indices.iter().enumerate() {
        if idx == u32::MAX || idx != first.checked_add(i as u32)? {
            return None;
        }
    }
    Some((first as usize, indices.len()))
}

impl AsRef<RecordBatch> for Batch {
    fn as_ref(&self) -> &RecordBatch {
        &self.inner
    }
}

/// Borrowed view of an Arrow record batch. Operators that do not retain data
/// use this instead of cloning the wrapper.
#[derive(Clone, Copy, Debug)]
pub struct BorrowedBatch<'a> {
    inner: &'a RecordBatch,
}

impl<'a> BorrowedBatch<'a> {
    /// Borrow a standard Arrow record batch after checking the schema.
    pub fn try_from_arrow(batch: &'a RecordBatch) -> Result<Self> {
        validate_schema(batch.schema().as_ref())?;
        Ok(Self { inner: batch })
    }

    /// Borrow an owned batch.
    pub fn from_batch(batch: &'a Batch) -> Self {
        Self {
            inner: batch.as_arrow(),
        }
    }

    /// Cheap owned copy: arrays share buffers.
    pub fn to_owned(self) -> Batch {
        Batch {
            inner: self.inner.clone(),
        }
    }

    /// Standard Arrow record batch.
    pub fn as_arrow(self) -> &'a RecordBatch {
        self.inner
    }

    /// Schema of this batch.
    pub fn schema(self) -> SchemaRef {
        self.inner.schema()
    }

    /// Number of rows.
    pub fn num_rows(self) -> usize {
        self.inner.num_rows()
    }

    /// Number of columns.
    pub fn num_columns(self) -> usize {
        self.inner.num_columns()
    }

    /// Column at `index`.
    pub fn column(self, index: usize) -> &'a ArrayRef {
        self.inner.column(index)
    }
}

impl AsRef<RecordBatch> for BorrowedBatch<'_> {
    fn as_ref(&self) -> &RecordBatch {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn sample() -> (ArrayRef, ArrayRef, Batch) {
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(4)]));
        let names: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a"),
            Some("b"),
            None,
            Some("d"),
        ]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = Batch::try_new(schema, vec![ids.clone(), names.clone()]).unwrap();
        (ids, names, batch)
    }

    #[test]
    fn try_new_shares_arrow_array_buffers() {
        let (ids, names, batch) = sample();
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 2);
        assert!(Arc::ptr_eq(batch.column(0), &ids));
        assert!(Arc::ptr_eq(batch.column(1), &names));
        assert!(Arc::ptr_eq(batch.column_by_name("id").expect("id"), &ids));
        assert_eq!(batch.as_arrow().schema().field(0).name(), "id");
    }

    #[test]
    fn from_arrow_and_into_arrow_are_identity() {
        let (_, _, batch) = sample();
        let arrow = batch.clone().into_arrow();
        let wrapped = Batch::from_arrow(arrow.clone()).unwrap();
        assert_eq!(wrapped.as_arrow(), &arrow);
        assert_eq!(wrapped, batch);
    }

    #[test]
    fn borrowed_batch_views_the_same_arrays() {
        let (ids, _, batch) = sample();
        let borrowed = BorrowedBatch::try_from_arrow(batch.as_arrow()).unwrap();
        assert_eq!(borrowed.num_rows(), 4);
        assert!(Arc::ptr_eq(borrowed.column(0), &ids));
        let owned = borrowed.to_owned();
        assert_eq!(owned, batch);
        let from_owned = BorrowedBatch::from_batch(&batch);
        assert_eq!(from_owned.num_columns(), 2);
    }

    #[test]
    fn slice_is_zero_copy_and_bounds_checked() {
        let (ids, _, batch) = sample();
        let mid = batch.slice(1, 2).unwrap();
        assert_eq!(mid.num_rows(), 2);
        let sliced_ids = mid.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(sliced_ids.values(), &[0, 3]);
        assert!(sliced_ids.is_null(0));
        let parent = ids.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(sliced_ids.values().as_ptr(), parent.values()[1..].as_ptr());
        assert!(batch.slice(3, 2).is_err());
        assert!(batch.slice(4, 0).is_ok());
        assert!(batch.slice(5, 0).is_err());
    }

    #[test]
    fn gather_slices_contiguous_indices() {
        let (ids, _, batch) = sample();
        let mid = batch.gather(&[1, 2]).unwrap();
        assert_eq!(mid.num_rows(), 2);
        let sliced_ids = mid.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let parent = ids.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(sliced_ids.values().as_ptr(), parent.values()[1..].as_ptr());
        let scattered = batch.gather(&[3, 0, 2]).unwrap();
        assert_eq!(scattered.num_rows(), 3);
        let taken = scattered
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_ne!(taken.values().as_ptr(), parent.values().as_ptr());
        assert!(batch.gather(&[]).unwrap().num_rows() == 0);
    }

    #[test]
    fn project_keeps_selected_arrow_arrays() {
        let (_, names, batch) = sample();
        let projected = batch.project(&[1]).unwrap();
        assert_eq!(projected.num_columns(), 1);
        assert_eq!(projected.schema().field(0).name(), "name");
        assert!(Arc::ptr_eq(projected.column(0), &names));
        assert!(batch.project(&[2]).is_err());
    }

    #[test]
    fn default_policy_is_vectorized_batch_size() {
        let policy = BatchPolicy::default();
        assert_eq!(policy.max_rows(), Some(DEFAULT_MAX_ROWS));
        assert_eq!(DEFAULT_MAX_ROWS, 4096);
        assert_eq!(
            policy.chunk_row_counts(10_000).collect::<Vec<_>>(),
            vec![4096, 4096, 1808]
        );
        assert!(BatchPolicy::new(0).is_err());
        assert_eq!(
            BatchPolicy::unlimited()
                .chunk_row_counts(10_000)
                .collect::<Vec<_>>(),
            vec![10_000]
        );
    }

    #[test]
    fn split_by_policy_covers_all_rows() {
        let (_, _, batch) = sample();
        let parts = batch.split_by_policy(&BatchPolicy::new(3).unwrap());
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].num_rows(), 3);
        assert_eq!(parts[1].num_rows(), 1);
        let empty = Batch::empty(batch.schema()).unwrap();
        assert_eq!(empty.split_by_policy(&BatchPolicy::default()).len(), 1);
        assert_eq!(empty.num_rows(), 0);
    }
}
