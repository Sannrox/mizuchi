//! Pull operator contract and lowering hook.
//!
//! Physical operators are [`Operator`] trees. The parent pulls the next Arrow
//! batch (ADR 0004). [`Lowering`] selects an implementation per operator;
//! [`CpuLowering`] is the default. Dispatch is once per batch, never per row.
//! Host Arrow batches stay the boundary between operators.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use super::CancelToken;
use mizuchi_core::batch::Batch;
use mizuchi_core::error::Result;
use mizuchi_core::expr::Expr;
use mizuchi_plan::NamedExpr;

/// Pull iterator of Arrow morsels.
pub trait Operator: Send {
    /// Next morsel, or `None` at end of input. Checks `cancel` between batches.
    fn next_batch(&mut self, cancel: &CancelToken) -> Result<Option<Batch>>;
}

/// Builds physical operators from a logical plan node.
///
/// Override a method to replace that operator. Unoverridden methods keep the
/// CPU implementation.
pub trait Lowering: Send + Sync {
    fn empty(&self, schema: SchemaRef) -> Result<Box<dyn Operator>> {
        CpuLowering::empty(schema)
    }

    fn scan(&self, batches: Vec<Batch>) -> Result<Box<dyn Operator>> {
        CpuLowering::scan(batches)
    }

    fn filter(&self, input: Box<dyn Operator>, predicate: Expr) -> Result<Box<dyn Operator>> {
        CpuLowering::filter(input, predicate)
    }

    fn project(
        &self,
        input: Box<dyn Operator>,
        exprs: Vec<NamedExpr>,
        schema: SchemaRef,
    ) -> Result<Box<dyn Operator>> {
        CpuLowering::project(input, exprs, schema)
    }

    fn limit(
        &self,
        input: Box<dyn Operator>,
        skip: u64,
        fetch: Option<u64>,
    ) -> Result<Box<dyn Operator>> {
        CpuLowering::limit(input, skip, fetch)
    }
}

/// Default CPU operators.
#[derive(Clone, Debug, Default)]
pub struct CpuLowering;

/// Session-held lowering. Clones share the same registry.
#[derive(Clone)]
pub(super) struct LoweringArc(pub(super) Arc<dyn Lowering>);

impl Default for LoweringArc {
    fn default() -> Self {
        Self(Arc::new(CpuLowering))
    }
}

impl std::fmt::Debug for LoweringArc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Lowering")
    }
}

impl Operator for Box<dyn Operator> {
    fn next_batch(&mut self, cancel: &CancelToken) -> Result<Option<Batch>> {
        (**self).next_batch(cancel)
    }
}
