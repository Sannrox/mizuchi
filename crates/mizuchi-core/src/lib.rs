//! Foundations: errors, Arrow batches, scalar expressions, cancellation.

pub mod batch;
mod cancel;
pub mod error;
pub mod expr;
pub mod lineage;
pub mod schema;
pub mod udaf;

pub use batch::{Batch, BatchPolicy, BorrowedBatch, DEFAULT_MAX_ROWS};
pub use cancel::CancelToken;
pub use error::{Error, Result};
pub use expr::{BinaryOp, Expr, UnaryOp};
pub use lineage::{InputScan, OutputWrite};
pub use schema::{from_arrow_schema, is_supported_data_type, to_arrow_schema};
pub use udaf::{CombineAgg, ProductI64};

/// Crate version, for receipts and diagnostics in embedding runners.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
