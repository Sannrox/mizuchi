//! mizuchi: an Arrow-native query and transform engine.
//!
//! Columnar memory is standard Apache Arrow record batches. See [`batch`] and
//! [`schema`]. Planned layers, outside in:
//! SQL / DataFrame -> logical plan -> optimizer -> physical plan -> execution.

pub mod batch;
mod error;
pub mod expr;
pub mod schema;

pub use batch::{Batch, BatchPolicy, BorrowedBatch, DEFAULT_MAX_ROWS};
pub use error::Error;
pub use expr::{BinaryOp, Expr, UnaryOp};
pub use schema::{from_arrow_schema, is_supported_data_type, to_arrow_schema};

pub use arrow::array::{ArrayRef, RecordBatch};
pub use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

/// Crate version, for receipts and diagnostics in embedding runners.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Plan a SQL statement. Placeholder until the SQL front end lands.
pub fn plan_sql(_sql: &str) -> Result<(), Error> {
    Err(Error::NotImplemented("SQL planning"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planning_is_not_implemented_yet() {
        assert_eq!(
            plan_sql("SELECT 1"),
            Err(Error::NotImplemented("SQL planning"))
        );
    }

    #[test]
    fn version_is_set() {
        assert!(!VERSION.is_empty());
    }
}
