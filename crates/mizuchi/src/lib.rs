//! mizuchi: an Arrow-native query and transform engine.
//!
//! Columnar memory is standard Apache Arrow record batches. See [`batch`] and
//! [`schema`]. Planned layers, outside in:
//! SQL / DataFrame -> logical plan -> optimizer -> physical plan -> execution.

pub mod batch;
mod error;
pub mod expr;
pub mod schema;
pub mod sql;

pub use batch::{Batch, BatchPolicy, BorrowedBatch, DEFAULT_MAX_ROWS};
pub use error::Error;
pub use expr::{BinaryOp, Expr, UnaryOp};
pub use schema::{from_arrow_schema, is_supported_data_type, to_arrow_schema};
pub use sql::parse_sql;

pub use arrow::array::{ArrayRef, RecordBatch};
pub use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

/// Crate version, for receipts and diagnostics in embedding runners.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Plan a SQL statement. Parsing uses ADR 0003; binding is not implemented yet.
pub fn plan_sql(sql: &str) -> Result<(), Error> {
    let _ = parse_sql(sql)?;
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
    fn plan_sql_fails_closed_on_syntax_errors() {
        assert!(matches!(plan_sql("SELECT FROM"), Err(Error::Parse(_))));
    }

    #[test]
    fn version_is_set() {
        assert!(!VERSION.is_empty());
    }
}
