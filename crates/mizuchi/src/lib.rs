//! mizuchi: an Arrow-native query and transform engine.
//!
//! Nothing executes yet. See `docs/decisions/0002-own-query-engine.md` and the
//! GitHub Issue roadmap. Planned layers, outside in:
//! SQL / DataFrame -> logical plan -> optimizer -> physical plan -> execution.

/// Crate version, for receipts and diagnostics in embedding runners.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Errors returned by the engine.
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// The requested feature is not implemented yet.
    NotImplemented(&'static str),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotImplemented(what) => write!(f, "not implemented: {what}"),
        }
    }
}

impl std::error::Error for Error {}

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
