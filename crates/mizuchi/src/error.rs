//! Engine errors.

/// Recoverable engine failure.
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// The requested feature is not implemented yet.
    NotImplemented(&'static str),
    /// An Arrow type the engine does not hold.
    UnsupportedType(String),
    /// Record batch construction, projection, or slicing failed.
    InvalidBatch(String),
    /// A batch-size policy value was rejected.
    InvalidPolicy(&'static str),
    /// Scalar expression evaluation failed.
    Eval(String),
    /// SQL lexing or parsing failed.
    Parse(String),
    /// Name resolution, type checking, or binding failed.
    Plan(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotImplemented(what) => write!(f, "not implemented: {what}"),
            Error::UnsupportedType(dt) => write!(f, "unsupported Arrow type: {dt}"),
            Error::InvalidBatch(reason) => write!(f, "invalid batch: {reason}"),
            Error::InvalidPolicy(reason) => write!(f, "invalid batch policy: {reason}"),
            Error::Eval(reason) => write!(f, "expression eval: {reason}"),
            Error::Parse(reason) => write!(f, "sql parse: {reason}"),
            Error::Plan(reason) => write!(f, "plan: {reason}"),
        }
    }
}

impl std::error::Error for Error {}

/// Result alias for engine operations.
pub type Result<T> = std::result::Result<T, Error>;
