//! SQL parser: sqlparser-rs with the PostgreSQL dialect.
//!
//! Syntax errors fail closed as [`Error::Parse`]. The parser does not drop
//! clauses. Features the engine does not implement fail later as
//! [`Error::NotImplemented`]. See `docs/decisions/0003-sql-parser-and-dialect.md`.

use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::error::{Error, Result};

pub use sqlparser::ast::Statement;

/// Parse `sql` into statements using [`PostgreSqlDialect`].
///
/// Empty input and syntax errors return [`Error::Parse`].
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .map_err(|err| Error::Parse(err.to_string()))?;
    if stmts.is_empty() {
        return Err(Error::Parse("empty SQL".into()));
    }
    Ok(stmts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_literal_parses() {
        let stmts = parse_sql("SELECT 1").expect("parse");
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], Statement::Query(_)));
    }

    #[test]
    fn postgres_cast_and_ilike_parse() {
        parse_sql("SELECT 1::bigint").expect(":: cast");
        parse_sql("SELECT 'a' ILIKE 'A'").expect("ILIKE");
        parse_sql("SELECT x FROM t WHERE x ILIKE '%foo%' LIMIT 5").expect("ILIKE + LIMIT");
    }

    #[test]
    fn syntax_error_is_parse() {
        let err = parse_sql("SELECT FROM").expect_err("syntax");
        assert!(
            matches!(err, Error::Parse(_)),
            "expected Parse, got {err:?}"
        );
        assert!(err.to_string().starts_with("sql parse:"), "{}", err);
    }

    #[test]
    fn empty_input_is_parse() {
        assert!(matches!(parse_sql("").unwrap_err(), Error::Parse(_)));
        assert!(matches!(parse_sql("   ").unwrap_err(), Error::Parse(_)));
    }

    #[test]
    fn order_by_limit_offset_stay_in_the_ast() {
        let stmts = parse_sql("SELECT a FROM t ORDER BY a LIMIT 10 OFFSET 2").expect("parse");
        let rendered = stmts[0].to_string();
        assert!(
            rendered.to_ascii_uppercase().contains("ORDER BY"),
            "{rendered}"
        );
        assert!(
            rendered.to_ascii_uppercase().contains("LIMIT"),
            "{rendered}"
        );
        assert!(
            rendered.to_ascii_uppercase().contains("OFFSET"),
            "{rendered}"
        );
    }

    #[test]
    fn unrecognized_clause_is_not_silently_dropped() {
        // Either the dialect keeps QUALIFY in the AST, or parse fails closed.
        match parse_sql("SELECT a FROM t QUALIFY a > 1") {
            Ok(stmts) => {
                let rendered = stmts[0].to_string().to_ascii_uppercase();
                assert!(
                    rendered.contains("QUALIFY"),
                    "parsed but QUALIFY missing: {rendered}"
                );
            }
            Err(Error::Parse(_)) => {}
            Err(other) => panic!("expected Parse or AST with QUALIFY, got {other:?}"),
        }
    }
}
