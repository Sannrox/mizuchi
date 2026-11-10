//! SQL parser: sqlparser-rs with the PostgreSQL dialect.
//!
//! Syntax errors fail closed as [`Error::Parse`]. The parser does not drop
//! clauses. Features the engine does not implement fail later as
//! [`Error::NotImplemented`]. See `docs/decisions/0003-sql-parser-and-dialect.md`.

use std::ops::ControlFlow;

use sqlparser::ast::{Expr, Statement, Visit, Visitor};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use mizuchi_core::error::{Error, Result};

/// Parse `sql` into statements using [`PostgreSqlDialect`].
///
/// Empty input, syntax errors, and expressions nested deeper than
/// [`MAX_EXPR_DEPTH`] return [`Error::Parse`].
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    if sql.len() <= INLINE_PARSE_BYTES {
        return parse_checked(sql);
    }
    // The parser builds operator chains (`a + a + ...`) in a loop, but
    // dropping one, rejected here or on a later syntax error inside the
    // parser, recurses once per level. Every level costs at least two bytes
    // of input, so a stack sized to the input holds the whole tree.
    let stack = PARSE_STACK_BASE.saturating_add(sql.len().saturating_mul(PARSE_STACK_PER_BYTE));
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("mizuchi-parse".into())
            .stack_size(stack)
            .spawn_scoped(scope, || parse_checked(sql))
            .map_err(|e| Error::Parse(format!("parser stack of {stack} bytes: {e}")))?
            .join()
            .map_err(|_| Error::Parse("parser panicked".into()))?
    })
}

/// Inputs this short parse on the caller's thread: their deepest possible
/// tree drops within a few hundred KiB of stack.
const INLINE_PARSE_BYTES: usize = 4 * 1024;
const PARSE_STACK_BASE: usize = 1024 * 1024;
/// Stack per input byte for a dedicated parse thread. Dropping a level costs
/// under 100 bytes of stack in an unoptimized build and each level takes at
/// least two input bytes, so this leaves more than 2x headroom.
const PARSE_STACK_PER_BYTE: usize = 128;

fn parse_checked(sql: &str) -> Result<Vec<Statement>> {
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .map_err(|err| Error::Parse(err.to_string()))?;
    if stmts.is_empty() {
        return Err(Error::Parse("empty SQL".into()));
    }
    for stmt in &stmts {
        if stmt.visit(&mut ExprDepth(0)).is_break() {
            return Err(Error::Parse(format!(
                "expression nesting exceeds {MAX_EXPR_DEPTH} levels"
            )));
        }
    }
    Ok(stmts)
}

/// Deepest expression nesting a statement may have. Binding, typing and
/// evaluation recurse once per level, so a deeper tree (for example a long
/// `a + a + ...` or `x OR x OR ...` chain) would overflow the caller's stack.
pub const MAX_EXPR_DEPTH: usize = 128;

/// Current expression nesting while visiting; breaks past [`MAX_EXPR_DEPTH`].
struct ExprDepth(usize);

impl Visitor for ExprDepth {
    type Break = ();

    fn pre_visit_expr(&mut self, _: &Expr) -> ControlFlow<()> {
        self.0 += 1;
        if self.0 > MAX_EXPR_DEPTH {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    fn post_visit_expr(&mut self, _: &Expr) -> ControlFlow<()> {
        self.0 -= 1;
        ControlFlow::Continue(())
    }
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
