# SQL coverage

Audit of the sqllogictest corpus against the binder and executor. Status is
what `Session::execute_sql` does today. Unsupported constructs fail closed and
are recorded as `skipif mizuchi` so default `cargo test` stays green.

The runner walks every `crates/mizuchi/tests/slt/*.slt` file. Supported records
execute; skipped records are parsed and ignored.

## Baseline (supported)

| Feature | Evidence |
| --- | --- |
| Filter, project, arithmetic, `IS NULL` | `select.slt` |
| String, math, `nullif`, `ARRAY` / `array_length` | `functions.slt` |
| Hash aggregate, `GROUP BY`, `COUNT(DISTINCT)` | `agg.slt` |
| INNER, LEFT, RIGHT, FULL OUTER, CROSS, LEFT SEMI, LEFT ANTI join | `join.slt` (#233) |
| Uncorrelated `FROM` subquery (derived table) | `subquery.slt` |
| `IN` lists, `IN`/`NOT IN (SELECT ...)`, `EXISTS`, scalar subquery, `LATERAL` | `subquery.slt` (#80) |
| `CAST` of integer, float, and text; `DECIMAL` / `NUMERIC` casts and arithmetic | `decimal.slt` (#82) |
| `DATE`, `TIMESTAMP`, `INTERVAL` casts, `EXTRACT`, `DATE_TRUNC`, `CURRENT_DATE`, `NOW()` | `datetime.slt` (#83) |
| Window functions (`OVER`, `ROW_NUMBER`, ranking, frames, named `WINDOW`) | `window.slt` (#78) |
| CTEs (`WITH`, `WITH RECURSIVE`) | `cte.slt` (#79) |
| `UNION` / `UNION ALL` / `UNION DISTINCT`, `INTERSECT`, `EXCEPT` (DISTINCT and ALL) | `setops.slt` (#81) |
| `MERGE INTO` match / update / insert / delete | `merge.slt` |

Arrow Date, Timestamp, Duration, and Interval are accepted at the schema
boundary (`schema.rs` unit tests). SQL `DATE` maps to Arrow `Date32`,
`TIMESTAMP` (without time zone) to `Timestamp(Microsecond)`, and
`INTERVAL … DAY` to `Interval(DayTime)`. `DECIMAL` / `NUMERIC` map onto
Arrow `Decimal32` / `Decimal64` / `Decimal128`.

Expressions nested deeper than `mizuchi::sql::MAX_EXPR_DEPTH` (128) levels,
such as a 200-term `x OR x OR ...` chain, fail as `Error::Parse` before
planning. `IN` lists of any length stay within the limit. The adversarial
suite (`crates/mizuchi/tests/adversarial.rs`) executes these shapes.

## Audited families

| Feature | Status | Evidence | Follow-up |
| --- | --- | --- | --- |
| `INNER JOIN` | supported | `join.slt` | #233 |
| `LEFT` / `RIGHT` / `FULL OUTER JOIN` | supported unbounded; `Error::Memory` when the join would spill | `join.slt` | #233 |
| `CROSS JOIN` | supported | `join.slt` | #233 |
| `LEFT SEMI` / `LEFT ANTI JOIN` | supported | `join.slt` | #233 |
| `IN` lists, `IN`/`NOT IN (SELECT ...)`, `EXISTS`, scalar subquery, `LATERAL` | supported | `subquery.slt` | #80 |
| `UNION` / `UNION ALL` / `UNION DISTINCT` | supported | `setops.slt` | #81 |
| `INTERSECT` / `EXCEPT` (DISTINCT and ALL) | supported | `setops.slt` | #81 |
| DECIMAL arithmetic and casts | supported | `decimal.slt` | #82 |
| DATE, TIMESTAMP, INTERVAL casts and functions | supported | `datetime.slt` | #83 |

Join null keys never match. `JOIN USING` and `NATURAL JOIN` are unimplemented.
Inner and left-semi joins honour a session memory cap (spill or stream). Outer
joins that would spill return `Error::Memory` rather than a partial result.
Null-aware anti (`NOT IN (SELECT ...)`) is the subquery family (#80).

Set operators keep left column names. Column counts must match. Compatible
integer, float, and null types widen; incompatible types fail at plan time.
Nulls compare equal. `UNION BY NAME` is unimplemented.
`CURRENT_DATE` and `NOW()` bind to the session clock (`SessionConfig.now_micros`
or `Session::with_now_micros`); the slt runner pins 2020-01-01 00:00:00 UTC.

`gaps.slt` is a pointer to the per-family files.
