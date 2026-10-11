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
| Inner join | `join.slt` |
| Uncorrelated `FROM` subquery (derived table) | `subquery.slt` |
| `IN` lists, `IN`/`NOT IN (SELECT ...)`, `EXISTS`, scalar subquery, `LATERAL` | `subquery.slt` (#80) |
| `CAST` of integer, float, and text; `DECIMAL` / `NUMERIC` casts and arithmetic | `decimal.slt` (#82) |
| Window functions (`OVER`, `ROW_NUMBER`, ranking, frames, named `WINDOW`) | `window.slt` (#78) |
| CTEs (`WITH`, `WITH RECURSIVE`) | `cte.slt` (#79) |
| `UNION` / `UNION ALL` / `UNION DISTINCT`, `INTERSECT`, `EXCEPT` (DISTINCT and ALL) | `setops.slt` (#81) |
| `MERGE INTO` match / update / insert / delete | `merge.slt` |

Arrow Date, Timestamp, Duration, and Interval are accepted at the schema
boundary (`schema.rs` unit tests). They have no SQL path yet. `DECIMAL` /
`NUMERIC` map onto Arrow `Decimal32` / `Decimal64` / `Decimal128`.

## Audited families

| Feature | Status | Evidence | Follow-up |
| --- | --- | --- | --- |
| `IN` lists, `IN`/`NOT IN (SELECT ...)`, `EXISTS`, scalar subquery, `LATERAL` | supported | `subquery.slt` | #80 |
| `UNION` / `UNION ALL` / `UNION DISTINCT` | supported | `setops.slt` | #81 |
| `INTERSECT` / `EXCEPT` (DISTINCT and ALL) | supported | `setops.slt` | #81 |
| DECIMAL arithmetic and casts | supported | `decimal.slt` | #82 |
| DATE, TIMESTAMP, INTERVAL casts and functions | skipped | `datetime.slt` | #83 |

Set operators keep left column names. Column counts must match. Compatible
integer, float, and null types widen; incompatible types fail at plan time.
Nulls compare equal. `UNION BY NAME` is unimplemented.
`CURRENT_DATE` and `NOW()` in `datetime.slt` keep placeholder expected rows
until a session clock exists.

`gaps.slt` is a pointer to the per-family files.
