# Function coverage

Built-in scalar functions are bound from SQL and from DataFrame `select` /
`filter` fragments. User-defined aggregates register on a [`Session`] and
participate in hash aggregate, parallel combine, and spill. Vectorized Python
UDFs stay on Arrow arrays (`DataFrame.udf`, `Grouped.agg_udf`).

Struct field access is out of this slice.

## Scalar

| Family | SQL | Notes |
| --- | --- | --- |
| String | `upper`, `lower`, `length` / `char_length` / `character_length`, `trim` / `ltrim` / `rtrim`, `concat`, `substring` | `length` is character count. `TRIM` is whitespace only. |
| Math | `abs`, `round`, `ceil` / `ceiling`, `floor`, `sqrt` | `round` is 0 decimal places. `ceil` / `floor` / `sqrt` / `round` return Float64. |
| Conditional | `CASE`, `coalesce`, `nullif` | `nullif(a, b)` rewrites to `CASE WHEN a = b THEN NULL ELSE a END`. |
| Array | `ARRAY[e, ...]`, `array_length(a, 1)` | One list per row. `array_length` requires a list (not a string). Dimension other than 1 is unimplemented. |
| Date/time | `EXTRACT(YEAR\|MONTH\|DAY\|EPOCH FROM x)`, `date_part('year'\|'month'\|'day'\|'epoch', x)` | Input is an Arrow Date or Timestamp column. SQL `DATE` / `TIMESTAMP` literals are a later family (`datetime.slt`). |

Evidence: `crates/mizuchi/tests/slt/functions.slt`, plus `extract_from_date32_column` for EXTRACT on Date32.

## Aggregates

| Kind | SQL | Notes |
| --- | --- | --- |
| Built-in | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COUNT(DISTINCT)` | `AVG` and `COUNT(DISTINCT)` do not spill. |
| User | registered name, one argument | Buffer type equals result type. `update` folds one argument cell into the buffer; `merge` folds two buffers. Replacing a UDAF drops cached query results. |

Rust: `Session::register_udaf` with [`CombineAgg`] (`ProductI64` is the Int64 product used in tests). Python: `Session.register_udaf(name, update, merge, identity)`.

Evidence: `udaf_product_parallel_and_spill` (workers=2 and a tiny memory cap with spill).

## Vectorized Python UDFs

`DataFrame.udf(name, func, *cols)` calls `func` once per batch with one
`pyarrow.Array` per column and appends an equal-length array. `Grouped.agg_udf`
calls `func` once per group. Zero-copy interchange is `register_arrow` /
`to_arrow`.

Row-at-a-time Python is the registered SQL UDAF path (GIL per value). Compare
the two with `python scripts/bench_udf.py` (not part of `cargo test`).
