# A governed run, end to end

Caps, grants, the policy context and the query report each have their own
reference. This page puts them into one run: an embedding runner opens a
session, runs one query over Parquet, writes the result, and records what the
engine reports.

The code is [`crates/mizuchi/examples/governed_run.rs`](../crates/mizuchi/examples/governed_run.rs).
Every claim below is an assertion in that file, and the default `cargo test`
runs it (`crates/mizuchi/tests/governed_run.rs`), so this page cannot drift
from the engine. Run it alone with:

```sh
cargo run -p mizuchi --example governed_run
```

## 1. Session: cap and grants

```rust
let cfg = SessionConfig {
    memory_limit: Some(CAP),                     // 64 MiB session pool
    workers: Some(1),
    spill_dir: Some(root.join("spill")),
    allowed_read_paths: vec![root.join("in")],
    allowed_write_paths: vec![root.join("out")],
    ..SessionConfig::default()
};
let mut session = Session::from_config(cfg)?;
session.register_parquet("orders", &input, ParquetReadOptions::default())?;
```

The engine reads only below `in/` and writes only below `out/`. Work that
outgrows the cap spills to `spill/` or fails with a typed memory error; it
never exceeds the cap.

## 2. Policy context

```rust
let policy = PolicyContext::new("acme", "analyst@acme", "nightly-runner")
    .allow_table("orders")
    .row_filter("orders", "region = 'eu'")
    .mask_column("orders", "ssn")
    .mark_table("orders", "tenant:acme")
    .mark_column("orders", "customer", "pii")
    .mark_table("totals", "tenant:acme")
    .mark_column("totals", "customer", "pii")
    .mark_column("totals", "total", "pii");
let session = session.with_policy(policy).with_governed(true);
```

- `allow_table`: tables this principal may scan. Others fail at planning.
- `row_filter`: a predicate added to every scan of the table.
- `mask_column`: the column is removed from the planning catalog, so
  `SELECT ssn FROM orders` fails to plan and `SELECT *` does not return it.
- `mark_table` / `mark_column`: opaque labels. The engine propagates them to
  output columns and checks them on writes; it never interprets them.
- `with_governed(true)`: every query and write must carry a policy context.

The runner owns the policy document. The engine applies it at plan time.

## 3. One query and its report

```rust
let result = session.execute_sql_reported(
    "SELECT customer, SUM(amount) AS total FROM orders \
     WHERE year = 2026 GROUP BY customer ORDER BY customer",
)?;
```

`result.report` is a `QueryReport`:

| Field | In this run |
| --- | --- |
| `engine_version` | the crate version, `mizuchi::VERSION` |
| `principal`, `actor` | `analyst@acme`, `nightly-runner`, from the policy context |
| `plan` | the executed logical and physical plan text |
| `inputs` | one scan of `orders`: `partitions` is `["year=2026"]` after pruning, and `columns` never includes `ssn` |
| `column_markings` | `customer`: `pii`, `tenant:acme`. `total`: `pii`, `tenant:acme` |

`total` carries `pii` although `amount` is unmarked: the `customer` grouping
key decides which rows each total sums, so its labels flow into every
aggregate.

Next to the report, `result.peak_bytes` is the session pool's high water for
this query, at or below the cap, and `result.spilled_bytes` counts spill file
bytes. A runner records both with the report.

## 4. Write

```rust
let write = session.write_parquet_reported_to(
    &output,
    "totals",
    &result.batches,
    &report.column_markings,
    ParquetWriteOptions::default(),
)?;
```

A governed write names its destination table. The labels declared for
`totals` must cover every label on the written columns, otherwise the write
is refused before any file is committed. The returned report lists `outputs`
with the path, rows, bytes and a deterministic `commit_id`, and repeats the
column markings that were written.

## 5. What is refused

- `SELECT ssn FROM orders`: the masked column does not exist for this
  principal.
- A write outside `allowed_write_paths`.
- A write whose column labels the destination table does not declare.
