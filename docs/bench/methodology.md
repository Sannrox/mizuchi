# Gigabyte-scale decision-support methodology

Release-profile runs of the TPC-H and TPC-DS-style query set at SF1 and SF10,
compared with DuckDB and DataFusion on the same generated Parquet and the same
host. Reproduce with `bash scripts/bench-gb.sh 1` and `bash scripts/bench-gb.sh 10`.
The optional GitHub workflow `.github/workflows/bench-gb.yml` smokes `--sf tiny`
on `workflow_dispatch` or a weekly schedule. It is not part of pull-request CI
and not part of default `cargo test`.

## Hardware

- single-node workstation, 32 GiB RAM, 10 host-reported threads.
- No other jobs pinned to the run.

## Software

- mizuchi 0.1.0, Rust 1.97.1, `--release`, Arrow/Parquet 60.0.0.
- DuckDB 1.5.6 (Python package).
- DataFusion 54.1.0 (Python package) with pyarrow 26.
- Python 3.11 for the reference runner.

## Data generation

`cargo run --release --example gb_bench -- generate --sf <tiny|1|10> --out bench-data`
writes Snappy Parquet under `bench-data/<label>/`. The generator is the same
deterministic loop as `tests/ds_common`, chunked at 65_536 rows. `date_dim`
stays 12 rows at every scale.

| label | generator `s` | customer | orders | lineitem | item | store_sales | date_dim |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tiny | 1 | 8 | 16 | 48 | 8 | 24 | 12 |
| sf1 | 125_000 | 1_000_000 | 2_000_000 | 6_000_000 | 1_000_000 | 3_000_000 | 12 |
| sf10 | 1_250_000 | 10_000_000 | 20_000_000 | 60_000_000 | 10_000_000 | 30_000_000 | 12 |

SF1 `lineitem` is six million rows (TPC-H SF1 order of magnitude). SF10 is ten
times that, except `date_dim`. Files under `bench-data/` are gitignored; only
this method and the committed results table are in git.

## Execution

1. Generate Parquet once per scale.
2. Each engine loads every table into memory (mizuchi `Session::register` after
   `read_parquet`; DuckDB `CREATE TABLE AS SELECT * FROM read_parquet`;
   DataFusion `register_record_batches` from a pyarrow table). Load time is
   excluded from query times.
3. Run the four queries from `crates/mizuchi/examples/gb_bench.rs` (same SQL as
   `tests/ds_common`).
4. Three trials per query. The first trial is warmup and is dropped. The
   published number is the median of the remaining two.
5. mizuchi uses `Session::with_workers` set to host-reported parallelism.
   DuckDB and DataFusion use their package defaults. A scaling table at SF1
   reruns the same queries with `--workers 1`, `2`, `4`, and `8`.
6. mizuchi clones the session per trial so the result cache cannot reuse a
   previous answer.

Ratio in the results table is `reference_ms / mizuchi_ms` (same convention as
`tests/engine_compare.rs`). Values below 1 mean the reference finished faster.

## Operator profiles

`Session::query_metrics` records inclusive `elapsed_ns` and pool `peak_bytes`
per physical operator. Exclusive time is inclusive minus the children's
inclusive times. Rank exclusive time for the CPU profile of the plan.
`peak_bytes` is the allocation profile: join and aggregate reserve only when
a `memory_limit` is set, so `gb_bench profile` uses a 24 GiB cap. `gb_bench
run` stays unbounded so SF10 wall times match [results.md](results.md).
Reproduce a
Parquet run with `bash scripts/profile-suite.sh tiny` (or `1` / `10` when
those scales are generated). Default `cargo test` uses the tiny in-memory
suite (`tests/suite_profile.rs`), not SF1 files. Findings and the ranked
limiters are in [limiters.md](limiters.md).

## What this is not

- Not the default offline `cargo test` bar.
- Not a GPU run (see [ADR 0006](../decisions/0006-gpu-acceleration.md)).
- Not a claim of TPC-H/TPC-DS certification; the schema and predicates are the
  project's decision-support subset with integer dates.
