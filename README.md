# mizuchi (蛟)

An Arrow-native query and transform engine written in Rust. Single-node first,
distributed later.

> Status: side project, experimental 0.1. Embedding runners should keep using
> their current engines until mizuchi passes a defined bar
> (see [ADR 0002](docs/decisions/0002-own-query-engine.md)). A 0.y release may
> break the public surface.

## Goals

- **Arrow in, Arrow out.** Columnar memory is standard Apache Arrow, so inputs
  and results move between tools without conversion.
- **Embeddable.** A library first; a runner such as
  [kako](https://github.com/Sannrox/kako) can plug it in behind its job
  contract.
- **Bounded resources.** Memory accounting, spill to disk, cancellation and
  thread limits that an embedding sandbox can rely on.
- **Explainable.** Plans and executed operators are inspectable, so a runner can
  record what ran (lineage).
- **Measured.** Correctness via sqllogictest
  ([coverage](docs/sql-coverage.md)) and benchmark comparisons against DuckDB
  and DataFusion.

## Architecture (planned)

```text
SQL / DataFrame -> logical plan -> optimizer -> physical plan -> execution
                                                     (vectorized, morsel-driven)
```

## Roadmap

Tracked as GitHub Issues and milestones:

1. Foundations: Arrow memory model, expressions, SQL parser, logical plan
2. Single-node execution: vectorized operators, Parquet/CSV, spill, scheduling
3. Optimizer: rule-based rewrites, pushdown, cost-based join ordering
4. Correctness and benchmarks
5. Integration with a sandboxed transform runner
6. Distributed execution
7. Python API
8. Advanced: incremental/streaming, caching, GPU research

## Embedding

Runners open a [`Session`](crates/mizuchi/src/exec/mod.rs) from
`SessionConfig` (memory limit, worker count, spill directory, allowed
read/write paths, network grant), register Arrow record batches,
Parquet/CSV/JSON/Avro/ORC files, Delta or Iceberg snapshots, or an
Arrow C stream, and pull result batches. Parquet reads project to
requested columns, skip row groups and pages that cannot match
(`ParquetReadOptions::filters`), and late-materialize remaining columns. Cooperative cancellation is a
`CancelToken`. `execute_sql_reported` / `write_parquet_reported` return
a `QueryReport` (inputs after partition prune, outputs, version, plan).
`write_delta` / `write_iceberg` commit a new snapshot; `TimeTravel`
reads a past version. The same commits run on a caller-supplied
`ObjectStore` (`write_delta_store` / `write_iceberg_store`).
`IcebergRestCatalog` is a `CatalogProvider` for the Iceberg REST
protocol (TCP only with `allow_network`). `Session::merge_sql` runs `MERGE INTO`.
`Session::stream_query` opens a continuous micro-batch query
(`StreamQuery`) with event-time watermarks, windows, and a
checkpointed Parquet sink (ADR 0009).
`Catalog` resolves `catalog.namespace.table` (default `mizuchi.public`),
optionally from a directory of JSON + Arrow schema files
(`Catalog::open_file`). `Session::execute_ddl` runs `CREATE` / `DROP` /
`ALTER TABLE`; `SHOW TABLES` lists a namespace (ADR 0010).
`SessionConfig.query_api` (off by default) records query history and
serves a versioned HTTP API (`/v1`, contract `mizuchi.query-api/v1` in
`/healthz`) so external UIs can list completed queries, inspect plans and
operator metrics, watch running queries, and cancel (ADR 0012). The JSON
schema is `crates/mizuchi/schemas/query-api-v1.json`. Bind address, optional bearer
token, and history path are set by the embedder; loopback listens do not
require `allow_network`.
`mizuchi::VERSION` and `Session::explain` are the strings a runner
records. A complete walk-through is `cargo run -p mizuchi --example embed`.
The engine opens TCP only when `SessionConfig.allow_network` is set
(ADR 0008). Runners apply OS filesystem, memory, and CPU limits; mizuchi
honors `SessionConfig` and returns `Error::Io` or `Error::Memory` when a
grant is denied. Optional Linux filesystem-limit tests use
`--features sandbox` and `.github/workflows/sandbox.yml`
(`workflow_dispatch` only). PostgreSQL source tests use
`--features postgres` and `.github/workflows/postgres.yml`
(`workflow_dispatch` only). Object-store S3 emulator tests (`--features s3
-- --ignored`) run in default CI against MinIO; the job fails if they
are skipped. Lake REST HTTP remains `.github/workflows/lake-store.yml`
(`workflow_dispatch` only). `Cluster` sizes in-process workers, scales
them, and may run speculative task copies. Flight workers
(`--features flight`, ADR 0011) are separate processes; the engine
binds TCP only with `allow_network`. Layouts for a runner to
deploy are under `deploy/`. Optional Flight tests use
`.github/workflows/flight.yml` (`workflow_dispatch` only).

## Releases

Pre-1.0. The public surface, SemVer rules, deprecation window, and tag
dry-run job are [ADR 0007](docs/decisions/0007-api-stability-and-releases.md).
The `mizuchi` crate is packagable (`publish = true`); the Python cdylib
stays `publish = false` and ships as a wheel.

1. Move `CHANGELOG.md` Unreleased notes into a versioned section.
2. Tag `v<version>` on `main` (0.1 is experimental).
3. Wait for `.github/workflows/release.yml` on that tag (or
   `workflow_dispatch`): `cargo publish --locked --dry-run -p mizuchi`
   plus `maturin build` wheels on Linux, macOS, and Windows.
4. A maintainer publishes to crates.io and PyPI only after that job is
   green. The workflow does not upload.

## Development

```sh
cargo fmt --all -- --check
cargo build --locked --workspace --all-targets
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
bash scripts/validate-workflow-skills.sh
```

Columnar kernel micro-benchmarks (filter, projection, aggregate, hash join):

```sh
cargo run --release -p mizuchi --example kernel_micro
```

Python (optional, not part of the default Cargo workspace):

```sh
pip install maturin
cd python && maturin develop
python examples/lazy_df.py
# UDFs and Arrow C Data Interface (optional extra):
pip install pyarrow
python examples/udf_arrow.py
# Vectorized vs row-wise UDF timing (not part of cargo test):
python ../scripts/bench_udf.py
```

Function coverage: [docs/functions.md](docs/functions.md).

Fuzz (nightly, not part of the default Cargo workspace):

```sh
cargo +nightly install cargo-fuzz
cargo +nightly fuzz run parse -- -max_total_time=30
```

Crashes under `fuzz/artifacts/` become regression cases in `crates/mizuchi/tests/fuzz_sql.rs`.

Gigabyte-scale comparison (not part of default `cargo test`):

```sh
bash scripts/bench-gb.sh 1    # SF1
bash scripts/bench-gb.sh 10   # SF10
```

Method and committed results: [docs/bench/methodology.md](docs/bench/methodology.md),
[docs/bench/results.md](docs/bench/results.md). Operator exclusive-time
profiles and ranked limiters: [docs/bench/limiters.md](docs/bench/limiters.md)
(`bash scripts/profile-suite.sh tiny`). Data lands in `bench-data/` (gitignored).
CI may smoke `scripts/bench-gb.sh tiny` from `.github/workflows/bench-gb.yml` on
manual dispatch; pull-request CI does not run it.

Contributor and agent policy: [AGENTS.md](AGENTS.md). Decisions:
[docs/decisions/](docs/decisions/README.md).

## Security

See [SECURITY.md](SECURITY.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
