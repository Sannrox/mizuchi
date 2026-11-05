# mizuchi (蛟)

An Arrow-native query and transform engine written in Rust. Single-node
first; distributed execution ships as experimental, opt-in pieces behind the
same API.

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

## Try it

The complete embedding walkthrough is one command from a clone (the
toolchain is pinned by `rust-toolchain.toml`):

```sh
cargo run -p mizuchi --example embed
```

It opens a capped session, registers data, explains and runs a query, writes
Parquet, and reads it back. Abridged from
[`examples/embed.rs`](crates/mizuchi/examples/embed.rs):

```rust
let cfg = SessionConfig {
    memory_limit: Some(32 * 1024 * 1024),
    workers: Some(1),
    spill_dir: Some(root.join("spill")),
    allowed_read_paths: vec![root.clone()],
    allowed_write_paths: vec![root.clone()],
    ..SessionConfig::default()
};
let mut session = Session::from_config(cfg)?.with_cancel(CancelToken::new());
session.register_parquet("t", root.join("data"), ParquetReadOptions::default())?;
let rows = session.execute_sql("SELECT a, count(*) FROM t GROUP BY a")?;
```

Python bindings and the benchmark suites are under
[Development](#development).

## Architecture

```text
SQL / DataFrame -> logical plan -> optimizer -> physical plan -> execution
                                                     (vectorized, morsel-driven)
```

Workspace crates (ADR [0013](docs/decisions/0013-workspace-crate-graph.md)):
`mizuchi-core` (batches, expressions, cancellation), `mizuchi-plan` (SQL and
optimizer), `mizuchi-io` (files and lake tables), `mizuchi-postgres`,
`mizuchi-exec` (operators and `Session`), `mizuchi-flight` (optional), and
the `mizuchi` facade that embedders depend on.

## Status and roadmap

Planning lives in GitHub Issues and milestones. Shipped today, behind the
API below and the linked decision records:

- Single-node SQL and DataFrame execution: vectorized operators,
  morsel-driven scheduling (ADR
  [0015](docs/decisions/0015-morsel-pipeline-scheduler.md)), spill,
  Parquet/CSV/JSON/Avro/ORC, lake tables.
- Optimizer: rule-based rewrites, predicate and projection pushdown,
  cost-based inner-join ordering.
- Correctness and benchmarks: sqllogictest coverage
  ([docs/sql-coverage.md](docs/sql-coverage.md)) and committed comparisons
  against reference engines ([docs/bench](docs/bench/results.md)).
- Streaming: continuous micro-batch queries with event-time watermarks and
  checkpointed sinks (ADR
  [0009](docs/decisions/0009-stream-watermarks-and-checkpoints.md)).
- Distributed execution, experimental and opt-in: Arrow Flight workers (ADR
  [0011](docs/decisions/0011-flight-workers.md)), task retry on worker loss,
  leased and fenced coordinator leadership with standby takeover (ADR
  [0016](docs/decisions/0016-fenced-coordinator-lease.md)), and batch queries
  that resume from their last committed stage after a takeover. Takeover,
  fencing and stage resume are integration-tested; distributed performance
  and operational readiness are not yet claimed.
- Python bindings as a development workflow.

The gating bar is unchanged (ADR 0002): single-node correctness, performance
and resource behavior under an embedding runner's limits. Open work toward
that bar is tracked in
[#209](https://github.com/Sannrox/mizuchi/issues/209) (aggregate spill under
a memory cap), [#210](https://github.com/Sannrox/mizuchi/issues/210) (memory
accounting and the engine's own peak) and
[#211](https://github.com/Sannrox/mizuchi/issues/211) (a constrained
benchmark profile). Later: caching, incremental computation, GPU research
(ADR [0006](docs/decisions/0006-gpu-acceleration.md)).

## Embedding

### Sessions and limits

Runners open a [`Session`](crates/mizuchi-exec/src/lib.rs) from
`SessionConfig` (memory limit, worker count, spill directory, allowed
read/write paths, network grant), register Arrow record batches,
Parquet/CSV/JSON/Avro/ORC files, Delta or Iceberg snapshots, or an
Arrow C stream, and pull result batches. Cooperative cancellation is a
`CancelToken`. The engine opens TCP only when `SessionConfig.allow_network`
is set (ADR [0008](docs/decisions/0008-network-sources-require-grant.md)).
Runners apply OS filesystem, memory, and CPU limits; mizuchi honors
`SessionConfig` and returns `Error::Io` or `Error::Memory` when a grant is
denied. `mizuchi::VERSION` and `Session::explain` are the strings a runner
records.

### Formats and lake tables

Parquet reads project to requested columns, skip row groups and pages that
cannot match (`ParquetReadOptions::filters`), and late-materialize remaining
columns. `write_delta` / `write_iceberg` commit a new snapshot; `TimeTravel`
reads a past version. The same commits run on a caller-supplied
`ObjectStore` (`write_delta_store` / `write_iceberg_store`).
`IcebergRestCatalog` is a `CatalogProvider` for the Iceberg REST protocol
(TCP only with `allow_network`). `Session::merge_sql` runs `MERGE INTO`.

### Catalog and DDL

`Catalog` resolves `catalog.namespace.table` (default `mizuchi.public`),
optionally from a directory of JSON + Arrow schema files
(`Catalog::open_file`). `Session::execute_ddl` runs `CREATE` / `DROP` /
`ALTER TABLE`; `SHOW TABLES` lists a namespace (ADR
[0010](docs/decisions/0010-pluggable-catalog.md)).

### Streaming

`Session::stream_query` opens a continuous micro-batch query (`StreamQuery`)
with event-time watermarks, windows, and a checkpointed Parquet sink (ADR
[0009](docs/decisions/0009-stream-watermarks-and-checkpoints.md)).

### Reports and the query API

`execute_sql_reported` / `write_parquet_reported` return a `QueryReport`
(inputs after partition prune, outputs, version, plan).
`SessionConfig.query_api` (off by default) records query history and serves
a versioned HTTP API (`/v1`, contract `mizuchi.query-api/v1` in `/healthz`)
so external UIs can list completed queries, inspect plans and operator
metrics, watch running queries, and cancel (ADR
[0012](docs/decisions/0012-query-history-api.md)). The JSON schema is
`crates/mizuchi-exec/schemas/query-api-v1.json`. Bind address, optional
bearer token or HS256 JWT/JWKS verifier, history path, and optional unpacked
console directory (`ui_dir`, served at `/ui/`) are set by the embedder (ADR
[0014](docs/decisions/0014-serve-console-at-ui.md)); loopback listens do not
require `allow_network`.

### Governance

`SessionConfig.governed` plus a `PolicyContext` (allowed tables, row
filters, column masks, opaque table and column markings) apply in planning;
off by default. Markings propagate through the plan into query and write
reports. Governed writes refuse a destination whose declared labels do not
cover the output.

### Distributed execution (experimental)

`Cluster` sizes in-process workers, scales them, and may run speculative
task copies. Flight workers (`--features flight`, ADR
[0011](docs/decisions/0011-flight-workers.md)) are separate processes; the
engine binds TCP only with `allow_network`. Coordinator leadership is a
leased, fenced singleton with standby takeover: every assignment carries a
fencing token, workers refuse work from a superseded coordinator, and a
batch query resumes from its last committed stage after a takeover (ADR
[0016](docs/decisions/0016-fenced-coordinator-lease.md)). Without a lease
directory a coordinator runs unfenced, as a single-coordinator deployment.
Layouts for a runner to deploy are under `deploy/`. Optional Flight tests
use `.github/workflows/flight.yml` (`workflow_dispatch` only).

## Releases

Pre-1.0. The public surface, SemVer rules, deprecation window, and tag
job are [ADR 0007](docs/decisions/0007-api-stability-and-releases.md).
The `mizuchi` crate is packagable (`publish = true`); the Python cdylib
stays `publish = false` and ships as a wheel. crates.io and PyPI stay
unpublished while a workspace crate pins a git dependency.

1. Run `scripts/changelog-draft.sh <version>`, edit the section it prints
   from the merged pull requests, and commit it to `CHANGELOG.md` as
   `## [<version>]`.
2. Tag `v<version>` on `main` (0.1 is experimental). Cut
   `v<version>-rc.N` first; it publishes as a pre-release.
3. The tag push starts main's `.github/workflows/release.yml` for that tag.
   It packages crates, builds Linux/macOS/Windows wheels, attests
   `SHA256SUMS`, publishes a GitHub Release, and pushes
   `ghcr.io/sannrox/mizuchi:<version>`. `workflow_dispatch` without a tag
   packages crates and wheels only. [RELEASING.md](RELEASING.md) covers
   retries and release candidates.
4. Tenkai reads `deploy/tenkai.toml` from the tag. The image tag matches
   `product.version`.

## Development

```sh
cargo fmt --all -- --check
cargo build --locked --workspace --all-targets
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
bash scripts/validate-workflow-skills.sh
```

Optional feature tests ship as manual workflows: Linux filesystem-limit
tests (`--features sandbox`, `.github/workflows/sandbox.yml`), PostgreSQL
source tests (`--features postgres`, `.github/workflows/postgres.yml`), and
lake REST HTTP (`.github/workflows/lake-store.yml`), all
`workflow_dispatch` only. Object-store S3 emulator tests (`--features s3 --
--ignored`) run in default CI against MinIO; the job fails if they are
skipped.

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
