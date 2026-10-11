# mizuchi (蛟)

An Arrow-native query and transform engine written in Rust. Single-node first,
distributed later.

> Status: side project, pre-alpha. Nothing executes yet. Transform runners keep
> using existing engines until mizuchi passes a defined bar
> (see [ADR 0002](docs/decisions/0002-own-query-engine.md)).

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
- **Measured.** Correctness via sqllogictest and benchmark comparisons against
  DuckDB and DataFusion.

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

## Development

```sh
cargo fmt --all -- --check
cargo build --locked --workspace --all-targets
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
bash scripts/validate-workflow-skills.sh
```

Python (optional, not part of the default Cargo workspace):

```sh
pip install maturin
cd python && maturin develop
python examples/lazy_df.py
# UDFs and Arrow C Data Interface (optional extra):
pip install pyarrow
python examples/udf_arrow.py
```

Fuzz (nightly, not part of the default Cargo workspace):

```sh
cargo +nightly install cargo-fuzz
cargo +nightly fuzz run parse -- -max_total_time=30
```

Crashes under `fuzz/artifacts/` become regression cases in `crates/mizuchi/tests/fuzz_sql.rs`.

Contributor and agent policy: [AGENTS.md](AGENTS.md). Decisions:
[docs/decisions/](docs/decisions/README.md).

## Security

See [SECURITY.md](SECURITY.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
