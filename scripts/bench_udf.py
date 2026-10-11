#!/usr/bin/env python3
"""Time vectorized Arrow UDFs against a row-wise Python loop.

Not part of `cargo test`. Needs a built `mizuchi` extension and pyarrow.

    cd python && maturin develop
    python ../scripts/bench_udf.py
"""

from __future__ import annotations

import time

import pyarrow as pa
import pyarrow.compute as pc

import mizuchi as mz

N = 200_000


def add_one(a: pa.Array) -> pa.Array:
    return pc.add(a, 1)


def main() -> None:
    values = list(range(N))
    src = pa.table({"a": pa.array(values, type=pa.int64())})
    s = mz.Session()
    s.register_arrow("t", src)

    t0 = time.perf_counter()
    vec = s.table("t").udf("b", add_one, "a").collect()
    vec_s = time.perf_counter() - t0
    assert len(vec) == N
    assert vec[0] == (0, 1)

    t0 = time.perf_counter()
    rows = s.table("t").collect()
    row = [(a, (a + 1) if a is not None else None) for (a,) in rows]
    row_s = time.perf_counter() - t0
    assert row[0] == (0, 1)

    print(f"rows={N}")
    print(f"vectorized_udf_s={vec_s:.4f}")
    print(f"row_python_s={row_s:.4f}")
    if row_s > 0:
        print(f"speedup={row_s / vec_s:.2f}")


if __name__ == "__main__":
    main()
