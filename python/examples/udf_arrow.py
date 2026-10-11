"""Scalar and aggregate Python UDFs plus zero-copy Arrow interchange."""

import pyarrow as pa
import pyarrow.compute as pc

import mizuchi as mz


def add_one(a: pa.Array) -> pa.Array:
    return pc.add(a, 1)


def times_two(a: pa.Array) -> pa.Array:
    return pc.multiply(a, 2)


def sum_udf(a: pa.Array) -> object:
    return pc.sum(a)


def main() -> None:
    src = pa.table({"a": pa.array([1, None, 3], type=pa.int64())})
    src_addr = src.column("a").chunk(0).buffers()[1].address

    s = mz.Session()
    s.register_arrow("t", src)
    out = pa.RecordBatchReader.from_stream(s.table("t").to_arrow()).read_all()
    out_addr = out.column("a").chunk(0).buffers()[1].address
    assert out.column("a").combine_chunks().to_pylist() == [1, None, 3]
    assert src_addr == out_addr, (src_addr, out_addr)

    class CapsuleTable:
        def __init__(self, table: pa.Table) -> None:
            self._t = table

        def __arrow_c_stream__(self, requested_schema=None):
            return self._t.__arrow_c_stream__(requested_schema)

    s.register_arrow("u", CapsuleTable(src))
    wrapped = s.table("u").collect()
    assert wrapped == [(1,), (None,), (3,)], wrapped

    rows = s.table("t").udf("b", add_one, "a").collect()
    assert rows == [(1, 2), (None, None), (3, 4)], rows

    i32_keys = pa.table(
        {
            "k": pa.array([1, 1, 2], type=pa.int32()),
            "v": pa.array([4, 5, 6], type=pa.int64()),
        }
    )
    s.register_arrow("i32g", i32_keys)
    i32_grouped = (
        s.table("i32g").group_by("k").agg_udf("s", sum_udf, "v").sort("k").collect()
    )
    assert i32_grouped == [(1, 9), (2, 6)], i32_grouped

    s.register("g", {"k": ["x", "x", "y", None], "v": [1, 2, 10, 7]})
    grouped = (
        s.table("g")
        .group_by("k")
        .agg_udf("s", sum_udf, "v")
        .sort("k")
        .collect()
    )
    # NULL grouping key is its own group; sort puts NULL last by default.
    assert grouped == [("x", 3), ("y", 10), (None, 7)], grouped

    doubled = s.table("g").udf("v", times_two, "v").sort("v").collect()
    assert doubled == [("x", 2), ("x", 4), (None, 14), ("y", 20)], doubled

    def product_update(state, value):
        if value is None:
            return state
        if state is None:
            return value
        return state * value

    s.register_udaf("product", product_update, product_update, None)
    s.register("p", {"g": [1, 1, 2], "v": [2, 3, 4]})
    prod = s.sql("SELECT g, product(v) AS p FROM p GROUP BY g").sort("g").collect()
    assert prod == [(1, 6), (2, 4)], prod

    arr = s.sql("SELECT ARRAY[1, 2, 3] AS a").collect()
    assert arr == [([1, 2, 3],)], arr

    print("ok")


if __name__ == "__main__":
    main()
