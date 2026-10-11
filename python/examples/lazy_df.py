"""Lazy DataFrame example: select, filter, join, group_by, sort, SQL."""

import mizuchi as mz


def main() -> None:
    s = mz.Session()
    s.register("t", {"a": [1, 2, 3], "b": ["x", "y", "z"]})
    s.register("u", {"id": [2, 3], "x": [10, 20]})

    rows = (
        s.table("t")
        .filter("a > 1")
        .join(s.table("u"), "a = id")
        .select("a", "x")
        .sort("a")
        .collect()
    )
    assert rows == [(2, 10), (3, 20)], rows

    sql_rows = s.sql("SELECT a FROM t WHERE a IS NOT NULL ORDER BY a").collect()
    assert sql_rows == [(1,), (2,), (3,)], sql_rows

    grouped = s.table("t").group_by("b").agg("COUNT(*) AS c").sort("b").collect()
    assert grouped == [("x", 1), ("y", 1), ("z", 1)], grouped

    # CAST AS INTEGER is Arrow Int32; collect still returns Python int.
    casted = s.sql("SELECT CAST(a AS INTEGER) AS a FROM t ORDER BY a").collect()
    assert casted == [(1,), (2,), (3,)], casted

    qualified = (
        s.table("t")
        .filter("a > 1")
        .join(s.table("u"), "l.a = r.id")
        .select("a", "x")
        .sort("a")
        .collect()
    )
    assert qualified == [(2, 10), (3, 20)], qualified

    try:
        s.register("n", {"a": [None, None]})
        raise AssertionError("expected all-null infer to fail")
    except ValueError:
        pass

    try:
        s.register("big", {"a": [2**63]})
        raise AssertionError("expected oversized int to fail")
    except ValueError:
        pass

    print("ok")


if __name__ == "__main__":
    main()
