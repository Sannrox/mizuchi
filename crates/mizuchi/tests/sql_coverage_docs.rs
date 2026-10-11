//! Default-test contract for the SQL coverage audit docs and slt files.

use std::fs;
use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    fs::read_to_string(root().join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))
}

#[test]
fn coverage_table_names_audited_families() {
    let text = read("docs/sql-coverage.md");
    for needle in [
        "Window functions",
        "ROW_NUMBER",
        "WITH RECURSIVE",
        "IN (SELECT",
        "EXISTS",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "DECIMAL",
        "DATE",
        "TIMESTAMP",
        "INTERVAL",
        "skipif mizuchi",
        "window.slt",
        "cte.slt",
        "subquery.slt",
        "setops.slt",
        "decimal.slt",
        "datetime.slt",
        "#78",
        "#79",
        "#80",
        "#81",
        "#82",
        "#83",
    ] {
        assert!(text.contains(needle), "coverage table missing {needle}");
    }
}

#[test]
fn per_feature_slt_files_exist_with_skipif_gaps() {
    let slt = root().join("crates/mizuchi/tests/slt");
    for (name, must_skip) in [
        ("window.slt", false),
        ("cte.slt", false),
        ("subquery.slt", true),
        ("setops.slt", true),
        ("decimal.slt", true),
        ("datetime.slt", true),
        ("select.slt", false),
        ("agg.slt", false),
        ("join.slt", false),
    ] {
        let path = slt.join(name);
        assert!(path.is_file(), "missing {name}");
        let text = fs::read_to_string(&path).unwrap();
        if must_skip {
            assert!(
                text.contains("skipif mizuchi"),
                "{name} should record skipif mizuchi gaps"
            );
        }
    }
    let sub = fs::read_to_string(slt.join("subquery.slt")).unwrap();
    assert!(
        sub.contains("SELECT a FROM (SELECT a FROM t) s"),
        "derived table should be a supported subquery.slt record"
    );
    let dec = fs::read_to_string(slt.join("decimal.slt")).unwrap();
    assert!(
        dec.contains("SELECT CAST(1 AS TEXT)"),
        "integer/text CAST should be a supported decimal.slt record"
    );
}
