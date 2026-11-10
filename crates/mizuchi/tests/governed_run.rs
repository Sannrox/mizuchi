//! Keeps `docs/governed-run.md` and its example in step with the engine.

#[path = "../examples/governed_run.rs"]
#[allow(dead_code)]
mod example;

#[test]
fn governed_run_example_passes() {
    example::main().unwrap();
}

#[test]
fn page_quotes_the_example() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let page = std::fs::read_to_string(root.join("docs/governed-run.md")).unwrap();
    assert!(page.contains("crates/mizuchi/examples/governed_run.rs"));
    for field in [
        "engine_version",
        "principal",
        "actor",
        "plan",
        "inputs",
        "partitions",
        "column_markings",
        "peak_bytes",
        "outputs",
        "commit_id",
    ] {
        assert!(page.contains(field), "page does not mention {field}");
    }
}
