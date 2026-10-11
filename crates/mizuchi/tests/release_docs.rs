//! Default-test contract for the release ADR and dry-run workflow.

use std::fs;
use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    fs::read_to_string(root().join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))
}

#[test]
fn adr_records_surface_semver_and_deprecation() {
    let text = read("docs/decisions/0007-api-stability-and-releases.md");
    for needle in [
        "Public surface",
        "crates/mizuchi/src/lib.rs",
        "0.y.z",
        "1.0.0",
        "Deprecation",
        "CHANGELOG.md",
        "cargo publish",
        "Linux",
        "macOS",
        "Windows",
        "crates.io",
        "PyPI",
    ] {
        assert!(text.contains(needle), "ADR missing {needle}");
    }
}

#[test]
fn release_workflow_is_tag_dispatch_dry_run() {
    let yml = read(".github/workflows/release.yml");
    assert!(yml.contains("workflow_dispatch"));
    assert!(yml.contains("tags:"));
    assert!(
        !yml.contains("pull_request"),
        "release workflow must not run on pull_request"
    );
    assert!(yml.contains("cargo publish --locked --dry-run -p mizuchi"));
    for os in ["ubuntu-latest", "macos-latest", "windows-latest"] {
        assert!(yml.contains(os), "wheels matrix missing {os}");
    }
    assert!(yml.contains("maturin build"));
}

#[test]
fn library_crate_is_publishable() {
    let toml = read("crates/mizuchi/Cargo.toml");
    assert!(
        toml.contains("publish = true"),
        "mizuchi crate must allow cargo publish dry-run"
    );
    let py = read("python/Cargo.toml");
    assert!(
        py.contains("publish = false"),
        "Python cdylib ships as a wheel, not crates.io"
    );
}
