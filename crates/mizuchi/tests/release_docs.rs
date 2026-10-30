//! Default-test contract for the release ADR and publishing workflow.

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
        "GitHub Release",
        "deploy/tenkai.toml",
    ] {
        assert!(text.contains(needle), "ADR missing {needle}");
    }
}

#[test]
fn release_workflow_publishes_on_tag() {
    let yml = read(".github/workflows/release.yml");
    assert!(yml.contains("workflow_dispatch"));
    assert!(yml.contains("tags:"));
    assert!(
        !yml.contains("pull_request"),
        "release workflow must not run on pull_request"
    );
    assert!(yml.contains("cargo publish --locked --dry-run -p mizuchi-core"));
    assert!(yml.contains("cargo package --list --locked -p"));
    for os in ["ubuntu-latest", "macos-latest", "windows-latest"] {
        assert!(yml.contains(os), "wheels matrix missing {os}");
    }
    assert!(yml.contains("maturin build"));
    assert!(yml.contains("gh release create"));
    assert!(yml.contains("attest-build-provenance"));
    assert!(yml.contains("SHA256SUMS"));
    assert!(yml.contains("ghcr.io/sannrox/mizuchi"));
    assert!(
        !yml.contains(":latest"),
        "release must not push a latest image tag"
    );
}

#[test]
fn first_release_notes_and_tenkai_entry_match_workspace_version() {
    let version = workspace_version();
    assert_eq!(version, "0.1.0", "first release is 0.1.0");
    let changelog = read("CHANGELOG.md");
    assert!(
        changelog.contains(&format!("## [{version}]")),
        "CHANGELOG.md missing ## [{version}]"
    );
    let pyproject = read("python/pyproject.toml");
    assert!(
        pyproject.contains(&format!("version = \"{version}\"")),
        "python/pyproject.toml version must match workspace {version}"
    );
    let tenkai = read("deploy/tenkai.toml");
    assert!(
        tenkai.contains(&format!("version = \"{version}\"")),
        "deploy/tenkai.toml product.version must match workspace {version}"
    );
    assert!(
        tenkai.contains(&format!("ghcr.io/sannrox/mizuchi:{version}")),
        "deploy/tenkai.toml must pin ghcr.io/sannrox/mizuchi:{version}"
    );
    assert!(
        !tenkai.contains("latest"),
        "Tenkai entry must not reference latest"
    );
    let dockerfile = read("deploy/Dockerfile");
    assert!(
        dockerfile.contains("--release"),
        "release image must be a --release build"
    );
    assert!(
        dockerfile.contains("--features flight"),
        "release image must build the Flight binary"
    );
}

fn workspace_version() -> String {
    for line in read("Cargo.toml").lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("version = \"")
            && let Some(end) = rest.find('"')
        {
            return rest[..end].to_string();
        }
    }
    panic!("workspace version missing from Cargo.toml");
}

#[test]
fn library_crate_is_publishable() {
    let toml = read("crates/mizuchi/Cargo.toml");
    for needle in [
        "publish = true",
        "license.workspace = true",
        "readme = \"../../README.md\"",
        "repository.workspace = true",
        "keywords = [",
    ] {
        assert!(toml.contains(needle), "crate metadata missing {needle}");
    }
    let py = read("python/Cargo.toml");
    assert!(
        py.contains("publish = false"),
        "Python cdylib ships as a wheel, not crates.io"
    );
    let pyproject = read("python/pyproject.toml");
    assert!(
        pyproject.contains("readme = \"README.md\""),
        "Python project must ship a README inside python/"
    );
    assert!(
        root().join("python/README.md").is_file(),
        "python/README.md missing"
    );
}

#[test]
fn python_lock_names_the_path_crate() {
    let lock = read("python/Cargo.lock");
    assert!(
        lock.contains("name = \"mizuchi\"") && lock.contains("name = \"mizuchi-py\""),
        "python/Cargo.lock must record the path crate"
    );
}

#[test]
fn package_list_includes_readme() {
    let out = std::process::Command::new("cargo")
        .args([
            "package",
            "--list",
            "--locked",
            "--offline",
            "--allow-dirty",
            "-p",
            "mizuchi",
        ])
        .current_dir(root())
        .output()
        .expect("cargo package --list");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let list = String::from_utf8_lossy(&out.stdout);
    for name in ["README.md", "src/lib.rs", "Cargo.toml"] {
        assert!(
            list.lines().any(|l| l == name),
            "package list missing {name}:\n{list}"
        );
    }
    let exec = std::process::Command::new("cargo")
        .args([
            "package",
            "--list",
            "--locked",
            "--offline",
            "--allow-dirty",
            "-p",
            "mizuchi-exec",
        ])
        .current_dir(root())
        .output()
        .expect("cargo package --list -p mizuchi-exec");
    assert!(
        exec.status.success(),
        "{}",
        String::from_utf8_lossy(&exec.stderr)
    );
    let exec_list = String::from_utf8_lossy(&exec.stdout);
    assert!(
        exec_list.lines().any(|l| l == "schemas/query-api-v1.json"),
        "mizuchi-exec package list missing schema:\n{exec_list}"
    );
}
