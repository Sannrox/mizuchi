//! Session resource config for embedding runners.

use std::path::{Path, PathBuf};

use mizuchi_core::error::{Error, Result};
use mizuchi_io::EncryptionSession;

/// Resource and path limits an embedding runner sets before opening a session.
///
/// Empty allow-lists mean the runner (or the OS sandbox) owns the fence;
/// a non-empty list is checked before the engine opens a file.
#[derive(Clone, Debug, Default)]
pub struct SessionConfig {
    /// Per-query working-set cap in bytes. `None` is unbounded aside from
    /// the process.
    pub memory_limit: Option<u64>,
    /// Morsel and stage worker threads. `None` uses one worker (the pull
    /// iterator path).
    pub workers: Option<usize>,
    /// Directory for operator spill files. Removed per query. Must sit
    /// under [`Self::allowed_write_paths`] when that list is non-empty.
    pub spill_dir: Option<PathBuf>,
    /// Directory for shuffle IPC files. Must sit under
    /// [`Self::allowed_write_paths`] when that list is non-empty.
    pub shuffle_dir: Option<PathBuf>,
    /// Roots that [`super::Session::register_parquet`],
    /// [`super::Session::register_csv`], Delta/Iceberg reads, and other
    /// session reads may open.
    pub allowed_read_paths: Vec<PathBuf>,
    /// Roots that [`super::Session::write_parquet`], Delta, and Iceberg
    /// writes may create.
    pub allowed_write_paths: Vec<PathBuf>,
    /// When true, [`super::Session::register_postgres`] may open TCP to a
    /// host the runner supplied. Default is false. An empty path allow-list
    /// does not grant network.
    pub allow_network: bool,
    /// Query-start clock in microseconds since the Unix epoch. `CURRENT_DATE`
    /// and `NOW()` bind to this instant. `None` uses the process clock.
    pub now_micros: Option<i64>,
    /// When set, the session records query history and may serve the
    /// versioned metrics HTTP API (ADR 0012). Default is off.
    pub query_api: Option<super::query_api::QueryApiConfig>,
    /// When true, execute refuses a missing [`super::PolicyContext`].
    /// Default is off.
    pub governed: bool,
    /// Policy applied in planning. Default is none (ungoverned).
    pub policy: Option<super::PolicyContext>,
    /// Caller-supplied keys for table files and query spills. Default is off
    /// (plaintext, same as today).
    pub encryption: Option<EncryptionSession>,
}

pub(super) fn resolve_roots(paths: Vec<PathBuf>) -> Result<Vec<PathBuf>> {
    paths.iter().map(|p| resolve(p)).collect()
}

pub(super) fn assert_allowed(path: &Path, roots: &[PathBuf]) -> Result<()> {
    allowed(path, roots).map(|_| ())
}

/// `path` resolved under one of `roots`, or `path` unchanged when `roots` is
/// empty. Callers that keep the path for later writes keep the resolved one,
/// so the checked location is the written location.
pub(super) fn allowed(path: &Path, roots: &[PathBuf]) -> Result<PathBuf> {
    if roots.is_empty() {
        return Ok(path.to_path_buf());
    }
    let resolved = resolve(path)?;
    if roots.iter().any(|root| resolved.starts_with(root)) {
        return Ok(resolved);
    }
    Err(Error::Io(format!(
        "{} is outside allowed paths",
        resolved.display()
    )))
}

/// Absolute path resolved the way the filesystem resolves it: the longest
/// existing prefix is canonicalized (symlinks and `..` in order, and macOS
/// `/var` vs `/private/var`), and the part that does not exist yet may only
/// name new entries. A dangling symlink counts as existing, so it fails to
/// canonicalize instead of being created through.
fn resolve(path: &Path) -> Result<PathBuf> {
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| Error::Io(e.to_string()))?
            .join(path)
    };
    let mut prefix = abs.as_path();
    let mut suffix = Vec::new();
    while std::fs::symlink_metadata(prefix).is_err() {
        match (prefix.file_name(), prefix.parent()) {
            (Some(name), Some(parent)) => {
                suffix.push(name);
                prefix = parent;
            }
            _ => {
                return Err(Error::Io(format!("{} cannot be resolved", path.display())));
            }
        }
    }
    let mut out = std::fs::canonicalize(prefix)
        .map_err(|e| Error::Io(format!("{}: {e}", prefix.display())))?;
    out.extend(suffix.iter().rev());
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_allow_list_accepts_any_path() {
        assert_allowed(Path::new("/tmp/anywhere"), &[]).unwrap();
    }

    #[test]
    fn path_inside_root_is_allowed() {
        let root = std::env::temp_dir().join(format!("mizuchi-allow-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let file = root.join("in.parquet");
        std::fs::write(&file, b"").unwrap();
        let roots = resolve_roots(vec![root.clone()]).unwrap();
        assert_allowed(&file, &roots).unwrap();
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn path_outside_root_is_rejected() {
        let root = std::env::temp_dir().join(format!("mizuchi-allow-out-{}", std::process::id()));
        let other = std::env::temp_dir().join(format!("mizuchi-deny-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&other).unwrap();
        let file = other.join("secret.parquet");
        std::fs::write(&file, b"").unwrap();
        let roots = resolve_roots(vec![root.clone()]).unwrap();
        let err = assert_allowed(&file, &roots).unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err:?}");
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&other);
    }

    #[test]
    fn parent_escape_is_rejected() {
        let root = std::env::temp_dir().join(format!("mizuchi-allow-esc-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let roots = resolve_roots(vec![root.clone()]).unwrap();
        let err = assert_allowed(&root.join("..").join("nope.parquet"), &roots).unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err:?}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[test]
    fn parent_after_symlink_follows_the_link() {
        let base = std::env::temp_dir().join(format!("mizuchi-allow-link-{}", std::process::id()));
        let root = base.join("root");
        let outside = base.join("outside").join("child");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("link")).unwrap();
        let roots = resolve_roots(vec![root.clone()]).unwrap();
        let escape = root.join("link").join("..").join("spill");
        let err = allowed(&escape, &roots).unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err:?}");
        let inside = allowed(&root.join("new").join("spill"), &roots).unwrap();
        assert!(inside.starts_with(&roots[0]), "{}", inside.display());
        let _ = std::fs::remove_dir_all(&base);
    }
}
