//! Session resource config for embedding runners.

use std::path::{Component, Path, PathBuf};

use crate::error::{Error, Result};

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
    /// Directory for operator spill files. Removed per query.
    pub spill_dir: Option<PathBuf>,
    /// Directory for shuffle IPC files.
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
}

pub(super) fn resolve_roots(paths: Vec<PathBuf>) -> Result<Vec<PathBuf>> {
    paths.iter().map(|p| resolve(p)).collect()
}

pub(super) fn assert_allowed(path: &Path, roots: &[PathBuf]) -> Result<()> {
    if roots.is_empty() {
        return Ok(());
    }
    let path = resolve(path)?;
    for root in roots {
        if path.starts_with(root) {
            return Ok(());
        }
    }
    Err(Error::Io(format!(
        "{} is outside allowed paths",
        path.display()
    )))
}

/// Absolute, `..`-normalized path. Canonicalizes the longest existing prefix
/// so allow-list roots on macOS (`/var` vs `/private/var`) still match.
fn resolve(path: &Path) -> Result<PathBuf> {
    let abs = absolute(path)?;
    let mut prefix = abs.clone();
    let mut suffix = PathBuf::new();
    while !prefix.as_os_str().is_empty() && !prefix.exists() {
        match (prefix.file_name(), prefix.parent()) {
            (Some(name), Some(parent)) => {
                let mut next = PathBuf::from(name);
                next.push(&suffix);
                suffix = next;
                prefix = parent.to_path_buf();
            }
            _ => break,
        }
    }
    if prefix.exists() {
        let mut out = std::fs::canonicalize(&prefix)
            .map_err(|e| Error::Io(format!("{}: {e}", prefix.display())))?;
        out.push(suffix);
        Ok(out)
    } else {
        Ok(abs)
    }
}

fn absolute(path: &Path) -> Result<PathBuf> {
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| Error::Io(e.to_string()))?
            .join(path)
    };
    Ok(normalize(&abs))
}

fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::Prefix(p) => out.push(p.as_os_str()),
            Component::RootDir => out.push(c.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                let _ = out.pop();
            }
            Component::Normal(s) => out.push(s),
        }
    }
    out
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
}
