//! Serve a caller-provided console directory at `/ui/` (ADR 0014).
//!
//! The console is a relative-URL zip. Mizuchi never vendors its source; the
//! embedder unpacks a release (or a local `pnpm build`) and points
//! [`super::query_api::QueryApiConfig::ui_dir`] at that folder.

use std::path::{Component, Path};

/// Strict CSP: same-origin API calls only. No extra connect origins.
pub const CSP: &str = "default-src 'self'; connect-src 'self'; object-src 'none'; \
     base-uri 'self'; form-action 'self'; frame-ancestors 'none'";

/// HTTP body for a console path, or a redirect to `ui/`.
pub struct UiResponse {
    pub status: u16,
    pub content_type: &'static str,
    pub cache_control: &'static str,
    pub location: Option<&'static str>,
    pub body: Vec<u8>,
}

/// `GET /` and `GET /ui` are relative redirects; `GET /ui/` and below are files.
pub fn is_ui_route(path: &str) -> bool {
    let p = path.split('?').next().unwrap_or(path);
    p == "/" || p == "/ui" || p == "/ui/" || p.starts_with("/ui/")
}

/// Resolve a request path against `root`. `rel` is the part after `/ui/`.
pub fn serve(root: &Path, rel: &str) -> UiResponse {
    let requested = decode_rel(rel);
    if !requested.is_empty() && !is_plain_relative(&requested) {
        return not_found();
    }
    match read_file(root, &requested) {
        Some(body) => file_response(&requested, body),
        None if is_asset_path(&requested) => not_found(),
        None => match read_file(root, "index.html") {
            Some(body) => file_response("index.html", with_relative_base(body, &requested)),
            None => not_found(),
        },
    }
}

fn decode_rel(rel: &str) -> String {
    let b = rel.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => {
                let hi = from_hex(b[i + 1]);
                let lo = from_hex(b[i + 2]);
                match (hi, lo) {
                    (Some(h), Some(l)) => {
                        out.push((h << 4) | l);
                        i += 3;
                    }
                    _ => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

fn from_hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn is_plain_relative(name: &str) -> bool {
    !name.contains('\\')
        && Path::new(name)
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
}

fn is_asset_path(name: &str) -> bool {
    name.starts_with("assets/") || content_type(name) != "application/octet-stream"
}

fn content_type(name: &str) -> &'static str {
    match name.rsplit('.').next().unwrap_or_default() {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" | "map" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

fn read_file(root: &Path, name: &str) -> Option<Vec<u8>> {
    let path = if name.is_empty() {
        root.join("index.html")
    } else {
        root.join(name)
    };
    let meta = std::fs::symlink_metadata(&path).ok()?;
    if !meta.file_type().is_file() {
        return None;
    }
    let canonical = path.canonicalize().ok()?;
    let root_canonical = root.canonicalize().ok()?;
    if !canonical.starts_with(&root_canonical) {
        return None;
    }
    std::fs::read(path).ok()
}

fn file_response(name: &str, body: Vec<u8>) -> UiResponse {
    let served = if name.is_empty() { "index.html" } else { name };
    UiResponse {
        status: 200,
        content_type: content_type(served),
        cache_control: if served.starts_with("assets/") {
            "public, max-age=31536000, immutable"
        } else {
            "no-cache"
        },
        location: None,
        body,
    }
}

fn not_found() -> UiResponse {
    UiResponse {
        status: 404,
        content_type: "application/json",
        cache_control: "no-cache",
        location: None,
        body: br#"{"error":"not found"}"#.to_vec(),
    }
}

fn with_relative_base(html: Vec<u8>, requested: &str) -> Vec<u8> {
    let depth = requested.matches('/').count();
    match base_insert_offset(&html) {
        Some(at) if depth > 0 => insert_base(&html, at, depth),
        _ => html,
    }
}

fn base_insert_offset(html: &[u8]) -> Option<usize> {
    if find_ignore_ascii_case(html, b"<base").is_some() {
        return None;
    }
    find_ignore_ascii_case(html, b"<head>").map(|at| at + b"<head>".len())
}

fn find_ignore_ascii_case(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window.eq_ignore_ascii_case(needle))
}

fn insert_base(html: &[u8], at: usize, depth: usize) -> Vec<u8> {
    let base = format!("<base href=\"{}\">", "../".repeat(depth));
    let mut out = Vec::with_capacity(html.len() + base.len());
    out.extend_from_slice(&html[..at]);
    out.extend_from_slice(base.as_bytes());
    out.extend_from_slice(&html[at..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn console_dir() -> PathBuf {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "mizuchi-ui-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(root.join("assets")).unwrap();
        std::fs::write(
            root.join("index.html"),
            "<!doctype html><html><head><title>mizuchi</title><script type=\"module\" src=\"./assets/app.js\"></script></head></html>",
        )
        .unwrap();
        std::fs::write(root.join("assets/app.js"), "export {}").unwrap();
        root
    }

    #[test]
    fn index_and_asset_are_served() {
        let root = console_dir();
        let index = serve(&root, "");
        assert_eq!(index.status, 200);
        assert!(index.content_type.starts_with("text/html"));
        assert!(String::from_utf8_lossy(&index.body).contains("mizuchi"));
        let asset = serve(&root, "assets/app.js");
        assert_eq!(asset.status, 200);
        assert_eq!(asset.content_type, "text/javascript; charset=utf-8");
        assert_eq!(asset.cache_control, "public, max-age=31536000, immutable");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn client_route_gets_index_with_base() {
        let root = console_dir();
        let page = serve(&root, "queries/q-1");
        assert_eq!(page.status, 200);
        let html = String::from_utf8_lossy(&page.body);
        assert!(html.contains("<base href=\"../\">"), "{html}");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn missing_asset_is_404_and_traversal_is_refused() {
        let root = console_dir();
        assert_eq!(serve(&root, "assets/missing.js").status, 404);
        assert_eq!(serve(&root, "../Cargo.toml").status, 404);
        assert_eq!(serve(&root, "%2e%2e/Cargo.toml").status, 404);
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_inside_root_is_refused() {
        let root = console_dir();
        std::os::unix::fs::symlink(root.join("index.html"), root.join("alias.html")).unwrap();
        assert_eq!(serve(&root, "alias.html").status, 404);
        let _ = std::fs::remove_dir_all(root);
    }
}
