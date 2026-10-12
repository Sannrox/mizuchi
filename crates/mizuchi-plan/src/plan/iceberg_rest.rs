//! Iceberg REST catalog backend for [`super::CatalogProvider`].
//!
//! In-process memory is the default. [`IcebergRestCatalog::connect`] opens TCP
//! only when the embedder passes `allow_network`. The engine never reads
//! credentials from the environment.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use serde_json::{Value, json};

use super::{CatalogProvider, DEFAULT_CATALOG, NamespaceIdent, TableIdent, TableMeta, TableStats};
use mizuchi_core::error::{Error, Result};
use mizuchi_core::schema::from_arrow_schema;

/// Iceberg REST catalog: memory, or HTTP when network is granted.
pub struct IcebergRestCatalog {
    catalog: String,
    inner: RestInner,
}

enum RestInner {
    Memory(Arc<Mutex<RestState>>),
    Http(HttpClient),
}

struct HttpClient {
    host: String,
    port: u16,
    path_prefix: String,
}

#[derive(Clone, Default)]
struct RestState {
    namespaces: BTreeMap<String, ()>,
    tables: BTreeMap<String, RestTable>,
}

#[derive(Clone)]
struct RestTable {
    ident: TableIdent,
    schema: SchemaRef,
    stats: TableStats,
    metadata: Value,
    location: String,
    metadata_location: String,
}

impl IcebergRestCatalog {
    /// In-process REST catalog. No sockets.
    pub fn memory() -> Self {
        Self {
            catalog: DEFAULT_CATALOG.into(),
            inner: RestInner::Memory(Arc::new(Mutex::new(RestState::default()))),
        }
    }

    /// HTTP client for a REST catalog at `base` (`http://host:port` or with a path).
    ///
    /// Returns [`Error::Io`] when `allow_network` is false or the URI is not HTTP.
    pub fn connect(base: &str, allow_network: bool) -> Result<Self> {
        if !allow_network {
            return Err(Error::Io("network not granted".into()));
        }
        let client = parse_http_base(base)?;
        Ok(Self {
            catalog: DEFAULT_CATALOG.into(),
            inner: RestInner::Http(client),
        })
    }

    /// Serve this memory catalog on a loopback listener until `listener` closes.
    /// Used by tests and the optional emulator job.
    pub fn serve_memory(listener: TcpListener) -> Result<()> {
        let state = Arc::new(Mutex::new(RestState::default()));
        listener
            .set_nonblocking(false)
            .map_err(|e| Error::Io(e.to_string()))?;
        for stream in listener.incoming() {
            let mut stream = match stream {
                Ok(s) => s,
                Err(_) => continue,
            };
            let _ = handle_http_stream(&state, &mut stream);
        }
        Ok(())
    }

    /// Metadata JSON last committed for `ident`.
    pub fn load_metadata(&self, ident: &TableIdent) -> Result<Value> {
        match &self.inner {
            RestInner::Memory(state) => {
                let guard = lock_state(state)?;
                Ok(guard.table(ident)?.metadata.clone())
            }
            RestInner::Http(http) => {
                let body = http_json(http, "GET", &table_path(ident), None)?;
                body.get("metadata")
                    .cloned()
                    .ok_or_else(|| Error::Io("REST load missing metadata".into()))
            }
        }
    }

    /// Replace table metadata after a FileIO commit. The catalog holds the
    /// current metadata pointer; `location` is the table root, not the
    /// metadata object URI.
    pub fn commit_metadata(
        &mut self,
        ident: &TableIdent,
        metadata: Value,
        location: impl Into<String>,
    ) -> Result<()> {
        let location = location.into();
        let schema = parse_schema_from_metadata(&metadata)?;
        let meta_loc = metadata_file_uri(&location, &metadata);
        let metadata = with_table_location(metadata, &location);
        match &mut self.inner {
            RestInner::Memory(state) => {
                let mut guard = lock_state(state)?;
                let table = guard.table_mut(ident)?;
                table.schema = schema;
                table.metadata = metadata;
                table.location = location;
                table.metadata_location = meta_loc;
                Ok(())
            }
            RestInner::Http(http) => {
                let register = json!({
                    "name": ident.table,
                    "overwrite": true,
                    "metadata-location": meta_loc,
                });
                let register_path = format!("/v1/namespaces/{}/register", ident.namespace);
                if http_json(http, "POST", &register_path, Some(&register)).is_ok() {
                    return Ok(());
                }
                let current = http_json(http, "GET", &table_path(ident), None).ok();
                let snapshot_id = current
                    .as_ref()
                    .and_then(|b| b.get("metadata"))
                    .and_then(|m| m.get("current-snapshot-id"))
                    .and_then(Value::as_i64);
                let uuid = current
                    .as_ref()
                    .and_then(|b| b.get("metadata"))
                    .and_then(|m| m.get("table-uuid"))
                    .and_then(Value::as_str)
                    .map(str::to_string);
                let mut requirements = Vec::new();
                if let Some(id) = snapshot_id {
                    requirements.push(json!({
                        "type": "assert-ref-snapshot-id",
                        "ref": "main",
                        "snapshot-id": id
                    }));
                } else if let Some(u) = uuid {
                    requirements.push(json!({"type": "assert-table-uuid", "uuid": u}));
                } else {
                    requirements.push(json!({"type": "assert-create"}));
                }
                let body = json!({
                    "identifier": {
                        "namespace": [ident.namespace],
                        "name": ident.table
                    },
                    "requirements": requirements,
                    "updates": [
                        {"action": "set-location", "location": location},
                        {"action": "assign-metadata", "metadata": metadata}
                    ]
                });
                http_json(http, "POST", &table_path(ident), Some(&body))?;
                Ok(())
            }
        }
    }

    /// Table root recorded by the catalog (object-store prefix or path).
    pub fn table_location(&self, ident: &TableIdent) -> Result<String> {
        match &self.inner {
            RestInner::Memory(state) => Ok(lock_state(state)?.table(ident)?.location.clone()),
            RestInner::Http(http) => {
                let body = http_json(http, "GET", &table_path(ident), None)?;
                body.get("metadata")
                    .and_then(|m| m.get("location"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .ok_or_else(|| Error::Io("REST load missing table location".into()))
            }
        }
    }
}

impl CatalogProvider for IcebergRestCatalog {
    fn get_table(&self, ident: &TableIdent) -> Result<TableMeta> {
        match &self.inner {
            RestInner::Memory(state) => {
                let t = lock_state(state)?.table(ident)?.clone();
                Ok(TableMeta {
                    ident: t.ident,
                    schema: t.schema,
                    stats: t.stats,
                })
            }
            RestInner::Http(http) => {
                let body = http_json(http, "GET", &table_path(ident), None)?;
                let md = body
                    .get("metadata")
                    .ok_or_else(|| Error::Io("REST load missing metadata".into()))?;
                Ok(TableMeta {
                    ident: ident.clone(),
                    schema: parse_schema_from_metadata(md)?,
                    stats: TableStats::default(),
                })
            }
        }
    }

    fn list_namespaces(&self) -> Result<Vec<NamespaceIdent>> {
        match &self.inner {
            RestInner::Memory(state) => {
                let guard = lock_state(state)?;
                Ok(guard
                    .namespaces
                    .keys()
                    .map(|ns| NamespaceIdent {
                        catalog: self.catalog.clone(),
                        namespace: ns.clone(),
                    })
                    .collect())
            }
            RestInner::Http(http) => {
                let body = http_json(http, "GET", "/v1/namespaces", None)?;
                let mut out = Vec::new();
                if let Some(arr) = body.get("namespaces").and_then(Value::as_array) {
                    for ns in arr {
                        let name = namespace_name(ns)?;
                        out.push(NamespaceIdent {
                            catalog: self.catalog.clone(),
                            namespace: name,
                        });
                    }
                }
                Ok(out)
            }
        }
    }

    fn list_tables(&self, ns: &NamespaceIdent) -> Result<Vec<String>> {
        self.ensure_catalog(&ns.catalog)?;
        match &self.inner {
            RestInner::Memory(state) => {
                let guard = lock_state(state)?;
                let mut out: Vec<String> = guard
                    .tables
                    .values()
                    .filter(|t| t.ident.namespace == ns.namespace)
                    .map(|t| t.ident.table.clone())
                    .collect();
                out.sort();
                Ok(out)
            }
            RestInner::Http(http) => {
                let path = format!("/v1/namespaces/{}/tables", ns.namespace);
                let body = http_json(http, "GET", &path, None)?;
                let mut out = Vec::new();
                if let Some(arr) = body.get("identifiers").and_then(Value::as_array) {
                    for id in arr {
                        if let Some(name) = id.get("name").and_then(Value::as_str) {
                            out.push(name.to_string());
                        }
                    }
                }
                out.sort();
                Ok(out)
            }
        }
    }

    fn create_table(&mut self, meta: TableMeta) -> Result<()> {
        self.ensure_catalog(&meta.ident.catalog)?;
        let metadata = empty_metadata(&meta)?;
        match &mut self.inner {
            RestInner::Memory(state) => {
                let mut guard = lock_state(state)?;
                guard.namespaces.insert(meta.ident.namespace.clone(), ());
                let key = table_key(&meta.ident);
                if guard.tables.contains_key(&key) {
                    return Err(Error::Plan(format!(
                        "table already exists: {}",
                        meta.ident.sql_name()
                    )));
                }
                guard.tables.insert(
                    key,
                    RestTable {
                        ident: meta.ident,
                        schema: from_arrow_schema(meta.schema)?,
                        stats: meta.stats,
                        metadata,
                        location: String::new(),
                        metadata_location: String::new(),
                    },
                );
                Ok(())
            }
            RestInner::Http(http) => {
                let ns_path = format!("/v1/namespaces/{}", meta.ident.namespace);
                let _ = http_json(
                    http,
                    "POST",
                    "/v1/namespaces",
                    Some(&json!({"namespace": [meta.ident.namespace]})),
                );
                let req = json!({
                    "name": meta.ident.table,
                    "schema": metadata.get("schemas").and_then(|s| s.as_array()).and_then(|a| a.first()).cloned().unwrap_or(json!({})),
                    "partition-spec": {"spec-id": 0, "fields": []},
                    "write-order": {"order-id": 0, "fields": []},
                    "properties": {}
                });
                http_json(http, "POST", &format!("{ns_path}/tables"), Some(&req))?;
                Ok(())
            }
        }
    }

    fn drop_table(&mut self, ident: &TableIdent, if_exists: bool) -> Result<()> {
        self.ensure_catalog(&ident.catalog)?;
        match &mut self.inner {
            RestInner::Memory(state) => {
                let mut guard = lock_state(state)?;
                match guard.tables.remove(&table_key(ident)) {
                    Some(_) => Ok(()),
                    None if if_exists => Ok(()),
                    None => Err(Error::Plan(format!(
                        "table not found: {}",
                        ident.sql_name()
                    ))),
                }
            }
            RestInner::Http(http) => match http_json(http, "DELETE", &table_path(ident), None) {
                Ok(_) => Ok(()),
                Err(e) if if_exists && is_not_found(&e) => Ok(()),
                Err(e) => Err(e),
            },
        }
    }

    fn alter_table(&mut self, ident: &TableIdent, schema: SchemaRef) -> Result<()> {
        let schema = from_arrow_schema(schema)?;
        match &mut self.inner {
            RestInner::Memory(state) => {
                let mut guard = lock_state(state)?;
                let table = guard.table_mut(ident)?;
                table.metadata = patch_schema_metadata(&table.metadata, &schema)?;
                table.schema = schema;
                Ok(())
            }
            RestInner::Http(http) => {
                let loaded = http_json(http, "GET", &table_path(ident), None)?;
                let md = loaded
                    .get("metadata")
                    .cloned()
                    .ok_or_else(|| Error::Io("REST load missing metadata".into()))?;
                let snapshot_id = md.get("current-snapshot-id").and_then(Value::as_i64);
                let patched = patch_schema_metadata(&md, &schema)?;
                let schema_id = patched
                    .get("current-schema-id")
                    .and_then(Value::as_i64)
                    .unwrap_or(0);
                let schema_doc = patched
                    .get("schemas")
                    .and_then(Value::as_array)
                    .and_then(|a| a.last())
                    .cloned()
                    .unwrap_or(json!({}));
                let mut requirements = Vec::new();
                if let Some(id) = snapshot_id {
                    requirements.push(json!({
                        "type": "assert-ref-snapshot-id",
                        "ref": "main",
                        "snapshot-id": id
                    }));
                }
                let body = json!({
                    "identifier": {
                        "namespace": [ident.namespace],
                        "name": ident.table
                    },
                    "requirements": requirements,
                    "updates": [
                        {"action": "add-schema", "schema": schema_doc, "last-column-id": schema_id},
                        {"action": "set-current-schema", "schema-id": schema_id}
                    ]
                });
                http_json(http, "POST", &table_path(ident), Some(&body))?;
                Ok(())
            }
        }
    }

    fn set_stats(&mut self, ident: &TableIdent, stats: TableStats) -> Result<()> {
        match &mut self.inner {
            RestInner::Memory(state) => {
                lock_state(state)?.table_mut(ident)?.stats = stats;
                Ok(())
            }
            RestInner::Http(_) => {
                // Stats are engine-local; the REST service stores schema and location.
                let _ = ident;
                let _ = stats;
                Ok(())
            }
        }
    }
}

impl IcebergRestCatalog {
    fn ensure_catalog(&self, catalog: &str) -> Result<()> {
        if catalog == self.catalog {
            Ok(())
        } else {
            Err(Error::Plan(format!("unknown catalog: {catalog}")))
        }
    }
}

impl RestState {
    fn table(&self, ident: &TableIdent) -> Result<&RestTable> {
        self.tables
            .get(&table_key(ident))
            .ok_or_else(|| Error::Plan(format!("table not found: {}", ident.sql_name())))
    }

    fn table_mut(&mut self, ident: &TableIdent) -> Result<&mut RestTable> {
        self.tables
            .get_mut(&table_key(ident))
            .ok_or_else(|| Error::Plan(format!("table not found: {}", ident.sql_name())))
    }
}

fn lock_state(state: &Mutex<RestState>) -> Result<std::sync::MutexGuard<'_, RestState>> {
    state
        .lock()
        .map_err(|_| Error::Io("REST catalog lock poisoned".into()))
}

fn table_key(ident: &TableIdent) -> String {
    format!("{}.{}", ident.namespace, ident.table)
}

fn table_path(ident: &TableIdent) -> String {
    format!("/v1/namespaces/{}/tables/{}", ident.namespace, ident.table)
}

fn empty_metadata(meta: &TableMeta) -> Result<Value> {
    let mut fields = Vec::new();
    for (i, f) in meta.schema.fields().iter().enumerate() {
        fields.push(json!({
            "id": i + 1,
            "name": f.name(),
            "required": false,
            "type": iceberg_type(f.data_type())?,
        }));
    }
    Ok(json!({
        "format-version": 2,
        "table-uuid": format!("{}-{}", meta.ident.namespace, meta.ident.table),
        "location": "",
        "last-updated-ms": 0,
        "current-schema-id": 0,
        "schemas": [{
            "type": "struct",
            "schema-id": 0,
            "fields": fields
        }],
        "current-snapshot-id": serde_json::Value::Null,
        "snapshots": [],
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "default-spec-id": 0,
        "properties": {}
    }))
}

fn iceberg_type(dt: &DataType) -> Result<&'static str> {
    Ok(match dt {
        DataType::Boolean => "boolean",
        DataType::Int32 => "int",
        DataType::Int64 => "long",
        DataType::Float32 => "float",
        DataType::Float64 => "double",
        DataType::Utf8 | DataType::LargeUtf8 => "string",
        DataType::Binary | DataType::LargeBinary => "binary",
        DataType::Date32 => "date",
        DataType::Timestamp(_, _) => "timestamp",
        _ => return Err(Error::UnsupportedType(format!("{dt}"))),
    })
}

fn from_iceberg_type(ty: &str) -> Result<DataType> {
    Ok(match ty {
        "boolean" => DataType::Boolean,
        "int" => DataType::Int32,
        "long" => DataType::Int64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "string" => DataType::Utf8,
        "binary" => DataType::Binary,
        "date" => DataType::Date32,
        "timestamp" | "timestamptz" => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
        other => return Err(Error::UnsupportedType(other.into())),
    })
}

fn parse_schema_from_metadata(md: &Value) -> Result<SchemaRef> {
    let schemas = md
        .get("schemas")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::Io("Iceberg metadata missing schemas".into()))?;
    let current = md.get("current-schema-id").and_then(Value::as_i64);
    let schema = schemas
        .iter()
        .find(|s| s.get("schema-id").and_then(Value::as_i64) == current)
        .or(schemas.last())
        .ok_or_else(|| Error::Io("Iceberg schema not found".into()))?;
    let fields = schema
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::Io("Iceberg schema has no fields".into()))?;
    let mut out = Vec::new();
    for f in fields {
        let name = f
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Io("Iceberg field missing name".into()))?;
        let ty = f
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Io("Iceberg field missing type".into()))?;
        out.push(Field::new(name, from_iceberg_type(ty)?, true));
    }
    from_arrow_schema(Arc::new(Schema::new(out)))
}

fn namespace_name(ns: &Value) -> Result<String> {
    match ns {
        Value::Array(parts) => Ok(parts
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(".")),
        Value::String(s) => Ok(s.clone()),
        _ => Err(Error::Io("invalid REST namespace".into())),
    }
}

fn is_not_found(err: &Error) -> bool {
    match err {
        Error::Plan(s) => s.contains("not found"),
        Error::Io(s) => {
            let t = s.to_ascii_lowercase();
            t.contains("not found") || t.contains("returned 404")
        }
        _ => false,
    }
}

fn metadata_file_uri(location: &str, metadata: &Value) -> String {
    if let Some(s) = metadata
        .get("metadata-location")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        return s.to_string();
    }
    let version = metadata
        .get("last-sequence-number")
        .and_then(Value::as_i64)
        .or_else(|| metadata.get("current-snapshot-id").and_then(Value::as_i64))
        .filter(|v| *v > 0)
        .unwrap_or(1);
    let loc = location.trim_end_matches('/');
    format!("{loc}/metadata/v{version}.metadata.json")
}

fn table_root_from_metadata_uri(uri: &str) -> String {
    if let Some(idx) = uri.rfind("/metadata/") {
        uri[..idx].to_string()
    } else {
        uri.trim_end_matches('/').to_string()
    }
}

fn with_table_location(mut metadata: Value, location: &str) -> Value {
    if let Some(obj) = metadata.as_object_mut() {
        obj.insert("location".into(), json!(location));
    }
    metadata
}

fn patch_schema_metadata(md: &Value, schema: &Schema) -> Result<Value> {
    let mut md = md.clone();
    let obj = md
        .as_object_mut()
        .ok_or_else(|| Error::Io("Iceberg metadata is not an object".into()))?;
    let current_id = obj
        .get("current-schema-id")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let next_id = current_id + 1;
    let mut fields = Vec::new();
    for (i, f) in schema.fields().iter().enumerate() {
        fields.push(json!({
            "id": i + 1,
            "name": f.name(),
            "required": false,
            "type": iceberg_type(f.data_type())?,
        }));
    }
    let new_schema = json!({
        "type": "struct",
        "schema-id": next_id,
        "fields": fields
    });
    let mut schemas = obj
        .get("schemas")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    schemas.push(new_schema);
    obj.insert("schemas".into(), Value::Array(schemas));
    obj.insert("current-schema-id".into(), json!(next_id));
    Ok(md)
}

fn json_eq_or_null(want: Option<&Value>, have: Option<&Value>) -> bool {
    match (want, have) {
        (None, None) => true,
        (Some(Value::Null), None) | (None, Some(Value::Null)) => true,
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

fn parse_http_base(base: &str) -> Result<HttpClient> {
    let rest = base
        .strip_prefix("http://")
        .ok_or_else(|| Error::Io("REST catalog URI must be http://host:port".into()))?;
    let (hostport, path) = match rest.split_once('/') {
        Some((hp, p)) => (hp, format!("/{p}")),
        None => (rest, String::new()),
    };
    let (host, port) = match hostport.split_once(':') {
        Some((h, p)) => (
            h.to_string(),
            p.parse::<u16>()
                .map_err(|_| Error::Io("invalid REST catalog port".into()))?,
        ),
        None => (hostport.to_string(), 80),
    };
    if host.is_empty() {
        return Err(Error::Io("REST catalog host is empty".into()));
    }
    Ok(HttpClient {
        host,
        port,
        path_prefix: path.trim_end_matches('/').to_string(),
    })
}

fn http_json(http: &HttpClient, method: &str, path: &str, body: Option<&Value>) -> Result<Value> {
    let payload = match body {
        Some(v) => Some(serde_json::to_vec(v).map_err(|e| Error::Io(e.to_string()))?),
        None => None,
    };
    let path = if http.path_prefix.is_empty() {
        path.to_string()
    } else {
        format!("{}{path}", http.path_prefix)
    };
    let (status, bytes) = http_request(&http.host, http.port, method, &path, payload.as_deref())?;
    if status == 404 {
        return Err(Error::Plan(format!("table not found: {path}")));
    }
    if status == 409 {
        let text = String::from_utf8_lossy(&bytes);
        if text.contains("conflict") {
            return Err(Error::Plan(format!("commit conflict: {path}")));
        }
        return Err(Error::Plan(format!("table already exists: {path}")));
    }
    if !(200..300).contains(&status) {
        return Err(Error::Io(format!("REST {method} {path} returned {status}")));
    }
    if bytes.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&bytes).map_err(|e| Error::Io(format!("REST JSON: {e}")))
}

fn http_request(
    host: &str,
    port: u16,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
) -> Result<(u16, Vec<u8>)> {
    let mut stream = TcpStream::connect((host, port)).map_err(|e| Error::Io(e.to_string()))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(|e| Error::Io(e.to_string()))?;
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}:{port}\r\nAccept: application/json\r\nConnection: close\r\n"
    );
    if let Some(b) = body {
        req.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            b.len()
        ));
    } else {
        req.push_str("Content-Length: 0\r\n");
    }
    req.push_str("\r\n");
    stream
        .write_all(req.as_bytes())
        .map_err(|e| Error::Io(e.to_string()))?;
    if let Some(b) = body {
        stream.write_all(b).map_err(|e| Error::Io(e.to_string()))?;
    }
    stream.flush().map_err(|e| Error::Io(e.to_string()))?;
    let (head, body) = read_http_message(&mut stream)?;
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| Error::Io("invalid HTTP status".into()))?;
    let decoded = decode_http_body(&head, body)?;
    Ok((status, decoded))
}

fn read_http_message(stream: &mut TcpStream) -> Result<(String, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        let n = stream
            .read(&mut tmp)
            .map_err(|e| Error::Io(e.to_string()))?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = find_header_end(&buf) {
            let head = String::from_utf8_lossy(&buf[..pos]).into_owned();
            let mut body = buf[pos + 4..].to_vec();
            let content_len =
                header_value(&head, "content-length").and_then(|s| s.parse::<usize>().ok());
            let chunked = header_value(&head, "transfer-encoding")
                .is_some_and(|s| s.to_ascii_lowercase().contains("chunked"));
            if let Some(len) = content_len {
                while body.len() < len {
                    let n = stream
                        .read(&mut tmp)
                        .map_err(|e| Error::Io(e.to_string()))?;
                    if n == 0 {
                        break;
                    }
                    body.extend_from_slice(&tmp[..n]);
                }
                body.truncate(len);
            } else if chunked {
                while !chunked_complete(&body) {
                    let n = stream
                        .read(&mut tmp)
                        .map_err(|e| Error::Io(e.to_string()))?;
                    if n == 0 {
                        break;
                    }
                    body.extend_from_slice(&tmp[..n]);
                }
            }
            return Ok((head, body));
        }
        if buf.len() > 1024 * 1024 {
            return Err(Error::Io("HTTP headers too large".into()));
        }
    }
    Err(Error::Io("invalid HTTP response".into()))
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn header_value<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    let want = name.to_ascii_lowercase();
    for line in head.lines().skip(1) {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        if k.trim().eq_ignore_ascii_case(&want) {
            return Some(v.trim());
        }
    }
    None
}

fn decode_http_body(head: &str, body: Vec<u8>) -> Result<Vec<u8>> {
    if header_value(head, "transfer-encoding")
        .is_some_and(|s| s.to_ascii_lowercase().contains("chunked"))
    {
        decode_chunked(&body)
    } else {
        Ok(body)
    }
}

fn chunked_complete(body: &[u8]) -> bool {
    decode_chunked(body).is_ok()
}

fn decode_chunked(body: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut rest = body;
    loop {
        let Some(idx) = rest.windows(2).position(|w| w == b"\r\n") else {
            return Err(Error::Io("incomplete chunked body".into()));
        };
        let size_line = std::str::from_utf8(&rest[..idx])
            .map_err(|_| Error::Io("invalid chunk size".into()))?;
        let size_line = size_line.split(';').next().unwrap_or(size_line).trim();
        let size = usize::from_str_radix(size_line, 16)
            .map_err(|_| Error::Io("invalid chunk size".into()))?;
        rest = &rest[idx + 2..];
        if size == 0 {
            return Ok(out);
        }
        if rest.len() < size + 2 {
            return Err(Error::Io("incomplete chunked body".into()));
        }
        out.extend_from_slice(&rest[..size]);
        if &rest[size..size + 2] != b"\r\n" {
            return Err(Error::Io("invalid chunk framing".into()));
        }
        rest = &rest[size + 2..];
    }
}

fn handle_http_stream(state: &Mutex<RestState>, stream: &mut TcpStream) -> Result<()> {
    let (head, body) = read_http_message(stream)?;
    let start = head.lines().next().unwrap_or("");
    let mut parts = start.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");
    let body = decode_http_body(&head, body)?;
    let body_val = if body.is_empty() {
        None
    } else {
        serde_json::from_slice::<Value>(&body).ok()
    };
    let (status, payload) = dispatch(state, method, path, body_val.as_ref());
    let body = serde_json::to_vec(&payload).unwrap_or_else(|_| b"{}".to_vec());
    let resp = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        if status == 200 {
            "OK"
        } else if status == 404 {
            "Not Found"
        } else if status == 409 {
            "Conflict"
        } else {
            "Error"
        },
        body.len()
    );
    stream
        .write_all(resp.as_bytes())
        .map_err(|e| Error::Io(e.to_string()))?;
    stream
        .write_all(&body)
        .map_err(|e| Error::Io(e.to_string()))?;
    Ok(())
}

fn dispatch(
    state: &Mutex<RestState>,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> (u16, Value) {
    let Ok(mut guard) = state.lock() else {
        return (500, json!({"error": "poisoned"}));
    };
    match (method, path) {
        ("GET", "/v1/config") => (200, json!({"defaults": {}, "overrides": {}})),
        ("GET", "/v1/namespaces") => {
            let namespaces: Vec<Value> = guard.namespaces.keys().map(|n| json!([n])).collect();
            (200, json!({"namespaces": namespaces}))
        }
        ("POST", "/v1/namespaces") => {
            if let Some(ns) = body
                .and_then(|b| b.get("namespace"))
                .and_then(Value::as_array)
                .and_then(|a| a.first())
                .and_then(Value::as_str)
            {
                guard.namespaces.insert(ns.to_string(), ());
            }
            (200, json!({}))
        }
        (method, path) => {
            if method == "POST"
                && let Some(rest) = path.strip_prefix("/v1/namespaces/")
                && let Some(ns) = rest.strip_suffix("/register")
            {
                return dispatch_register(&mut guard, ns, body);
            }
            dispatch_table(&mut guard, method, path, body)
        }
    }
}

fn dispatch_register(guard: &mut RestState, ns: &str, body: Option<&Value>) -> (u16, Value) {
    let name = body
        .and_then(|b| b.get("name"))
        .and_then(Value::as_str)
        .unwrap_or("");
    if name.is_empty() {
        return (400, json!({"error": "missing name"}));
    }
    let overwrite = body
        .and_then(|b| b.get("overwrite"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let meta_loc = body
        .and_then(|b| b.get("metadata-location"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let location = table_root_from_metadata_uri(meta_loc);
    let ident = match TableIdent::from_parts(DEFAULT_CATALOG, ns, name) {
        Ok(i) => i,
        Err(e) => return (400, json!({"error": e.to_string()})),
    };
    let key = table_key(&ident);
    if guard.tables.contains_key(&key) && !overwrite {
        return (409, json!({"error": "exists"}));
    }
    let existing = guard.tables.get(&key).cloned();
    let mut metadata = existing
        .as_ref()
        .map(|t| t.metadata.clone())
        .unwrap_or_else(|| {
            json!({
                "format-version": 2,
                "table-uuid": format!("{ns}-{name}"),
                "current-schema-id": 0,
                "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
                "snapshots": [],
                "current-snapshot-id": serde_json::Value::Null,
            })
        });
    metadata = with_table_location(metadata, &location);
    let schema_ref = parse_schema_from_metadata(&metadata).unwrap_or_else(|_| {
        existing
            .map(|t| t.schema)
            .unwrap_or_else(|| Arc::new(Schema::empty()))
    });
    guard.namespaces.insert(ns.to_string(), ());
    guard.tables.insert(
        key,
        RestTable {
            ident,
            schema: schema_ref,
            stats: TableStats::default(),
            metadata: metadata.clone(),
            location,
            metadata_location: meta_loc.to_string(),
        },
    );
    (
        200,
        json!({"metadata-location": meta_loc, "metadata": metadata}),
    )
}

fn dispatch_table(
    guard: &mut RestState,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> (u16, Value) {
    let rest = match path.strip_prefix("/v1/namespaces/") {
        Some(r) => r,
        None => return (404, json!({"error": "not found"})),
    };
    let Some((ns, after)) = rest.split_once("/tables") else {
        return (404, json!({"error": "not found"}));
    };
    if after.is_empty() || after == "/" {
        if method == "GET" {
            let identifiers: Vec<Value> = guard
                .tables
                .values()
                .filter(|t| t.ident.namespace == ns)
                .map(|t| json!({"namespace": [ns], "name": t.ident.table}))
                .collect();
            return (200, json!({"identifiers": identifiers}));
        }
        if method == "POST" {
            let name = body
                .and_then(|b| b.get("name"))
                .and_then(Value::as_str)
                .unwrap_or("");
            if name.is_empty() {
                return (400, json!({"error": "missing name"}));
            }
            let ident = match TableIdent::from_parts(DEFAULT_CATALOG, ns, name) {
                Ok(i) => i,
                Err(e) => return (400, json!({"error": e.to_string()})),
            };
            let schema = body
                .and_then(|b| b.get("schema"))
                .cloned()
                .unwrap_or(json!({"type": "struct", "schema-id": 0, "fields": []}));
            let metadata = json!({
                "format-version": 2,
                "table-uuid": format!("{ns}-{name}"),
                "location": "",
                "current-schema-id": schema.get("schema-id").and_then(Value::as_i64).unwrap_or(0),
                "schemas": [schema],
                "snapshots": [],
                "current-snapshot-id": serde_json::Value::Null,
            });
            let schema_ref = match parse_schema_from_metadata(&metadata) {
                Ok(s) => s,
                Err(_) => Arc::new(Schema::empty()),
            };
            let key = table_key(&ident);
            if guard.tables.contains_key(&key) {
                return (409, json!({"error": "exists"}));
            }
            guard.namespaces.insert(ns.to_string(), ());
            guard.tables.insert(
                key,
                RestTable {
                    ident: ident.clone(),
                    schema: schema_ref,
                    stats: TableStats::default(),
                    metadata: metadata.clone(),
                    location: String::new(),
                    metadata_location: String::new(),
                },
            );
            return (200, json!({"metadata-location": "", "metadata": metadata}));
        }
        return (405, json!({"error": "method"}));
    }
    let table = after.trim_start_matches('/');
    let ident = match TableIdent::from_parts(DEFAULT_CATALOG, ns, table) {
        Ok(i) => i,
        Err(e) => return (400, json!({"error": e.to_string()})),
    };
    let key = table_key(&ident);
    match method {
        "GET" => match guard.tables.get(&key) {
            Some(t) => {
                let meta_loc = if t.metadata_location.is_empty() {
                    metadata_file_uri(&t.location, &t.metadata)
                } else {
                    t.metadata_location.clone()
                };
                (
                    200,
                    json!({
                        "metadata-location": meta_loc,
                        "metadata": t.metadata
                    }),
                )
            }
            None => (404, json!({"error": "not found"})),
        },
        "DELETE" => {
            if guard.tables.remove(&key).is_some() {
                (200, json!({}))
            } else {
                (404, json!({"error": "not found"}))
            }
        }
        "POST" => dispatch_commit(guard, &key, body),
        _ => (405, json!({"error": "method"})),
    }
}

fn dispatch_commit(guard: &mut RestState, key: &str, body: Option<&Value>) -> (u16, Value) {
    if let Some(reqs) = body
        .and_then(|b| b.get("requirements"))
        .and_then(Value::as_array)
    {
        for req in reqs {
            let ty = req.get("type").and_then(Value::as_str).unwrap_or("");
            match ty {
                "assert-create" => {
                    if guard.tables.contains_key(key) {
                        return (409, json!({"error": "commit conflict"}));
                    }
                }
                "assert-table-uuid" => {
                    let Some(t) = guard.tables.get(key) else {
                        return (404, json!({"error": "not found"}));
                    };
                    let want = req.get("uuid").and_then(Value::as_str).unwrap_or("");
                    let have = t
                        .metadata
                        .get("table-uuid")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    if want != have {
                        return (409, json!({"error": "commit conflict"}));
                    }
                }
                "assert-ref-snapshot-id" => {
                    let Some(t) = guard.tables.get(key) else {
                        return (404, json!({"error": "not found"}));
                    };
                    let want = req.get("snapshot-id");
                    let have = t.metadata.get("current-snapshot-id");
                    if !json_eq_or_null(want, have) {
                        return (409, json!({"error": "commit conflict"}));
                    }
                }
                _ => {}
            }
        }
    }
    let Some(t) = guard.tables.get_mut(key) else {
        return (404, json!({"error": "not found"}));
    };
    if let Some(md) = body.and_then(|b| b.get("metadata")) {
        t.metadata = md.clone();
        if let Ok(s) = parse_schema_from_metadata(md) {
            t.schema = s;
        }
    }
    if let Some(loc) = body.and_then(|b| b.get("location")).and_then(Value::as_str) {
        t.location = loc.to_string();
        t.metadata = with_table_location(t.metadata.clone(), loc);
    }
    if let Some(updates) = body
        .and_then(|b| b.get("updates"))
        .and_then(Value::as_array)
    {
        for u in updates {
            let action = u.get("action").and_then(Value::as_str).unwrap_or("");
            match action {
                "set-location" => {
                    if let Some(loc) = u.get("location").and_then(Value::as_str) {
                        t.location = loc.to_string();
                        t.metadata = with_table_location(t.metadata.clone(), loc);
                    }
                }
                "assign-metadata" => {
                    if let Some(md) = u.get("metadata") {
                        t.metadata = md.clone();
                        if let Ok(s) = parse_schema_from_metadata(md) {
                            t.schema = s;
                        }
                        if let Some(loc) = md.get("location").and_then(Value::as_str)
                            && t.location.is_empty()
                        {
                            t.location = loc.to_string();
                        }
                    }
                }
                "add-schema" => {
                    if let Some(schema) = u.get("schema")
                        && let Some(obj) = t.metadata.as_object_mut()
                    {
                        let mut schemas = obj
                            .get("schemas")
                            .and_then(Value::as_array)
                            .cloned()
                            .unwrap_or_default();
                        schemas.push(schema.clone());
                        obj.insert("schemas".into(), Value::Array(schemas));
                    }
                }
                "set-current-schema" => {
                    if let Some(id) = u.get("schema-id") {
                        if let Some(obj) = t.metadata.as_object_mut() {
                            obj.insert("current-schema-id".into(), id.clone());
                        }
                        if let Ok(s) = parse_schema_from_metadata(&t.metadata) {
                            t.schema = s;
                        }
                    }
                }
                _ => {}
            }
        }
    }
    let meta_loc = if t.metadata_location.is_empty() {
        metadata_file_uri(&t.location, &t.metadata)
    } else {
        t.metadata_location.clone()
    };
    (
        200,
        json!({"metadata-location": meta_loc, "metadata": t.metadata}),
    )
}

/// Spawn the in-process REST server used by the ignored HTTP test.
pub fn spawn_memory_server() -> Result<(u16, thread::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| Error::Io(e.to_string()))?;
    let port = listener
        .local_addr()
        .map_err(|e| Error::Io(e.to_string()))?
        .port();
    let handle = thread::spawn(move || {
        let _ = IcebergRestCatalog::serve_memory(listener);
    });
    Ok((port, handle))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::CatalogProvider;
    use std::sync::Arc as StdArc;

    fn sample_meta(name: &str) -> TableMeta {
        let schema = StdArc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        TableMeta {
            ident: TableIdent::parse(name).unwrap(),
            schema,
            stats: TableStats::default(),
        }
    }

    #[test]
    fn memory_rest_catalog_create_load_drop() {
        let mut cat = IcebergRestCatalog::memory();
        cat.create_table(sample_meta("sales.t")).unwrap();
        let ns = NamespaceIdent {
            catalog: DEFAULT_CATALOG.into(),
            namespace: "sales".into(),
        };
        assert_eq!(cat.list_tables(&ns).unwrap(), vec!["t".to_string()]);
        let got = CatalogProvider::get_table(&cat, &TableIdent::parse("sales.t").unwrap()).unwrap();
        assert_eq!(got.schema.field(0).name(), "id");
        cat.commit_metadata(
            &got.ident,
            json!({
                "format-version": 2,
                "current-schema-id": 0,
                "schemas": [{"schema-id": 0, "fields": [
                    {"id": 1, "name": "id", "type": "long"},
                    {"id": 2, "name": "n", "type": "string"}
                ]}],
                "snapshots": []
            }),
            "s3://bucket/sales/t",
        )
        .unwrap();
        assert_eq!(
            cat.table_location(&TableIdent::parse("sales.t").unwrap())
                .unwrap(),
            "s3://bucket/sales/t"
        );
        CatalogProvider::drop_table(&mut cat, &TableIdent::parse("sales.t").unwrap(), false)
            .unwrap();
        assert!(CatalogProvider::get_table(&cat, &TableIdent::parse("sales.t").unwrap()).is_err());
    }

    #[test]
    fn alter_table_keeps_snapshots() {
        let mut cat = IcebergRestCatalog::memory();
        cat.create_table(sample_meta("sales.t")).unwrap();
        let ident = TableIdent::parse("sales.t").unwrap();
        cat.commit_metadata(
            &ident,
            json!({
                "format-version": 2,
                "current-schema-id": 0,
                "schemas": [{"schema-id": 0, "fields": [
                    {"id": 1, "name": "id", "type": "long"}
                ]}],
                "current-snapshot-id": 1,
                "snapshots": [{"snapshot-id": 1, "summary": {"operation": "append"}}]
            }),
            "s3://bucket/sales/t",
        )
        .unwrap();
        let new_schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("n", DataType::Utf8, true),
        ]));
        cat.alter_table(&ident, new_schema).unwrap();
        let md = cat.load_metadata(&ident).unwrap();
        assert_eq!(
            md.get("current-snapshot-id").and_then(Value::as_i64),
            Some(1)
        );
        assert_eq!(
            md.get("snapshots")
                .and_then(Value::as_array)
                .map(|a| a.len()),
            Some(1)
        );
        assert_eq!(cat.table_location(&ident).unwrap(), "s3://bucket/sales/t");
        assert_eq!(
            CatalogProvider::get_table(&cat, &ident)
                .unwrap()
                .schema
                .fields()
                .len(),
            2
        );
    }

    #[test]
    fn connect_without_grant_fails() {
        match IcebergRestCatalog::connect("http://127.0.0.1:8181", false) {
            Err(e) => assert!(e.to_string().contains("network not granted")),
            Ok(_) => panic!("connect must fail without network grant"),
        }
    }

    #[test]
    #[ignore = "binds a loopback REST catalog; run from the lake-store job"]
    fn http_rest_catalog_round_trip() {
        let (port, _h) = spawn_memory_server().unwrap();
        // Give the listener a moment to accept.
        thread::sleep(Duration::from_millis(20));
        let mut cat =
            IcebergRestCatalog::connect(&format!("http://127.0.0.1:{port}"), true).unwrap();
        cat.create_table(sample_meta("t")).unwrap();
        let got = CatalogProvider::get_table(&cat, &TableIdent::parse("t").unwrap()).unwrap();
        assert_eq!(got.schema.field(0).name(), "id");
        let ns = NamespaceIdent {
            catalog: DEFAULT_CATALOG.into(),
            namespace: "public".into(),
        };
        assert_eq!(cat.list_tables(&ns).unwrap(), vec!["t".to_string()]);
        cat.commit_metadata(
            &got.ident,
            json!({
                "format-version": 2,
                "current-schema-id": 0,
                "schemas": [{"schema-id": 0, "fields": [
                    {"id": 1, "name": "id", "type": "long"}
                ]}],
                "snapshots": [{"snapshot-id": 1}]
            }),
            "s3://bucket/t",
        )
        .unwrap();
        assert_eq!(
            cat.table_location(&TableIdent::parse("t").unwrap())
                .unwrap(),
            "s3://bucket/t"
        );
    }
}
