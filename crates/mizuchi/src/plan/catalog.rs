//! Table catalog: namespaces, schemas, partitions, and pluggable backends.
//!
//! Unqualified names resolve in [`DEFAULT_CATALOG`] / [`DEFAULT_NAMESPACE`].
//! Two-part names are `namespace.table`. Three-part names are
//! `catalog.namespace.table`. Memory and file backends share
//! [`CatalogProvider`]. An embedder supplies any other backend.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use serde_json::{Value, json};

use super::stats::TableStats;
use crate::error::{Error, Result};
use crate::schema::from_arrow_schema;

/// Default catalog name for unqualified and two-part names.
pub const DEFAULT_CATALOG: &str = "mizuchi";
/// Default namespace (schema) for unqualified names.
pub const DEFAULT_NAMESPACE: &str = "public";

/// Three-part table name: catalog, namespace, table.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableIdent {
    /// Catalog name.
    pub catalog: String,
    /// Namespace (schema) name.
    pub namespace: String,
    /// Table name.
    pub table: String,
}

impl TableIdent {
    /// Build an ident from already-split parts.
    pub fn from_parts(catalog: &str, namespace: &str, table: &str) -> Result<Self> {
        Ok(Self {
            catalog: ident_part(catalog)?,
            namespace: ident_part(namespace)?,
            table: ident_part(table)?,
        })
    }

    /// Parse 1, 2, or 3 dot-separated parts.
    pub fn parse(name: &str) -> Result<Self> {
        let parts: Vec<&str> = name.split('.').collect();
        match parts.as_slice() {
            [table] => Self::from_parts(DEFAULT_CATALOG, DEFAULT_NAMESPACE, table),
            [namespace, table] => Self::from_parts(DEFAULT_CATALOG, namespace, table),
            [catalog, namespace, table] => Self::from_parts(catalog, namespace, table),
            _ => Err(Error::Plan(format!("invalid table name: {name}"))),
        }
    }

    /// Internal map key `catalog.namespace.table`.
    pub fn key(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.namespace, self.table)
    }

    /// Shortest SQL name that still resolves to this ident from the defaults.
    pub fn sql_name(&self) -> String {
        if self.catalog == DEFAULT_CATALOG && self.namespace == DEFAULT_NAMESPACE {
            self.table.clone()
        } else if self.catalog == DEFAULT_CATALOG {
            format!("{}.{}", self.namespace, self.table)
        } else {
            self.key()
        }
    }
}

/// Namespace (schema) inside a catalog.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NamespaceIdent {
    /// Catalog name.
    pub catalog: String,
    /// Namespace name.
    pub namespace: String,
}

/// One catalog table: schema, stats, and known partitions.
#[derive(Clone, Debug)]
pub struct TableMeta {
    /// Three-part name.
    pub ident: TableIdent,
    /// Arrow schema.
    pub schema: SchemaRef,
    /// Stats and partition inventory.
    pub stats: TableStats,
}

/// Pluggable catalog backend. Memory and file stores implement this; an
/// embedder can add an external service by implementing the trait.
pub trait CatalogProvider {
    /// Look up a table. Missing names are [`Error::Plan`].
    fn get_table(&self, ident: &TableIdent) -> Result<TableMeta>;
    /// Namespaces in this catalog (or all catalogs for a facade).
    fn list_namespaces(&self) -> Result<Vec<NamespaceIdent>>;
    /// Tables in `ns`.
    fn list_tables(&self, ns: &NamespaceIdent) -> Result<Vec<String>>;
    /// Create or replace `meta`.
    fn create_table(&mut self, meta: TableMeta) -> Result<()>;
    /// Drop a table. Missing names are [`Error::Plan`] unless `if_exists`.
    fn drop_table(&mut self, ident: &TableIdent, if_exists: bool) -> Result<()>;
    /// Replace the schema of an existing table.
    fn alter_table(&mut self, ident: &TableIdent, schema: SchemaRef) -> Result<()>;
    /// Attach stats (including partitions) to an existing table.
    fn set_stats(&mut self, ident: &TableIdent, stats: TableStats) -> Result<()>;
}

/// In-memory catalog, optionally mirrored to a directory of JSON + Arrow
/// schema files.
#[derive(Clone, Debug, Default)]
pub struct Catalog {
    tables: BTreeMap<String, CatalogTable>,
    file_dir: Option<PathBuf>,
}

#[derive(Clone, Debug)]
struct CatalogTable {
    ident: TableIdent,
    schema: SchemaRef,
    stats: Option<TableStats>,
}

impl Catalog {
    /// Empty in-memory catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Clone table metadata without a file persist directory. Planning overlays
    /// must not rewrite a file catalog.
    pub(crate) fn planning_copy(&self) -> Self {
        Self {
            tables: self.tables.clone(),
            file_dir: None,
        }
    }

    /// Load a file catalog, creating `dir` when it does not exist.
    pub fn open_file(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|e| io_err(&dir, e))?;
        let mut cat = Self {
            tables: BTreeMap::new(),
            file_dir: Some(dir.clone()),
        };
        load_dir(&dir, &mut cat)?;
        Ok(cat)
    }

    /// Register `name` with an Arrow schema. Names are matched case-insensitively.
    /// Replacing a name drops any previously attached statistics.
    pub fn register(&mut self, name: impl Into<String>, schema: SchemaRef) -> Result<()> {
        let ident = TableIdent::parse(&name.into())?;
        let schema = from_arrow_schema(schema)?;
        self.tables.insert(
            ident.key(),
            CatalogTable {
                ident: ident.clone(),
                schema,
                stats: None,
            },
        );
        self.persist(&ident)
    }

    /// Attach table statistics used by cost-based rewrites.
    pub fn set_stats(&mut self, name: impl Into<String>, stats: TableStats) -> Result<()> {
        let ident = TableIdent::parse(&name.into())?;
        let table = self
            .tables
            .get_mut(&ident.key())
            .ok_or_else(|| Error::Plan(format!("table not found: {}", ident.sql_name())))?;
        table.stats = Some(stats);
        self.persist(&ident)
    }

    /// Record Hive partition columns and values for pruning.
    pub fn set_partitions(
        &mut self,
        name: impl Into<String>,
        columns: Vec<String>,
        partitions: Vec<String>,
    ) -> Result<()> {
        let ident = TableIdent::parse(&name.into())?;
        let table = self
            .tables
            .get_mut(&ident.key())
            .ok_or_else(|| Error::Plan(format!("table not found: {}", ident.sql_name())))?;
        let mut stats = table.stats.clone().unwrap_or_default();
        stats.partition_columns = columns;
        stats.partitions = partitions;
        table.stats = Some(stats);
        self.persist(&ident)
    }

    /// Look up a table schema.
    pub fn table(&self, name: &str) -> Result<&SchemaRef> {
        Ok(&self.entry(name)?.schema)
    }

    /// Look up table statistics, if registered.
    pub fn stats(&self, name: &str) -> Option<&TableStats> {
        self.entry(name).ok().and_then(|t| t.stats.as_ref())
    }

    /// Known Hive partition directories for `name`.
    pub fn partitions(&self, name: &str) -> Result<Vec<String>> {
        Ok(self
            .entry(name)?
            .stats
            .as_ref()
            .map(|s| s.partitions.clone())
            .unwrap_or_default())
    }

    /// Namespaces that currently hold at least one table.
    pub fn list_namespaces(&self) -> Vec<NamespaceIdent> {
        let mut out: Vec<NamespaceIdent> = self
            .tables
            .values()
            .map(|t| NamespaceIdent {
                catalog: t.ident.catalog.clone(),
                namespace: t.ident.namespace.clone(),
            })
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// Table names in `namespace` of the default catalog, or `catalog.namespace`.
    pub fn list_tables(&self, namespace: Option<&str>) -> Result<Vec<String>> {
        let ns = match namespace {
            None => NamespaceIdent {
                catalog: DEFAULT_CATALOG.into(),
                namespace: DEFAULT_NAMESPACE.into(),
            },
            Some(raw) => {
                let parts: Vec<&str> = raw.split('.').collect();
                match parts.as_slice() {
                    [n] => NamespaceIdent {
                        catalog: DEFAULT_CATALOG.into(),
                        namespace: normalize_part(n),
                    },
                    [c, n] => NamespaceIdent {
                        catalog: normalize_part(c),
                        namespace: normalize_part(n),
                    },
                    _ => return Err(Error::Plan(format!("invalid namespace: {raw}"))),
                }
            }
        };
        let mut out: Vec<String> = self
            .tables
            .values()
            .filter(|t| t.ident.catalog == ns.catalog && t.ident.namespace == ns.namespace)
            .map(|t| t.ident.table.clone())
            .collect();
        out.sort();
        Ok(out)
    }

    /// Drop a table. `if_exists` turns a missing name into a no-op.
    pub fn drop_table(&mut self, name: impl Into<String>, if_exists: bool) -> Result<()> {
        let ident = TableIdent::parse(&name.into())?;
        match self.tables.remove(&ident.key()) {
            Some(_) => {
                if let Some(dir) = &self.file_dir {
                    remove_files(dir, &ident)?;
                }
                Ok(())
            }
            None if if_exists => Ok(()),
            None => Err(Error::Plan(format!(
                "table not found: {}",
                ident.sql_name()
            ))),
        }
    }

    /// Replace the schema of an existing table, keeping stats that still apply.
    pub fn alter_schema(&mut self, name: impl Into<String>, schema: SchemaRef) -> Result<()> {
        let ident = TableIdent::parse(&name.into())?;
        let schema = from_arrow_schema(schema)?;
        let table = self
            .tables
            .get_mut(&ident.key())
            .ok_or_else(|| Error::Plan(format!("table not found: {}", ident.sql_name())))?;
        table.schema = schema;
        self.persist(&ident)
    }

    fn entry(&self, name: &str) -> Result<&CatalogTable> {
        let ident = TableIdent::parse(name)?;
        self.tables
            .get(&ident.key())
            .ok_or_else(|| Error::Plan(format!("table not found: {}", ident.sql_name())))
    }

    fn persist(&self, ident: &TableIdent) -> Result<()> {
        let Some(dir) = &self.file_dir else {
            return Ok(());
        };
        let Some(table) = self.tables.get(&ident.key()) else {
            return Ok(());
        };
        write_entry(dir, table)
    }
}

impl CatalogProvider for Catalog {
    fn get_table(&self, ident: &TableIdent) -> Result<TableMeta> {
        let table = self.entry(&ident.key())?;
        Ok(TableMeta {
            ident: table.ident.clone(),
            schema: table.schema.clone(),
            stats: table.stats.clone().unwrap_or_default(),
        })
    }

    fn list_namespaces(&self) -> Result<Vec<NamespaceIdent>> {
        Ok(Catalog::list_namespaces(self))
    }

    fn list_tables(&self, ns: &NamespaceIdent) -> Result<Vec<String>> {
        let raw = if ns.catalog == DEFAULT_CATALOG {
            ns.namespace.clone()
        } else {
            format!("{}.{}", ns.catalog, ns.namespace)
        };
        Catalog::list_tables(self, Some(&raw))
    }

    fn create_table(&mut self, meta: TableMeta) -> Result<()> {
        self.register(meta.ident.key(), meta.schema.clone())?;
        if meta.stats != TableStats::default() {
            self.set_stats(meta.ident.key(), meta.stats)?;
        }
        Ok(())
    }

    fn drop_table(&mut self, ident: &TableIdent, if_exists: bool) -> Result<()> {
        Catalog::drop_table(self, ident.key(), if_exists)
    }

    fn alter_table(&mut self, ident: &TableIdent, schema: SchemaRef) -> Result<()> {
        self.alter_schema(ident.key(), schema)
    }

    fn set_stats(&mut self, ident: &TableIdent, stats: TableStats) -> Result<()> {
        Catalog::set_stats(self, ident.key(), stats)
    }
}

fn normalize_part(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn ident_part(part: &str) -> Result<String> {
    let p = normalize_part(part);
    if p.is_empty()
        || p == "."
        || p == ".."
        || p.contains('/')
        || p.contains('\\')
        || p.contains('\0')
    {
        return Err(Error::Plan(format!("invalid identifier: {part}")));
    }
    Ok(p)
}

fn io_err(path: &Path, err: std::io::Error) -> Error {
    Error::Io(format!("{}: {err}", path.display()))
}

fn entry_paths(dir: &Path, ident: &TableIdent) -> (PathBuf, PathBuf) {
    let ns = dir.join(&ident.catalog).join(&ident.namespace);
    (
        ns.join(format!("{}.json", ident.table)),
        ns.join(format!("{}.arrow", ident.table)),
    )
}

fn write_entry(dir: &Path, table: &CatalogTable) -> Result<()> {
    let (json_path, arrow_path) = entry_paths(dir, &table.ident);
    if let Some(parent) = json_path.parent() {
        fs::create_dir_all(parent).map_err(|e| io_err(parent, e))?;
    }
    let file = File::create(&arrow_path).map_err(|e| io_err(&arrow_path, e))?;
    let mut writer =
        FileWriter::try_new(file, table.schema.as_ref()).map_err(|e| Error::Io(e.to_string()))?;
    writer.finish().map_err(|e| Error::Io(e.to_string()))?;
    let stats = table.stats.clone().unwrap_or_default();
    let body = json!({
        "catalog": table.ident.catalog,
        "namespace": table.ident.namespace,
        "table": table.ident.table,
        "partition_columns": stats.partition_columns,
        "partitions": stats.partitions,
        "row_count": stats.row_count,
    });
    fs::write(
        &json_path,
        serde_json::to_vec_pretty(&body).map_err(|e| Error::Io(e.to_string()))?,
    )
    .map_err(|e| io_err(&json_path, e))
}

fn remove_files(dir: &Path, ident: &TableIdent) -> Result<()> {
    let (json_path, arrow_path) = entry_paths(dir, ident);
    for p in [json_path, arrow_path] {
        if p.exists() {
            fs::remove_file(&p).map_err(|e| io_err(&p, e))?;
        }
    }
    Ok(())
}

fn load_dir(dir: &Path, cat: &mut Catalog) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for catalog_ent in fs::read_dir(dir).map_err(|e| io_err(dir, e))? {
        let catalog_ent = catalog_ent.map_err(|e| io_err(dir, e))?;
        let catalog_path = catalog_ent.path();
        if !catalog_path.is_dir() {
            continue;
        }
        for ns_ent in fs::read_dir(&catalog_path).map_err(|e| io_err(&catalog_path, e))? {
            let ns_ent = ns_ent.map_err(|e| io_err(&catalog_path, e))?;
            let ns_path = ns_ent.path();
            if !ns_path.is_dir() {
                continue;
            }
            for file in fs::read_dir(&ns_path).map_err(|e| io_err(&ns_path, e))? {
                let file = file.map_err(|e| io_err(&ns_path, e))?;
                let path = file.path();
                if path.extension().and_then(|e| e.to_str()) != Some("json") {
                    continue;
                }
                load_entry(cat, &path)?;
            }
        }
    }
    Ok(())
}

fn load_entry(cat: &mut Catalog, json_path: &Path) -> Result<()> {
    let text = fs::read_to_string(json_path).map_err(|e| io_err(json_path, e))?;
    let v: Value = serde_json::from_str(&text).map_err(|e| Error::Io(format!("catalog: {e}")))?;
    let ident = TableIdent::from_parts(
        v["catalog"]
            .as_str()
            .ok_or_else(|| Error::Io("catalog json catalog".into()))?,
        v["namespace"]
            .as_str()
            .ok_or_else(|| Error::Io("catalog json namespace".into()))?,
        v["table"]
            .as_str()
            .ok_or_else(|| Error::Io("catalog json table".into()))?,
    )?;
    let arrow_path = json_path.with_extension("arrow");
    let schema = read_schema(&arrow_path)?;
    let partitions = json_string_vec(&v["partitions"]);
    let partition_columns = json_string_vec(&v["partition_columns"]);
    let row_count = v["row_count"].as_u64().unwrap_or(0);
    cat.tables.insert(
        ident.key(),
        CatalogTable {
            ident: ident.clone(),
            schema,
            stats: Some(TableStats {
                row_count,
                partition_columns,
                partitions,
                ..TableStats::default()
            }),
        },
    );
    Ok(())
}

fn json_string_vec(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn read_schema(path: &Path) -> Result<SchemaRef> {
    let file = File::open(path).map_err(|e| io_err(path, e))?;
    let reader = FileReader::try_new(file, None).map_err(|e| Error::Io(e.to_string()))?;
    Ok(reader.schema())
}

/// Apply ADD COLUMN / RENAME COLUMN to an Arrow schema.
pub fn apply_alter(schema: &Schema, action: &AlterTable) -> Result<SchemaRef> {
    match action {
        AlterTable::AddColumn { name, data_type } => {
            if schema.field_with_name(name).is_ok() {
                return Err(Error::Plan(format!("column {name} already exists")));
            }
            let mut fields: Vec<Field> =
                schema.fields().iter().map(|f| f.as_ref().clone()).collect();
            fields.push(Field::new(name, data_type.clone(), true));
            Ok(Arc::new(Schema::new(fields)))
        }
        AlterTable::RenameColumn { old, new } => {
            if schema.field_with_name(new).is_ok() {
                return Err(Error::Plan(format!("column {new} already exists")));
            }
            let mut found = false;
            let fields: Vec<Field> = schema
                .fields()
                .iter()
                .map(|f| {
                    if f.name() == old {
                        found = true;
                        Field::new(new, f.data_type().clone(), f.is_nullable())
                            .with_metadata(f.metadata().clone())
                    } else {
                        f.as_ref().clone()
                    }
                })
                .collect();
            if !found {
                return Err(Error::Plan(format!("column {old} is not in the table")));
            }
            Ok(Arc::new(Schema::new(fields)))
        }
    }
}

/// Bound catalog statement.
#[derive(Clone, Debug)]
pub enum Ddl {
    /// `CREATE TABLE`.
    CreateTable {
        /// Table name.
        ident: TableIdent,
        /// Column schema.
        schema: SchemaRef,
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
    },
    /// `DROP TABLE`.
    DropTable {
        /// Table name.
        ident: TableIdent,
        /// `IF EXISTS`.
        if_exists: bool,
    },
    /// `ALTER TABLE`.
    AlterTable {
        /// Table name.
        ident: TableIdent,
        /// Add or rename a column.
        action: AlterTable,
    },
    /// `SHOW TABLES`.
    ShowTables {
        /// Optional `FROM namespace` / `FROM catalog.namespace`.
        namespace: Option<String>,
    },
}

/// Bound ALTER TABLE action.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlterTable {
    /// Append a nullable column.
    AddColumn {
        /// New column name.
        name: String,
        /// Arrow type.
        data_type: DataType,
    },
    /// Rename an existing column.
    RenameColumn {
        /// Current name.
        old: String,
        /// New name.
        new: String,
    },
}

pub(crate) fn normalize_name(name: &str) -> String {
    TableIdent::parse(name)
        .map(|i| i.sql_name())
        .unwrap_or_else(|_| name.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("year", DataType::Int64, true),
        ]))
    }

    fn tmp(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("mizuchi-catalog-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn unqualified_name_uses_default_namespace() {
        let mut c = Catalog::new();
        c.register("T", schema()).unwrap();
        assert_eq!(c.table("t").unwrap().field(0).name(), "id");
        assert_eq!(c.list_tables(None).unwrap(), vec!["t".to_string()]);
    }

    #[test]
    fn three_part_names_round_trip_on_memory_and_file() {
        let dir = tmp("file");
        let ident = "lake.sales.orders";
        let mut mem = Catalog::new();
        mem.register(ident, schema()).unwrap();
        mem.set_partitions(
            ident,
            vec!["year".into()],
            vec!["year=2020".into(), "year=2021".into()],
        )
        .unwrap();

        let mut file = Catalog::open_file(&dir).unwrap();
        file.create_table(mem.get_table(&TableIdent::parse(ident).unwrap()).unwrap())
            .unwrap();
        drop(file);

        let loaded = Catalog::open_file(&dir).unwrap();
        assert_eq!(loaded.table("lake.sales.orders").unwrap().fields().len(), 2);
        assert_eq!(
            loaded.partitions(ident).unwrap(),
            vec!["year=2020".to_string(), "year=2021".to_string()]
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn drop_and_alter_on_provider_trait() {
        let mut c = Catalog::new();
        let ident = TableIdent::parse("public.items").unwrap();
        c.create_table(TableMeta {
            ident: ident.clone(),
            schema: schema(),
            stats: TableStats::default(),
        })
        .unwrap();
        let added = apply_alter(
            c.table("items").unwrap(),
            &AlterTable::AddColumn {
                name: "note".into(),
                data_type: DataType::Utf8,
            },
        )
        .unwrap();
        c.alter_table(&ident, added).unwrap();
        assert_eq!(c.table("items").unwrap().fields().len(), 3);
        CatalogProvider::drop_table(&mut c, &ident, false).unwrap();
        assert!(c.table("items").is_err());
    }

    #[test]
    fn bind_ddl_create_drop_alter_show() {
        match crate::plan::bind_ddl("CREATE TABLE t (id BIGINT, year BIGINT)").unwrap() {
            Ddl::CreateTable {
                ident,
                schema,
                if_not_exists,
            } => {
                assert_eq!(ident.sql_name(), "t");
                assert!(!if_not_exists);
                assert_eq!(schema.fields().len(), 2);
                assert_eq!(schema.field(0).name(), "id");
            }
            other => panic!("{other:?}"),
        }
        match crate::plan::bind_ddl("DROP TABLE IF EXISTS public.items").unwrap() {
            Ddl::DropTable { ident, if_exists } => {
                assert_eq!(ident.sql_name(), "items");
                assert!(if_exists);
            }
            other => panic!("{other:?}"),
        }
        match crate::plan::bind_ddl("ALTER TABLE t ADD COLUMN note TEXT").unwrap() {
            Ddl::AlterTable { ident, action } => {
                assert_eq!(ident.sql_name(), "t");
                assert_eq!(
                    action,
                    AlterTable::AddColumn {
                        name: "note".into(),
                        data_type: DataType::Utf8,
                    }
                );
            }
            other => panic!("{other:?}"),
        }
        match crate::plan::bind_ddl("SHOW TABLES FROM lake.sales").unwrap() {
            Ddl::ShowTables { namespace } => {
                assert_eq!(namespace.as_deref(), Some("lake.sales"));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn three_part_name_binds_a_scan() {
        let mut c = Catalog::new();
        c.register("lake.sales.orders", schema()).unwrap();
        let plan = crate::plan::plan_sql("SELECT id FROM lake.sales.orders", &c).unwrap();
        let text = plan.explain();
        assert!(text.contains("Scan lake.sales.orders"), "{text}");
        let qualified =
            crate::plan::plan_sql("SELECT orders.id FROM lake.sales.orders", &c).unwrap();
        assert!(
            qualified.explain().contains("Scan lake.sales.orders"),
            "{}",
            qualified.explain()
        );
        let dotted =
            crate::plan::plan_sql("SELECT lake.sales.orders.id FROM lake.sales.orders", &c)
                .unwrap();
        assert!(dotted.explain().contains("Scan lake.sales.orders"));
    }

    #[test]
    fn ident_parts_reject_path_segments() {
        assert!(TableIdent::parse("..").is_err());
        assert!(TableIdent::parse("../x").is_err());
        assert!(TableIdent::parse("a/b").is_err());
        assert!(Catalog::new().register("..", schema()).is_err());
        assert!(crate::plan::bind_ddl("DROP TABLE a, b").is_err());
    }
}
