//! PostgreSQL table and query scans with filter and projection pushdown.
//!
//! TCP is opened only by [`read_postgres`]. `Session` requires
//! `SessionConfig.allow_network` before calling it. Database
//! writes are out of scope.

use mizuchi_core::batch::{Batch, BatchPolicy};
use mizuchi_core::error::{Error, Result};
use mizuchi_core::expr::{BinaryOp, Expr, UnaryOp};

/// Connection facts an embedder already decided to use.
#[derive(Clone, PartialEq, Eq)]
pub struct PostgresSource {
    /// Hostname or address the embedder supplied.
    pub host: String,
    /// TCP port.
    pub port: u16,
    /// Database name.
    pub database: String,
    /// Role name.
    pub user: String,
    /// Optional password. Never written to error strings or [`Debug`].
    pub password: Option<String>,
    /// When true, the client uses TLS. Default tests use a local server
    /// without TLS.
    pub ssl: bool,
}

impl std::fmt::Debug for PostgresSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSource")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "_"))
            .field("ssl", &self.ssl)
            .finish()
    }
}

impl Default for PostgresSource {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 5432,
            database: String::new(),
            user: String::new(),
            password: None,
            ssl: false,
        }
    }
}

/// Options for [`read_postgres`] and [`postgres_scan_sql`].
#[derive(Clone, Debug)]
pub struct PostgresReadOptions<'a> {
    /// `schema.table` or `table`. Mutually exclusive with [`Self::query`].
    pub table: Option<&'a str>,
    /// Subquery text. Mutually exclusive with [`Self::table`].
    pub query: Option<&'a str>,
    /// Columns to select. `None` selects `*`.
    pub projection: Option<&'a [String]>,
    /// Predicates pushed into `WHERE`.
    pub filters: Option<&'a [Expr]>,
    /// Integer column that splits parallel range scans.
    pub partition_column: Option<&'a str>,
    /// Inclusive lower bound for [`Self::partition_column`].
    pub lower_bound: Option<i64>,
    /// Inclusive upper bound for [`Self::partition_column`].
    pub upper_bound: Option<i64>,
    /// Parallel range count. `1` is a single scan.
    pub num_partitions: usize,
    /// Morsel size for decoded batches.
    pub policy: BatchPolicy,
}

impl Default for PostgresReadOptions<'static> {
    fn default() -> Self {
        Self {
            table: None,
            query: None,
            projection: None,
            filters: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: 1,
            policy: BatchPolicy::default(),
        }
    }
}

/// Build one `SELECT` per partition with projection and filters pushed down.
pub fn postgres_scan_sql(opts: &PostgresReadOptions<'_>) -> Result<Vec<String>> {
    let from = match (opts.table, opts.query) {
        (Some(table), None) => quote_table(table),
        (None, Some(query)) => format!("({query}) AS _mizuchi_src"),
        _ => {
            return Err(Error::Plan(
                "PostgreSQL source needs a table or a query".into(),
            ));
        }
    };
    let select = match opts.projection {
        None => "*".to_string(),
        Some([]) => {
            return Err(Error::Plan("PostgreSQL projection is empty".into()));
        }
        Some(names) => names
            .iter()
            .map(|n| quote_ident(n))
            .collect::<Vec<_>>()
            .join(", "),
    };
    let mut where_parts = Vec::new();
    if let Some(filters) = opts.filters {
        for pred in filters {
            where_parts.push(expr_sql(pred)?);
        }
    }
    let base = |extra: Option<String>| {
        let mut sql = format!("SELECT {select} FROM {from}");
        let mut parts = where_parts.clone();
        if let Some(extra) = extra {
            parts.push(extra);
        }
        if !parts.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&parts.join(" AND "));
        }
        sql
    };
    match (
        opts.partition_column,
        opts.lower_bound,
        opts.upper_bound,
        opts.num_partitions,
    ) {
        (None, None, None, 0 | 1) => Ok(vec![base(None)]),
        (Some(col), Some(lo), Some(hi), n) if n >= 1 => {
            if lo > hi {
                return Err(Error::Plan(
                    "PostgreSQL partition lower_bound is greater than upper_bound".into(),
                ));
            }
            let ident = quote_ident(col);
            let span = i128::from(hi) - i128::from(lo);
            let n128 = i128::from(n as u32);
            let mut sqls = Vec::with_capacity(n);
            for i in 0..n {
                let start = i64::try_from(i128::from(lo) + (span * i128::from(i as u32)) / n128)
                    .map_err(|_| Error::Plan("PostgreSQL partition bound overflow".into()))?;
                if i + 1 == n {
                    sqls.push(base(Some(format!(
                        "{ident} >= {start} AND {ident} <= {hi}"
                    ))));
                } else {
                    let end =
                        i64::try_from(i128::from(lo) + (span * i128::from((i + 1) as u32)) / n128)
                            .map_err(|_| {
                                Error::Plan("PostgreSQL partition bound overflow".into())
                            })?;
                    sqls.push(base(Some(format!(
                        "{ident} >= {start} AND {ident} < {end}"
                    ))));
                }
            }
            Ok(sqls)
        }
        _ => Err(Error::Plan(
            "PostgreSQL partition column, bounds, and num_partitions must be set together".into(),
        )),
    }
}

/// Scan PostgreSQL and yield Arrow batches. Opens TCP.
pub fn read_postgres(source: &PostgresSource, opts: PostgresReadOptions<'_>) -> Result<Vec<Batch>> {
    let sqls = postgres_scan_sql(&opts)?;
    #[cfg(not(feature = "client"))]
    {
        let _ = (source, sqls);
        Err(Error::Io(
            "PostgreSQL source requires the postgres crate feature".into(),
        ))
    }
    #[cfg(feature = "client")]
    {
        read_postgres_connect(source, &sqls, opts.policy)
    }
}

#[cfg(feature = "client")]
fn read_postgres_connect(
    source: &PostgresSource,
    sqls: &[String],
    policy: BatchPolicy,
) -> Result<Vec<Batch>> {
    if sqls.len() <= 1 {
        return match sqls.first() {
            Some(sql) => run_query(source, sql, policy),
            None => Ok(Vec::new()),
        };
    }
    let source = source.clone();
    let handles: Vec<_> = sqls
        .iter()
        .cloned()
        .map(|sql| {
            let src = source.clone();
            std::thread::spawn(move || run_query(&src, &sql, policy))
        })
        .collect();
    let mut out = Vec::new();
    for handle in handles {
        let part = handle
            .join()
            .map_err(|_| Error::Io("PostgreSQL partition worker panicked".into()))??;
        out.extend(part);
    }
    Ok(out)
}

#[cfg(feature = "client")]
fn run_query(source: &PostgresSource, sql: &str, policy: BatchPolicy) -> Result<Vec<Batch>> {
    use arrow::array::{
        ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
        Int64Builder, StringBuilder,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let mut client = connect_client(source)?;
    let stmt = client
        .prepare(sql)
        .map_err(|e| Error::Io(format!("PostgreSQL prepare failed: {e}")))?;
    let mut fields = Vec::new();
    for col in stmt.columns() {
        fields.push(Field::new(col.name(), pg_arrow_type(col.type_())?, true));
    }
    let schema = mizuchi_core::schema::from_arrow_schema(Arc::new(Schema::new(fields)))?;
    let rows = client
        .query(&stmt, &[])
        .map_err(|e| Error::Io(format!("PostgreSQL query failed: {e}")))?;

    let mut bools: Vec<BooleanBuilder> = Vec::new();
    let mut i16s: Vec<Int16Builder> = Vec::new();
    let mut i32s: Vec<Int32Builder> = Vec::new();
    let mut i64s: Vec<Int64Builder> = Vec::new();
    let mut f32s: Vec<Float32Builder> = Vec::new();
    let mut f64s: Vec<Float64Builder> = Vec::new();
    let mut utf8s: Vec<StringBuilder> = Vec::new();
    let mut kinds = Vec::new();
    for field in schema.fields() {
        match field.data_type() {
            DataType::Boolean => {
                kinds.push(0u8);
                bools.push(BooleanBuilder::new());
            }
            DataType::Int16 => {
                kinds.push(1);
                i16s.push(Int16Builder::new());
            }
            DataType::Int32 => {
                kinds.push(2);
                i32s.push(Int32Builder::new());
            }
            DataType::Int64 => {
                kinds.push(3);
                i64s.push(Int64Builder::new());
            }
            DataType::Float32 => {
                kinds.push(4);
                f32s.push(Float32Builder::new());
            }
            DataType::Float64 => {
                kinds.push(5);
                f64s.push(Float64Builder::new());
            }
            DataType::Utf8 => {
                kinds.push(6);
                utf8s.push(StringBuilder::new());
            }
            other => {
                return Err(Error::UnsupportedType(other.to_string()));
            }
        }
    }

    for row in &rows {
        let mut b = 0;
        let mut s16 = 0;
        let mut s32 = 0;
        let mut s64 = 0;
        let mut sf32 = 0;
        let mut sf64 = 0;
        let mut su = 0;
        for (i, kind) in kinds.iter().enumerate() {
            match kind {
                0 => {
                    append_opt(&mut bools[b], row.get::<_, Option<bool>>(i));
                    b += 1;
                }
                1 => {
                    append_opt(&mut i16s[s16], row.get::<_, Option<i16>>(i));
                    s16 += 1;
                }
                2 => {
                    append_opt(&mut i32s[s32], row.get::<_, Option<i32>>(i));
                    s32 += 1;
                }
                3 => {
                    append_opt(&mut i64s[s64], row.get::<_, Option<i64>>(i));
                    s64 += 1;
                }
                4 => {
                    append_opt(&mut f32s[sf32], row.get::<_, Option<f32>>(i));
                    sf32 += 1;
                }
                5 => {
                    append_opt(&mut f64s[sf64], row.get::<_, Option<f64>>(i));
                    sf64 += 1;
                }
                6 => {
                    match row.get::<_, Option<String>>(i) {
                        Some(v) => utf8s[su].append_value(v),
                        None => utf8s[su].append_null(),
                    }
                    su += 1;
                }
                _ => {}
            }
        }
    }

    let mut b = 0;
    let mut s16 = 0;
    let mut s32 = 0;
    let mut s64 = 0;
    let mut sf32 = 0;
    let mut sf64 = 0;
    let mut su = 0;
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(kinds.len());
    for kind in &kinds {
        match kind {
            0 => {
                cols.push(Arc::new(bools[b].finish()));
                b += 1;
            }
            1 => {
                cols.push(Arc::new(i16s[s16].finish()));
                s16 += 1;
            }
            2 => {
                cols.push(Arc::new(i32s[s32].finish()));
                s32 += 1;
            }
            3 => {
                cols.push(Arc::new(i64s[s64].finish()));
                s64 += 1;
            }
            4 => {
                cols.push(Arc::new(f32s[sf32].finish()));
                sf32 += 1;
            }
            5 => {
                cols.push(Arc::new(f64s[sf64].finish()));
                sf64 += 1;
            }
            6 => {
                cols.push(Arc::new(utf8s[su].finish()));
                su += 1;
            }
            _ => {}
        }
    }
    let rb = RecordBatch::try_new(schema.clone(), cols)
        .map_err(|e| Error::InvalidBatch(e.to_string()))?;
    let batch = Batch::from_arrow(rb)?;
    let mut out: Vec<Batch> = batch
        .split_by_policy(&policy)
        .into_iter()
        .filter(|b| b.num_rows() > 0)
        .collect();
    if out.is_empty() {
        out.push(Batch::empty(schema)?);
    }
    Ok(out)
}

#[cfg(feature = "client")]
fn append_opt<T, B>(builder: &mut B, value: Option<T>)
where
    B: arrow::array::ArrayBuilder + Extend<Option<T>>,
{
    builder.extend(std::iter::once(value));
}

#[cfg(feature = "client")]
fn pg_arrow_type(ty: &postgres::types::Type) -> Result<arrow::datatypes::DataType> {
    use arrow::datatypes::DataType;
    use postgres::types::Type;
    match *ty {
        Type::BOOL => Ok(DataType::Boolean),
        Type::INT2 => Ok(DataType::Int16),
        Type::INT4 => Ok(DataType::Int32),
        Type::INT8 => Ok(DataType::Int64),
        Type::FLOAT4 => Ok(DataType::Float32),
        Type::FLOAT8 => Ok(DataType::Float64),
        Type::TEXT | Type::VARCHAR | Type::NAME | Type::JSON | Type::JSONB => Ok(DataType::Utf8),
        _ => Err(Error::UnsupportedType(format!(
            "PostgreSQL type {}",
            ty.name()
        ))),
    }
}

#[cfg(feature = "client")]
fn connect_client(source: &PostgresSource) -> Result<postgres::Client> {
    let mut cfg = postgres::Config::new();
    cfg.host(&source.host)
        .port(source.port)
        .user(&source.user)
        .dbname(&source.database);
    if let Some(password) = &source.password {
        cfg.password(password);
    }
    if source.ssl {
        let connector = native_tls::TlsConnector::new()
            .map_err(|e| Error::Io(format!("PostgreSQL TLS: {e}")))?;
        let connector = postgres_native_tls::MakeTlsConnector::new(connector);
        cfg.connect(connector)
            .map_err(|e| Error::Io(format!("PostgreSQL connect failed: {e}")))
    } else {
        cfg.connect(postgres::NoTls)
            .map_err(|e| Error::Io(format!("PostgreSQL connect failed: {e}")))
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn quote_table(name: &str) -> String {
    name.split('.')
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

fn expr_sql(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Column(name) => Ok(quote_ident(name)),
        Expr::Literal(arr) => literal_sql(arr.as_ref()),
        Expr::Binary(op, left, right) => {
            let op_sql = match op {
                BinaryOp::Add => "+",
                BinaryOp::Sub => "-",
                BinaryOp::Mul => "*",
                BinaryOp::Div => "/",
                BinaryOp::Eq => "=",
                BinaryOp::NotEq => "<>",
                BinaryOp::Lt => "<",
                BinaryOp::LtEq => "<=",
                BinaryOp::Gt => ">",
                BinaryOp::GtEq => ">=",
                BinaryOp::And => "AND",
                BinaryOp::Or => "OR",
            };
            Ok(format!(
                "({} {} {})",
                expr_sql(left)?,
                op_sql,
                expr_sql(right)?
            ))
        }
        Expr::Unary(UnaryOp::Not, inner) => Ok(format!("(NOT {})", expr_sql(inner)?)),
        Expr::Unary(UnaryOp::IsNull, inner) => Ok(format!("({} IS NULL)", expr_sql(inner)?)),
        Expr::Unary(UnaryOp::IsNotNull, inner) => Ok(format!("({} IS NOT NULL)", expr_sql(inner)?)),
        _ => Err(Error::Plan("filter cannot be pushed to PostgreSQL".into())),
    }
}

fn literal_sql(arr: &dyn arrow::array::Array) -> Result<String> {
    use arrow::array::{BooleanArray, Int64Array, StringArray};
    if arr.len() != 1 {
        return Err(Error::Plan(
            "PostgreSQL filter literals must be scalar".into(),
        ));
    }
    if arr.is_null(0) {
        return Ok("NULL".into());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return Ok(a.value(0).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return Ok(if a.value(0) { "TRUE" } else { "FALSE" }.into());
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return Ok(format!("'{}'", a.value(0).replace('\'', "''")));
    }
    Err(Error::Plan(
        "PostgreSQL filter literal type is not supported".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use std::sync::Arc;

    fn i64_lit(v: i64) -> Expr {
        Expr::Literal(Arc::new(Int64Array::from(vec![v])))
    }

    #[test]
    fn scan_sql_pushes_projection_and_filter() {
        let projection = ["id".to_string(), "name".to_string()];
        let pred = Expr::Binary(
            BinaryOp::Gt,
            Box::new(Expr::Column("id".into())),
            Box::new(i64_lit(5)),
        );
        let filters = [pred];
        let sqls = postgres_scan_sql(&PostgresReadOptions {
            table: Some("public.items"),
            projection: Some(&projection),
            filters: Some(&filters),
            ..PostgresReadOptions::default()
        })
        .unwrap();
        assert_eq!(sqls.len(), 1);
        assert_eq!(
            sqls[0],
            "SELECT \"id\", \"name\" FROM \"public\".\"items\" WHERE (\"id\" > 5)"
        );
    }

    #[test]
    fn scan_sql_partitions_integer_ranges() {
        let sqls = postgres_scan_sql(&PostgresReadOptions {
            table: Some("t"),
            partition_column: Some("id"),
            lower_bound: Some(0),
            upper_bound: Some(99),
            num_partitions: 2,
            ..PostgresReadOptions::default()
        })
        .unwrap();
        assert_eq!(sqls.len(), 2);
        assert!(sqls[0].contains("\"id\" >= 0 AND \"id\" < 49"));
        assert!(sqls[1].contains("\"id\" >= 49 AND \"id\" <= 99"));
        assert!(sqls[0].starts_with("SELECT * FROM \"t\" WHERE "));
    }

    #[test]
    fn scan_sql_partitions_wide_i64_range() {
        let sqls = postgres_scan_sql(&PostgresReadOptions {
            table: Some("t"),
            partition_column: Some("id"),
            lower_bound: Some(0),
            upper_bound: Some(i64::MAX),
            num_partitions: 3,
            ..PostgresReadOptions::default()
        })
        .unwrap();
        assert_eq!(sqls.len(), 3);
        assert!(sqls[2].contains(&format!("<= {max}", max = i64::MAX)));
    }

    #[test]
    fn source_debug_redacts_password() {
        let src = PostgresSource {
            host: "db".into(),
            port: 5432,
            database: "m".into(),
            user: "u".into(),
            password: Some("super-secret".into()),
            ssl: true,
        };
        let text = format!("{src:?}");
        assert!(!text.contains("super-secret"));
        assert!(text.contains("password: Some(\"_\")") || text.contains("password: Some(_)"));
        assert!(text.contains("ssl: true"));
    }
}
