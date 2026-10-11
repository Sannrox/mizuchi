//! SQLLogicTest runner for the adopted corpus under `tests/slt`.
//!
//! Coverage of window, CTE, subquery, set, DECIMAL, and date/time SQL is
//! recorded in `docs/sql-coverage.md`. Gaps live behind `skipif mizuchi`.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use mizuchi::{Batch, Session};

const ENGINE: &str = "mizuchi";

#[test]
fn slt_corpus() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/slt");
    let mut files: Vec<PathBuf> = fs::read_dir(&root)
        .unwrap_or_else(|e| panic!("read {}: {e}", root.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("slt"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no .slt files under {}", root.display());
    for path in files {
        run_file(&path);
    }
}

fn run_file(path: &Path) {
    let src = fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let recs = parse_slt(&src);
    let mut db = Db::new();
    for (i, rec) in recs.into_iter().enumerate() {
        rec.run(&mut db)
            .unwrap_or_else(|e| panic!("{} record {}: {e}", path.display(), i + 1));
    }
}

struct Db {
    tables: BTreeMap<String, (SchemaRef, Vec<Vec<Cell>>)>,
}

impl Db {
    fn new() -> Self {
        Self {
            tables: BTreeMap::new(),
        }
    }

    fn session(&self) -> Session {
        let mut s = Session::new();
        for (name, (schema, rows)) in &self.tables {
            let batch = rows_to_batch(schema.clone(), rows);
            s.register(name, schema.clone(), vec![batch]).unwrap();
        }
        s
    }
}

#[derive(Clone)]
enum Cell {
    Null,
    I64(i64),
    F64(f64),
    Utf8(String),
}

enum Record {
    Skip,
    StatementOk(String),
    StatementError(String),
    Query {
        sql: String,
        types: String,
        sort: SortMode,
        expected: Vec<String>,
    },
}

#[derive(Clone, Copy)]
enum SortMode {
    No,
    Row,
    Value,
}

impl Record {
    fn run(self, db: &mut Db) -> Result<(), String> {
        match self {
            Record::Skip => Ok(()),
            Record::StatementOk(sql) => exec_statement(db, &sql),
            Record::StatementError(sql) => match exec_statement(db, &sql) {
                Err(_) => Ok(()),
                Ok(()) => Err(format!("expected error from {sql}")),
            },
            Record::Query {
                sql,
                types,
                sort,
                expected,
            } => {
                let out = db
                    .session()
                    .execute_sql(&sql)
                    .map_err(|e| format!("{sql}: {e}"))?;
                let mut got = format_result(&out, &types)?;
                apply_sort(&mut got, types.len(), sort);
                let mut expected = expected;
                apply_sort(&mut expected, types.len(), sort);
                if got != expected {
                    return Err(format!("{sql}\n got: {got:?}\nwant: {expected:?}"));
                }
                Ok(())
            }
        }
    }
}

fn parse_slt(src: &str) -> Vec<Record> {
    let lines: Vec<&str> = src.lines().collect();
    let mut i = 0;
    let mut out = Vec::new();
    while i < lines.len() {
        let line = lines[i].trim();
        if line.is_empty() || line.starts_with('#') {
            i += 1;
            continue;
        }
        let mut skip = false;
        let mut j = i;
        while j < lines.len() {
            let t = lines[j].trim();
            if let Some(rest) = t.strip_prefix("skipif ") {
                skip = skip || rest.trim() == ENGINE;
                j += 1;
                continue;
            }
            if let Some(rest) = t.strip_prefix("onlyif ") {
                skip = skip || rest.trim() != ENGINE;
                j += 1;
                continue;
            }
            break;
        }
        if j >= lines.len() {
            break;
        }
        let head = lines[j].trim();
        if head == "halt" {
            break;
        }
        if head.starts_with("hash-threshold") {
            i = j + 1;
            continue;
        }
        if skip {
            i = skip_record(lines.as_slice(), j);
            out.push(Record::Skip);
            continue;
        }
        if head.starts_with("statement ok") {
            let (sql, next) = take_sql(lines.as_slice(), j + 1);
            out.push(Record::StatementOk(sql));
            i = next;
            continue;
        }
        if head.starts_with("statement error") {
            let (sql, next) = take_sql(lines.as_slice(), j + 1);
            out.push(Record::StatementError(sql));
            i = next;
            continue;
        }
        if let Some(rest) = head.strip_prefix("query ") {
            let mut parts = rest.split_whitespace();
            let types = parts.next().unwrap_or("").to_string();
            let sort = match parts.next() {
                Some("rowsort") => SortMode::Row,
                Some("valuesort") => SortMode::Value,
                _ => SortMode::No,
            };
            let (sql, after_sql) = take_sql(lines.as_slice(), j + 1);
            let (expected, next) = take_expected(lines.as_slice(), after_sql);
            out.push(Record::Query {
                sql,
                types,
                sort,
                expected,
            });
            i = next;
            continue;
        }
        panic!("unrecognized SLT directive at line {}: {head}", j + 1);
    }
    out
}

fn skip_record(lines: &[&str], start: usize) -> usize {
    let mut i = start + 1;
    while i < lines.len() {
        let t = lines[i].trim();
        if t == "----" {
            i += 1;
            while i < lines.len() && !lines[i].trim().is_empty() {
                i += 1;
            }
            return i;
        }
        if t.is_empty() {
            return i + 1;
        }
        i += 1;
    }
    i
}

fn take_sql(lines: &[&str], mut i: usize) -> (String, usize) {
    let mut sql = Vec::new();
    while i < lines.len() {
        let t = lines[i];
        if t.trim() == "----" || t.trim().is_empty() {
            break;
        }
        sql.push(t);
        i += 1;
    }
    (sql.join("\n"), i)
}

fn take_expected(lines: &[&str], mut i: usize) -> (Vec<String>, usize) {
    if i < lines.len() && lines[i].trim() == "----" {
        i += 1;
    }
    let mut expected = Vec::new();
    while i < lines.len() {
        let t = lines[i];
        if t.trim().is_empty() {
            break;
        }
        expected.push(t.to_string());
        i += 1;
    }
    (expected, i)
}

fn exec_statement(db: &mut Db, sql: &str) -> Result<(), String> {
    let trimmed = sql.trim();
    let upper = trimmed.to_ascii_uppercase();
    if upper.starts_with("CREATE TABLE") {
        create_table(db, trimmed)
    } else if upper.starts_with("INSERT INTO") {
        insert_into(db, trimmed)
    } else if upper.starts_with("MERGE") {
        merge_into(db, trimmed)
    } else {
        db.session()
            .execute_sql(trimmed)
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

fn merge_into(db: &mut Db, sql: &str) -> Result<(), String> {
    let mut session = db.session();
    let stats = session.merge_sql(sql).map_err(|e| e.to_string())?;
    let (schema, batches) = session
        .table_data(&stats.table)
        .map_err(|e| e.to_string())?;
    let key = db
        .tables
        .keys()
        .find(|k| k.eq_ignore_ascii_case(&stats.table))
        .cloned()
        .unwrap_or_else(|| stats.table.clone());
    let rows = batches_to_rows(&schema, &batches)?;
    db.tables.insert(key, (schema, rows));
    Ok(())
}

fn batches_to_rows(schema: &SchemaRef, batches: &[Batch]) -> Result<Vec<Vec<Cell>>, String> {
    let mut rows = Vec::new();
    for batch in batches {
        for r in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(schema.fields().len());
            for c in 0..schema.fields().len() {
                let arr = batch.column(c);
                if arr.is_null(r) {
                    row.push(Cell::Null);
                    continue;
                }
                row.push(match schema.field(c).data_type() {
                    DataType::Int64 => {
                        let a = arr
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| "expected int64".to_string())?;
                        Cell::I64(a.value(r))
                    }
                    DataType::Float64 => {
                        let a = arr
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or_else(|| "expected float64".to_string())?;
                        Cell::F64(a.value(r))
                    }
                    DataType::Utf8 => {
                        let a = arr
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| "expected utf8".to_string())?;
                        Cell::Utf8(a.value(r).to_string())
                    }
                    other => return Err(format!("unsupported merge result type {other}")),
                });
            }
            rows.push(row);
        }
    }
    Ok(rows)
}

fn create_table(db: &mut Db, sql: &str) -> Result<(), String> {
    let rest = sql
        .trim()
        .strip_prefix("CREATE TABLE")
        .or_else(|| sql.trim().strip_prefix("create table"))
        .ok_or_else(|| format!("not CREATE TABLE: {sql}"))?
        .trim();
    let name_end = rest
        .find('(')
        .ok_or_else(|| format!("missing columns: {sql}"))?;
    let name = rest[..name_end].trim().to_string();
    let cols = rest[name_end..]
        .trim()
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| format!("bad column list: {sql}"))?;
    let mut fields = Vec::new();
    for part in cols.split(',') {
        let mut it = part.split_whitespace();
        let col = it.next().ok_or_else(|| format!("empty column in {sql}"))?;
        let ty = it.next().unwrap_or("INTEGER");
        let dt = match ty.to_ascii_uppercase().as_str() {
            "INT" | "INTEGER" | "BIGINT" | "SMALLINT" => DataType::Int64,
            "TEXT" | "VARCHAR" | "CHAR" | "STRING" => DataType::Utf8,
            "REAL" | "FLOAT" | "DOUBLE" => DataType::Float64,
            other => return Err(format!("unsupported slt type {other}")),
        };
        fields.push(Field::new(col, dt, true));
    }
    let schema = Arc::new(Schema::new(fields));
    db.tables.insert(name, (schema, Vec::new()));
    Ok(())
}

fn insert_into(db: &mut Db, sql: &str) -> Result<(), String> {
    let rest = sql
        .trim()
        .strip_prefix("INSERT INTO")
        .or_else(|| sql.trim().strip_prefix("insert into"))
        .ok_or_else(|| format!("not INSERT: {sql}"))?
        .trim();
    let values_at = rest
        .to_ascii_uppercase()
        .find("VALUES")
        .ok_or_else(|| format!("missing VALUES: {sql}"))?;
    let name = rest[..values_at].trim().to_string();
    let values = rest[values_at + 6..].trim();
    let (schema, rows) = db
        .tables
        .get_mut(&name)
        .ok_or_else(|| format!("unknown table {name}"))?;
    let n = schema.fields().len();
    for tuple in split_tuples(values)? {
        let cells = parse_tuple(&tuple, n)?;
        rows.push(cells);
    }
    Ok(())
}

fn split_tuples(s: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut depth = 0;
    let mut in_str = false;
    for ch in s.chars() {
        match ch {
            '\'' if !in_str => {
                in_str = true;
                if depth > 0 {
                    cur.push(ch);
                }
            }
            '\'' if in_str => {
                in_str = false;
                if depth > 0 {
                    cur.push(ch);
                }
            }
            '(' if !in_str => {
                depth += 1;
                if depth > 1 {
                    cur.push(ch);
                }
            }
            ')' if !in_str => {
                depth -= 1;
                if depth == 0 {
                    out.push(std::mem::take(&mut cur));
                } else {
                    cur.push(ch);
                }
            }
            _ if depth > 0 => cur.push(ch),
            _ => {}
        }
    }
    if depth != 0 || in_str {
        return Err(format!("unbalanced VALUES {s}"));
    }
    Ok(out)
}

fn parse_tuple(s: &str, n: usize) -> Result<Vec<Cell>, String> {
    let mut cells = Vec::new();
    let mut cur = String::new();
    let mut in_str = false;
    for ch in s.chars() {
        match ch {
            '\'' if !in_str => {
                in_str = true;
                cur.push(ch);
            }
            '\'' if in_str => {
                in_str = false;
                cur.push(ch);
            }
            ',' if !in_str => {
                cells.push(parse_cell(cur.trim())?);
                cur.clear();
            }
            _ => cur.push(ch),
        }
    }
    if !cur.trim().is_empty() {
        cells.push(parse_cell(cur.trim())?);
    }
    if cells.len() != n {
        return Err(format!("expected {n} values, got {}: {s}", cells.len()));
    }
    Ok(cells)
}

fn parse_cell(s: &str) -> Result<Cell, String> {
    if s.eq_ignore_ascii_case("NULL") {
        return Ok(Cell::Null);
    }
    if let Some(inner) = s.strip_prefix('\'').and_then(|t| t.strip_suffix('\'')) {
        return Ok(Cell::Utf8(inner.replace("''", "'")));
    }
    if s.contains('.') {
        let v: f64 = s.parse().map_err(|_| format!("bad float {s}"))?;
        return Ok(Cell::F64(v));
    }
    let v: i64 = s.parse().map_err(|_| format!("bad int {s}"))?;
    Ok(Cell::I64(v))
}

fn rows_to_batch(schema: SchemaRef, rows: &[Vec<Cell>]) -> Batch {
    let mut columns: Vec<arrow::array::ArrayRef> = Vec::new();
    for (c, field) in schema.fields().iter().enumerate() {
        match field.data_type() {
            DataType::Int64 => {
                let vals: Vec<Option<i64>> = rows
                    .iter()
                    .map(|r| match r[c] {
                        Cell::Null => None,
                        Cell::I64(v) => Some(v),
                        _ => panic!("int column"),
                    })
                    .collect();
                columns.push(Arc::new(Int64Array::from(vals)));
            }
            DataType::Float64 => {
                let vals: Vec<Option<f64>> = rows
                    .iter()
                    .map(|r| match r[c] {
                        Cell::Null => None,
                        Cell::F64(v) => Some(v),
                        Cell::I64(v) => Some(v as f64),
                        _ => panic!("float column"),
                    })
                    .collect();
                columns.push(Arc::new(Float64Array::from(vals)));
            }
            DataType::Utf8 => {
                let vals: Vec<Option<String>> = rows
                    .iter()
                    .map(|r| match &r[c] {
                        Cell::Null => None,
                        Cell::Utf8(s) => Some(s.clone()),
                        _ => panic!("text column"),
                    })
                    .collect();
                columns.push(Arc::new(StringArray::from(vals)));
            }
            other => panic!("slt column type {other}"),
        }
    }
    if rows.is_empty() {
        return Batch::empty(schema).unwrap();
    }
    Batch::try_new(schema, columns).unwrap()
}

fn format_result(batches: &[Batch], types: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            for (col, ty) in types.chars().enumerate() {
                out.push(format_cell(batch, col, row, ty)?);
            }
        }
    }
    Ok(out)
}

fn format_cell(batch: &Batch, col: usize, row: usize, ty: char) -> Result<String, String> {
    let arr = batch.column(col);
    if arr.is_null(row) {
        return Ok("NULL".into());
    }
    match ty {
        'I' => {
            let a = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "expected int64".to_string())?;
            Ok(a.value(row).to_string())
        }
        'T' => {
            let a = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "expected utf8".to_string())?;
            Ok(a.value(row).to_string())
        }
        'R' => {
            if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
                return Ok(format!("{}", a.value(row)));
            }
            if arr.data_type().is_decimal() {
                let fmt = ArrayFormatter::try_new(arr.as_ref(), &FormatOptions::default())
                    .map_err(|e| e.to_string())?;
                return Ok(fmt.value(row).to_string());
            }
            Err("expected float64 or decimal".into())
        }
        other => Err(format!("unknown result type {other}")),
    }
}

fn apply_sort(values: &mut [String], width: usize, mode: SortMode) {
    match mode {
        SortMode::No => {}
        SortMode::Value => values.sort(),
        SortMode::Row => {
            if width == 0 {
                return;
            }
            let mut rows: Vec<Vec<String>> = values.chunks(width).map(|c| c.to_vec()).collect();
            rows.sort();
            let flat: Vec<String> = rows.into_iter().flatten().collect();
            values.clone_from_slice(&flat);
        }
    }
}

#[test]
fn skipif_mizuchi_does_not_run() {
    let recs = parse_slt("skipif mizuchi\nquery I\nSELECT 1 UNION SELECT 2\n----\n1\n2\n");
    assert!(matches!(recs.as_slice(), [Record::Skip]));
}
