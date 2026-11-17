//! Compare the tiny decision-support suite against DuckDB and DataFusion.
//!
//! Same SQL and same generated tables. Live CLIs are used when present;
//! otherwise committed goldens under `tests/compare/goldens` keep CI offline.
//!
//! Timing is not like for like. mizuchi's figure is query execution on
//! preloaded tables. A reference CLI is timed as one whole process (startup,
//! CSV load and every query) divided evenly across the queries, reported as
//! `amortized_process_ms`; `amortized_time_ratio` is that figure over
//! `mizuchi_ms`. Use `scripts/bench-gb.sh` for per-query comparisons.

mod ds_common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use mizuchi::Batch;

use ds_common::{QUERIES, load, tables};

struct EngineRun {
    source: &'static str,
    /// Query time for mizuchi; amortized whole-process time for a reference.
    elapsed_ms: f64,
    rows: Vec<Vec<String>>,
}

fn golden_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/compare/goldens")
}

fn format_batches(batches: &[Batch]) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for batch in batches {
        for r in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());
            for c in 0..batch.num_columns() {
                row.push(format_cell(batch.column(c), r));
            }
            rows.push(row);
        }
    }
    rows
}

fn format_cell(arr: &ArrayRef, row: usize) -> String {
    if arr.is_null(row) {
        return "NULL".into();
    }
    match arr.data_type() {
        DataType::Int64 => arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::Float64 => {
            let v = arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row);
            format!("{v:.8}")
        }
        DataType::Utf8 => arr
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row)
            .to_string(),
        other => panic!("unexpected compare type {other}"),
    }
}

fn write_csv(path: &Path, batch: &Batch) {
    use std::io::Write;
    let mut f = fs::File::create(path).expect("csv create");
    let names: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    writeln!(f, "{}", names.join(",")).unwrap();
    for r in 0..batch.num_rows() {
        let mut cells = Vec::new();
        for c in 0..batch.num_columns() {
            let arr = batch.column(c);
            if arr.is_null(r) {
                cells.push(String::new());
                continue;
            }
            let cell = format_cell(arr, r);
            if matches!(arr.data_type(), DataType::Utf8) {
                cells.push(format!("\"{}\"", cell.replace('"', "\"\"")));
            } else {
                cells.push(cell);
            }
        }
        writeln!(f, "{}", cells.join(",")).unwrap();
    }
}

fn rows_match(left: &[Vec<String>], right: &[Vec<String>]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .all(|(a, b)| a.len() == b.len() && a.iter().zip(b).all(|(x, y)| cells_close(x, y)))
}

fn cells_close(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    let (Ok(x), Ok(y)) = (a.parse::<f64>(), b.parse::<f64>()) else {
        return false;
    };
    let diff = (x - y).abs();
    diff <= 1e-6 || diff <= 1e-6 * x.abs().max(y.abs())
}

/// Golden format: one row per line, cells joined by `|`. A backslash,
/// `|` or newline inside a cell is escaped as `\\`, `\|` or `\n`.
fn encode_rows(rows: &[Vec<String>]) -> String {
    let escape = |c: &String| {
        c.replace('\\', "\\\\")
            .replace('|', "\\|")
            .replace('\n', "\\n")
    };
    rows.iter()
        .map(|r| r.iter().map(escape).collect::<Vec<_>>().join("|"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Inverse of [`encode_rows`].
fn decode_rows(s: &str) -> Vec<Vec<String>> {
    s.lines()
        .filter(|l| !l.is_empty())
        .map(|line| {
            let mut cells = vec![String::new()];
            let mut chars = line.chars();
            while let Some(ch) = chars.next() {
                let cell = cells.last_mut().unwrap();
                match ch {
                    '\\' => match chars.next() {
                        Some('n') => cell.push('\n'),
                        Some(other) => cell.push(other),
                        None => cell.push('\\'),
                    },
                    '|' => cells.push(String::new()),
                    other => cell.push(other),
                }
            }
            cells
        })
        .collect()
}

fn run_mizuchi() -> Vec<EngineRun> {
    let (session, _) = load(1);
    QUERIES
        .iter()
        .map(|q| {
            let start = Instant::now();
            let out = session
                .execute_sql(q.sql)
                .unwrap_or_else(|e| panic!("{}: {e}", q.id));
            EngineRun {
                source: "live",
                elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
                rows: format_batches(&out),
            }
        })
        .collect()
}

fn load_golden(engine: &str, id: &str) -> Option<Vec<Vec<String>>> {
    let path = golden_dir().join(engine).join(format!("{id}.txt"));
    fs::read_to_string(path).ok().map(|s| decode_rows(&s))
}

fn write_tables_csv(dir: &Path) {
    fs::create_dir_all(dir).unwrap();
    for (name, batch) in tables(1) {
        write_csv(&dir.join(format!("{name}.csv")), &batch);
    }
}

/// `path` as a single-quoted SQL string literal, with `'` doubled.
fn sql_path(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "''"))
}

fn csv_create_sql(dir: &Path) -> String {
    let mut s = String::new();
    for name in tables(1).keys() {
        let path = dir.join(format!("{name}.csv"));
        s.push_str(&format!(
            "CREATE TABLE {name} AS SELECT * FROM read_csv_auto({}, HEADER=true);\n",
            sql_path(&path)
        ));
    }
    s
}

/// One CSV cell and whether it was quoted. Only an unquoted empty cell is
/// NULL; `""` is an empty string.
#[derive(Default)]
struct CsvCell {
    text: String,
    quoted: bool,
}

/// RFC 4180 records: quoted cells may hold commas, newlines and `""` for a
/// quote. Blank lines are skipped.
fn parse_csv_records(text: &str) -> Vec<Vec<CsvCell>> {
    let mut records = Vec::new();
    let mut record = Vec::new();
    let mut cell = CsvCell::default();
    let mut in_quotes = false;
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if in_quotes {
            match ch {
                '"' if chars.peek() == Some(&'"') => {
                    chars.next();
                    cell.text.push('"');
                }
                '"' => in_quotes = false,
                other => cell.text.push(other),
            }
            continue;
        }
        match ch {
            '"' => {
                in_quotes = true;
                cell.quoted = true;
            }
            ',' => record.push(std::mem::take(&mut cell)),
            '\r' => {}
            '\n' => {
                record.push(std::mem::take(&mut cell));
                let blank =
                    record.len() == 1 && !record[0].quoted && record[0].text.trim().is_empty();
                if blank {
                    record.clear();
                } else {
                    records.push(std::mem::take(&mut record));
                }
            }
            other => cell.text.push(other),
        }
    }
    if cell.quoted || !cell.text.trim().is_empty() || !record.is_empty() {
        record.push(cell);
        records.push(record);
    }
    records
}

/// Reference CSV output (header first) as normalized cells: NULL for an
/// unquoted empty cell, integers without a fraction, other numbers to eight
/// places.
fn parse_csv_rows(text: &str) -> Vec<Vec<String>> {
    let mut records = parse_csv_records(text).into_iter();
    let Some(header) = records.next() else {
        return Vec::new();
    };
    let width = header.len();
    records
        .map(|record| {
            let mut cells: Vec<String> = record
                .into_iter()
                .map(|c| {
                    let text = if c.quoted {
                        c.text
                    } else {
                        c.text.trim().to_string()
                    };
                    if text.is_empty() && !c.quoted {
                        "NULL".into()
                    } else if let Ok(v) = text.parse::<f64>() {
                        if v.fract() == 0.0 && v.abs() < 1e15 {
                            format!("{:.0}", v)
                        } else {
                            format!("{v:.8}")
                        }
                    } else {
                        text
                    }
                })
                .collect();
            cells.resize(width.max(cells.len()), "NULL".into());
            cells
        })
        .collect()
}

/// Runs the queries on an installed duckdb CLI. Any failure is an error: a
/// present binary that cannot produce results must not fall back to goldens.
fn run_duckdb(dir: &Path, bin: &Path) -> Result<Vec<EngineRun>, String> {
    let mut sql = csv_create_sql(dir);
    for q in QUERIES {
        let out = dir.join(format!("duckdb-{}.csv", q.id));
        sql.push_str(&format!(
            "COPY ({}) TO {} (FORMAT CSV, HEADER true);\n",
            q.sql,
            sql_path(&out)
        ));
    }
    let start = Instant::now();
    let got = Command::new(bin)
        .args(["-c", &sql])
        .output()
        .map_err(|e| format!("could not start {}: {e}", bin.display()))?;
    let total = start.elapsed().as_secs_f64() * 1000.0;
    if !got.status.success() {
        return Err(format!(
            "exited with {}: {}",
            got.status,
            String::from_utf8_lossy(&got.stderr)
        ));
    }
    let per = total / QUERIES.len() as f64;
    let mut out = Vec::new();
    for q in QUERIES {
        let path = dir.join(format!("duckdb-{}.csv", q.id));
        let text = fs::read_to_string(&path).map_err(|e| format!("no result for {}: {e}", q.id))?;
        out.push(EngineRun {
            source: "live",
            elapsed_ms: per,
            rows: parse_csv_rows(&text),
        });
    }
    Ok(out)
}

fn df_create_sql(dir: &Path) -> String {
    let mut s = String::new();
    for name in tables(1).keys() {
        let path = dir.join(format!("{name}.csv"));
        s.push_str(&format!(
            "CREATE EXTERNAL TABLE {name} STORED AS CSV LOCATION {} OPTIONS ('has_header' 'true');\n",
            sql_path(&path)
        ));
    }
    s
}

/// Runs the queries on an installed datafusion-cli; failures are errors, as
/// for [`run_duckdb`].
fn run_datafusion(dir: &Path, bin: &Path) -> Result<Vec<EngineRun>, String> {
    let mut script = df_create_sql(dir);
    for q in QUERIES {
        script.push_str(q.sql);
        script.push_str(";\n");
    }
    let path = dir.join("datafusion.sql");
    fs::write(&path, &script).map_err(|e| format!("could not write script: {e}"))?;
    let start = Instant::now();
    let got = Command::new(bin)
        .arg("-f")
        .arg(&path)
        .output()
        .map_err(|e| format!("could not start {}: {e}", bin.display()))?;
    let total = start.elapsed().as_secs_f64() * 1000.0;
    if !got.status.success() {
        return Err(format!(
            "exited with {}: {}",
            got.status,
            String::from_utf8_lossy(&got.stderr)
        ));
    }
    let blocks = split_df_tables(&String::from_utf8_lossy(&got.stdout));
    if blocks.len() != QUERIES.len() {
        return Err(format!(
            "expected {} result tables, got {}; stderr: {}",
            QUERIES.len(),
            blocks.len(),
            String::from_utf8_lossy(&got.stderr)
        ));
    }
    let per = total / QUERIES.len() as f64;
    Ok(QUERIES
        .iter()
        .zip(blocks)
        .map(|(_q, rows)| EngineRun {
            source: "live",
            elapsed_ms: per,
            rows,
        })
        .collect())
}

fn split_df_tables(stdout: &str) -> Vec<Vec<Vec<String>>> {
    stdout
        .split("\n\n")
        .map(parse_cli_table)
        .filter(|rows| !rows.is_empty())
        .collect()
}

/// Parse datafusion-cli default table output (header, separator, rows).
fn parse_cli_table(stdout: &str) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for line in stdout.lines() {
        let t = line.trim();
        if t.is_empty() || t.starts_with('+') || t.starts_with('|') && t.contains("---") {
            continue;
        }
        if !t.starts_with('|') {
            continue;
        }
        let cells: Vec<String> = t
            .trim_matches('|')
            .split('|')
            .map(|c| {
                let c = c.trim();
                if c.is_empty() || c.eq_ignore_ascii_case("null") {
                    "NULL".into()
                } else if let Ok(v) = c.parse::<f64>() {
                    if v.fract() == 0.0 && v.abs() < 1e15 {
                        format!("{:.0}", v)
                    } else {
                        format!("{v:.8}")
                    }
                } else {
                    c.to_string()
                }
            })
            .collect();
        if cells.is_empty() {
            continue;
        }
        rows.push(cells);
    }
    if rows.len() > 1 {
        rows.remove(0);
    }
    rows
}

fn which(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let p = dir.join(name);
        if p.is_file() {
            return Some(p);
        }
    }
    None
}

/// Live results when the engine's binary is installed, goldens only when it
/// is absent. An installed binary whose run fails panics with its error.
fn reference_for(
    engine: &'static str,
    live: Option<Result<Vec<EngineRun>, String>>,
) -> Vec<EngineRun> {
    match live {
        Some(Ok(live)) => return live,
        Some(Err(e)) => panic!("{engine} is installed but its run failed: {e}"),
        None => {}
    }
    QUERIES
        .iter()
        .map(|q| EngineRun {
            source: "golden",
            elapsed_ms: 0.0,
            rows: load_golden(engine, q.id).unwrap_or_else(|| {
                panic!(
                    "missing golden tests/compare/goldens/{}/{}.txt",
                    engine, q.id
                )
            }),
        })
        .collect()
}

fn report_json(
    mizuchi: &[EngineRun],
    duckdb: &[EngineRun],
    datafusion: &[EngineRun],
) -> (String, bool) {
    let mut ok = true;
    let mut s = String::from("{\n  \"suite\": \"engine-compare\",\n  \"queries\": [\n");
    for i in 0..QUERIES.len() {
        let m = &mizuchi[i];
        let d = &duckdb[i];
        let f = &datafusion[i];
        let d_match = rows_match(&m.rows, &d.rows);
        let f_match = rows_match(&m.rows, &f.rows);
        if !d_match || !f_match {
            ok = false;
        }
        let d_ratio = if m.elapsed_ms > 0.0 && d.source == "live" {
            d.elapsed_ms / m.elapsed_ms
        } else {
            0.0
        };
        let f_ratio = if m.elapsed_ms > 0.0 && f.source == "live" {
            f.elapsed_ms / m.elapsed_ms
        } else {
            0.0
        };
        s.push_str("    {\n");
        s.push_str(&format!("      \"id\": \"{}\",\n", QUERIES[i].id));
        s.push_str(&format!("      \"mizuchi_ms\": {:.3},\n", m.elapsed_ms));
        s.push_str(&format!(
            "      \"duckdb\": {{\"source\": \"{}\", \"amortized_process_ms\": {:.3}, \"match\": {}, \"amortized_time_ratio\": {:.3}}},\n",
            d.source, d.elapsed_ms, d_match, d_ratio
        ));
        s.push_str(&format!(
            "      \"datafusion\": {{\"source\": \"{}\", \"amortized_process_ms\": {:.3}, \"match\": {}, \"amortized_time_ratio\": {:.3}}}\n",
            f.source, f.elapsed_ms, f_match, f_ratio
        ));
        s.push_str("    }");
        if i + 1 < QUERIES.len() {
            s.push(',');
        }
        s.push('\n');
        if !d_match {
            eprintln!(
                "{} duckdb mismatch\n mizuchi: {:?}\n duckdb: {:?}",
                QUERIES[i].id, m.rows, d.rows
            );
        }
        if !f_match {
            eprintln!(
                "{} datafusion mismatch\n mizuchi: {:?}\n datafusion: {:?}",
                QUERIES[i].id, m.rows, f.rows
            );
        }
    }
    s.push_str("  ]\n}\n");
    (s, ok)
}

/// Scratch directory owned by one test run: the process id and a counter keep
/// concurrent runs apart, and drop removes only this run's files.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(1);
        let path = std::env::temp_dir().join(format!(
            "mizuchi-{tag}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn tiny_compare_matches_reference_goldens() {
    let dir = TempDir::new("engine-compare");
    let tmp = &dir.0;
    write_tables_csv(tmp);
    let mizuchi = run_mizuchi();
    let duckdb = reference_for("duckdb", which("duckdb").map(|b| run_duckdb(tmp, &b)));
    let datafusion = reference_for(
        "datafusion",
        which("datafusion-cli").map(|b| run_datafusion(tmp, &b)),
    );
    let (json, ok) = report_json(&mizuchi, &duckdb, &datafusion);
    println!("{json}");
    assert!(json.contains("\"amortized_time_ratio\""));
    assert!(json.contains("\"match\""));
    assert!(ok, "result diffs against reference engines");
}

#[test]
#[ignore]
fn record_goldens_if_requested() {
    if std::env::var("MIZUCHI_RECORD_GOLDENS").ok().as_deref() != Some("1") {
        return;
    }
    let dir = TempDir::new("engine-compare-record");
    let tmp = &dir.0;
    write_tables_csv(tmp);
    let duck = run_duckdb(
        tmp,
        &which("duckdb").expect("duckdb CLI required to record goldens"),
    )
    .unwrap_or_else(|e| panic!("duckdb failed: {e}"));
    let df = run_datafusion(
        tmp,
        &which("datafusion-cli").expect("datafusion-cli required to record goldens"),
    )
    .unwrap_or_else(|e| panic!("datafusion failed: {e}"));
    for (engine, runs) in [("duckdb", duck), ("datafusion", df)] {
        let dir = golden_dir().join(engine);
        fs::create_dir_all(&dir).unwrap();
        for (q, run) in QUERIES.iter().zip(runs) {
            fs::write(dir.join(format!("{}.txt", q.id)), encode_rows(&run.rows)).unwrap();
        }
    }
}

/// Writes an executable shell script that prints to stderr and exits 1.
#[cfg(unix)]
fn failing_stub(dir: &Path, name: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join(name);
    fs::write(&path, "#!/bin/sh\necho stub-engine-broke >&2\nexit 3\n").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    path
}

#[cfg(unix)]
#[test]
fn an_installed_reference_that_fails_reports_its_stderr() {
    let dir = TempDir::new("engine-compare-stub");
    let tmp = &dir.0;
    write_tables_csv(tmp);
    let duck = run_duckdb(tmp, &failing_stub(tmp, "duckdb"))
        .err()
        .expect("a failing stub must not succeed");
    assert!(duck.contains("stub-engine-broke"), "{duck}");
    let df = run_datafusion(tmp, &failing_stub(tmp, "datafusion-cli"))
        .err()
        .expect("a failing stub must not succeed");
    assert!(df.contains("stub-engine-broke"), "{df}");
}

#[test]
#[should_panic(expected = "duckdb is installed but its run failed: stub-engine-broke")]
fn a_failed_reference_run_does_not_fall_back_to_goldens() {
    reference_for("duckdb", Some(Err("stub-engine-broke".into())));
}

#[test]
fn reference_csv_with_quoted_commas_round_trips_through_goldens() {
    let csv =
        "name,note,v\n\"a,b\",\"say \"\"hi\"\"\",1\n\"x|y\",\"\",\n\"two\nlines\",plain,2.5\n";
    let rows = parse_csv_rows(csv);
    let want: Vec<Vec<String>> = [
        ["a,b", "say \"hi\"", "1"],
        ["x|y", "", "NULL"],
        ["two\nlines", "plain", "2.50000000"],
    ]
    .iter()
    .map(|r| r.iter().map(|c| c.to_string()).collect())
    .collect();
    assert_eq!(rows, want);
    assert_eq!(decode_rows(&encode_rows(&rows)), rows);
}

#[test]
fn sql_paths_escape_single_quotes() {
    assert_eq!(sql_path(Path::new("/tmp/it's.csv")), "'/tmp/it''s.csv'");
}
