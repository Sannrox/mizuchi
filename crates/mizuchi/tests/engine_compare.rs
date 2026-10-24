//! Compare the tiny decision-support suite against DuckDB and DataFusion.
//!
//! Same SQL and same generated tables. Live CLIs are used when present;
//! otherwise committed goldens under `tests/compare/goldens` keep CI offline.
//! Timing ratios are `reference_ms / mizuchi_ms`.

mod ds_common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use mizuchi::Batch;

use ds_common::{QUERIES, load, tables};

struct EngineRun {
    source: &'static str,
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

fn encode_rows(rows: &[Vec<String>]) -> String {
    rows.iter()
        .map(|r| r.join("|"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn decode_rows(s: &str) -> Vec<Vec<String>> {
    s.lines()
        .filter(|l| !l.is_empty())
        .map(|l| l.split('|').map(|c| c.to_string()).collect())
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

fn csv_create_sql(dir: &Path) -> String {
    let mut s = String::new();
    for name in tables(1).keys() {
        let path = dir.join(format!("{name}.csv"));
        s.push_str(&format!(
            "CREATE TABLE {name} AS SELECT * FROM read_csv_auto('{}', HEADER=true);\n",
            path.display()
        ));
    }
    s
}

fn parse_csv_rows(stdout: &str) -> Vec<Vec<String>> {
    let mut lines = stdout.lines().filter(|l| !l.trim().is_empty());
    let header = match lines.next() {
        Some(h) => h,
        None => return Vec::new(),
    };
    let width = header.split(',').count();
    lines
        .map(|line| {
            let mut cells: Vec<String> = line.split(',').map(|c| c.trim().to_string()).collect();
            if cells.len() < width {
                cells.resize(width, String::new());
            }
            cells
                .into_iter()
                .map(|c| {
                    if c.is_empty() {
                        "NULL".into()
                    } else if let Ok(v) = c.parse::<f64>() {
                        if v.fract() == 0.0 && v.abs() < 1e15 {
                            format!("{:.0}", v)
                        } else {
                            format!("{v:.8}")
                        }
                    } else {
                        c.trim_matches('"').to_string()
                    }
                })
                .collect()
        })
        .collect()
}

fn run_duckdb(dir: &Path) -> Option<Vec<EngineRun>> {
    let bin = which("duckdb")?;
    let mut sql = csv_create_sql(dir);
    for q in QUERIES {
        let out = dir.join(format!("duckdb-{}.csv", q.id));
        sql.push_str(&format!(
            "COPY ({}) TO '{}' (FORMAT CSV, HEADER true);\n",
            q.sql,
            out.display()
        ));
    }
    let start = Instant::now();
    let got = Command::new(&bin).args(["-c", &sql]).output().ok()?;
    let total = start.elapsed().as_secs_f64() * 1000.0;
    if !got.status.success() {
        eprintln!("duckdb failed: {}", String::from_utf8_lossy(&got.stderr));
        return None;
    }
    let per = total / QUERIES.len() as f64;
    let mut out = Vec::new();
    for q in QUERIES {
        let text = fs::read_to_string(dir.join(format!("duckdb-{}.csv", q.id))).ok()?;
        out.push(EngineRun {
            source: "live",
            elapsed_ms: per,
            rows: parse_csv_rows(&text),
        });
    }
    Some(out)
}

fn df_create_sql(dir: &Path) -> String {
    let mut s = String::new();
    for name in tables(1).keys() {
        let path = dir.join(format!("{name}.csv"));
        s.push_str(&format!(
            "CREATE EXTERNAL TABLE {name} STORED AS CSV LOCATION '{}' OPTIONS ('has_header' 'true');\n",
            path.display()
        ));
    }
    s
}

fn run_datafusion(dir: &Path) -> Option<Vec<EngineRun>> {
    let bin = which("datafusion-cli")?;
    let mut script = df_create_sql(dir);
    for q in QUERIES {
        script.push_str(q.sql);
        script.push_str(";\n");
    }
    let path = dir.join("datafusion.sql");
    fs::write(&path, &script).ok()?;
    let start = Instant::now();
    let got = Command::new(&bin)
        .args(["-f", path.to_str()?])
        .output()
        .ok()?;
    let total = start.elapsed().as_secs_f64() * 1000.0;
    if !got.status.success() {
        eprintln!(
            "datafusion failed: {}",
            String::from_utf8_lossy(&got.stderr)
        );
        return None;
    }
    let blocks = split_df_tables(&String::from_utf8_lossy(&got.stdout));
    if blocks.len() != QUERIES.len() {
        eprintln!(
            "datafusion expected {} result tables, got {}",
            QUERIES.len(),
            blocks.len()
        );
        return None;
    }
    let per = total / QUERIES.len() as f64;
    Some(
        QUERIES
            .iter()
            .zip(blocks)
            .map(|(_q, rows)| EngineRun {
                source: "live",
                elapsed_ms: per,
                rows,
            })
            .collect(),
    )
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

fn reference_for(engine: &'static str, live: Option<Vec<EngineRun>>) -> Vec<EngineRun> {
    if let Some(live) = live {
        return live;
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
            "      \"duckdb\": {{\"source\": \"{}\", \"elapsed_ms\": {:.3}, \"match\": {}, \"time_ratio\": {:.3}}},\n",
            d.source, d.elapsed_ms, d_match, d_ratio
        ));
        s.push_str(&format!(
            "      \"datafusion\": {{\"source\": \"{}\", \"elapsed_ms\": {:.3}, \"match\": {}, \"time_ratio\": {:.3}}}\n",
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

#[test]
fn tiny_compare_matches_reference_goldens() {
    let tmp = std::env::temp_dir().join("mizuchi-engine-compare");
    let _ = fs::remove_dir_all(&tmp);
    write_tables_csv(&tmp);
    let mizuchi = run_mizuchi();
    let duckdb = reference_for("duckdb", run_duckdb(&tmp));
    let datafusion = reference_for("datafusion", run_datafusion(&tmp));
    let (json, ok) = report_json(&mizuchi, &duckdb, &datafusion);
    println!("{json}");
    assert!(json.contains("\"time_ratio\""));
    assert!(json.contains("\"match\""));
    assert!(ok, "result diffs against reference engines");
}

#[test]
#[ignore]
fn record_goldens_if_requested() {
    if std::env::var("MIZUCHI_RECORD_GOLDENS").ok().as_deref() != Some("1") {
        return;
    }
    let tmp = std::env::temp_dir().join("mizuchi-engine-compare-record");
    let _ = fs::remove_dir_all(&tmp);
    write_tables_csv(&tmp);
    let duck = run_duckdb(&tmp).expect("duckdb CLI required to record goldens");
    let df = run_datafusion(&tmp).expect("datafusion-cli required to record goldens");
    for (engine, runs) in [("duckdb", duck), ("datafusion", df)] {
        let dir = golden_dir().join(engine);
        fs::create_dir_all(&dir).unwrap();
        for (q, run) in QUERIES.iter().zip(runs) {
            fs::write(dir.join(format!("{}.txt", q.id)), encode_rows(&run.rows)).unwrap();
        }
    }
}
