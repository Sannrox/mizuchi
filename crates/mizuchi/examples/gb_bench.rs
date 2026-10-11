//! Generate Snappy Parquet for the decision-support suite and time mizuchi.
//!
//! Large files stay under `bench-data/` (gitignored). Published SF1/SF10
//! numbers live in `docs/bench/`. Default `cargo test` does not run this.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use mizuchi::{Batch, ParquetReadOptions, Session, VERSION, read_parquet};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

const SEGMENTS: [&str; 5] = [
    "AUTOMOBILE",
    "BUILDING",
    "FURNITURE",
    "MACHINERY",
    "HOUSEHOLD",
];
const FLAGS: [&str; 3] = ["R", "A", "N"];
const STATUS: [&str; 2] = ["O", "F"];
const CATEGORIES: [&str; 3] = ["Books", "Electronics", "Home"];

const D_1994_01_01: i64 = 731;
const D_1995_03_15: i64 = 1169;
const D_1998_09_01: i64 = 2435;

/// Parquet row-group / write chunk. Independent of the engine morsel size.
const WRITE_CHUNK: usize = 65_536;

struct QuerySpec {
    id: &'static str,
    sql: &'static str,
}

const QUERIES: &[QuerySpec] = &[
    QuerySpec {
        id: "tpch_q1",
        sql: "SELECT l_returnflag, l_linestatus, \
              SUM(l_quantity) AS sum_qty, \
              SUM(l_extendedprice) AS sum_base_price, \
              SUM(l_extendedprice * (1.0 - l_discount)) AS sum_disc_price, \
              SUM(l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax)) AS sum_charge, \
              AVG(l_quantity) AS avg_qty, \
              AVG(l_extendedprice) AS avg_price, \
              AVG(l_discount) AS avg_disc, \
              COUNT(*) AS count_order \
              FROM lineitem \
              WHERE l_shipdate <= 2435 \
              GROUP BY l_returnflag, l_linestatus \
              ORDER BY l_returnflag, l_linestatus",
    },
    QuerySpec {
        id: "tpch_q3",
        sql: "SELECT l_orderkey, o_orderdate, o_shippriority, \
              SUM(l_extendedprice * (1.0 - l_discount)) AS revenue \
              FROM customer \
              JOIN orders ON c_custkey = o_custkey \
              JOIN lineitem ON l_orderkey = o_orderkey \
              WHERE c_mktsegment = 'BUILDING' \
              AND o_orderdate < 1169 \
              AND l_shipdate > 1169 \
              GROUP BY l_orderkey, o_orderdate, o_shippriority \
              ORDER BY revenue DESC, o_orderdate \
              LIMIT 10",
    },
    QuerySpec {
        id: "tpch_q6",
        sql: "SELECT SUM(l_extendedprice * l_discount) AS revenue \
              FROM lineitem \
              WHERE l_shipdate >= 731 AND l_shipdate < 1096 \
              AND l_discount >= 0.05 AND l_discount <= 0.07 \
              AND l_quantity < 24.0",
    },
    QuerySpec {
        id: "tpcds_store_sales",
        sql: "SELECT i_category, SUM(ss_sales_price * ss_quantity) AS revenue \
              FROM store_sales \
              JOIN item ON ss_item_sk = i_item_sk \
              JOIN date_dim ON ss_sold_date_sk = d_date_sk \
              WHERE d_year = 1994 \
              GROUP BY i_category \
              ORDER BY revenue DESC",
    },
];

const TABLES: [&str; 6] = [
    "customer",
    "orders",
    "lineitem",
    "item",
    "date_dim",
    "store_sales",
];

struct Scale {
    label: &'static str,
    /// Generator `s` matching `tests/ds_common` (`n_line = 48 * s`).
    s: usize,
}

struct Counts {
    n_cust: usize,
    n_orders: usize,
    n_line: usize,
    n_item: usize,
    n_sales: usize,
    n_dates: usize,
}

impl Scale {
    fn parse(raw: &str) -> Scale {
        match raw {
            "tiny" => Scale {
                label: "tiny",
                s: 1,
            },
            "1" | "sf1" => Scale {
                label: "sf1",
                s: 125_000,
            },
            "10" | "sf10" => Scale {
                label: "sf10",
                s: 1_250_000,
            },
            other => {
                eprintln!("unknown --sf {other} (use tiny, 1, or 10)");
                std::process::exit(2);
            }
        }
    }

    fn counts(&self) -> Counts {
        Counts {
            n_cust: 8 * self.s,
            n_orders: 16 * self.s,
            n_line: 48 * self.s,
            n_item: 8 * self.s,
            n_sales: 24 * self.s,
            n_dates: 12,
        }
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let cmd = args.next().unwrap_or_else(|| usage());
    match cmd.as_str() {
        "generate" => {
            let cfg = parse_flags(args);
            generate(&cfg);
        }
        "run" => {
            let cfg = parse_flags(args);
            run_mizuchi(&cfg);
        }
        _ => usage(),
    }
}

struct Flags {
    scale: Scale,
    dir: PathBuf,
    repeats: usize,
    json: Option<PathBuf>,
    workers: usize,
    force: bool,
}

fn usage() -> ! {
    eprintln!(
        "usage:
  gb_bench generate --sf tiny|1|10 --out DIR [--force]
  gb_bench run --sf tiny|1|10 --dir DIR [--repeats N] [--workers N] [--json PATH|-]"
    );
    std::process::exit(2);
}

fn parse_flags(mut args: impl Iterator<Item = String>) -> Flags {
    let mut sf = None;
    let mut dir = None;
    let mut repeats = 3usize;
    let mut json = None;
    let mut workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let mut force = false;
    while let Some(a) = args.next() {
        match a.as_str() {
            "--sf" => sf = Some(args.next().unwrap_or_else(|| usage())),
            "--out" | "--dir" => dir = Some(PathBuf::from(args.next().unwrap_or_else(|| usage()))),
            "--repeats" => {
                repeats = args
                    .next()
                    .unwrap_or_else(|| usage())
                    .parse()
                    .unwrap_or_else(|_| usage());
            }
            "--workers" => {
                workers = args
                    .next()
                    .unwrap_or_else(|| usage())
                    .parse()
                    .unwrap_or_else(|_| usage());
            }
            "--json" => json = Some(PathBuf::from(args.next().unwrap_or_else(|| usage()))),
            "--force" => force = true,
            _ => usage(),
        }
    }
    let scale = Scale::parse(&sf.unwrap_or_else(|| usage()));
    let dir = dir.unwrap_or_else(|| usage());
    Flags {
        scale,
        dir,
        repeats: repeats.max(1),
        json,
        workers: workers.max(1),
        force,
    }
}

fn data_dir(flags: &Flags) -> PathBuf {
    flags.dir.join(flags.scale.label)
}

fn generate(flags: &Flags) {
    let out = data_dir(flags);
    if out.join("lineitem.parquet").exists() && !flags.force {
        eprintln!(
            "skip generate: {} exists (pass --force to overwrite)",
            out.display()
        );
        return;
    }
    fs::create_dir_all(&out).expect("mkdir bench-data");
    let c = flags.scale.counts();
    eprintln!(
        "generate {} s={} customer={} orders={} lineitem={} item={} sales={} dates={}",
        flags.scale.label,
        flags.scale.s,
        c.n_cust,
        c.n_orders,
        c.n_line,
        c.n_item,
        c.n_sales,
        c.n_dates
    );
    write_customer(&out.join("customer.parquet"), c.n_cust);
    write_orders(&out.join("orders.parquet"), c.n_orders, c.n_cust);
    write_lineitem(&out.join("lineitem.parquet"), c.n_line, c.n_orders);
    write_item(&out.join("item.parquet"), c.n_item);
    write_date_dim(&out.join("date_dim.parquet"), c.n_dates);
    write_store_sales(
        &out.join("store_sales.parquet"),
        c.n_sales,
        c.n_item,
        c.n_dates,
    );
    let manifest = format!(
        "{{\n  \"sf\": \"{}\",\n  \"s\": {},\n  \"n_cust\": {},\n  \"n_orders\": {},\n  \"n_line\": {},\n  \"n_item\": {},\n  \"n_sales\": {},\n  \"n_dates\": {}\n}}\n",
        flags.scale.label,
        flags.scale.s,
        c.n_cust,
        c.n_orders,
        c.n_line,
        c.n_item,
        c.n_sales,
        c.n_dates
    );
    fs::write(out.join("manifest.json"), manifest).expect("manifest");
    eprintln!("wrote {}", out.display());
}

fn writer(path: &Path, schema: SchemaRef) -> ArrowWriter<File> {
    let file = File::create(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(WRITE_CHUNK))
        .build();
    ArrowWriter::try_new(file, schema, Some(props)).expect("parquet writer")
}

fn finish(w: ArrowWriter<File>, path: &Path) {
    w.close()
        .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
}

fn i64_field(name: &str) -> Field {
    Field::new(name, DataType::Int64, true)
}

fn f64_field(name: &str) -> Field {
    Field::new(name, DataType::Float64, true)
}

fn utf8_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
}

fn record(fields: Vec<Field>, cols: Vec<ArrayRef>) -> (SchemaRef, Batch) {
    let schema: SchemaRef = Arc::new(Schema::new(fields));
    let batch = Batch::try_new(schema.clone(), cols).expect("batch");
    (schema, batch)
}

fn write_chunk(w: &mut ArrowWriter<File>, batch: &Batch) {
    w.write(batch.as_arrow()).expect("parquet write");
}

fn write_customer(path: &Path, n: usize) {
    let mut w = None;
    let mut start = 0usize;
    while start < n {
        let len = (n - start).min(WRITE_CHUNK);
        let mut c_custkey = Vec::with_capacity(len);
        let mut c_nationkey = Vec::with_capacity(len);
        let mut c_mktsegment = Vec::with_capacity(len);
        for i in start..start + len {
            c_custkey.push(i as i64 + 1);
            c_nationkey.push((i % 5) as i64);
            c_mktsegment.push(SEGMENTS[i % 5]);
        }
        let (schema, batch) = record(
            vec![
                i64_field("c_custkey"),
                i64_field("c_nationkey"),
                utf8_field("c_mktsegment"),
            ],
            vec![
                Arc::new(Int64Array::from(c_custkey)),
                Arc::new(Int64Array::from(c_nationkey)),
                Arc::new(StringArray::from(c_mktsegment)),
            ],
        );
        if w.is_none() {
            w = Some(writer(path, schema));
        }
        write_chunk(w.as_mut().unwrap(), &batch);
        start += len;
    }
    finish(w.expect("customer rows"), path);
}

fn write_orders(path: &Path, n: usize, n_cust: usize) {
    let mut w = None;
    let mut start = 0usize;
    while start < n {
        let len = (n - start).min(WRITE_CHUNK);
        let mut o_orderkey = Vec::with_capacity(len);
        let mut o_custkey = Vec::with_capacity(len);
        let mut o_orderdate = Vec::with_capacity(len);
        let mut o_shippriority = Vec::with_capacity(len);
        for i in start..start + len {
            o_orderkey.push(i as i64 + 1);
            o_custkey.push((i % n_cust) as i64 + 1);
            o_orderdate.push(if i.is_multiple_of(2) {
                D_1995_03_15 - 20 - (i as i64 % 40)
            } else {
                D_1995_03_15 + 20 + (i as i64 % 40)
            });
            o_shippriority.push((i % 3) as i64);
        }
        let (schema, batch) = record(
            vec![
                i64_field("o_orderkey"),
                i64_field("o_custkey"),
                i64_field("o_orderdate"),
                i64_field("o_shippriority"),
            ],
            vec![
                Arc::new(Int64Array::from(o_orderkey)),
                Arc::new(Int64Array::from(o_custkey)),
                Arc::new(Int64Array::from(o_orderdate)),
                Arc::new(Int64Array::from(o_shippriority)),
            ],
        );
        if w.is_none() {
            w = Some(writer(path, schema));
        }
        write_chunk(w.as_mut().unwrap(), &batch);
        start += len;
    }
    finish(w.expect("orders rows"), path);
}

fn write_lineitem(path: &Path, n: usize, n_orders: usize) {
    let mut w = None;
    let mut start = 0usize;
    while start < n {
        let len = (n - start).min(WRITE_CHUNK);
        let mut l_orderkey = Vec::with_capacity(len);
        let mut l_quantity = Vec::with_capacity(len);
        let mut l_extendedprice = Vec::with_capacity(len);
        let mut l_discount = Vec::with_capacity(len);
        let mut l_tax = Vec::with_capacity(len);
        let mut l_returnflag = Vec::with_capacity(len);
        let mut l_linestatus = Vec::with_capacity(len);
        let mut l_shipdate = Vec::with_capacity(len);
        for i in start..start + len {
            l_orderkey.push((i % n_orders) as i64 + 1);
            let q6 = i.is_multiple_of(3);
            l_quantity.push(if q6 { 10.0 } else { 30.0 + (i % 20) as f64 });
            l_extendedprice.push(1000.0 + (i as f64) * 10.0);
            l_discount.push(if q6 {
                0.06
            } else {
                0.02 + (i % 8) as f64 * 0.01
            });
            l_tax.push(0.01 * ((i % 8) as f64 + 1.0));
            l_returnflag.push(FLAGS[i % 3]);
            l_linestatus.push(STATUS[i % 2]);
            l_shipdate.push(match i % 4 {
                0 => D_1994_01_01 + (i as i64 % 300),
                1 => D_1995_03_15 + 10 + (i as i64 % 50),
                _ => D_1998_09_01 - (i as i64 % 200),
            });
        }
        let (schema, batch) = record(
            vec![
                i64_field("l_orderkey"),
                f64_field("l_quantity"),
                f64_field("l_extendedprice"),
                f64_field("l_discount"),
                f64_field("l_tax"),
                utf8_field("l_returnflag"),
                utf8_field("l_linestatus"),
                i64_field("l_shipdate"),
            ],
            vec![
                Arc::new(Int64Array::from(l_orderkey)),
                Arc::new(Float64Array::from(l_quantity)),
                Arc::new(Float64Array::from(l_extendedprice)),
                Arc::new(Float64Array::from(l_discount)),
                Arc::new(Float64Array::from(l_tax)),
                Arc::new(StringArray::from(l_returnflag)),
                Arc::new(StringArray::from(l_linestatus)),
                Arc::new(Int64Array::from(l_shipdate)),
            ],
        );
        if w.is_none() {
            w = Some(writer(path, schema));
        }
        write_chunk(w.as_mut().unwrap(), &batch);
        start += len;
        if start.is_multiple_of(WRITE_CHUNK * 16) || start == n {
            eprintln!("  lineitem {start}/{n}");
        }
    }
    finish(w.expect("lineitem rows"), path);
}

fn write_item(path: &Path, n: usize) {
    let mut w = None;
    let mut start = 0usize;
    while start < n {
        let len = (n - start).min(WRITE_CHUNK);
        let mut i_item_sk = Vec::with_capacity(len);
        let mut i_category = Vec::with_capacity(len);
        for i in start..start + len {
            i_item_sk.push(i as i64 + 1);
            i_category.push(CATEGORIES[i % 3]);
        }
        let (schema, batch) = record(
            vec![i64_field("i_item_sk"), utf8_field("i_category")],
            vec![
                Arc::new(Int64Array::from(i_item_sk)),
                Arc::new(StringArray::from(i_category)),
            ],
        );
        if w.is_none() {
            w = Some(writer(path, schema));
        }
        write_chunk(w.as_mut().unwrap(), &batch);
        start += len;
    }
    finish(w.expect("item rows"), path);
}

fn write_date_dim(path: &Path, n: usize) {
    let mut d_date_sk = Vec::with_capacity(n);
    let mut d_year = Vec::with_capacity(n);
    for i in 0..n {
        d_date_sk.push(i as i64 + 1);
        d_year.push(1992 + (i as i64 % 6));
    }
    let (schema, batch) = record(
        vec![i64_field("d_date_sk"), i64_field("d_year")],
        vec![
            Arc::new(Int64Array::from(d_date_sk)),
            Arc::new(Int64Array::from(d_year)),
        ],
    );
    let mut w = writer(path, schema);
    write_chunk(&mut w, &batch);
    finish(w, path);
}

fn write_store_sales(path: &Path, n: usize, n_item: usize, n_dates: usize) {
    let mut w = None;
    let mut start = 0usize;
    while start < n {
        let len = (n - start).min(WRITE_CHUNK);
        let mut ss_item_sk = Vec::with_capacity(len);
        let mut ss_sold_date_sk = Vec::with_capacity(len);
        let mut ss_sales_price = Vec::with_capacity(len);
        let mut ss_quantity = Vec::with_capacity(len);
        for i in start..start + len {
            ss_item_sk.push((i % n_item) as i64 + 1);
            ss_sold_date_sk.push((i % n_dates) as i64 + 1);
            ss_sales_price.push(5.0 + (i % 20) as f64);
            ss_quantity.push((1 + i % 5) as f64);
        }
        let (schema, batch) = record(
            vec![
                i64_field("ss_item_sk"),
                i64_field("ss_sold_date_sk"),
                f64_field("ss_sales_price"),
                f64_field("ss_quantity"),
            ],
            vec![
                Arc::new(Int64Array::from(ss_item_sk)),
                Arc::new(Int64Array::from(ss_sold_date_sk)),
                Arc::new(Float64Array::from(ss_sales_price)),
                Arc::new(Float64Array::from(ss_quantity)),
            ],
        );
        if w.is_none() {
            w = Some(writer(path, schema));
        }
        write_chunk(w.as_mut().unwrap(), &batch);
        start += len;
    }
    finish(w.expect("store_sales rows"), path);
}

fn run_mizuchi(flags: &Flags) {
    let dir = data_dir(flags);
    if !dir.join("lineitem.parquet").exists() {
        eprintln!("missing {}; run generate first", dir.display());
        std::process::exit(1);
    }
    eprintln!(
        "mizuchi {} load {} workers={}",
        VERSION,
        dir.display(),
        flags.workers
    );
    let mut session = Session::new().with_workers(flags.workers).expect("workers");
    let mut input_bytes = 0u64;
    for name in TABLES {
        let path = dir.join(format!("{name}.parquet"));
        let batches = read_parquet(&path, ParquetReadOptions::default())
            .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let schema = batches[0].schema();
        input_bytes += batches
            .iter()
            .map(|b| b.as_arrow().get_array_memory_size() as u64)
            .sum::<u64>();
        session
            .register(name, schema, batches)
            .unwrap_or_else(|e| panic!("register {name}: {e}"));
        eprintln!("  loaded {name}");
    }

    let mut query_json = Vec::new();
    for spec in QUERIES {
        let mut trials = Vec::with_capacity(flags.repeats);
        let mut rows = 0usize;
        for trial in 0..flags.repeats {
            // Clone drops the result cache so each trial is a full execute.
            let run = session.clone();
            let start = Instant::now();
            let out = run
                .execute_sql(spec.sql)
                .unwrap_or_else(|e| panic!("{}: {e}", spec.id));
            let ms = start.elapsed().as_secs_f64() * 1000.0;
            rows = out.iter().map(|b| b.num_rows()).sum();
            eprintln!("  {} trial {} {:.3} ms rows={rows}", spec.id, trial, ms);
            trials.push(ms);
        }
        let reported = reported_ms(&trials);
        query_json.push(format!(
            "    {{\n      \"id\": \"{}\",\n      \"trials_ms\": [{}],\n      \"reported_ms\": {:.3},\n      \"rows\": {}\n    }}",
            spec.id,
            trials
                .iter()
                .map(|v| format!("{v:.3}"))
                .collect::<Vec<_>>()
                .join(", "),
            reported,
            rows
        ));
    }

    let body = format!(
        "{{\n  \"engine\": \"mizuchi\",\n  \"version\": \"{VERSION}\",\n  \"sf\": \"{}\",\n  \"repeats\": {},\n  \"workers\": {},\n  \"input_bytes\": {input_bytes},\n  \"queries\": [\n{}\n  ]\n}}\n",
        flags.scale.label,
        flags.repeats,
        flags.workers,
        query_json.join(",\n")
    );
    match &flags.json {
        Some(p) if p.as_os_str() == "-" => print!("{body}"),
        Some(p) => {
            if let Some(parent) = p.parent() {
                fs::create_dir_all(parent).ok();
            }
            let mut f = File::create(p).unwrap_or_else(|e| panic!("{}: {e}", p.display()));
            f.write_all(body.as_bytes()).expect("write json");
            eprintln!("wrote {}", p.display());
        }
        None => print!("{body}"),
    }
}

fn reported_ms(trials: &[f64]) -> f64 {
    let kept: Vec<f64> = if trials.len() > 1 {
        trials[1..].to_vec()
    } else {
        trials.to_vec()
    };
    if kept.is_empty() {
        return 0.0;
    }
    let mut sorted = kept;
    sorted.sort_by(|a, b| a.partial_cmp(b).expect("ms"));
    let n = sorted.len();
    if !n.is_multiple_of(2) {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    }
}
