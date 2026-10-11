//! Shared tiny decision-support generator and query set.
//!
//! Used by the suite harness and by the reference-engine comparison.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use mizuchi::{Batch, Session};

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

pub struct QuerySpec {
    pub id: &'static str,
    pub sql: &'static str,
}

pub const QUERIES: &[QuerySpec] = &[
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

fn i64_field(name: &str) -> Field {
    Field::new(name, DataType::Int64, true)
}

fn f64_field(name: &str) -> Field {
    Field::new(name, DataType::Float64, true)
}

fn utf8_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
}

fn batch(fields: Vec<Field>, cols: Vec<ArrayRef>) -> Batch {
    let schema: SchemaRef = Arc::new(Schema::new(fields));
    Batch::try_new(schema, cols).expect("batch")
}

pub fn batch_bytes(batch: &Batch) -> u64 {
    batch.as_arrow().get_array_memory_size() as u64
}

pub fn tables(scale: u32) -> BTreeMap<String, Batch> {
    let s = scale.max(1) as usize;
    let n_cust = 8 * s;
    let n_orders = 16 * s;
    let n_line = 48 * s;
    let n_item = 8 * s;
    let n_sales = 24 * s;
    let n_dates = 12;
    let mut out = BTreeMap::new();

    let mut c_custkey = Vec::with_capacity(n_cust);
    let mut c_nationkey = Vec::with_capacity(n_cust);
    let mut c_mktsegment = Vec::with_capacity(n_cust);
    for i in 0..n_cust {
        c_custkey.push(i as i64 + 1);
        c_nationkey.push((i % 5) as i64);
        c_mktsegment.push(SEGMENTS[i % 5].to_string());
    }
    out.insert(
        "customer".into(),
        batch(
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
        ),
    );

    let mut o_orderkey = Vec::with_capacity(n_orders);
    let mut o_custkey = Vec::with_capacity(n_orders);
    let mut o_orderdate = Vec::with_capacity(n_orders);
    let mut o_shippriority = Vec::with_capacity(n_orders);
    for i in 0..n_orders {
        o_orderkey.push(i as i64 + 1);
        o_custkey.push((i % n_cust) as i64 + 1);
        o_orderdate.push(if i % 2 == 0 {
            D_1995_03_15 - 20 - (i as i64 % 40)
        } else {
            D_1995_03_15 + 20 + (i as i64 % 40)
        });
        o_shippriority.push((i % 3) as i64);
    }
    out.insert(
        "orders".into(),
        batch(
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
        ),
    );

    let mut l_orderkey = Vec::with_capacity(n_line);
    let mut l_quantity = Vec::with_capacity(n_line);
    let mut l_extendedprice = Vec::with_capacity(n_line);
    let mut l_discount = Vec::with_capacity(n_line);
    let mut l_tax = Vec::with_capacity(n_line);
    let mut l_returnflag = Vec::with_capacity(n_line);
    let mut l_linestatus = Vec::with_capacity(n_line);
    let mut l_shipdate = Vec::with_capacity(n_line);
    for i in 0..n_line {
        l_orderkey.push((i % n_orders) as i64 + 1);
        let q6 = i % 3 == 0;
        l_quantity.push(if q6 { 10.0 } else { 30.0 + (i % 20) as f64 });
        l_extendedprice.push(1000.0 + (i as f64) * 10.0);
        l_discount.push(if q6 {
            0.06
        } else {
            0.02 + (i % 8) as f64 * 0.01
        });
        l_tax.push(0.01 * ((i % 8) as f64 + 1.0));
        l_returnflag.push(FLAGS[i % 3].to_string());
        l_linestatus.push(STATUS[i % 2].to_string());
        l_shipdate.push(match i % 4 {
            0 => D_1994_01_01 + (i as i64 % 300),
            1 => D_1995_03_15 + 10 + (i as i64 % 50),
            _ => D_1998_09_01 - (i as i64 % 200),
        });
    }
    out.insert(
        "lineitem".into(),
        batch(
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
        ),
    );

    let mut i_item_sk = Vec::with_capacity(n_item);
    let mut i_category = Vec::with_capacity(n_item);
    for i in 0..n_item {
        i_item_sk.push(i as i64 + 1);
        i_category.push(CATEGORIES[i % 3].to_string());
    }
    out.insert(
        "item".into(),
        batch(
            vec![i64_field("i_item_sk"), utf8_field("i_category")],
            vec![
                Arc::new(Int64Array::from(i_item_sk)),
                Arc::new(StringArray::from(i_category)),
            ],
        ),
    );

    let mut d_date_sk = Vec::with_capacity(n_dates);
    let mut d_year = Vec::with_capacity(n_dates);
    for i in 0..n_dates {
        d_date_sk.push(i as i64 + 1);
        d_year.push(1992 + (i as i64 % 6));
    }
    out.insert(
        "date_dim".into(),
        batch(
            vec![i64_field("d_date_sk"), i64_field("d_year")],
            vec![
                Arc::new(Int64Array::from(d_date_sk)),
                Arc::new(Int64Array::from(d_year)),
            ],
        ),
    );

    let mut ss_item_sk = Vec::with_capacity(n_sales);
    let mut ss_sold_date_sk = Vec::with_capacity(n_sales);
    let mut ss_sales_price = Vec::with_capacity(n_sales);
    let mut ss_quantity = Vec::with_capacity(n_sales);
    for i in 0..n_sales {
        ss_item_sk.push((i % n_item) as i64 + 1);
        ss_sold_date_sk.push((i % n_dates) as i64 + 1);
        ss_sales_price.push(5.0 + (i % 20) as f64);
        ss_quantity.push((1 + i % 5) as f64);
    }
    out.insert(
        "store_sales".into(),
        batch(
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
        ),
    );
    out
}

pub fn session_from_tables(tables: &BTreeMap<String, Batch>) -> (Session, u64) {
    let mut session = Session::new();
    let mut input_bytes = 0;
    for (name, b) in tables {
        input_bytes += batch_bytes(b);
        session
            .register(name, b.schema(), vec![b.clone()])
            .expect("register");
    }
    (session, input_bytes)
}

pub fn load(scale: u32) -> (Session, u64) {
    session_from_tables(&tables(scale))
}
