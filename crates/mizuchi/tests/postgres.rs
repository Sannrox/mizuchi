//! PostgreSQL source tests. The live scan is not part of default `cargo test`.

use std::fs;
use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    fs::read_to_string(root().join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))
}

#[test]
fn optional_workflow_is_dispatch_only() {
    for rel in [
        "crates/mizuchi/tests/postgres.rs",
        ".github/workflows/postgres.yml",
    ] {
        assert!(root().join(rel).is_file(), "missing {rel}");
    }
    let yml = read(".github/workflows/postgres.yml");
    assert!(yml.contains("workflow_dispatch"));
    assert!(
        !yml.contains("pull_request"),
        "postgres workflow must not run on pull_request"
    );
    assert!(yml.contains("--features postgres"));
    assert!(yml.contains("--ignored"));
    let cargo = read("crates/mizuchi/Cargo.toml");
    assert!(cargo.contains("mizuchi-postgres/client"));
}

#[cfg(feature = "postgres")]
mod live {
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array};
    use mizuchi::{
        BinaryOp, Expr, PostgresReadOptions, PostgresSource, Session, SessionConfig,
        postgres_scan_sql, read_postgres,
    };
    use postgres::{Client, NoTls};

    fn source_from_url(url: &str) -> PostgresSource {
        let mut host = String::new();
        let mut port = 5432u16;
        let mut database = String::new();
        let mut user = String::new();
        let mut password = None;
        for part in url.split_whitespace() {
            let Some((k, v)) = part.split_once('=') else {
                continue;
            };
            match k {
                "host" => host = v.to_string(),
                "port" => port = v.parse().unwrap_or(5432),
                "dbname" | "database" => database = v.to_string(),
                "user" => user = v.to_string(),
                "password" => password = Some(v.to_string()),
                _ => {}
            }
        }
        PostgresSource {
            host,
            port,
            database,
            user,
            password,
            ssl: false,
        }
    }

    #[test]
    #[ignore = "needs a PostgreSQL server; not default cargo test"]
    fn postgres_pushes_filter_and_projection() {
        let url = std::env::var("MIZUCHI_POSTGRES_URL").expect("MIZUCHI_POSTGRES_URL");
        let mut client = Client::connect(&url, NoTls).expect("connect");
        client
            .batch_execute(
                "DROP TABLE IF EXISTS mizuchi_src;
             CREATE TABLE mizuchi_src (id BIGINT, name TEXT);
             INSERT INTO mizuchi_src VALUES (1, 'a'), (8, 'b'), (12, 'c');",
            )
            .expect("seed");

        let source = source_from_url(&url);
        let projection = ["id".to_string()];
        let pred = Expr::Binary(
            BinaryOp::Gt,
            Box::new(Expr::Column("id".into())),
            Box::new(Expr::Literal(Arc::new(Int64Array::from(vec![5i64])))),
        );
        let filters = [pred];
        let opts = PostgresReadOptions {
            table: Some("mizuchi_src"),
            projection: Some(&projection),
            filters: Some(&filters),
            ..PostgresReadOptions::default()
        };
        let sqls = postgres_scan_sql(&opts).unwrap();
        assert_eq!(sqls.len(), 1);
        assert!(sqls[0].contains("WHERE (\"id\" > 5)"));
        assert!(sqls[0].starts_with("SELECT \"id\" FROM"));

        let mut session = Session::from_config(SessionConfig {
            allow_network: true,
            ..SessionConfig::default()
        })
        .unwrap();
        session
            .register_postgres("t", &source, opts)
            .expect("register");
        let batches = session.execute_sql("SELECT id FROM t").unwrap();
        let mut ids = Vec::new();
        for batch in &batches {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..col.len() {
                if col.is_valid(i) {
                    ids.push(col.value(i));
                }
            }
        }
        ids.sort();
        assert_eq!(ids, vec![8, 12]);

        let part_opts = PostgresReadOptions {
            table: Some("mizuchi_src"),
            partition_column: Some("id"),
            lower_bound: Some(1),
            upper_bound: Some(12),
            num_partitions: 2,
            ..PostgresReadOptions::default()
        };
        let part_sqls = postgres_scan_sql(&part_opts).unwrap();
        assert_eq!(part_sqls.len(), 2);
        let parts = read_postgres(&source, part_opts).unwrap();
        let mut n = 0u64;
        for batch in &parts {
            n += batch.num_rows() as u64;
        }
        assert_eq!(n, 3);
    }
}
