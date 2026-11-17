//! Local S3-compatible emulator job. Live tests are not part of default
//! `cargo test`.
//!
//! Enable with `--features s3 -- --ignored` and point the store at an emulator
//! the runner started (`MIZUCHI_S3_ENDPOINT`, `MIZUCHI_S3_BUCKET`,
//! `MIZUCHI_S3_ACCESS_KEY`, `MIZUCHI_S3_SECRET_KEY`).

use std::fs;
use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    fs::read_to_string(root().join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))
}

#[test]
fn s3_ci_job_runs_emulator_and_fails_if_skipped() {
    let yml = read(".github/workflows/ci.yml");
    assert!(
        yml.contains("S3 emulator"),
        "CI must include an S3 emulator job"
    );
    assert!(
        yml.contains("--features s3"),
        "S3 job must enable the s3 feature"
    );
    assert!(yml.contains("--ignored"), "S3 job must run ignored tests");
    assert!(
        yml.contains("S3 emulator tests were skipped"),
        "S3 job must fail when ignored tests are absent"
    );
    assert!(
        yml.contains("scripts/start-s3-emulator.sh"),
        "S3 job must start the emulator via scripts/start-s3-emulator.sh"
    );
    let bootstrap = read("scripts/start-s3-emulator.sh");
    assert!(
        bootstrap.contains("alpine/minio"),
        "emulator bootstrap must pull a public MinIO image"
    );
    assert!(
        yml.contains("pull_request"),
        "S3 emulator job belongs on the default CI workflow"
    );
}

#[cfg(feature = "s3")]
mod emulator {
    use std::fmt;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use arrow::array::{Array, Int64Array, StringArray};
    use futures::executor::block_on;
    use futures::stream::BoxStream;
    use futures::{StreamExt, TryStreamExt};
    use mizuchi::{
        Batch, BinaryOp, DataType, Expr, Field, ObjectStore, ParquetReadOptions,
        ParquetWriteOptions, Schema, WriteMode, read_parquet_store, read_parquet_store_reported,
        write_parquet_store,
    };
    use object_store::aws::AmazonS3Builder;
    use object_store::client::SpawnedReqwestConnector;
    use object_store::path::Path as ObjectPath;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
    };

    struct LiveS3 {
        _rt: tokio::runtime::Runtime,
        store: Arc<dyn ObjectStore>,
    }

    fn connect() -> LiveS3 {
        let endpoint = std::env::var("MIZUCHI_S3_ENDPOINT").expect("MIZUCHI_S3_ENDPOINT");
        let bucket = std::env::var("MIZUCHI_S3_BUCKET").expect("MIZUCHI_S3_BUCKET");
        let key = std::env::var("MIZUCHI_S3_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into());
        let secret = std::env::var("MIZUCHI_S3_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_io()
            .enable_time()
            .build()
            .expect("tokio runtime");
        let handle = rt.handle().clone();
        let store = AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .with_access_key_id(key)
            .with_secret_access_key(secret)
            .with_region("us-east-1")
            .with_virtual_hosted_style_request(false)
            .with_allow_http(true)
            .with_http_connector(SpawnedReqwestConnector::new(handle))
            .build()
            .expect("s3 store");
        LiveS3 {
            _rt: rt,
            store: Arc::new(store),
        }
    }

    /// A name unique to this process and call, so concurrent runs against one
    /// bucket or catalog never share objects or tables.
    fn unique(base: &str) -> String {
        static SEQ: AtomicU64 = AtomicU64::new(1);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!(
            "{base}_{}_{nanos}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// A per-run object prefix. Drop deletes every object under it, and on a
    /// passing test asserts nothing is left.
    struct RunPrefix {
        store: Arc<dyn ObjectStore>,
        name: String,
    }

    impl RunPrefix {
        fn new(store: &Arc<dyn ObjectStore>, base: &str) -> Self {
            Self {
                store: Arc::clone(store),
                name: unique(base),
            }
        }

        fn as_str(&self) -> &str {
            &self.name
        }
    }

    impl Drop for RunPrefix {
        fn drop(&mut self) {
            let root = ObjectPath::from(self.name.as_str());
            let locations = self.store.list(Some(&root)).map_ok(|m| m.location).boxed();
            let deleted = block_on(self.store.delete_stream(locations).try_collect::<Vec<_>>());
            if !std::thread::panicking() {
                deleted.expect("delete run prefix");
                assert!(
                    list_keys(self.store.as_ref(), &self.name).is_empty(),
                    "{} not cleaned up",
                    self.name
                );
            }
        }
    }

    fn sample() -> Batch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("year", DataType::Int64, true),
            Field::new("id", DataType::Int64, true),
        ]));
        Batch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(2020), Some(2021)])),
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
            ],
        )
        .unwrap()
    }

    fn ids(batches: &[Batch]) -> Vec<i64> {
        let mut out = Vec::new();
        for b in batches {
            let col = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..col.len() {
                if !col.is_null(i) {
                    out.push(col.value(i));
                }
            }
        }
        out.sort();
        out
    }

    /// Sorted `(id, name)` rows.
    fn id_names(batches: &[Batch]) -> Vec<(i64, String)> {
        let mut out = Vec::new();
        for b in batches {
            let id = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let name = b
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            out.extend((0..b.num_rows()).map(|i| (id.value(i), name.value(i).to_string())));
        }
        out.sort();
        out
    }

    fn list_keys(store: &dyn ObjectStore, prefix: &str) -> Vec<String> {
        let root = ObjectPath::parse(prefix.trim_matches('/')).expect("prefix");
        let metas: Vec<ObjectMeta> = block_on(store.list(Some(&root)).try_collect()).expect("list");
        let mut out: Vec<String> = metas.into_iter().map(|m| m.location.to_string()).collect();
        out.sort();
        out
    }

    fn write_year(store: &dyn ObjectStore, prefix: &str, batches: &[Batch], mode: WriteMode) {
        let cols = [String::from("year")];
        write_parquet_store(
            store,
            prefix,
            batches,
            ParquetWriteOptions {
                partition_by: &cols,
                mode,
                encryption: None,
            },
        )
        .unwrap();
    }

    /// Fails copy/rename when the source path contains `needle` so staging
    /// objects exist and must be deleted. Other operations go to the inner store.
    #[derive(Debug)]
    struct FailPuts {
        inner: Arc<dyn ObjectStore>,
        needle: &'static str,
    }

    impl fmt::Display for FailPuts {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "FailPuts")
        }
    }

    impl ObjectStore for FailPuts {
        fn put_opts<'a, 'b, 'c>(
            &'a self,
            location: &'b ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Pin<Box<dyn Future<Output = StoreResult<PutResult>> + Send + 'c>>
        where
            'a: 'c,
            'b: 'c,
            Self: 'c,
        {
            let inner = Arc::clone(&self.inner);
            let loc = location.clone();
            Box::pin(async move { inner.put_opts(&loc, payload, opts).await })
        }

        fn put_multipart_opts<'a, 'b, 'c>(
            &'a self,
            location: &'b ObjectPath,
            opts: PutMultipartOptions,
        ) -> Pin<Box<dyn Future<Output = StoreResult<Box<dyn MultipartUpload>>> + Send + 'c>>
        where
            'a: 'c,
            'b: 'c,
            Self: 'c,
        {
            let inner = Arc::clone(&self.inner);
            let loc = location.clone();
            Box::pin(async move { inner.put_multipart_opts(&loc, opts).await })
        }

        fn get_opts<'a, 'b, 'c>(
            &'a self,
            location: &'b ObjectPath,
            options: GetOptions,
        ) -> Pin<Box<dyn Future<Output = StoreResult<GetResult>> + Send + 'c>>
        where
            'a: 'c,
            'b: 'c,
            Self: 'c,
        {
            let inner = Arc::clone(&self.inner);
            let loc = location.clone();
            Box::pin(async move { inner.get_opts(&loc, options).await })
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, StoreResult<ObjectPath>>,
        ) -> BoxStream<'static, StoreResult<ObjectPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_delimiter<'a, 'b, 'c>(
            &'a self,
            prefix: Option<&'b ObjectPath>,
        ) -> Pin<Box<dyn Future<Output = StoreResult<ListResult>> + Send + 'c>>
        where
            'a: 'c,
            'b: 'c,
            Self: 'c,
        {
            let inner = Arc::clone(&self.inner);
            let prefix = prefix.cloned();
            Box::pin(async move { inner.list_with_delimiter(prefix.as_ref()).await })
        }

        fn copy_opts<'a, 'b, 'c, 'd>(
            &'a self,
            from: &'b ObjectPath,
            to: &'c ObjectPath,
            options: CopyOptions,
        ) -> Pin<Box<dyn Future<Output = StoreResult<()>> + Send + 'd>>
        where
            'a: 'd,
            'b: 'd,
            'c: 'd,
            Self: 'd,
        {
            let inner = Arc::clone(&self.inner);
            let from = from.clone();
            let to = to.clone();
            let needle = self.needle;
            Box::pin(async move {
                if from.as_ref().contains(needle) {
                    return Err(object_store::Error::Generic {
                        store: "FailPuts",
                        source: Box::new(std::io::Error::other("injected staging failure")),
                    });
                }
                inner.copy_opts(&from, &to, options).await
            })
        }
    }

    #[test]
    #[ignore = "needs a local S3-compatible emulator; not default cargo test"]
    fn s3_emulator_round_trip_and_prune() {
        let live = connect();
        let store = live.store.as_ref();
        let prefix = RunPrefix::new(&live.store, "mizuchi-emu");
        let prefix = prefix.as_str();
        write_year(store, prefix, &[sample()], WriteMode::Overwrite);
        let out = read_parquet_store(store, prefix, ParquetReadOptions::default()).unwrap();
        assert_eq!(ids(&out), vec![1, 2]);

        let pred = Expr::Binary(
            BinaryOp::Eq,
            Box::new(Expr::column("year")),
            Box::new(Expr::lit_i64(2020)),
        );
        let opts = ParquetReadOptions {
            filters: Some(std::slice::from_ref(&pred)),
            ..ParquetReadOptions::default()
        };
        let (pruned, scan) = read_parquet_store_reported(store, prefix, opts).unwrap();
        assert_eq!(ids(&pruned), vec![1]);
        assert_eq!(scan.partitions, vec!["year=2020".to_string()]);
        assert!(
            list_keys(store, prefix)
                .iter()
                .all(|k| !k.contains(".mizuchi-staging-")),
            "successful write must delete staging objects"
        );
    }

    #[test]
    #[ignore = "needs a local S3-compatible emulator; not default cargo test"]
    fn s3_emulator_failed_commit_cleans_staging() {
        let live = connect();
        let store = live.store.as_ref();
        let prefix = RunPrefix::new(&live.store, "mizuchi-emu-fail");
        let prefix = prefix.as_str();
        write_year(store, prefix, &[sample()], WriteMode::Overwrite);
        let before =
            ids(&read_parquet_store(store, prefix, ParquetReadOptions::default()).unwrap());
        assert_eq!(before, vec![1, 2]);

        let failing = FailPuts {
            inner: Arc::clone(&live.store),
            needle: ".mizuchi-staging-",
        };
        let cols = [String::from("year")];
        let err = write_parquet_store(
            &failing,
            prefix,
            &[sample()],
            ParquetWriteOptions {
                partition_by: &cols,
                mode: WriteMode::Append,
                encryption: None,
            },
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("injected staging failure"),
            "{err}"
        );

        let after = ids(&read_parquet_store(store, prefix, ParquetReadOptions::default()).unwrap());
        assert_eq!(after, before);
        assert!(
            list_keys(store, prefix)
                .iter()
                .all(|k| !k.contains(".mizuchi-staging-")),
            "failed write must delete staging objects"
        );
    }

    #[test]
    #[ignore = "needs a local S3-compatible emulator; not default cargo test"]
    fn s3_emulator_delta_iceberg_and_rest() {
        use mizuchi::{
            CatalogProvider, IcebergRestCatalog, TableIdent, TableMeta, TimeTravel,
            read_delta_store, read_iceberg_store, write_delta_store, write_iceberg_store,
        };

        let live = connect();
        let store = live.store.as_ref();
        let delta = RunPrefix::new(&live.store, "emu-delta");
        let ice = RunPrefix::new(&live.store, "emu-ice");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = Batch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![Some(1), Some(2)])),
                Arc::new(arrow::array::StringArray::from(vec![Some("a"), Some("b")])),
            ],
        )
        .unwrap();
        let expected = vec![(1, "a".to_string()), (2, "b".to_string())];
        write_delta_store(
            store,
            delta.as_str(),
            std::slice::from_ref(&batch),
            WriteMode::Overwrite,
        )
        .unwrap();
        let got = read_delta_store(store, delta.as_str(), TimeTravel::Latest).unwrap();
        assert_eq!(id_names(&got), expected);
        write_iceberg_store(store, ice.as_str(), &[batch], WriteMode::Overwrite).unwrap();
        let got = read_iceberg_store(store, ice.as_str(), TimeTravel::Latest).unwrap();
        assert_eq!(id_names(&got), expected);

        // An external catalog outlives the run, so the table name is unique
        // and dropped at the end; otherwise use an in-process memory server.
        let (base, _server) = match std::env::var("MIZUCHI_ICEBERG_REST") {
            Ok(base) => (base, None),
            Err(_) => {
                let (port, h) = mizuchi::plan::spawn_memory_server().expect("rest listener");
                std::thread::sleep(std::time::Duration::from_millis(30));
                (format!("http://127.0.0.1:{port}"), Some(h))
            }
        };
        let mut cat = IcebergRestCatalog::connect(&base, true).expect("rest catalog");
        let ident = TableIdent::parse(&unique("emu")).unwrap();
        cat.create_table(TableMeta {
            ident: ident.clone(),
            schema,
            stats: mizuchi::TableStats::default(),
        })
        .unwrap();
        assert!(CatalogProvider::get_table(&cat, &ident).is_ok());
        cat.drop_table(&ident, false).unwrap();
        assert!(CatalogProvider::get_table(&cat, &ident).is_err());
    }
}
