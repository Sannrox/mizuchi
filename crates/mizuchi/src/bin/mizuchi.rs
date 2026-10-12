//! Process entry for Flight workers and the coordinator.
//!
//! Binds TCP only when `MIZUCHI_ALLOW_NETWORK` is set.

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use mizuchi::{Batch, RecordBatch, RemoteCluster, serve_worker};

fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        return Err("usage: mizuchi worker|coordinator [flags]".into());
    }
    let cmd = args.remove(0);
    match cmd.as_str() {
        "worker" => worker(&args),
        "coordinator" => coordinator(&args),
        other => Err(format!("unknown command {other}")),
    }
}

fn allow_network() -> bool {
    matches!(
        env::var("MIZUCHI_ALLOW_NETWORK")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes"
    )
}

fn spill_dir() -> Option<PathBuf> {
    env::var_os("MIZUCHI_SPILL").map(PathBuf::from)
}

fn worker(args: &[String]) -> Result<(), String> {
    let bind = flag(args, "--bind")
        .or_else(|| env::var("MIZUCHI_BIND").ok())
        .unwrap_or_else(|| "0.0.0.0:8815".into());
    let addr: SocketAddr = bind.parse().map_err(|e| format!("bind: {e}"))?;
    serve_worker(addr, allow_network(), spill_dir()).map_err(|e| e.to_string())
}

fn coordinator(args: &[String]) -> Result<(), String> {
    let workers = flag(args, "--workers")
        .or_else(|| env::var("MIZUCHI_WORKERS").ok())
        .ok_or_else(|| "coordinator needs --workers or MIZUCHI_WORKERS".to_string())?;
    let addrs = workers
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    let cluster =
        RemoteCluster::connect(addrs, allow_network(), spill_dir()).map_err(|e| e.to_string())?;
    cluster.heartbeat();
    if cluster.live_workers() < 2 {
        return Err("need two live workers".into());
    }
    let schema = std::sync::Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![std::sync::Arc::new(Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
        ]))],
    )
    .map_err(|e| e.to_string())?;
    cluster
        .register(
            "t",
            schema,
            vec![Batch::from_arrow(batch).map_err(|e| e.to_string())?],
        )
        .map_err(|e| e.to_string())?;
    let sql = flag(args, "--sql").unwrap_or_else(|| "SELECT SUM(a) AS s FROM t".into());
    let out = cluster.execute_sql(&sql).map_err(|e| e.to_string())?;
    println!("rows={}", out.iter().map(|b| b.num_rows()).sum::<usize>());
    Ok(())
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.windows(2).find(|w| w[0] == name).map(|w| w[1].clone())
}
