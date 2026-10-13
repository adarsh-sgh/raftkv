//! Closed-loop load generator. `--spawn 3` starts an in-process 3-node cluster (separate
//! runtimes, file storage in a temp dir) so results are reproducible with one command.
//! Prints throughput and latency percentiles as a Markdown table row.

use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;
use raftkv::{Client, ClusterOpts, LocalCluster};

#[derive(Clone, Copy, ValueEnum, Debug)]
enum Workload {
    Put,
    Get,
    /// 50% get, 50% put.
    Mixed,
}

#[derive(Parser)]
struct Args {
    #[arg(long, value_delimiter = ',')]
    addrs: Vec<String>,
    /// Start an in-process cluster of this many nodes instead of using --addrs.
    #[arg(long)]
    spawn: Option<u64>,
    #[arg(long, default_value_t = false)]
    no_fsync: bool,
    #[arg(long, value_enum, default_value = "put")]
    workload: Workload,
    #[arg(long, default_value_t = 32)]
    clients: usize,
    #[arg(long, default_value_t = 10)]
    secs: u64,
    #[arg(long, default_value_t = 2)]
    warmup_secs: u64,
    #[arg(long, default_value_t = 128)]
    value_size: usize,
    #[arg(long, default_value_t = 1000)]
    keys: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a = Args::parse();
    let dir = tempfile::tempdir()?;
    let (addrs, _cluster) = match a.spawn {
        Some(n) => {
            let opts = ClusterOpts {
                fsync: !a.no_fsync,
                ..ClusterOpts::default()
            };
            let c = LocalCluster::start(n, dir.path(), opts).await?;
            c.leader(Duration::from_secs(10)).await.ok_or("no leader")?;
            (c.addrs(), Some(c))
        }
        None => (a.addrs.clone(), None),
    };

    // Preload keys so gets hit real values.
    let mut c = Client::new(addrs.clone());
    let value = vec![b'x'; a.value_size];
    if !matches!(a.workload, Workload::Put) {
        for k in 0..a.keys {
            c.put(format!("key-{k}").as_bytes(), &value).await?;
        }
    }

    let start = Instant::now();
    let warm_end = start + Duration::from_secs(a.warmup_secs);
    let end = warm_end + Duration::from_secs(a.secs);
    let tasks: Vec<_> = (0..a.clients)
        .map(|i| {
            let (addrs, value, workload, keys) = (addrs.clone(), value.clone(), a.workload, a.keys);
            tokio::spawn(async move {
                let mut c = Client::new(addrs);
                let mut h = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("hist");
                let mut rng = raft::Rng::new(i as u64 + 1);
                let mut errors = 0u64;
                while Instant::now() < end {
                    let key = format!("key-{}", rng.range(0, keys));
                    let get = match workload {
                        Workload::Get => true,
                        Workload::Put => false,
                        Workload::Mixed => rng.chance(0.5),
                    };
                    let t = Instant::now();
                    let ok = if get {
                        c.get(key.as_bytes()).await.is_ok()
                    } else {
                        c.put(key.as_bytes(), &value).await.is_ok()
                    };
                    if t >= warm_end {
                        if ok {
                            h.record(t.elapsed().as_micros() as u64).expect("record");
                        } else {
                            errors += 1;
                        }
                    }
                }
                (h, errors)
            })
        })
        .collect();
    let mut all = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;
    let mut errors = 0;
    for t in tasks {
        let (h, e) = t.await?;
        all.add(h)?;
        errors += e;
    }
    let ms = |q: f64| all.value_at_quantile(q) as f64 / 1000.0;
    println!(
        "| {:?} | {} | {} | {} | {:.0} | {:.2} | {:.2} | {:.2} | {} |",
        a.workload,
        a.clients,
        a.value_size,
        if a.no_fsync { "off" } else { "on" },
        all.len() as f64 / a.secs as f64,
        ms(0.5),
        ms(0.99),
        ms(0.999),
        errors
    );
    Ok(())
}
