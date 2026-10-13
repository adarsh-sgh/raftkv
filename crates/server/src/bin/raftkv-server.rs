//! raftkv-server --id 1 --listen 127.0.0.1:7001 --data-dir data/1 \
//!     --bootstrap 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
//! A node started without --bootstrap and empty data waits to be added via AddMember.

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use raft::Member;
use raftkv::NodeConfig;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    id: u64,
    #[arg(long)]
    listen: String,
    /// Address advertised to peers and clients (defaults to --listen).
    #[arg(long)]
    advertise: Option<String>,
    #[arg(long)]
    data_dir: PathBuf,
    /// Initial members of a new cluster: id=addr,id=addr,...
    #[arg(long)]
    bootstrap: Option<String>,
    #[arg(long, default_value_t = 20)]
    tick_ms: u64,
    #[arg(long, default_value_t = 10_000)]
    snapshot_every: u64,
    /// Skip fsync (unsafe on power loss; faster).
    #[arg(long)]
    no_fsync: bool,
}

fn parse_members(s: &str) -> Vec<Member> {
    s.split(',')
        .map(|kv| {
            let (id, addr) = kv.split_once('=').expect("id=addr");
            Member {
                id: id.parse().expect("numeric id"),
                addr: addr.to_string(),
            }
        })
        .collect()
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let a = Args::parse();
    let addr = a.advertise.clone().unwrap_or_else(|| a.listen.clone());
    let mut cfg = NodeConfig::new(a.id, addr, a.data_dir);
    cfg.bootstrap = a.bootstrap.as_deref().map(parse_members);
    cfg.tick = Duration::from_millis(a.tick_ms);
    cfg.snapshot_every = a.snapshot_every;
    cfg.fsync = !a.no_fsync;
    let listener = tokio::net::TcpListener::bind(&a.listen).await?;
    eprintln!("raftkv node {} listening on {}", a.id, a.listen);
    tokio::select! {
        r = raftkv::serve(cfg, listener) => r,
        _ = tokio::signal::ctrl_c() => Ok(()),
    }
}
