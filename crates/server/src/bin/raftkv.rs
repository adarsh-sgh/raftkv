//! CLI: raftkv --addrs 127.0.0.1:7001,127.0.0.1:7002 put k v | get k | del k |
//!      cas k <expected|-> <new|-> | status | add <id> <addr> | remove <id>

use clap::{Parser, Subcommand};
use raftkv::Client;
use raftkv::pb;
use raftkv::pb::admin_client::AdminClient;

#[derive(Parser)]
struct Args {
    #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:7001")]
    addrs: Vec<String>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Get {
        key: String,
    },
    Put {
        key: String,
        value: String,
    },
    Del {
        key: String,
    },
    /// `-` means absent.
    Cas {
        key: String,
        expected: String,
        new: String,
    },
    /// Status of every node in --addrs.
    Status,
    Add {
        id: u64,
        addr: String,
    },
    Remove {
        id: u64,
    },
}

fn opt(s: &str) -> Option<&[u8]> {
    (s != "-").then_some(s.as_bytes())
}

async fn admin(
    addrs: &[String],
) -> Result<AdminClient<tonic::transport::Channel>, Box<dyn std::error::Error>> {
    // Membership changes must go to the leader; find it via Status.
    for a in addrs {
        if let Ok(mut c) = AdminClient::connect(format!("http://{a}")).await
            && let Ok(s) = c.status(pb::StatusRequest {}).await
            && s.get_ref().role == "leader"
        {
            return Ok(c);
        }
    }
    Err("no leader reachable".into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a = Args::parse();
    let mut c = Client::new(a.addrs.clone());
    match a.cmd {
        Cmd::Get { key } => match c.get(key.as_bytes()).await? {
            Some(v) => println!("{}", String::from_utf8_lossy(&v)),
            None => println!("(nil)"),
        },
        Cmd::Put { key, value } => c.put(key.as_bytes(), value.as_bytes()).await?,
        Cmd::Del { key } => c.delete(key.as_bytes()).await?,
        Cmd::Cas { key, expected, new } => {
            let (ok, prev) = c.cas(key.as_bytes(), opt(&expected), opt(&new)).await?;
            let prev = prev.map_or("(nil)".into(), |p| String::from_utf8_lossy(&p).into_owned());
            println!("swapped={ok} prev={prev}");
        }
        Cmd::Status => {
            for addr in &a.addrs {
                match AdminClient::connect(format!("http://{addr}")).await {
                    Ok(mut ac) => match ac.status(pb::StatusRequest {}).await {
                        Ok(s) => {
                            let s = s.into_inner();
                            let members: Vec<u64> = s.members.iter().map(|m| m.id).collect();
                            println!(
                                "{addr} id={} {} term={} leader={} commit={} applied={} snapshot={} members={members:?} keys={}",
                                s.id,
                                s.role,
                                s.term,
                                s.leader,
                                s.commit,
                                s.applied,
                                s.snapshot_index,
                                s.keys
                            );
                        }
                        Err(e) => println!("{addr} error: {}", e.message()),
                    },
                    Err(_) => println!("{addr} unreachable"),
                }
            }
        }
        Cmd::Add { id, addr } => {
            admin(&a.addrs)
                .await?
                .add_member(pb::Member { id, addr })
                .await?;
            println!("added {id}");
        }
        Cmd::Remove { id } => {
            admin(&a.addrs)
                .await?
                .remove_member(pb::RemoveMemberRequest { id })
                .await?;
            println!("removed {id}");
        }
    }
    Ok(())
}
