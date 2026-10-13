//! Replays one simulation: `cargo run --release -p sim --example replay -- <seed> [nofaults]`.
//! Runs are deterministic, so a failing seed from CI reproduces exactly.

fn main() {
    let seed: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let faults = std::env::args().nth(2).is_none();
    let r = sim::run(sim::SimConfig {
        faults,
        ..sim::SimConfig::new(seed)
    });
    println!("{:?}\n{:?}", r.outcome, r.stats);
}
