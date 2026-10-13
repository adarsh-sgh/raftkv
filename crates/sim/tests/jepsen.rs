//! Jepsen-style runs: many seeds, each a different schedule of partitions, crashes, message
//! loss/duplication/reordering (and membership changes). Every history must be linearizable.
//! `SIM_SEEDS=n` sets the number of seeds (CI runs more than the default).

use lincheck::Outcome;
use sim::{Report, SimConfig, Stats, run};

fn seeds() -> u64 {
    std::env::var("SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(24)
}

/// Runs seeds in parallel and returns (seed, report) sorted by seed.
fn run_all(cfg: impl Fn(u64) -> SimConfig + Sync) -> Vec<(u64, Report)> {
    let n = seeds();
    let threads = std::thread::available_parallelism().map_or(4, |p| p.get()) as u64;
    let mut out: Vec<(u64, Report)> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let cfg = &cfg;
                s.spawn(move || {
                    (t..n)
                        .step_by(threads as usize)
                        .map(|seed| (seed, run(cfg(seed))))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().expect("sim thread"))
            .collect()
    });
    out.sort_by_key(|(s, _)| *s);
    out
}

fn assert_all_linearizable(reports: &[(u64, Report)]) -> Stats {
    let mut total = Stats::default();
    for (seed, r) in reports {
        assert_eq!(
            r.outcome,
            Outcome::Linearizable,
            "seed {seed}: {:?}",
            r.stats
        );
        assert!(
            r.stats.ops_ok > 100,
            "seed {seed}: too little progress {:?}",
            r.stats
        );
        assert!(
            r.stats.heal_ticks < 1000,
            "seed {seed}: no recovery after heal {:?}",
            r.stats
        );
        total.add(&r.stats);
    }
    eprintln!("{} seeds: {total:?}", reports.len());
    total
}

#[test]
fn linearizable_under_partitions_crashes_and_message_loss() {
    let total = assert_all_linearizable(&run_all(SimConfig::new));
    assert!(total.crashes > 0 && total.partitions > 0 && total.dropped > 0);
    assert!(total.snapshots_sent > 0, "InstallSnapshot never exercised");
}

#[test]
fn linearizable_on_five_nodes() {
    let total = assert_all_linearizable(&run_all(|seed| SimConfig {
        nodes: 5,
        ..SimConfig::new(seed)
    }));
    assert!(total.crashes > 0);
}

#[test]
fn linearizable_through_membership_changes() {
    let total = assert_all_linearizable(&run_all(|seed| SimConfig {
        membership: true,
        ..SimConfig::new(seed)
    }));
    assert!(total.conf_changes > 0 && total.snapshots_sent > 0);
}

/// The harness must be able to fail: leaders that answer reads locally (no ReadIndex, no
/// lease) serve stale data once partitioned away, and the checker has to catch it.
#[test]
fn detects_stale_reads_from_unsafe_local_reads() {
    let reports = run_all(|seed| SimConfig {
        unsafe_local_reads: true,
        check_quorum: false,
        ..SimConfig::new(seed)
    });
    let caught = reports
        .iter()
        .filter(|(_, r)| matches!(r.outcome, Outcome::Illegal(_)))
        .count();
    eprintln!("stale reads caught in {caught}/{} seeds", reports.len());
    assert!(caught > 0, "checker missed injected stale reads");
}
