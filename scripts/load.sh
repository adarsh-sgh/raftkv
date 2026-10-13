#!/usr/bin/env bash
# Load matrix against an in-process 3-node cluster (fresh temp dir per row).
# Usage: scripts/load.sh [secs]   (default 10s per row after 2s warmup)
set -euo pipefail
cd "$(dirname "$0")/.."
secs=${1:-10}
cargo build -q --release -p raftkv --bin raftkv-load
bin=target/release/raftkv-load
echo "| workload | clients | value B | fsync | ops/s | p50 ms | p99 ms | p99.9 ms | errors |"
echo "|---|---:|---:|---|---:|---:|---:|---:|---:|"
run() { "$bin" --spawn 3 --secs "$secs" "$@"; }
run --workload put --clients 1
run --workload put --clients 32
run --workload put --clients 256
run --workload put --clients 32 --no-fsync
run --workload put --clients 256 --no-fsync
run --workload get --clients 32
run --workload get --clients 256
run --workload mixed --clients 64
