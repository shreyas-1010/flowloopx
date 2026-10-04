#!/usr/bin/env bash
# Benchmark matrix: {edge, level} x {backpressure on, off} x {steady, overload}.
#
#   scripts/run_bench.sh [build_dir] [seconds_per_run]
#
# Writes results/bench_<timestamp>.csv and prints a summary table.
# For trustworthy numbers: run on a machine with several cores, close other workloads, and give the
# load generator its own cores (e.g. `taskset -c 0-3 server`, `taskset -c 4-7 bench`).
set -euo pipefail

BUILD_DIR="${1:-build}"
SECS="${2:-10}"
WARMUP=2
PORT="${PORT:-9300}"
SERVER_WORKERS="${SERVER_WORKERS:-4}"
BENCH_THREADS="${BENCH_THREADS:-2}"
SERVER_CPUS="${SERVER_CPUS:-}"     # e.g. "0-3"
BENCH_CPUS="${BENCH_CPUS:-}"       # e.g. "4-7"

SERVER="$BUILD_DIR/flowloopx_server"
BENCH="$BUILD_DIR/flowloopx_bench"
[[ -x "$SERVER" && -x "$BENCH" ]] || { echo "build first: cmake -B build && cmake --build build -j"; exit 1; }

ulimit -n "$(ulimit -Hn)" 2>/dev/null || true
mkdir -p results
OUT="results/bench_$(date +%Y%m%d_%H%M%S).csv"
echo "mode,backpressure,scenario,rss_peak_kb,$("$BENCH" --csv-header --duration 0 --warmup 0 --conns 0 2>/dev/null | head -1)" > "$OUT"

# Optional CPU pinning. Returns the command prefix; taskset exec()s, so $! stays the real PID.
prefix() { if [[ -n "$1" ]] && command -v taskset >/dev/null; then echo "taskset -c $1"; fi; }

wait_port() {
  for _ in $(seq 1 100); do
    (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && return 0
    sleep 0.1
  done
  echo "server did not start" >&2; return 1
}

run_one() {  # mode bp scenario queue_capacity bench-args...
  local mode="$1" bp="$2" scenario="$3" queue="$4"; shift 4
  $(prefix "$SERVER_CPUS") "$SERVER" --port "$PORT" --mode "$mode" --backpressure "$bp" \
      --workers "$SERVER_WORKERS" --queue "$queue" >/dev/null 2>&1 &
  local spid=$!
  wait_port
  local row
  row=$($(prefix "$BENCH_CPUS") "$BENCH" --port "$PORT" --threads "$BENCH_THREADS" --duration "$SECS" \
        --warmup "$WARMUP" --csv --label "${mode}_bp${bp}_${scenario}" "$@")
  local rss
  rss=$(awk '/VmHWM/ {print $2}' "/proc/$spid/status" 2>/dev/null || echo 0)
  kill -INT "$spid"; wait "$spid" 2>/dev/null || true
  echo "$mode,$bp,$scenario,$rss,$row" >> "$OUT"
  echo "  done: $mode bp=$bp $scenario"
}

echo "Writing $OUT"
for mode in et lt; do
  for bp in on off; do
    # steady: many connections, light pipelining, cheap handler -> I/O-bound; queue sized so nothing is shed
    run_one "$mode" "$bp" steady   4096 --conns 512 --pipeline 8 --payload 64
    # overload: CPU-bound handler, deep pipelines, queue (128) far smaller than demand,
    # plus clients that flood and never read (slow consumers)
    run_one "$mode" "$bp" overload 128  --conns 512 --pipeline 32 --work 20000 --stalled 32
  done
done

echo
awk -F, 'NR==1 {printf "%-5s %-4s %-9s %10s %10s %10s %9s %9s %9s %8s\n","mode","bp","scenario","rss_kb","ok_rps","busy_rps","p50_us","p99_us","p999_us","stalled_closed"; next}
         {printf "%-5s %-4s %-9s %10s %10s %10s %9s %9s %9s %8s\n",$1,$2,$3,$4,$12,$13,$14,$15,$16,$21}' "$OUT"
echo
echo "columns: ok_rps = successful responses/s, busy_rps = shed responses/s (backpressure off only),"
echo "         closed_stalled = flooding slow consumers disconnected by the server"
