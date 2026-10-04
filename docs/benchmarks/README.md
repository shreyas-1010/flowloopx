# Benchmark data

Raw output of `scripts/run_bench.sh build 10` behind the README results table.

- Hardware: Intel Core i5-8265U (4 cores / 8 threads), client and server on the same machine
- Pinning: `SERVER_CPUS=0,1,4,5 BENCH_CPUS=2,3,6,7 BENCH_THREADS=2`
- 4 workers, 10 s per run, one run per cell
- Columns: see the header row; `rss_peak_kb` is the server process peak RSS
