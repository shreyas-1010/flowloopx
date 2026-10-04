# FlowLoopX

A small, readable **epoll-driven TCP server in C++17/20** built around three ideas: an event loop
that never blocks, memory that stays bounded per connection, and a measurement harness that lets you
check those claims instead of trusting them.

```
 clients ──► epoll loop (1 thread) ──► bounded task queue ──► worker pool (N threads)
              accept / readv / writev        try_push, never blocks        run request handlers
                      ▲                                                          │
                      └───────────── completion queue + eventfd ◄────────────────┘
```

* **Non-blocking sockets, edge- or level-triggered epoll** (`--mode et|lt`).
* **RAII `Connection`** owning the fd and its pooled buffers; **`readv`** scatter-reads and **`writev`**
  gather-writes with no intermediate staging buffer.
* **Bounded memory:** hard per-connection input/output limits, plus **backpressure** (stop reading from
  a socket when its output backs up or no worker capacity is free).
* **No hot-path allocations:** buffers, output chunks and job objects are pooled; a test counts
  `operator new` and asserts **0 allocations across 80,000 steady-state requests**.
* **I/O is separated from work:** fixed thread pool + bounded queue; the loop only ever calls
  `try_push`. Slow handlers cannot stall other sockets (there is a test for it).
* **Graceful shutdown:** stop accepting, finish in-flight work, flush output, then exit
  (`SIGINT`/`SIGTERM`).
* **Benchmark harness** (`flowloopx_bench`, `scripts/run_bench.sh`) comparing edge vs level trigger and
  backpressure on vs off, reporting throughput, p50/p99/p99.9 latency, peak RSS and shed/disconnect counts.

Linux only (epoll, `accept4`, `eventfd`). IPv4. No third-party dependencies.

## Build

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release        # add -DFLX_CXX_STANDARD=17 for C++17 (default 20)
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Sanitizer builds: `-DFLX_SANITIZE=address,undefined` or `-DFLX_SANITIZE=thread`.

## Run

```bash
./build/flowloopx_server --port 9000 --mode et --backpressure on --workers 4
printf 'PING\nECHO hello\nWORK 100000\nSTATS\nQUIT\n' | nc localhost 9000
```

Built-in line protocol (swap it by passing your own `LineHandler` to `flx::Server`):

| Request        | Response                                                      |
|----------------|---------------------------------------------------------------|
| `PING`         | `PONG`                                                        |
| `ECHO <text>`  | `<text>`                                                      |
| `WORK <iters>` | hex digest after `<iters>` rounds of CPU work (compute-bound) |
| `SLEEP <ms>`   | `OK` after sleeping (simulates blocking I/O in a worker)      |
| `STATS`        | one line of server counters                                   |
| `QUIT`         | `BYE`, then the connection closes                             |

Responses on a connection are always in request order (one batch in flight per connection), even
when many requests are pipelined across several workers.

Server options: `--port --bind --mode --backpressure --workers --queue --max-conns --max-inbuf
--out-high --max-outbuf --drain-ms --stats-every`.

## Benchmark

```bash
ulimit -n 65536                                   # one fd per socket, on both sides
scripts/run_bench.sh build 10                     # full matrix, 10 s per run -> results/bench_*.csv

# Individual runs:
./build/flowloopx_bench --port 9000 --conns 512 --pipeline 8 --duration 10
./build/flowloopx_bench --port 9000 --conns 2000 --idle 20000 --duration 10   # concurrency: 22k sockets
./build/flowloopx_bench --port 9000 --conns 512 --pipeline 32 --work 20000 --stalled 32   # overload
```

The matrix runs `{edge, level} x {backpressure on, off} x {steady, overload}`:

* **steady**: many connections, light pipelining, cheap handler, queue sized so nothing is shed. Measures
  the cost of the epoll mode and of the backpressure bookkeeping.
* **overload**: CPU-bound handler, deep pipelines, a 128-slot queue far smaller than demand, plus
  "stalled" clients that flood requests and never read. Shows what each policy does when demand
  exceeds capacity: backpressure on = nothing lost, latency absorbed in socket buffers, flooding clients
  parked at bounded memory; backpressure off = excess requests answered `-BUSY` and flooding clients
  disconnected.

For trustworthy numbers use a multi-core machine and keep the load generator off the server's cores:

```bash
SERVER_CPUS=0-3 BENCH_CPUS=4-7 BENCH_THREADS=4 scripts/run_bench.sh build 10
```

### Results

Hardware: Intel Core i5-8265U (4 cores / 8 threads), Linux, client and server on the same machine,
pinned to separate physical cores (`SERVER_CPUS=0,1,4,5 BENCH_CPUS=2,3,6,7`). 4 workers, 10 s per
run, one run per cell. Differences under ~10% are within run-to-run noise. Raw data:
[docs/benchmarks/matrix_i5-8265U.csv](docs/benchmarks/matrix_i5-8265U.csv).

**Concurrency:** 22,000 simultaneous TCP connections (2,000 active + 20,000 idle) held for 30 s on the
same machine, with 0 connection failures and 0 server-side disconnects. Server process peak RSS was
7.7 MB (user-space only; kernel socket buffers are not included). In a snapshot taken during the run
the buffer pool had created 1 input buffer and 1 output chunk and reused them ~290,000 times.

| Mode | Backpressure | Scenario | ok req/s | shed req/s | p50 (us) | p99 (us) | Peak RSS (MB) | Stalled clients closed |
|------|--------------|----------|---------:|-----------:|---------:|---------:|--------------:|-----------------------:|
| ET | on  | steady   | 1,303,791 | 0 | 3,072 | 3,712 | 5.2 | 0 |
| ET | off | steady   | 1,173,913 | 0 | 3,200 | 7,936 | 5.2 | 0 |
| LT | on  | steady   | 1,315,740 | 0 | 3,072 | 4,864 | 5.2 | 0 |
| LT | off | steady   | 1,291,083 | 0 | 3,072 | 3,328 | 5.2 | 0 |
| ET | on  | overload | 131,621 | 0 | 118,784 | 147,456 | 18.4 | 0 |
| ET | off | overload | 111,596 | 4,402,738 | 40,960 | 45,056 | 4.8 | 32 |
| LT | on  | overload | 131,890 | 0 | 118,784 | 131,072 | 18.5 | 0 |
| LT | off | overload | 114,469 | 3,866,778 | 38,912 | 47,104 | 4.6 | 32 |

Steady: 512 connections, 8 requests in flight each, 64-byte ECHO. Overload: 512 connections x 32
in flight, CPU-bound handler, 128-slot task queue, plus 32 clients that flood and never read.

**Findings**

- Edge vs level triggering made no measurable difference at this load (steady-state throughput within 1%
  in the cleanest comparison; the one larger gap was not reproduced in the other mode).
- Under overload, backpressure completed about 15-18% more successful requests and dropped none; with it
  off the server answered millions of `-BUSY` per second and disconnected every flooding client.
- Backpressure trades latency for completeness: p50 was about 3x higher because requests queue instead of
  being rejected. Latency for the no-backpressure case only covers admitted requests.
- Memory stayed bounded in every configuration. Parking the 32 stalled clients cost ~14 MB with
  backpressure on; there is no idle timeout, so they stay parked.

**Profiling (perf + per-thread CPU):** the event-loop thread sat at ~74% CPU at 512 and at 1024
connections, and throughput did not rise when client load doubled, so a single loop was not the
bottleneck on this hardware. The user-space profile was flat (largest single item ~11%: request handling);
most time was in kernel TCP. Multi-loop `SO_REUSEPORT` was deliberately not added.

## How it works

See [docs/DESIGN.md](docs/DESIGN.md) for the connection state machine, the backpressure rules and the
edge-triggered pitfalls. Short version:

1. The loop reads with `readv` into a pooled buffer (plus a 64 KiB stack spill iovec) up to the
   per-connection limit.
2. Complete lines are copied (one `memcpy`) into a recycled `Job` and pushed to the bounded queue.
3. A worker runs the handler and posts the job back through a completion list + `eventfd`.
4. The loop appends the response to the connection's chunked output queue and drains it with `writev`.

| Limit / rule                     | Default  | Effect                                                             |
|----------------------------------|----------|--------------------------------------------------------------------|
| `max_inbuf_bytes`                | 64 KiB   | Hard input cap. Backpressure on: pause reads. Off: close connection. |
| `out_high_watermark`             | 256 KiB  | Backpressure on: stop dispatching/reading until output drains below half. |
| `max_outbuf_bytes`               | 1 MiB    | Hard output cap, always enforced (safety net).                     |
| `max_batch_bytes`                | 16 KiB   | Longest request line; longer gets `-ERR line too long` and a close. |
| `queue_capacity`                 | 1024     | Bounded task queue. Full: on = wait and retry; off = `-BUSY`.      |

Worst-case memory is roughly `connections x (max_inbuf + out_high + one batch of response)`, independent
of how fast clients send or how slowly they read.

## Tests

`ctest` runs two executables:

* `flowloopx_tests`: buffer/`readv`, `writev` queue, pools, bounded queue + thread pool, and end-to-end
  server tests in **both ET and LT** with backpressure **on and off**: command protocol, pipelined
  ordering across workers, concurrent clients, a slow handler not blocking the loop, oversize lines,
  lossless backpressure under a flooding client, hard-limit disconnect without backpressure, overload
  behaviour (`-BUSY` vs queueing), graceful shutdown, and 500 idle sockets.
* `flowloopx_alloc_test`: the zero-allocation steady-state check described above.

The suite is clean under AddressSanitizer + UBSan and ThreadSanitizer.

## Honest limits

* "Zero-copy" here means scatter/gather I/O without a staging buffer. Bytes are still copied once
  into the job payload, and once from the response into the output chunks.
* A connection has at most one batch in flight. That guarantees ordering but caps per-connection
  parallelism.
* No TLS, no IPv6, no idle/slow-client timeout (a client that stops reading is parked at bounded
  memory indefinitely).
* A single event-loop thread. Scaling past one core of I/O means one loop per core with `SO_REUSEPORT`.

## Layout

```
include/flowloopx/   public headers (server, connection, buffer, pool, thread_pool, handler)
src/                 implementation (server.cpp holds the event loop)
apps/                flowloopx_server (binary) and flowloopx_bench (load generator)
tests/               unit/integration tests and the allocation test
scripts/run_bench.sh benchmark matrix
docs/DESIGN.md       design notes
```

