#pragma once
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "flowloopx/handler.hpp"

namespace flx {

struct Config {
  std::string bind_addr = "0.0.0.0";
  uint16_t port = 9000;              // 0 = pick an ephemeral port (see Server::port())
  int backlog = 4096;

  bool edge_triggered = true;        // EPOLLET vs level-triggered
  bool backpressure = true;          // pause reads instead of shedding/disconnecting
  bool tcp_nodelay = true;

  size_t worker_threads = 0;         // 0 = hardware_concurrency (min 2)
  size_t queue_capacity = 1024;      // bounded task queue (jobs waiting for a worker)

  size_t max_connections = 100000;
  size_t max_inbuf_bytes = 64 * 1024;       // hard per-connection input limit
  size_t max_batch_bytes = 16 * 1024;       // max bytes handed to a worker at once (= max line length)
  size_t out_high_watermark = 256 * 1024;   // backpressure: stop reading/dispatching above this
  size_t max_outbuf_bytes = 1024 * 1024;    // hard per-connection output limit (safety net)

  int drain_timeout_ms = 5000;       // graceful shutdown budget
  int epoll_max_events = 1024;
};

// Counters are safe to read from any thread.
struct Metrics {
  std::atomic<uint64_t> accepted{0};
  std::atomic<uint64_t> closed{0};
  std::atomic<uint64_t> rejected_conns{0};   // over max_connections / out of fds
  std::atomic<uint64_t> requests{0};         // lines processed by workers
  std::atomic<uint64_t> jobs{0};             // batches dispatched
  std::atomic<uint64_t> bytes_in{0};
  std::atomic<uint64_t> bytes_out{0};
  std::atomic<uint64_t> bp_pauses{0};        // reads paused by backpressure
  std::atomic<uint64_t> queue_waits{0};      // dispatch deferred: no job slot (backpressure on)
  std::atomic<uint64_t> shed_busy{0};        // requests answered -BUSY (backpressure off)
  std::atomic<uint64_t> overflow_closes{0};  // connections closed for exceeding buffer limits
  std::atomic<int64_t> active{0};
};

class Server {
 public:
  // Binds and listens immediately (throws std::system_error). If `handler` is empty the
  // built-in command protocol is used.
  explicit Server(Config cfg, LineHandler handler = {});
  ~Server();
  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  uint16_t port() const;

  // Blocking event loop; returns after stop() and a graceful drain.
  void run();

  // Thread-safe and async-signal-safe (atomic store + write(2) on an eventfd).
  void stop() noexcept;

  const Metrics& metrics() const;
  std::string stats_string() const;
  const Config& config() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace flx
