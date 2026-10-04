// Dependency-free tests: unit tests for the building blocks plus end-to-end tests
// that run a real Server on an ephemeral port in both epoll modes.
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "flowloopx/buffer.hpp"
#include "flowloopx/server.hpp"
#include "flowloopx/thread_pool.hpp"

using namespace flx;
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

static int g_failures = 0;
#define CHECK(cond)                                                                   \
  do {                                                                                \
    if (!(cond)) {                                                                    \
      std::fprintf(stderr, "    CHECK failed: %s (%s:%d)\n", #cond, __FILE__, __LINE__); \
      ++g_failures;                                                                   \
    }                                                                                 \
  } while (0)

// ------------------------------------------------------------------ helpers

static void set_nonblock(int fd) { ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL) | O_NONBLOCK); }

struct Client {
  int fd = -1;
  std::string rbuf;

  explicit Client(uint16_t port) {
    fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof sa) < 0) {
      ::close(fd);
      fd = -1;
    }
  }
  ~Client() {
    if (fd >= 0) ::close(fd);
  }
  bool ok() const { return fd >= 0; }

  void send_all(const std::string& s) {
    size_t off = 0;
    while (off < s.size()) {
      ssize_t n = ::send(fd, s.data() + off, s.size() - off, MSG_NOSIGNAL);
      if (n <= 0) return;
      off += static_cast<size_t>(n);
    }
  }

  // Reads until a '\n' is available; returns false on EOF/timeout.
  bool read_line(std::string& line, int timeout_ms = 5000) {
    const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
    for (;;) {
      size_t nl = rbuf.find('\n');
      if (nl != std::string::npos) {
        line = rbuf.substr(0, nl);
        rbuf.erase(0, nl + 1);
        return true;
      }
      const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
      if (left <= 0) return false;
      pollfd p{fd, POLLIN, 0};
      if (::poll(&p, 1, static_cast<int>(left)) <= 0) return false;
      char buf[65536];
      ssize_t n = ::recv(fd, buf, sizeof buf, 0);
      if (n <= 0) return false;
      rbuf.append(buf, static_cast<size_t>(n));
    }
  }

  std::string roundtrip(const std::string& req) {
    send_all(req + "\n");
    std::string l;
    return read_line(l) ? l : "<timeout/eof>";
  }

  // True if the peer closes within the timeout.
  bool wait_closed(int timeout_ms = 5000) {
    const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
    while (Clock::now() < deadline) {
      pollfd p{fd, POLLIN, 0};
      if (::poll(&p, 1, 50) > 0) {
        char buf[4096];
        ssize_t n = ::recv(fd, buf, sizeof buf, 0);
        if (n == 0) return true;
        if (n < 0 && errno != EAGAIN && errno != EINTR) return true;
      }
    }
    return false;
  }
};

struct RunningServer {
  std::unique_ptr<Server> server;
  std::thread thread;
  explicit RunningServer(Config cfg) {
    cfg.port = 0;
    cfg.bind_addr = "127.0.0.1";
    server = std::make_unique<Server>(cfg);
    thread = std::thread([this] { server->run(); });
  }
  uint16_t port() const { return server->port(); }
  void stop() {
    if (thread.joinable()) {
      server->stop();
      thread.join();
    }
  }
  ~RunningServer() { stop(); }
};

// Metrics are independent atomics updated by the loop thread, so a test that has just seen a
// client-visible effect (e.g. a closed socket) must poll for the matching counter, not assert it.
template <class Pred>
static bool wait_until(Pred pred, int timeout_ms = 3000) {
  const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
  while (Clock::now() < deadline) {
    if (pred()) return true;
    std::this_thread::sleep_for(2ms);
  }
  return pred();
}

static bool wait_active(Server& srv, int64_t want, int timeout_ms = 3000) {
  return wait_until([&] { return srv.metrics().active.load() == want; }, timeout_ms);
}

static Config base_config(bool et, bool bp) {
  Config c;
  c.edge_triggered = et;
  c.backpressure = bp;
  c.worker_threads = 4;
  c.queue_capacity = 256;
  c.drain_timeout_ms = 3000;
  return c;
}

// ------------------------------------------------------------------ unit tests

static void test_buffer_readv_spill() {
  int sv[2];
  CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);
  const size_t total = 100 * 1024;
  std::string data(total, 0);
  for (size_t i = 0; i < total; ++i) data[i] = static_cast<char>('a' + i % 26);
  CHECK(::write(sv[1], data.data(), 60 * 1024) == 60 * 1024);  // fits in the AF_UNIX buffer

  Buffer b;  // initial capacity is only 4 KiB: most of the data must come through the spill iovec
  ssize_t n = b.read_fd(sv[0], 80 * 1024);
  CHECK(n == 60 * 1024);
  CHECK(b.readable() == 60 * 1024);
  CHECK(std::memcmp(b.peek(), data.data(), 60 * 1024) == 0);

  b.retrieve(1000);
  CHECK(b.readable() == 60 * 1024 - 1000);
  CHECK(b.peek()[0] == data[1000]);
  b.retrieve(b.readable());
  CHECK(b.readable() == 0);

  // max_bytes is respected
  CHECK(::write(sv[1], data.data(), 10000) == 10000);
  n = b.read_fd(sv[0], 6000);
  CHECK(n == 6000);
  b.append("xyz", 3);
  CHECK(b.readable() == 6003);
  ::close(sv[0]);
  ::close(sv[1]);
}

static void test_outqueue_gather() {
  int sv[2];
  CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);
  set_nonblock(sv[0]);
  ObjectPool<Chunk> pool(8);
  OutQueue q(pool);

  const size_t total = 100 * 1024;  // spans several 16 KiB chunks
  std::string data(total, 0);
  for (size_t i = 0; i < total; ++i) data[i] = static_cast<char>(i * 31 + 7);
  q.append(data.data(), data.size());
  CHECK(q.size() == total);

  std::string got;
  std::vector<char> tmp(8192);
  for (int guard = 0; guard < 100000 && got.size() < total; ++guard) {
    if (!q.empty()) {
      ssize_t w = q.write_to(sv[0]);
      CHECK(w >= 0 || errno == EAGAIN);
    }
    ssize_t r = ::recv(sv[1], tmp.data(), tmp.size(), MSG_DONTWAIT);
    if (r > 0) got.append(tmp.data(), static_cast<size_t>(r));
  }
  CHECK(q.empty());
  CHECK(got == data);
  CHECK(pool.reused() + pool.created() >= 7);
  ::close(sv[0]);
  ::close(sv[1]);
}

static void test_object_pool_reuse() {
  ObjectPool<Buffer> pool(4);
  auto a = pool.acquire();
  pool.release(std::move(a));
  auto b = pool.acquire();
  CHECK(pool.created() == 1);
  CHECK(pool.reused() == 1);
}

static void test_bounded_queue_and_pool() {
  BoundedQueue<int> q(3);
  CHECK(q.try_push(1) && q.try_push(2) && q.try_push(3));
  CHECK(!q.try_push(4));  // full: producer is told immediately, never blocked
  int v = 0;
  CHECK(q.pop(v) && v == 1);
  CHECK(q.try_push(4));
  q.close();
  CHECK(!q.try_push(5));  // closed
  CHECK(q.pop(v) && v == 2);
  CHECK(q.pop(v) && v == 3);
  CHECK(q.pop(v) && v == 4);
  CHECK(!q.pop(v));  // closed and drained

  // Pool: bounded queue rejects when workers are saturated; shutdown drains queued work.
  static std::atomic<int> ran{0};
  static std::atomic<bool> gate{false};
  ran = 0;
  gate = false;
  ThreadPool pool(1, 2);
  auto blocker = [](void*) {
    while (!gate) std::this_thread::sleep_for(1ms);
    ++ran;
  };
  CHECK(pool.try_post({blocker, nullptr}));
  std::this_thread::sleep_for(50ms);  // worker picks up the first task
  CHECK(pool.try_post({blocker, nullptr}));
  CHECK(pool.try_post({blocker, nullptr}));
  CHECK(!pool.try_post({blocker, nullptr}));  // queue (cap 2) is full
  gate = true;
  pool.shutdown();
  CHECK(ran == 3);  // everything accepted before shutdown ran
}

// ------------------------------------------------------------------ end-to-end tests

static void test_basic_commands(bool et, bool bp) {
  RunningServer rs(base_config(et, bp));
  Client c(rs.port());
  CHECK(c.ok());
  CHECK(c.roundtrip("PING") == "PONG");
  CHECK(c.roundtrip("ECHO hello world") == "hello world");
  CHECK(c.roundtrip("WORK 1000").size() == 16);
  CHECK(c.roundtrip("nonsense") == "-ERR");
  c.send_all("PING\r\n");  // CRLF tolerated
  std::string l;
  CHECK(c.read_line(l) && l == "PONG");
  CHECK(c.roundtrip("STATS").find("requests=") != std::string::npos);
  c.send_all("QUIT\n");
  CHECK(c.read_line(l) && l == "BYE");
  CHECK(c.wait_closed());
}

static void test_pipelined_ordering(bool et, bool bp) {
  RunningServer rs(base_config(et, bp));
  Client c(rs.port());
  const int N = 5000;
  std::string batch;
  for (int i = 0; i < N; ++i) {
    // alternate cheap and expensive work: with 4 workers, order only survives if the server enforces it
    batch += (i % 7 == 0) ? "WORK 20000\n" : ("ECHO #" + std::to_string(i) + "\n");
  }
  std::thread sender([&] { c.send_all(batch); });
  bool in_order = true;
  for (int i = 0; i < N; ++i) {
    std::string l;
    if (!c.read_line(l)) { in_order = false; break; }
    if (i % 7 == 0) { if (l.size() != 16) in_order = false; }
    else if (l != "#" + std::to_string(i)) in_order = false;
  }
  sender.join();
  CHECK(in_order);
}

static void test_many_clients(bool et, bool bp) {
  RunningServer rs(base_config(et, bp));
  std::atomic<int> good{0};
  std::vector<std::thread> ts;
  const int T = 32, R = 200;
  for (int t = 0; t < T; ++t) {
    ts.emplace_back([&, t] {
      Client c(rs.port());
      bool ok = c.ok();
      for (int i = 0; ok && i < R; ++i) {
        std::string msg = "m" + std::to_string(t) + "_" + std::to_string(i);
        ok = c.roundtrip("ECHO " + msg) == msg;
      }
      if (ok) ++good;
    });
  }
  for (auto& t : ts) t.join();
  CHECK(good == T);
}

static void test_event_loop_never_blocks(bool et) {
  RunningServer rs(base_config(et, true));
  Client slow(rs.port()), fast(rs.port());
  slow.send_all("SLEEP 600\n");  // occupies a worker for 600 ms
  std::this_thread::sleep_for(50ms);
  const auto t0 = Clock::now();
  CHECK(fast.roundtrip("PING") == "PONG");
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count();
  CHECK(ms < 300);  // the loop kept serving other sockets while the worker slept
  std::string l;
  CHECK(slow.read_line(l) && l == "OK");
}

static void test_line_too_long(bool et) {
  RunningServer rs(base_config(et, true));
  Client c(rs.port());
  c.send_all("ECHO " + std::string(40 * 1024, 'a'));  // no newline, bigger than max_batch_bytes
  std::string l;
  CHECK(c.read_line(l) && l == "-ERR line too long");
  CHECK(c.wait_closed());
}

// With backpressure ON, a client that floods without reading is paused (bounded memory) but loses
// nothing: once it starts reading, every request that was fully sent is answered, in order.
static void test_backpressure_lossless(bool et) {
  Config cfg = base_config(et, true);
  cfg.max_inbuf_bytes = 32 * 1024;
  cfg.max_batch_bytes = 8 * 1024;
  cfg.out_high_watermark = 64 * 1024;
  RunningServer rs(cfg);
  Client c(rs.port());
  set_nonblock(c.fd);

  const std::string line = "ECHO " + std::string(250, 'q') + "\n";  // 256 B incl. newline
  uint64_t sent_bytes = 0;
  const auto until = Clock::now() + 1500ms;
  while (Clock::now() < until) {  // flood without reading until the pipe is jammed
    const size_t off = sent_bytes % line.size();  // resume mid-line after a partial send
    ssize_t n = ::send(c.fd, line.data() + off, line.size() - off, MSG_NOSIGNAL);
    if (n > 0) sent_bytes += static_cast<uint64_t>(n);
    else if (errno == EAGAIN) std::this_thread::sleep_for(1ms);
    else break;
  }
  CHECK(rs.server->metrics().bp_pauses.load() > 0);
  CHECK(rs.server->metrics().overflow_closes.load() == 0);
  CHECK(rs.server->metrics().active.load() == 1);

  ::shutdown(c.fd, SHUT_WR);
  const uint64_t expected = sent_bytes / line.size();  // a partial trailing line is never answered
  uint64_t got = 0;
  bool ok = true;
  const auto deadline = Clock::now() + 20s;
  std::string acc;
  char buf[65536];
  while (Clock::now() < deadline) {
    pollfd p{c.fd, POLLIN, 0};
    if (::poll(&p, 1, 100) <= 0) continue;
    ssize_t n = ::recv(c.fd, buf, sizeof buf, 0);
    if (n == 0) break;
    if (n < 0) continue;
    acc.append(buf, static_cast<size_t>(n));
    size_t pos;
    while ((pos = acc.find('\n')) != std::string::npos) {
      if (pos != 250 || acc.find_first_not_of('q') < pos) ok = false;  // exactly the 250-byte payload back
      acc.erase(0, pos + 1);
      ++got;
    }
  }
  CHECK(ok);
  CHECK(got == expected);
}

// With backpressure OFF the same flood hits the hard limits and the connection is dropped.
static void test_no_backpressure_overflow_closes(bool et) {
  Config cfg = base_config(et, false);
  cfg.max_inbuf_bytes = 32 * 1024;
  cfg.max_batch_bytes = 8 * 1024;
  cfg.max_outbuf_bytes = 128 * 1024;
  RunningServer rs(cfg);
  Client c(rs.port());
  set_nonblock(c.fd);
  const std::string line = "ECHO " + std::string(250, 'q') + "\n";
  const auto until = Clock::now() + 6s;
  while (Clock::now() < until && rs.server->metrics().overflow_closes.load() == 0) {
    ssize_t n = ::send(c.fd, line.data(), line.size(), MSG_NOSIGNAL);
    if (n < 0 && errno == EAGAIN) std::this_thread::sleep_for(1ms);
    else if (n < 0) break;
  }
  CHECK(wait_until([&] { return rs.server->metrics().overflow_closes.load() >= 1; }));
  CHECK(wait_active(*rs.server, 0));
}

// Saturate a tiny pool: backpressure ON queues everything (no -BUSY); OFF sheds with -BUSY.
static void test_overload(bool et, bool bp) {
  Config cfg = base_config(et, bp);
  cfg.worker_threads = 1;
  cfg.queue_capacity = 2;
  RunningServer rs(cfg);
  const int K = 10;
  std::vector<std::unique_ptr<Client>> cs;
  for (int i = 0; i < K; ++i) cs.push_back(std::make_unique<Client>(rs.port()));
  for (auto& c : cs) c->send_all("SLEEP 60\n");
  int ok = 0, busy = 0;
  for (auto& c : cs) {
    std::string l;
    if (c->read_line(l, 15000)) (l == "OK" ? ok : busy) += 1;
  }
  if (bp) {
    CHECK(ok == K);
    CHECK(busy == 0);
    CHECK(rs.server->metrics().queue_waits.load() > 0);
  } else {
    CHECK(busy > 0);
    CHECK(ok + busy == K);
    CHECK(rs.server->metrics().shed_busy.load() == static_cast<uint64_t>(busy));
  }
}

static void test_graceful_shutdown(bool et) {
  RunningServer rs(base_config(et, true));
  Client c(rs.port());
  c.send_all("SLEEP 400\n");
  std::this_thread::sleep_for(100ms);  // job is in flight on a worker
  const auto t0 = Clock::now();
  std::thread stopper([&] { rs.stop(); });
  std::string l;
  CHECK(c.read_line(l, 3000) && l == "OK");  // in-flight work completes and is flushed
  CHECK(c.wait_closed(3000));
  stopper.join();
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count();
  CHECK(ms < 2500);  // drained promptly, no hang
  Client late(rs.port());
  CHECK(!late.ok());  // listener is closed
}

static void test_idle_connections(bool et) {
  RunningServer rs(base_config(et, true));
  std::vector<std::unique_ptr<Client>> idle;
  for (int i = 0; i < 500; ++i) idle.push_back(std::make_unique<Client>(rs.port()));
  Client c(rs.port());
  CHECK(c.roundtrip("PING") == "PONG");
  CHECK(wait_active(*rs.server, 501));
  idle.clear();
  CHECK(wait_active(*rs.server, 1));
  CHECK(c.roundtrip("PING") == "PONG");
}

// ------------------------------------------------------------------ runner

static void run(const char* name, const std::function<void()>& fn) {
  const int before = g_failures;
  const auto t0 = Clock::now();
  fn();
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count();
  std::printf("[%s] %-48s %lld ms\n", g_failures == before ? " OK " : "FAIL", name, static_cast<long long>(ms));
  std::fflush(stdout);
}

int main() {
  run("buffer: readv scatter/spill", test_buffer_readv_spill);
  run("outqueue: writev gather", test_outqueue_gather);
  run("pool: object reuse", test_object_pool_reuse);
  run("bounded queue + thread pool", test_bounded_queue_and_pool);

  for (bool et : {true, false}) {
    const std::string m = et ? "ET" : "LT";
    for (bool bp : {true, false}) {
      const std::string s = m + (bp ? " bp=on " : " bp=off ");
      run((s + "basic commands").c_str(), [&] { test_basic_commands(et, bp); });
      run((s + "pipelined ordering").c_str(), [&] { test_pipelined_ordering(et, bp); });
      run((s + "many concurrent clients").c_str(), [&] { test_many_clients(et, bp); });
      run((s + "overload behaviour").c_str(), [&] { test_overload(et, bp); });
    }
    run((m + " event loop never blocks").c_str(), [&] { test_event_loop_never_blocks(et); });
    run((m + " line too long").c_str(), [&] { test_line_too_long(et); });
    run((m + " backpressure is lossless").c_str(), [&] { test_backpressure_lossless(et); });
    run((m + " no-backpressure overflow closes").c_str(), [&] { test_no_backpressure_overflow_closes(et); });
    run((m + " graceful shutdown").c_str(), [&] { test_graceful_shutdown(et); });
    run((m + " idle connections").c_str(), [&] { test_idle_connections(et); });
  }

  if (g_failures) {
    std::printf("\n%d check(s) FAILED\n", g_failures);
    return 1;
  }
  std::printf("\nall tests passed\n");
  return 0;
}
