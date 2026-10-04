#include "flowloopx/server.hpp"

#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <csignal>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <deque>
#include <mutex>
#include <sstream>
#include <system_error>
#include <thread>

#include "flowloopx/buffer.hpp"
#include "flowloopx/connection.hpp"
#include "flowloopx/pool.hpp"
#include "flowloopx/socket_util.hpp"
#include "flowloopx/thread_pool.hpp"

namespace flx {
namespace {

using Clock = std::chrono::steady_clock;
constexpr size_t kReadBudget = 256 * 1024;  // bytes read per connection per turn (fairness)
constexpr size_t kTooLong = static_cast<size_t>(-1);

[[noreturn]] void throw_errno(const char* what) {
  throw std::system_error(errno, std::generic_category(), what);
}

}  // namespace

struct Server::Impl {
  // A batch of complete request lines travelling loop -> worker -> loop.
  // Jobs and their strings are recycled, so steady state allocates nothing.
  struct Job {
    std::shared_ptr<Connection> conn;
    std::string payload;
    std::string response;
    bool close_after = false;
    Impl* owner = nullptr;
  };

  Config cfg;
  LineHandler handler;
  Metrics metrics;

  // Pools are declared first so they are destroyed last (connections return buffers to them).
  ObjectPool<Buffer> buf_pool{4096};
  ObjectPool<Chunk> chunk_pool{2048};

  UniqueFd epfd, wakefd, listenfd, sparefd;
  uint16_t bound_port = 0;
  char listen_tag = 0, wake_tag = 0;

  std::atomic<bool> stop_requested{false};
  std::atomic<bool> wake_pending{false};
  bool draining = false;

  std::vector<std::shared_ptr<Connection>> conns;  // indexed by fd
  std::vector<std::shared_ptr<Connection>> graveyard;  // closed this iteration; freed at its end
  std::vector<std::shared_ptr<Connection>> ready;      // budget-limited reads to resume
  std::deque<std::shared_ptr<Connection>> waiting;     // complete requests, no job slot yet

  std::vector<std::unique_ptr<Job>> job_store;
  std::vector<Job*> job_free;
  size_t inflight = 0;
  size_t max_inflight = 0;

  std::mutex done_mu;
  std::vector<Job*> done_shared;  // guarded by done_mu
  std::vector<Job*> done_local;

  std::unique_ptr<ThreadPool> pool;

  // ------------------------------------------------------------ setup

  Impl(Config c, LineHandler h) : cfg(std::move(c)), handler(std::move(h)) {
    if (cfg.worker_threads == 0)
      cfg.worker_threads = std::max<size_t>(2, std::thread::hardware_concurrency());
    if (cfg.max_batch_bytes > cfg.max_inbuf_bytes) cfg.max_batch_bytes = cfg.max_inbuf_bytes;
    max_inflight = cfg.queue_capacity + cfg.worker_threads;

    std::signal(SIGPIPE, SIG_IGN);  // write errors are handled via errno

    if (!handler) {
      handler = [this](std::string_view line, std::string& out) {
        return handle_command(line, out, [this] { return stats_string(); });
      };
    }

    epfd.reset(::epoll_create1(EPOLL_CLOEXEC));
    if (!epfd) throw_errno("epoll_create1");
    wakefd.reset(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
    if (!wakefd) throw_errno("eventfd");
    sparefd.reset(::open("/dev/null", O_RDONLY | O_CLOEXEC));

    listenfd = create_listener(cfg.bind_addr, cfg.port, cfg.backlog);
    bound_port = local_port(listenfd.get());

    ctl(EPOLL_CTL_ADD, listenfd.get(), EPOLLIN, &listen_tag);
    ctl(EPOLL_CTL_ADD, wakefd.get(), EPOLLIN, &wake_tag);
  }

  void ctl(int op, int fd, uint32_t events, void* ptr) {
    epoll_event ev{};
    ev.events = events;
    ev.data.ptr = ptr;
    if (::epoll_ctl(epfd.get(), op, fd, &ev) < 0 && op != EPOLL_CTL_DEL) throw_errno("epoll_ctl");
  }

  std::string stats_string() const {
    std::ostringstream o;
    o << "active=" << metrics.active.load() << " accepted=" << metrics.accepted.load()
      << " closed=" << metrics.closed.load() << " requests=" << metrics.requests.load()
      << " jobs=" << metrics.jobs.load() << " bytes_in=" << metrics.bytes_in.load()
      << " bytes_out=" << metrics.bytes_out.load() << " bp_pauses=" << metrics.bp_pauses.load()
      << " queue_waits=" << metrics.queue_waits.load() << " shed_busy=" << metrics.shed_busy.load()
      << " overflow_closes=" << metrics.overflow_closes.load()
      << " buf_new=" << buf_pool.created() << " buf_reused=" << buf_pool.reused()
      << " chunk_new=" << chunk_pool.created() << " chunk_reused=" << chunk_pool.reused()
      << " queue_hwm=" << (pool ? pool->queue_high_water() : 0) << " mode="
      << (cfg.edge_triggered ? "ET" : "LT") << " backpressure=" << (cfg.backpressure ? "on" : "off");
    return o.str();
  }

  // ------------------------------------------------------------ worker side

  static void run_job(void* arg) {
    Job* j = static_cast<Job*>(arg);
    j->owner->execute(*j);
  }

  void execute(Job& j) {
    std::string_view rest(j.payload);
    while (!rest.empty()) {
      const size_t nl = rest.find('\n');
      std::string_view line = rest.substr(0, nl);
      rest = (nl == std::string_view::npos) ? std::string_view{} : rest.substr(nl + 1);
      if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
      if (line.empty()) continue;
      metrics.requests.fetch_add(1, std::memory_order_relaxed);
      try {
        if (handler(line, j.response) == Action::Close) {
          j.close_after = true;
          break;
        }
      } catch (...) {
        j.response += "-ERR internal\n";
      }
    }
    {
      std::lock_guard<std::mutex> lk(done_mu);
      done_shared.push_back(&j);
    }
    // Coalesce wakeups: only the first completion after the loop drained needs a syscall.
    if (!wake_pending.exchange(true)) {
      const uint64_t one = 1;
      [[maybe_unused]] auto r = ::write(wakefd.get(), &one, sizeof one);
    }
  }

  // ------------------------------------------------------------ job slots

  Job* acquire_job() {
    if (inflight >= max_inflight) return nullptr;
    Job* j;
    if (!job_free.empty()) {
      j = job_free.back();
      job_free.pop_back();
    } else {
      job_store.push_back(std::make_unique<Job>());
      j = job_store.back().get();
      j->owner = this;
    }
    ++inflight;
    return j;
  }

  void release_job(Job* j) {
    j->conn.reset();
    j->payload.clear();
    j->response.clear();
    j->close_after = false;
    if (j->payload.capacity() > 256 * 1024) std::string().swap(j->payload);
    if (j->response.capacity() > 256 * 1024) std::string().swap(j->response);
    job_free.push_back(j);
    --inflight;
  }

  // ------------------------------------------------------------ connection lifecycle

  void accept_all() {
    for (int i = 0; i < 256; ++i) {
      int fd = ::accept4(listenfd.get(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
      if (fd < 0) {
        if (errno == EINTR || errno == ECONNABORTED) continue;
        if (errno == EMFILE || errno == ENFILE) {
          // Out of fds: free the spare, accept-and-drop the pending connection, re-arm the spare.
          sparefd.reset();
          int victim = ::accept4(listenfd.get(), nullptr, nullptr, SOCK_CLOEXEC);
          if (victim >= 0) ::close(victim);
          sparefd.reset(::open("/dev/null", O_RDONLY | O_CLOEXEC));
          metrics.rejected_conns.fetch_add(1, std::memory_order_relaxed);
        }
        return;  // EAGAIN or unrecoverable: wait for the next readiness event
      }
      if (static_cast<size_t>(metrics.active.load()) >= cfg.max_connections) {
        ::close(fd);
        metrics.rejected_conns.fetch_add(1, std::memory_order_relaxed);
        continue;
      }
      if (cfg.tcp_nodelay) set_nodelay(fd);

      auto c = std::make_shared<Connection>(UniqueFd(fd), buf_pool, chunk_pool);
      const uint32_t ev = cfg.edge_triggered ? (EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET)
                                             : (EPOLLIN | EPOLLRDHUP);
      c->registered = ev;
      try {
        ctl(EPOLL_CTL_ADD, fd, ev, c.get());
      } catch (...) {
        continue;  // c's destructor closes the fd
      }
      if (static_cast<size_t>(fd) >= conns.size()) conns.resize(static_cast<size_t>(fd) + 1);
      conns[static_cast<size_t>(fd)] = std::move(c);
      metrics.accepted.fetch_add(1, std::memory_order_relaxed);
      metrics.active.fetch_add(1, std::memory_order_relaxed);
    }
  }

  void close_conn(Connection& c) {
    if (c.closed) return;
    const int fd = c.fd();
    ::epoll_ctl(epfd.get(), EPOLL_CTL_DEL, fd, nullptr);
    c.close();  // fd closed now; the object itself is freed at the end of the iteration
    if (fd >= 0 && static_cast<size_t>(fd) < conns.size() && conns[static_cast<size_t>(fd)].get() == &c)
      graveyard.push_back(std::move(conns[static_cast<size_t>(fd)]));
    metrics.closed.fetch_add(1, std::memory_order_relaxed);
    metrics.active.fetch_sub(1, std::memory_order_relaxed);
  }

  void overflow_close(Connection& c) {
    close_conn(c);  // close first so observers of overflow_closes also see active already decremented
    metrics.overflow_closes.fetch_add(1, std::memory_order_relaxed);
  }

  // ------------------------------------------------------------ I/O

  // Returns false if the connection was closed.
  bool append_out(Connection& c, const char* p, size_t n) {
    c.out.append(p, n);
    if (c.out.size() > cfg.max_outbuf_bytes) {
      overflow_close(c);
      return false;
    }
    return true;
  }

  bool flush(Connection& c) {
    while (!c.out.empty()) {
      const ssize_t n = c.flush_some();
      if (n > 0) {
        metrics.bytes_out.fetch_add(static_cast<uint64_t>(n), std::memory_order_relaxed);
        continue;
      }
      if (n < 0 && errno == EINTR) continue;
      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        c.can_write = false;
        return true;
      }
      close_conn(c);
      return false;
    }
    return true;
  }

  // Backpressure gate with hysteresis. Returns true if reading must stay paused.
  bool read_blocked(Connection& c) {
    if (!cfg.backpressure) return false;
    const size_t in = c.buffered_in();
    if (c.read_paused) {
      if (in <= cfg.max_inbuf_bytes / 2 && c.out.size() <= cfg.out_high_watermark / 2)
        c.read_paused = false;
    } else if (in >= cfg.max_inbuf_bytes || c.out.size() >= cfg.out_high_watermark) {
      c.read_paused = true;
      metrics.bp_pauses.fetch_add(1, std::memory_order_relaxed);
    }
    return c.read_paused;
  }

  void do_read(Connection& c) {
    size_t budget = kReadBudget;
    while (c.can_read && !c.eof && !c.closing && !draining) {
      if (read_blocked(c)) return;
      const size_t used = c.buffered_in();
      const size_t room = cfg.max_inbuf_bytes > used ? cfg.max_inbuf_bytes - used : 0;
      if (room == 0) {  // only reachable with backpressure off: enforce the hard limit
        overflow_close(c);
        return;
      }
      const ssize_t n = c.receive(room);
      if (n > 0) {
        metrics.bytes_in.fetch_add(static_cast<uint64_t>(n), std::memory_order_relaxed);
        // Level-triggered: a short read means drained, the kernel re-notifies on new data.
        // Edge-triggered: must keep reading until EAGAIN or the edge is lost.
        if (!cfg.edge_triggered && static_cast<size_t>(n) < room) c.can_read = false;
        budget -= std::min(budget, static_cast<size_t>(n));
        if (budget == 0) {
          if (c.can_read && !c.in_ready) {
            c.in_ready = true;
            ready.push_back(c.shared_from_this());
          }
          return;
        }
        continue;
      }
      if (n == 0) {
        c.eof = true;
        c.can_read = false;
        return;
      }
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        c.can_read = false;
        return;
      }
      close_conn(c);
      return;
    }
  }

  // Length of the complete-lines prefix to hand to a worker; 0 = none yet; kTooLong = protocol error.
  size_t batch_len(Connection& c) {
    const size_t avail = c.buffered_in();
    if (avail == 0) return 0;
    const size_t n = std::min(avail, cfg.max_batch_bytes);
    const char* base = c.in->peek();
    if (const void* p = ::memrchr(base, '\n', n)) return static_cast<const char*>(p) - base + 1;
    return avail >= cfg.max_batch_bytes ? kTooLong : 0;
  }

  void try_dispatch(Connection& c) {
    while (!c.closed && !c.closing && !draining && !c.job_inflight) {
      if (cfg.backpressure && c.out.size() >= cfg.out_high_watermark) return;
      const size_t take = batch_len(c);
      if (take == 0) return;
      if (take == kTooLong) {
        c.drop_input();
        c.closing = true;
        append_out(c, "-ERR line too long\n", 19);
        return;
      }
      Job* j = acquire_job();
      if (j) {
        j->conn = c.shared_from_this();
        j->payload.assign(c.in->peek(), take);
        if (pool->try_post(Task{&Impl::run_job, j})) {
          c.in->retrieve(take);
          c.job_inflight = true;
          c.waiting = false;
          metrics.jobs.fetch_add(1, std::memory_order_relaxed);
          return;
        }
        release_job(j);  // queue full
      }
      // No capacity right now.
      if (cfg.backpressure) {
        // Leave the bytes in the input buffer; reads stop once it fills. Retried on completion.
        if (!c.waiting) {
          c.waiting = true;
          waiting.push_back(c.shared_from_this());
          metrics.queue_waits.fetch_add(1, std::memory_order_relaxed);
        }
        return;
      }
      // Backpressure off: shed this batch with one -BUSY per request line and keep going.
      size_t lines = 0;
      for (const char *p = c.in->peek(), *e = p + take; (p = static_cast<const char*>(::memchr(p, '\n', static_cast<size_t>(e - p)))); ++p)
        ++lines;
      c.in->retrieve(take);
      metrics.shed_busy.fetch_add(lines, std::memory_order_relaxed);
      for (size_t i = 0; i < lines; ++i)
        if (!append_out(c, "-BUSY\n", 6)) return;
    }
  }

  void finish_check(Connection& c) {
    if (c.job_inflight || !c.out.empty()) return;
    if (c.closing || draining || (c.eof && batch_len(c) == 0)) close_conn(c);
  }

  void update_interest(Connection& c) {  // level-triggered mode only
    const bool want_read = !c.eof && !c.closing && !draining && !c.read_paused;
    const bool want_write = !c.out.empty() && !c.can_write;
    const uint32_t ev = (want_read ? (EPOLLIN | EPOLLRDHUP) : 0u) | (want_write ? EPOLLOUT : 0u);
    if (ev != c.registered) {
      c.registered = ev;
      epoll_event e{};
      e.events = ev;
      e.data.ptr = &c;
      ::epoll_ctl(epfd.get(), EPOLL_CTL_MOD, c.fd(), &e);
    }
  }

  // Central state machine for a connection; call after any event that may change what it can do.
  void service(Connection& c) {
    if (c.closed) return;
    if (!c.out.empty() && c.can_write && !flush(c)) return;
    try_dispatch(c);
    if (c.closed) return;
    do_read(c);
    if (c.closed) return;
    try_dispatch(c);
    if (c.closed) return;
    if (!c.out.empty() && c.can_write && !flush(c)) return;
    finish_check(c);
    if (c.closed) return;
    c.release_input_if_empty();
    if (!cfg.edge_triggered) update_interest(c);
  }

  void on_conn_event(Connection& c, uint32_t ev) {
    if (c.closed) return;
    if (ev & (EPOLLERR | EPOLLHUP)) {
      close_conn(c);
      return;
    }
    if (ev & (EPOLLIN | EPOLLRDHUP)) c.can_read = true;  // RDHUP: read on to observe EOF
    if (ev & EPOLLOUT) c.can_write = true;
    service(c);
  }

  // ------------------------------------------------------------ completions

  void process_completions() {
    {
      std::lock_guard<std::mutex> lk(done_mu);
      done_local.swap(done_shared);
    }
    for (Job* j : done_local) {
      std::shared_ptr<Connection> c = std::move(j->conn);
      c->job_inflight = false;
      bool alive = !c->closed;
      if (alive && !j->response.empty()) alive = append_out(*c, j->response.data(), j->response.size());
      if (alive && j->close_after) c->closing = true;
      release_job(j);
      if (alive) service(*c);
    }
    done_local.clear();

    // Job slots were freed: retry connections that were waiting for one.
    if (!waiting.empty()) {
      std::deque<std::shared_ptr<Connection>> batch;
      batch.swap(waiting);
      for (auto& c : batch) {
        c->waiting = false;
        if (!c->closed) service(*c);
      }
    }
  }

  void process_ready() {
    if (ready.empty()) return;
    std::vector<std::shared_ptr<Connection>> batch;
    batch.swap(ready);
    for (auto& c : batch) {
      c->in_ready = false;
      if (!c->closed) service(*c);
    }
  }

  // ------------------------------------------------------------ shutdown

  void begin_drain() {
    draining = true;
    ::epoll_ctl(epfd.get(), EPOLL_CTL_DEL, listenfd.get(), nullptr);
    listenfd.reset();  // stop accepting: new connection attempts are refused
    for (size_t fd = 0; fd < conns.size(); ++fd) {
      if (auto c = conns[fd]) {
        c->in_ready = false;
        service(*c);  // idle connections close; busy ones finish their in-flight batch first
      }
    }
  }

  void run() {
    pool = std::make_unique<ThreadPool>(cfg.worker_threads, cfg.queue_capacity);
    std::vector<epoll_event> events(static_cast<size_t>(cfg.epoll_max_events));
    Clock::time_point deadline{};

    for (;;) {
      int timeout = (ready.empty() && waiting.empty()) ? -1 : 0;
      if (draining) timeout = 20;
      const int n = ::epoll_wait(epfd.get(), events.data(), static_cast<int>(events.size()), timeout);
      if (n < 0 && errno != EINTR) throw_errno("epoll_wait");

      bool woke = false;
      for (int i = 0; i < n; ++i) {
        void* tag = events[static_cast<size_t>(i)].data.ptr;
        const uint32_t ev = events[static_cast<size_t>(i)].events;
        if (tag == &listen_tag) {
          if (!draining) accept_all();
        } else if (tag == &wake_tag) {
          uint64_t v;
          [[maybe_unused]] auto r = ::read(wakefd.get(), &v, sizeof v);
          wake_pending.store(false);  // must precede the swap in process_completions()
          woke = true;
        } else {
          on_conn_event(*static_cast<Connection*>(tag), ev);
        }
      }
      if (woke || draining || inflight > 0) process_completions();
      process_ready();

      if (!draining && stop_requested.load()) {
        begin_drain();
        deadline = Clock::now() + std::chrono::milliseconds(cfg.drain_timeout_ms);
      }
      if (draining) {
        if (metrics.active.load() == 0 && inflight == 0) break;
        if (Clock::now() >= deadline) break;
      }
      graveyard.clear();
    }

    for (auto& c : conns)
      if (c) close_conn(*c);
    pool->shutdown();  // workers finish whatever is still queued, then exit
    graveyard.clear();
  }

  void stop() noexcept {
    stop_requested.store(true);
    const uint64_t one = 1;
    [[maybe_unused]] auto r = ::write(wakefd.get(), &one, sizeof one);
  }

  ~Impl() {
    if (pool) pool->shutdown();
  }
};

Server::Server(Config cfg, LineHandler handler)
    : impl_(std::make_unique<Impl>(std::move(cfg), std::move(handler))) {}
Server::~Server() = default;
uint16_t Server::port() const { return impl_->bound_port; }
void Server::run() { impl_->run(); }
void Server::stop() noexcept { impl_->stop(); }
const Metrics& Server::metrics() const { return impl_->metrics; }
std::string Server::stats_string() const { return impl_->stats_string(); }
const Config& Server::config() const { return impl_->cfg; }

}  // namespace flx
