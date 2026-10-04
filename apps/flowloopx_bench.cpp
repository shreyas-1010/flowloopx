// flowloopx_bench: epoll-based load generator with a latency histogram.
//
//   active connections : closed-loop clients, each keeps `--pipeline` requests in flight
//   idle connections   : connect and sit there (concurrency / memory test)
//   stalled connections: flood requests and never read responses (slow-consumer test)
//
// Output is one human-readable summary plus (optionally) one CSV row.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;
inline uint64_t now_ns() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count());
}

// Log-linear histogram in microseconds (16 sub-buckets per power of two, ~6% resolution).
class Histogram {
 public:
  static constexpr int kBuckets = 40 * 16;
  void record(uint64_t us) {
    ++b_[index(us)];
    ++count_;
  }
  void merge(const Histogram& o) {
    for (int i = 0; i < kBuckets; ++i) b_[i] += o.b_[i];
    count_ += o.count_;
  }
  uint64_t count() const { return count_; }
  uint64_t percentile(double p) const {
    if (count_ == 0) return 0;
    const uint64_t target = std::max<uint64_t>(1, static_cast<uint64_t>(p / 100.0 * static_cast<double>(count_) + 0.5));
    uint64_t seen = 0;
    for (int i = 0; i < kBuckets; ++i) {
      seen += b_[i];
      if (seen >= target) return value(i);
    }
    return value(kBuckets - 1);
  }

 private:
  static int index(uint64_t v) {
    if (v < 16) return static_cast<int>(v);
    const int e = 63 - __builtin_clzll(v);
    const int idx = (e - 3) * 16 + static_cast<int>((v >> (e - 4)) & 15);
    return std::min(idx, kBuckets - 1);
  }
  static uint64_t value(int idx) {
    if (idx < 16) return static_cast<uint64_t>(idx);
    const int e = idx / 16 + 3;
    return static_cast<uint64_t>(16 + idx % 16) << (e - 4);
  }
  uint64_t b_[kBuckets] = {};
  uint64_t count_ = 0;
};

struct Options {
  std::string host = "127.0.0.1";
  int port = 9000;
  int conns = 64;
  int idle = 0;
  int stalled = 0;
  int pipeline = 1;
  int payload = 32;
  uint64_t work = 0;
  int duration = 10;
  int warmup = 2;
  int threads = 1;
  std::string label = "run";
  bool csv = false;
  bool csv_header = false;
};

enum class Phase : int { Ramp = 0, Warmup = 1, Measure = 2, Stop = 3 };

enum class Kind { Active, Idle, Stalled };

struct Client {
  int fd = -1;
  Kind kind = Kind::Active;
  bool connected = false;
  std::string out;      // pending bytes to send
  size_t off = 0;
  std::vector<uint64_t> sent_at;  // ring of send timestamps (FIFO)
  size_t head = 0, tail = 0, inflight = 0;
  bool line_start = true;
  char first = 0;
  bool want_out = false;
};

struct Result {
  Histogram hist;
  uint64_t ok = 0, busy = 0, errors = 0;
  uint64_t connected = 0, connect_failed = 0;
  uint64_t closed_active = 0, closed_idle = 0, closed_stalled = 0;
  uint64_t stalled_bytes = 0;
};

class Worker {
 public:
  Worker(const Options& o, int active, int idle, int stalled, std::atomic<int>& phase,
         std::atomic<int>& ready_count)
      : o_(o), active_(active), idle_(idle), stalled_(stalled), phase_(phase), ready_(ready_count) {
    if (o.work > 0) {
      req_ = "WORK " + std::to_string(o.work) + "\n";
    } else {
      req_ = "ECHO " + std::string(static_cast<size_t>(std::max(1, o.payload)), 'x') + "\n";
    }
    flood_ = "ECHO " + std::string(1024, 's') + "\n";
  }

  Result run() {
    ep_ = ::epoll_create1(0);
    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(static_cast<uint16_t>(o_.port));
    ::inet_pton(AF_INET, o_.host.c_str(), &sa.sin_addr);

    int todo_active = active_, todo_idle = idle_, todo_stalled = stalled_;
    int connecting = 0;
    const int total = active_ + idle_ + stalled_;
    int settled = 0;  // connected or failed
    bool reported = false;
    std::vector<epoll_event> evs(512);

    while (phase_.load() != static_cast<int>(Phase::Stop)) {
      // Ramp: keep a bounded number of connects in flight so the listen backlog never overflows.
      while (connecting < 128 && (todo_active + todo_idle + todo_stalled) > 0) {
        Kind k = todo_active > 0 ? Kind::Active : (todo_idle > 0 ? Kind::Idle : Kind::Stalled);
        (k == Kind::Active ? todo_active : k == Kind::Idle ? todo_idle : todo_stalled)--;
        int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        if (fd < 0) {
          ++res_.connect_failed;
          ++settled;
          continue;
        }
        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        int r = ::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof sa);
        if (r < 0 && errno != EINPROGRESS) {
          ::close(fd);
          ++res_.connect_failed;
          ++settled;
          continue;
        }
        auto* c = new Client();
        c->fd = fd;
        c->kind = k;
        if (k == Kind::Active) c->sent_at.assign(static_cast<size_t>(o_.pipeline) + 1, 0);
        epoll_event ev{};
        ev.events = EPOLLOUT;  // connect completion
        ev.data.ptr = c;
        ::epoll_ctl(ep_, EPOLL_CTL_ADD, fd, &ev);
        clients_.push_back(c);
        ++connecting;
      }

      const int n = ::epoll_wait(ep_, evs.data(), static_cast<int>(evs.size()), 20);
      for (int i = 0; i < n; ++i) {
        auto* c = static_cast<Client*>(evs[static_cast<size_t>(i)].data.ptr);
        const uint32_t e = evs[static_cast<size_t>(i)].events;
        if (c->fd < 0) continue;
        if (!c->connected) {
          int err = 0;
          socklen_t len = sizeof err;
          ::getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &err, &len);
          --connecting;
          ++settled;
          if (err != 0 || (e & (EPOLLERR | EPOLLHUP))) {
            ++res_.connect_failed;
            drop(c, false);
            continue;
          }
          c->connected = true;
          ++res_.connected;
          start_client(c);
          continue;
        }
        handle(c, e);
      }

      if (!reported && settled >= total) {
        reported = true;
        ready_.fetch_add(1);
      }
    }
    for (auto* c : clients_) {
      if (c->fd >= 0) ::close(c->fd);
      delete c;
    }
    ::close(ep_);
    return std::move(res_);
  }

 private:
  void rearm(Client* c, uint32_t events) {
    epoll_event ev{};
    ev.events = events;
    ev.data.ptr = c;
    ::epoll_ctl(ep_, EPOLL_CTL_MOD, c->fd, &ev);
  }

  void start_client(Client* c) {
    if (c->kind == Kind::Active) {
      rearm(c, EPOLLIN);
      for (int i = 0; i < o_.pipeline; ++i) queue_request(c);
      flush(c);
    } else if (c->kind == Kind::Idle) {
      rearm(c, EPOLLRDHUP);  // only notice if the server closes us
    } else {
      rearm(c, EPOLLOUT | EPOLLRDHUP);  // flood, never read
    }
  }

  void queue_request(Client* c) {
    c->out += req_;
    c->sent_at[c->tail] = now_ns();
    c->tail = (c->tail + 1) % c->sent_at.size();
    ++c->inflight;
  }

  void flush(Client* c) {
    while (c->off < c->out.size()) {
      ssize_t n = ::send(c->fd, c->out.data() + c->off, c->out.size() - c->off, MSG_NOSIGNAL);
      if (n > 0) {
        c->off += static_cast<size_t>(n);
        continue;
      }
      if (n < 0 && errno == EINTR) continue;
      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        if (!c->want_out) {
          c->want_out = true;
          rearm(c, EPOLLIN | EPOLLOUT);
        }
        return;
      }
      drop(c, true);
      return;
    }
    c->out.clear();
    c->off = 0;
    if (c->want_out) {
      c->want_out = false;
      rearm(c, EPOLLIN);
    }
  }

  void drop(Client* c, bool count) {
    if (c->fd < 0) return;
    if (count) {
      if (c->kind == Kind::Active) ++res_.closed_active;
      else if (c->kind == Kind::Idle) ++res_.closed_idle;
      else ++res_.closed_stalled;
    }
    ::epoll_ctl(ep_, EPOLL_CTL_DEL, c->fd, nullptr);
    ::close(c->fd);
    c->fd = -1;
  }

  void handle(Client* c, uint32_t e) {
    if (c->kind == Kind::Idle) {
      if (e & (EPOLLRDHUP | EPOLLHUP | EPOLLERR)) drop(c, true);
      return;
    }
    if (c->kind == Kind::Stalled) {
      if (e & (EPOLLRDHUP | EPOLLHUP | EPOLLERR)) {
        drop(c, true);
        return;
      }
      if (e & EPOLLOUT) {
        // Send a bounded amount per wakeup, then stop; we never read.
        for (int i = 0; i < 64; ++i) {
          ssize_t n = ::send(c->fd, flood_.data(), flood_.size(), MSG_NOSIGNAL | MSG_DONTWAIT);
          if (n <= 0) break;
          res_.stalled_bytes += static_cast<uint64_t>(n);
        }
      }
      return;
    }

    if (e & EPOLLOUT) flush(c);
    if (c->fd < 0) return;
    if (e & (EPOLLERR | EPOLLHUP)) {
      drop(c, true);
      return;
    }
    if (e & EPOLLIN) {
      char buf[65536];
      for (;;) {
        ssize_t n = ::recv(c->fd, buf, sizeof buf, 0);
        if (n > 0) {
          on_data(c, buf, static_cast<size_t>(n));
          if (c->fd < 0) return;
          continue;
        }
        if (n == 0) {
          drop(c, true);
          return;
        }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        drop(c, true);
        return;
      }
      flush(c);
    }
  }

  void on_data(Client* c, const char* p, size_t n) {
    const bool measuring = phase_.load(std::memory_order_relaxed) == static_cast<int>(Phase::Measure);
    for (size_t i = 0; i < n; ++i) {
      if (c->line_start) {
        c->first = p[i];
        c->line_start = false;
      }
      if (p[i] != '\n') continue;
      c->line_start = true;
      if (c->inflight == 0) continue;  // unexpected extra line
      const uint64_t sent = c->sent_at[c->head];
      c->head = (c->head + 1) % c->sent_at.size();
      --c->inflight;
      if (measuring) {
        if (c->first == '-') {
          // -BUSY / -ERR: the server shed this request
          ++res_.busy;
        } else {
          res_.hist.record((now_ns() - sent) / 1000);
          ++res_.ok;
        }
      }
      queue_request(c);
    }
  }

  const Options& o_;
  int active_, idle_, stalled_;
  std::atomic<int>& phase_;
  std::atomic<int>& ready_;
  std::string req_, flood_;
  int ep_ = -1;
  std::vector<Client*> clients_;
  Result res_;
};

[[noreturn]] void usage() {
  std::fprintf(stderr,
      "Usage: flowloopx_bench [options]\n"
      "  --host H --port P      target (default 127.0.0.1:9000)\n"
      "  --conns N              active closed-loop connections (default 64)\n"
      "  --idle N               extra idle connections held open\n"
      "  --stalled N            connections that flood requests and never read\n"
      "  --pipeline D           requests in flight per active connection (default 1)\n"
      "  --payload B            ECHO payload bytes (default 32)\n"
      "  --work ITERS           send WORK ITERS instead of ECHO (CPU-bound handler)\n"
      "  --duration S --warmup S  measured / warmup seconds (default 10 / 2)\n"
      "  --threads T            client threads (default 1)\n"
      "  --label NAME           label for the CSV row\n"
      "  --csv / --csv-header   print a CSV row / header\n");
  std::exit(2);
}

}  // namespace

int main(int argc, char** argv) {
  Options o;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto next = [&]() -> std::string {
      if (i + 1 >= argc) usage();
      return argv[++i];
    };
    if (a == "--host") o.host = next();
    else if (a == "--port") o.port = std::stoi(next());
    else if (a == "--conns") o.conns = std::stoi(next());
    else if (a == "--idle") o.idle = std::stoi(next());
    else if (a == "--stalled") o.stalled = std::stoi(next());
    else if (a == "--pipeline") o.pipeline = std::max(1, std::stoi(next()));
    else if (a == "--payload") o.payload = std::stoi(next());
    else if (a == "--work") o.work = std::stoull(next());
    else if (a == "--duration") o.duration = std::stoi(next());
    else if (a == "--warmup") o.warmup = std::stoi(next());
    else if (a == "--threads") o.threads = std::max(1, std::stoi(next()));
    else if (a == "--label") o.label = next();
    else if (a == "--csv") o.csv = true;
    else if (a == "--csv-header") o.csv_header = true;
    else usage();
  }

  std::atomic<int> phase{static_cast<int>(Phase::Ramp)};
  std::atomic<int> ready{0};
  std::vector<Result> results(static_cast<size_t>(o.threads));
  std::vector<std::thread> threads;
  auto share = [&](int total, int t) { return total / o.threads + (t < total % o.threads ? 1 : 0); };
  for (int t = 0; t < o.threads; ++t) {
    threads.emplace_back([&, t] {
      Worker w(o, share(o.conns, t), share(o.idle, t), share(o.stalled, t), phase, ready);
      results[static_cast<size_t>(t)] = w.run();
    });
  }

  // Wait for the ramp (bounded), warm up, measure, stop.
  const auto ramp_deadline = Clock::now() + std::chrono::seconds(60);
  while (ready.load() < o.threads && Clock::now() < ramp_deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  phase = static_cast<int>(Phase::Warmup);
  std::this_thread::sleep_for(std::chrono::seconds(o.warmup));
  phase = static_cast<int>(Phase::Measure);
  const auto t0 = Clock::now();
  std::this_thread::sleep_for(std::chrono::seconds(o.duration));
  const double secs = std::chrono::duration<double>(Clock::now() - t0).count();
  phase = static_cast<int>(Phase::Stop);
  for (auto& t : threads) t.join();

  Result total;
  for (auto& r : results) {
    total.hist.merge(r.hist);
    total.ok += r.ok;
    total.busy += r.busy;
    total.connected += r.connected;
    total.connect_failed += r.connect_failed;
    total.closed_active += r.closed_active;
    total.closed_idle += r.closed_idle;
    total.closed_stalled += r.closed_stalled;
    total.stalled_bytes += r.stalled_bytes;
  }
  const double rps = static_cast<double>(total.ok) / secs;
  const double busy_rps = static_cast<double>(total.busy) / secs;
  const uint64_t p50 = total.hist.percentile(50), p99 = total.hist.percentile(99),
                 p999 = total.hist.percentile(99.9);

  if (o.csv_header) {
    std::puts("label,active,idle,stalled,pipeline,payload,work,ok_rps,busy_rps,p50_us,p99_us,p999_us,"
              "connected,connect_failed,closed_active,closed_idle,closed_stalled");
  }
  if (o.csv) {
    std::printf("%s,%d,%d,%d,%d,%d,%llu,%.0f,%.0f,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu\n", o.label.c_str(),
                o.conns, o.idle, o.stalled, o.pipeline, o.payload, (unsigned long long)o.work, rps, busy_rps,
                (unsigned long long)p50, (unsigned long long)p99, (unsigned long long)p999,
                (unsigned long long)total.connected, (unsigned long long)total.connect_failed,
                (unsigned long long)total.closed_active, (unsigned long long)total.closed_idle,
                (unsigned long long)total.closed_stalled);
  } else {
    std::printf(
        "connections : %llu established (%llu failed)\n"
        "throughput  : %.0f ok req/s   (%.0f shed/s)\n"
        "latency us  : p50=%llu  p99=%llu  p99.9=%llu\n"
        "closed by server: active=%llu idle=%llu stalled=%llu\n",
        (unsigned long long)total.connected, (unsigned long long)total.connect_failed, rps, busy_rps,
        (unsigned long long)p50, (unsigned long long)p99, (unsigned long long)p999,
        (unsigned long long)total.closed_active, (unsigned long long)total.closed_idle,
        (unsigned long long)total.closed_stalled);
  }
  return 0;
}
