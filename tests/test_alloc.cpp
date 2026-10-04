// Verifies the "no hot-path allocations" property empirically: replaces global operator new,
// counts allocations made on the event-loop thread and the worker threads only, and asserts that
// after warm-up a steady stream of requests allocates (essentially) nothing.
// GCC can't see that our replacement operator new is malloc-backed and warns on the free() in
// operator delete; the pairing is intentional.
#pragma GCC diagnostic ignored "-Wmismatched-new-delete"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <new>
#include <string>
#include <thread>
#include <vector>

#include "flowloopx/server.hpp"

static thread_local bool t_count = false;
static std::atomic<unsigned long long> g_allocs{0};

void* operator new(std::size_t n) {
  if (t_count) g_allocs.fetch_add(1, std::memory_order_relaxed);
  if (void* p = std::malloc(n ? n : 1)) return p;
  throw std::bad_alloc();
}
void* operator new[](std::size_t n) { return operator new(n); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

static int connect_to(uint16_t port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in sa{};
  sa.sin_family = AF_INET;
  sa.sin_port = htons(port);
  ::inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof sa) < 0) {
    ::close(fd);
    return -1;
  }
  return fd;
}

static bool roundtrips(int fd, int n, const std::string& req) {
  char buf[256];
  for (int i = 0; i < n; ++i) {
    if (::send(fd, req.data(), req.size(), MSG_NOSIGNAL) != static_cast<ssize_t>(req.size())) return false;
    size_t got = 0;
    while (got < req.size() - 5) {  // reply = payload + newline = request minus "ECHO "
      ssize_t r = ::recv(fd, buf, sizeof buf, 0);
      if (r <= 0) return false;
      got += static_cast<size_t>(r);
    }
  }
  return true;
}

// Connections persist across both phases, so the measured window contains no accepts: any
// allocation counted there is a per-request cost.
static int run_mode(bool et) {
  flx::Config cfg;
  cfg.port = 0;
  cfg.bind_addr = "127.0.0.1";
  cfg.edge_triggered = et;
  cfg.worker_threads = 3;
  auto handler = [](std::string_view line, std::string& out) {
    t_count = true;  // marks this worker thread as "server side" (flag is per-thread, set once)
    return flx::handle_command(line, out, {});
  };
  flx::Server server(cfg, handler);
  std::thread loop([&] {
    t_count = true;
    server.run();
  });

  const std::string req = "ECHO 0123456789abcdef0123456789abcdef\n";
  const int kClients = 8;
  std::vector<int> fds;
  for (int i = 0; i < kClients; ++i) fds.push_back(connect_to(server.port()));

  auto phase = [&](int per_client) {
    std::vector<std::thread> ts;
    std::atomic<int> ok{0};
    for (int i = 0; i < kClients; ++i)
      ts.emplace_back([&, i] { ok += roundtrips(fds[static_cast<size_t>(i)], per_client, req) ? 1 : 0; });
    for (auto& t : ts) t.join();
    return ok.load() == kClients;
  };

  bool ok = phase(3000);  // warm-up: pools fill, strings reach working capacity
  const auto before = g_allocs.load();
  ok = phase(10000) && ok;  // 80,000 requests, zero accepts
  const auto after = g_allocs.load();
  for (int fd : fds) ::close(fd);
  server.stop();
  loop.join();

  const unsigned long long delta = after - before;
  const bool pass = ok && delta <= 4;
  std::printf("[%s] %s: %llu allocations on server threads across 80000 steady-state requests\n",
              pass ? " OK " : "FAIL", et ? "ET" : "LT", delta);
  return pass ? 0 : 1;
}

int main() { return run_mode(true) | run_mode(false); }
