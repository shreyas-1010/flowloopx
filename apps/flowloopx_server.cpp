#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>

#include "flowloopx/server.hpp"

namespace {

flx::Server* g_server = nullptr;

void on_signal(int) {
  if (g_server) g_server->stop();  // async-signal-safe
}

void usage(const char* argv0) {
  std::cerr << "Usage: " << argv0 << " [options]\n"
            << "  --port N              listen port (default 9000)\n"
            << "  --bind ADDR           bind address (default 0.0.0.0)\n"
            << "  --mode et|lt          edge- or level-triggered epoll (default et)\n"
            << "  --backpressure on|off pause reads under pressure vs shed/disconnect (default on)\n"
            << "  --workers N           worker threads (default: hw concurrency, min 2)\n"
            << "  --queue N             bounded task-queue capacity (default 1024)\n"
            << "  --max-conns N         connection cap (default 100000)\n"
            << "  --max-inbuf BYTES     per-connection input limit (default 65536)\n"
            << "  --out-high BYTES      output high watermark (default 262144)\n"
            << "  --max-outbuf BYTES    per-connection output hard limit (default 1048576)\n"
            << "  --drain-ms N          graceful shutdown budget (default 5000)\n"
            << "  --stats-every SEC     print counters periodically (default 0 = off)\n";
}

}  // namespace

int main(int argc, char** argv) {
  flx::Config cfg;
  int stats_every = 0;

  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    auto next = [&]() -> std::string {
      if (i + 1 >= argc) {
        usage(argv[0]);
        std::exit(2);
      }
      return argv[++i];
    };
    if (a == "--port") cfg.port = static_cast<uint16_t>(std::stoi(next()));
    else if (a == "--bind") cfg.bind_addr = next();
    else if (a == "--mode") cfg.edge_triggered = (next() != "lt");
    else if (a == "--backpressure") cfg.backpressure = (next() != "off");
    else if (a == "--workers") cfg.worker_threads = std::stoul(next());
    else if (a == "--queue") cfg.queue_capacity = std::stoul(next());
    else if (a == "--max-conns") cfg.max_connections = std::stoul(next());
    else if (a == "--max-inbuf") cfg.max_inbuf_bytes = std::stoul(next());
    else if (a == "--out-high") cfg.out_high_watermark = std::stoul(next());
    else if (a == "--max-outbuf") cfg.max_outbuf_bytes = std::stoul(next());
    else if (a == "--drain-ms") cfg.drain_timeout_ms = std::stoi(next());
    else if (a == "--stats-every") stats_every = std::stoi(next());
    else {
      usage(argv[0]);
      return a == "--help" || a == "-h" ? 0 : 2;
    }
  }

  try {
    flx::Server server(cfg);
    g_server = &server;

    struct sigaction sa {};
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    std::cerr << "flowloopx listening on " << cfg.bind_addr << ":" << server.port() << " ("
              << (cfg.edge_triggered ? "edge" : "level") << "-triggered, backpressure "
              << (cfg.backpressure ? "on" : "off") << ")" << std::endl;

    std::thread reporter;
    std::atomic<bool> done{false};
    if (stats_every > 0) {
      reporter = std::thread([&] {
        int slept = 0;
        while (!done) {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          if (++slept >= stats_every * 10) {
            slept = 0;
            std::cerr << server.stats_string() << std::endl;
          }
        }
      });
    }

    server.run();
    done = true;
    if (reporter.joinable()) reporter.join();
    g_server = nullptr;
    std::cerr << "flowloopx stopped cleanly: " << server.stats_string() << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "fatal: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}
