#include "flowloopx/handler.hpp"

#include <charconv>
#include <chrono>
#include <cstdint>
#include <thread>

namespace flx {
namespace {

uint64_t parse_u64(std::string_view s, uint64_t cap) {
  uint64_t v = 0;
  std::from_chars(s.data(), s.data() + s.size(), v);
  return v > cap ? cap : v;
}

}  // namespace

Action handle_command(std::string_view line, std::string& out,
                      const std::function<std::string()>& stats) {
  const size_t sp = line.find(' ');
  const std::string_view cmd = line.substr(0, sp);
  const std::string_view arg = sp == std::string_view::npos ? std::string_view{} : line.substr(sp + 1);

  if (cmd == "PING") {
    out += "PONG\n";
  } else if (cmd == "ECHO") {
    out.append(arg.data(), arg.size());
    out += '\n';
  } else if (cmd == "WORK") {
    const uint64_t iters = parse_u64(arg, 50'000'000);
    uint64_t h = 1469598103934665603ULL;  // FNV-1a over a counter: pure CPU, not optimizable away
    for (uint64_t i = 0; i < iters; ++i) {
      h ^= i;
      h *= 1099511628211ULL;
    }
    static const char* hex = "0123456789abcdef";
    char buf[17];
    for (int i = 15; i >= 0; --i, h >>= 4) buf[i] = hex[h & 0xF];
    buf[16] = '\n';
    out.append(buf, 17);
  } else if (cmd == "SLEEP") {
    std::this_thread::sleep_for(std::chrono::milliseconds(parse_u64(arg, 2000)));
    out += "OK\n";
  } else if (cmd == "STATS") {
    out += stats ? stats() : std::string("n/a");
    out += '\n';
  } else if (cmd == "QUIT") {
    out += "BYE\n";
    return Action::Close;
  } else {
    out += "-ERR\n";
  }
  return Action::Continue;
}

}  // namespace flx
