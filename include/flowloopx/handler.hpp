#pragma once
#include <functional>
#include <string>
#include <string_view>

namespace flx {

enum class Action { Continue, Close };

// Called on a worker thread, once per request line (without the trailing \n / \r\n).
// Append the full response (including its terminating '\n') to `out`.
// Return Action::Close to close the connection after the response is flushed.
using LineHandler = std::function<Action(std::string_view line, std::string& out)>;

// Built-in line protocol:
//   PING            -> PONG
//   ECHO <text>     -> <text>
//   WORK <iters>    -> hex digest after <iters> rounds of CPU work (simulates compute)
//   SLEEP <ms>      -> OK after sleeping (simulates blocking I/O in a worker, capped at 2 s)
//   STATS           -> one line of server counters
//   QUIT            -> BYE, then the connection is closed
//   anything else   -> -ERR
Action handle_command(std::string_view line, std::string& out,
                      const std::function<std::string()>& stats);

}  // namespace flx
