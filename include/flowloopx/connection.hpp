#pragma once
#include <memory>

#include "flowloopx/buffer.hpp"
#include "flowloopx/fd.hpp"

namespace flx {

// One client socket. RAII: the destructor closes the fd and returns every
// pooled buffer. All members are touched by the event-loop thread only; the
// worker pool never sees a Connection (it only sees a Job's payload/response).
class Connection : public std::enable_shared_from_this<Connection> {
 public:
  Connection(UniqueFd fd, ObjectPool<Buffer>& buffers, ObjectPool<Chunk>& chunks)
      : out(chunks), buffers_(buffers), fd_(std::move(fd)) {}
  ~Connection() { close(); }
  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  int fd() const noexcept { return fd_.get(); }

  // Scatter-read (readv) up to max_bytes (> 0) into the input buffer, acquiring one lazily.
  ssize_t receive(size_t max_bytes) {
    if (!in) in = buffers_.acquire();
    return in->read_fd(fd_.get(), max_bytes);
  }
  size_t buffered_in() const noexcept { return in ? in->readable() : 0; }
  void drop_input() { buffers_.release(std::move(in)); }
  void release_input_if_empty() {
    if (in && in->readable() == 0) buffers_.release(std::move(in));
  }

  // Gather-write (writev) of queued output. Returns bytes written or -1 (errno).
  ssize_t flush_some() { return out.write_to(fd_.get()); }

  // Idempotent. Closes the socket immediately and returns buffers to the pools.
  void close() noexcept {
    closed = true;
    out.clear();
    drop_input();
    fd_.reset();
  }

  // ---- state (event-loop thread only) ----
  std::unique_ptr<Buffer> in;  // only held while there is unprocessed input
  OutQueue out;

  bool closed = false;
  bool can_read = false;        // kernel may have data for us (edge/level seen, not yet EAGAIN)
  bool can_write = true;        // last write did not hit EAGAIN
  bool eof = false;             // peer shut down its write side
  bool closing = false;         // close once in-flight work and output are done
  bool read_paused = false;     // backpressure: not reading from this socket
  bool job_inflight = false;    // at most one batch per connection => responses stay ordered
  bool waiting = false;         // queued for a job slot
  bool in_ready = false;        // queued for a deferred (budget-limited) read
  uint32_t registered = 0;      // epoll interest currently registered (level-triggered mode)

 private:
  ObjectPool<Buffer>& buffers_;
  UniqueFd fd_;
};

}  // namespace flx
