#pragma once
#include <sys/types.h>

#include <cstddef>
#include <cstring>
#include <memory>
#include <vector>

#include "flowloopx/pool.hpp"

namespace flx {

// Contiguous, reusable input buffer.
//
// readFd() uses readv(2) with two iovecs: the buffer's own free tail and a
// 64 KiB stack "spill" area. The kernel scatters straight into both, so a
// single syscall can drain a large burst without pre-growing the buffer and
// without a separate staging read. Only bytes that actually landed in the
// spill area are copied (and only then does the buffer grow).
class Buffer {
 public:
  static constexpr size_t kInitial = 4096;
  static constexpr size_t kSpill = 64 * 1024;
  static constexpr size_t kMaxRetainedCapacity = 128 * 1024;

  Buffer() : data_(kInitial) {}

  size_t readable() const noexcept { return w_ - r_; }
  size_t writable() const noexcept { return data_.size() - w_; }
  size_t capacity() const noexcept { return data_.size(); }
  const char* peek() const noexcept { return data_.data() + r_; }

  void retrieve(size_t n) noexcept {
    r_ += n;
    if (r_ == w_) r_ = w_ = 0;  // fully drained: reuse from the front
  }

  void append(const char* p, size_t n) {
    ensure_writable(n);
    std::memcpy(data_.data() + w_, p, n);
    w_ += n;
  }

  // Reads at most max_bytes (> 0). Returns readv(2)'s result; errno is preserved.
  ssize_t read_fd(int fd, size_t max_bytes);

  // Called when returned to a pool: forget contents, drop oversized storage.
  void reset();

 private:
  void ensure_writable(size_t n);

  std::vector<char> data_;
  size_t r_ = 0;
  size_t w_ = 0;
};

// Fixed-size block for the output queue.
struct Chunk {
  static constexpr size_t kSize = 16 * 1024;
  char data[kSize];
  size_t rd = 0;
  size_t wr = 0;
  void reset() { rd = wr = 0; }
};

// FIFO of pooled chunks. writeTo() uses writev(2) to gather up to
// kMaxIov chunks into one syscall; nothing is copied into a staging buffer.
class OutQueue {
 public:
  static constexpr int kMaxIov = 16;

  explicit OutQueue(ObjectPool<Chunk>& pool) : pool_(pool) {}
  ~OutQueue() { clear(); }
  OutQueue(const OutQueue&) = delete;
  OutQueue& operator=(const OutQueue&) = delete;

  size_t size() const noexcept { return bytes_; }
  bool empty() const noexcept { return bytes_ == 0; }

  void append(const char* p, size_t n);
  void append(const char* s) { append(s, std::strlen(s)); }

  // One writev(2). Returns bytes written, or -1 with errno set.
  ssize_t write_to(int fd);

  void clear();

 private:
  ObjectPool<Chunk>& pool_;
  std::vector<std::unique_ptr<Chunk>> q_;
  size_t head_ = 0;
  size_t bytes_ = 0;
};

}  // namespace flx
