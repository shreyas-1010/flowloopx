#pragma once
#include <unistd.h>

#include <utility>

namespace flx {

// Move-only owner of a file descriptor (RAII).
class UniqueFd {
 public:
  UniqueFd() noexcept = default;
  explicit UniqueFd(int fd) noexcept : fd_(fd) {}
  UniqueFd(UniqueFd&& o) noexcept : fd_(o.release()) {}
  UniqueFd& operator=(UniqueFd&& o) noexcept {
    if (this != &o) reset(o.release());
    return *this;
  }
  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;
  ~UniqueFd() { reset(); }

  int get() const noexcept { return fd_; }
  explicit operator bool() const noexcept { return fd_ >= 0; }
  int release() noexcept { return std::exchange(fd_, -1); }
  void reset(int fd = -1) noexcept {
    if (fd_ >= 0) ::close(fd_);
    fd_ = fd;
  }

 private:
  int fd_ = -1;
};

}  // namespace flx
