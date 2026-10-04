#include "flowloopx/buffer.hpp"

#include <sys/uio.h>

#include <algorithm>

namespace flx {

void Buffer::ensure_writable(size_t n) {
  if (writable() >= n) return;
  if (r_ > 0) {  // compact first: cheaper than growing
    size_t len = readable();
    std::memmove(data_.data(), data_.data() + r_, len);
    r_ = 0;
    w_ = len;
    if (writable() >= n) return;
  }
  data_.resize(std::max(w_ + n, data_.size() * 2));
}

ssize_t Buffer::read_fd(int fd, size_t max_bytes) {
  ensure_writable(std::min<size_t>(max_bytes, kInitial));

  char spill[kSpill];
  const size_t w0 = std::min(writable(), max_bytes);
  const size_t w1 = std::min(max_bytes - w0, sizeof spill);

  iovec iov[2];
  iov[0].iov_base = data_.data() + w_;
  iov[0].iov_len = w0;
  iov[1].iov_base = spill;
  iov[1].iov_len = w1;

  const ssize_t n = ::readv(fd, iov, w1 > 0 ? 2 : 1);
  if (n <= 0) return n;

  if (static_cast<size_t>(n) <= w0) {
    w_ += static_cast<size_t>(n);
  } else {
    w_ += w0;
    append(spill, static_cast<size_t>(n) - w0);
  }
  return n;
}

void Buffer::reset() {
  r_ = w_ = 0;
  if (data_.size() > kMaxRetainedCapacity) {
    std::vector<char>(kInitial).swap(data_);
  }
}

// ---------------------------------------------------------------- OutQueue

void OutQueue::append(const char* p, size_t n) {
  bytes_ += n;
  while (n > 0) {
    if (head_ == q_.size() || q_.back()->wr == Chunk::kSize) {
      if (head_ > 0 && head_ == q_.size()) {  // all consumed: recycle the slot array
        q_.clear();
        head_ = 0;
      }
      q_.push_back(pool_.acquire());
    }
    Chunk& c = *q_.back();
    const size_t take = std::min(n, Chunk::kSize - c.wr);
    std::memcpy(c.data + c.wr, p, take);
    c.wr += take;
    p += take;
    n -= take;
  }
}

ssize_t OutQueue::write_to(int fd) {
  iovec iov[kMaxIov];
  int cnt = 0;
  for (size_t i = head_; i < q_.size() && cnt < kMaxIov; ++i, ++cnt) {
    iov[cnt].iov_base = q_[i]->data + q_[i]->rd;
    iov[cnt].iov_len = q_[i]->wr - q_[i]->rd;
  }
  if (cnt == 0) return 0;

  const ssize_t n = ::writev(fd, iov, cnt);
  if (n <= 0) return n;

  size_t left = static_cast<size_t>(n);
  bytes_ -= left;
  while (left > 0) {
    Chunk& c = *q_[head_];
    const size_t avail = c.wr - c.rd;
    if (left >= avail) {
      left -= avail;
      pool_.release(std::move(q_[head_]));
      ++head_;
    } else {
      c.rd += left;
      left = 0;
    }
  }
  if (head_ == q_.size()) {
    q_.clear();
    head_ = 0;
  } else if (head_ >= 64) {  // keep the slot array from growing under steady streaming
    q_.erase(q_.begin(), q_.begin() + static_cast<std::ptrdiff_t>(head_));
    head_ = 0;
  }
  return n;
}

void OutQueue::clear() {
  for (size_t i = head_; i < q_.size(); ++i) pool_.release(std::move(q_[i]));
  q_.clear();
  head_ = 0;
  bytes_ = 0;
}

}  // namespace flx
