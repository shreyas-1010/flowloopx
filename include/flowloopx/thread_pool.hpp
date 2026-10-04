#pragma once
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <thread>
#include <vector>

namespace flx {

// Fixed-capacity MPMC queue. Producers never block (try_push); consumers
// block in pop() until an item arrives or the queue is closed and drained.
template <class T>
class BoundedQueue {
 public:
  explicit BoundedQueue(size_t capacity) : buf_(capacity ? capacity : 1), cap_(buf_.size()) {}

  // Non-blocking. False if full or closed.
  bool try_push(T v) {
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (closed_ || size_ == cap_) return false;
      buf_[(head_ + size_) % cap_] = std::move(v);
      ++size_;
      if (size_ > high_water_) high_water_ = size_;
    }
    cv_.notify_one();
    return true;
  }

  // Blocks. False only when closed AND empty (items queued before close() are still delivered).
  bool pop(T& out) {
    std::unique_lock<std::mutex> lk(mu_);
    cv_.wait(lk, [&] { return size_ > 0 || closed_; });
    if (size_ == 0) return false;
    out = std::move(buf_[head_]);
    head_ = (head_ + 1) % cap_;
    --size_;
    return true;
  }

  void close() {
    {
      std::lock_guard<std::mutex> lk(mu_);
      closed_ = true;
    }
    cv_.notify_all();
  }

  size_t size() const {
    std::lock_guard<std::mutex> lk(mu_);
    return size_;
  }
  size_t high_water() const {
    std::lock_guard<std::mutex> lk(mu_);
    return high_water_;
  }
  size_t capacity() const noexcept { return cap_; }

 private:
  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::vector<T> buf_;
  size_t cap_;
  size_t head_ = 0;
  size_t size_ = 0;
  size_t high_water_ = 0;
  bool closed_ = false;
};

// Allocation-free task: plain function pointer + context pointer.
struct Task {
  void (*fn)(void*) = nullptr;
  void* arg = nullptr;
};

// Fixed pool of worker threads fed by a bounded queue.
class ThreadPool {
 public:
  ThreadPool(size_t threads, size_t queue_capacity);
  ~ThreadPool();
  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  // Never blocks. False when the queue is full (caller decides: backpressure or shed).
  bool try_post(Task t) { return queue_.try_push(t); }

  // Stops accepting work, lets workers finish everything already queued, joins. Idempotent.
  void shutdown();

  size_t queue_size() const { return queue_.size(); }
  size_t queue_high_water() const { return queue_.high_water(); }
  size_t queue_capacity() const noexcept { return queue_.capacity(); }
  size_t threads() const noexcept { return workers_.size(); }

 private:
  void worker_main();

  BoundedQueue<Task> queue_;
  std::vector<std::thread> workers_;
};

}  // namespace flx
