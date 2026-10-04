#pragma once
#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

namespace flx {

// Free-list object pool. NOT thread-safe: owned and used by the event-loop
// thread only. T must be default-constructible and provide reset().
// Reuse/creation counters are atomic so they can be read from any thread
// (e.g. by the STATS command).
template <class T>
class ObjectPool {
 public:
  explicit ObjectPool(size_t max_retained) : max_retained_(max_retained) {}

  std::unique_ptr<T> acquire() {
    if (!free_.empty()) {
      auto p = std::move(free_.back());
      free_.pop_back();
      reused_.fetch_add(1, std::memory_order_relaxed);
      return p;
    }
    created_.fetch_add(1, std::memory_order_relaxed);
    return std::make_unique<T>();
  }

  void release(std::unique_ptr<T> p) {
    if (!p) return;
    p->reset();
    if (free_.size() < max_retained_) free_.push_back(std::move(p));
  }

  uint64_t created() const { return created_.load(std::memory_order_relaxed); }
  uint64_t reused() const { return reused_.load(std::memory_order_relaxed); }
  size_t idle() const { return free_.size(); }

 private:
  size_t max_retained_;
  std::vector<std::unique_ptr<T>> free_;
  std::atomic<uint64_t> created_{0};
  std::atomic<uint64_t> reused_{0};
};

}  // namespace flx
