#include "flowloopx/thread_pool.hpp"

namespace flx {

ThreadPool::ThreadPool(size_t threads, size_t queue_capacity) : queue_(queue_capacity) {
  if (threads == 0) threads = 1;
  workers_.reserve(threads);
  for (size_t i = 0; i < threads; ++i) workers_.emplace_back([this] { worker_main(); });
}

ThreadPool::~ThreadPool() { shutdown(); }

void ThreadPool::shutdown() {
  queue_.close();
  for (auto& t : workers_)
    if (t.joinable()) t.join();
}

void ThreadPool::worker_main() {
  Task t;
  while (queue_.pop(t)) {
    try {
      t.fn(t.arg);
    } catch (...) {
      // A task must never take a worker down.
    }
  }
}

}  // namespace flx
