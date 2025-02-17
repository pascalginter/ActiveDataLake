#pragma once
#include <functional>
#include <thread>
#include <cassert>

namespace hpqp {

struct BlockedRange {
  size_t begin;  // inclusive
  size_t end;    // exclusive
};


void parallel_for_each(size_t threads, BlockedRange range, const std::function<void(size_t)>& cb) {
  // don't use more threads than available in hardware
  assert(threads <= std::thread::hardware_concurrency());

  // just work on main thread if thread count is <= 1
  if (threads <= 1) {
    for (size_t i=range.begin; i!= range.end; i++) cb(i);
    return;
  }
  std::vector<std::thread> workers;
  workers.reserve(threads);
  // start multiple threads, each doing a fixed chunk of the work
  std::atomic<size_t> index(range.begin);
  for (auto tid = 0u; tid < threads - 1; ++tid) {
    workers.emplace_back([&]() {
      size_t i;
      while ((i = index.fetch_add(1)) < range.end) {
        cb(i);
      }
    });
  }

  // wait for all threads to finish
  for (auto& t : workers) {
    t.join();
  }
}


void simple_parallel_for(size_t threads, BlockedRange range, const std::function<void(BlockedRange)>& cb) {
  // don't use more threads than available in hardware
  assert(threads <= std::thread::hardware_concurrency());

  // just work on main thread if thread count is <= 1
  if (threads <= 1) {
    return cb(range);
  }
  std::vector<std::thread> workers;
  workers.reserve(threads);
  // start multiple threads, each doing a fixed chunk of the work
  auto chunk_size = 10000;
  std::atomic<size_t> index(range.begin);
  for (auto tid = 0u; tid < threads - 1; ++tid) {
    workers.emplace_back([&]() {
      size_t i;
      while ((i = index.fetch_add(chunk_size)) < range.end) {
        cb({i, std::min(i + chunk_size, range.end)});
      }
    });
  }

  // wait for all threads to finish
  for (auto& t : workers) {
    t.join();
  }
}

template <class T>
class enumerable_thread_specific {
  std::vector<std::thread::id> threads;
  std::vector<T> thread_specifics;
  std::mutex mtx;
  std::function<T()> constructor;

 public:
  using Iterator = typename std::vector<T>::iterator;

  enumerable_thread_specific(std::function<T()> cb) : constructor(cb) {
    threads.reserve(std::thread::hardware_concurrency());
    thread_specifics.reserve(std::thread::hardware_concurrency());
  }

  T& local() {
    auto thread_id = std::this_thread::get_id();
    std::lock_guard guard(mtx);
    auto it = std::find(threads.begin(), threads.end(), thread_id);
    size_t pos = std::distance(threads.begin(), it);
    if (it == threads.end()) {
      std::cout << "no match" << std::endl;
      threads.push_back(thread_id);
      thread_specifics.push_back(constructor());
      pos = thread_specifics.size() - 1;
    }else {
      std::cout << "match" << std::endl;
    }
    std::cout << thread_specifics[pos].size() << std::endl;
    return thread_specifics[pos];
  }

  Iterator begin() { return thread_specifics.begin(); }
  Iterator end() { return thread_specifics.end(); }
};

}  // namespace hpqp
