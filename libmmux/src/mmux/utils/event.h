#ifndef MMUX_EVENT_H
#define MMUX_EVENT_H

#include <iostream>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>

namespace mmux {
namespace utils {

template<class T>
class atomic_unique_ptr {
 public:
  using pointer = T *;
  constexpr atomic_unique_ptr() noexcept : ptr() {}
  explicit atomic_unique_ptr(pointer p) noexcept : ptr(p) {}
  atomic_unique_ptr(atomic_unique_ptr &&p) noexcept : ptr(p.release()) {}
  atomic_unique_ptr &operator=(atomic_unique_ptr &&p) noexcept {
    reset(p.release());
    return *this;
  }
  explicit atomic_unique_ptr(std::unique_ptr<T> &&p) noexcept : ptr(p.release()) {}
  atomic_unique_ptr &operator=(std::unique_ptr<T> &&p) noexcept {
    reset(p.release());
    return *this;
  }

  void reset(pointer p = pointer()) {
    auto old = ptr.exchange(p);
    if (old) delete old;
  }
  explicit operator pointer() const { return ptr; }
  pointer operator->() const { return ptr; }
  pointer get() const { return ptr; }
  explicit operator bool() const { return ptr != pointer(); }
  pointer release() { return ptr.exchange(pointer()); }
  ~atomic_unique_ptr() { reset(); }

 private:
  std::atomic<pointer> ptr;
};

template<typename T>
class event {
 public:
  explicit event() : event_data_() {}

  bool is_set() const {
    return event_data_.get() != nullptr;
  }

  const T &get() const {
    return *(event_data_.get());
  }

  void operator()(const T& val) {
    set(val);
  }

  void set(const T &val) {
    event_data_ = std::make_unique<T>(val);
    cv_.notify_all();
  }

  bool wait(int64_t timeout_ms) {
    using namespace std::chrono_literals;
    std::unique_lock<std::mutex> cv_lock(mtx_);
    return cv_.wait_for(cv_lock, timeout_ms * 1ms, [this]() -> bool {
      return is_set();
    });
  }

  void wait() {
    std::unique_lock<std::mutex> cv_lock(mtx_);
    cv_.wait(cv_lock, [this]() -> bool {
      return is_set();
    });
  }

 private:
  atomic_unique_ptr<T> event_data_;
  std::mutex mtx_;
  std::condition_variable cv_;
};

}
}

#endif //MMUX_EVENT_H
