#ifndef JIFFY_EVENT_H
#define JIFFY_EVENT_H

#include <iostream>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>

namespace jiffy {
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
/* Event class */
class event {
 public:
  /**
   * @brief Constructor
   */

  explicit event() : event_data_() {}

  /**
   * @brief Check if event data is set
   * @return Bool value, true if event data is set
   */

  bool is_set() const {
    return event_data_.get() != nullptr;
  }

  /**
   * @brief Fetch event data
   * @return Event data
   */

  const T &get() const {
    return *(event_data_.get());
  }

  /**
   * @brief Parenthesis operator
   * @param val Event data value
   */

  void operator()(const T& val) {
    set(val);
  }

  /**
   * @brief Set event data and notify all waiting condition variable
   * @param val Event data to be set
   */

  void set(const T &val) {
    event_data_ = std::make_unique<T>(val);
    cv_.notify_all();
  }

  /**
   * @brief Wait for timeout time
   * @param timeout_ms timeout
   * @return Bool value, true if event data is set
   */

  bool wait(int64_t timeout_ms) {
    using namespace std::chrono_literals;
    std::unique_lock<std::mutex> cv_lock(mtx_);
    return cv_.wait_for(cv_lock, timeout_ms * 1ms, [this]() -> bool {
      return is_set();
    });
  }

  /**
   * @brief Wait without time out
   */

  void wait() {
    std::unique_lock<std::mutex> cv_lock(mtx_);
    cv_.wait(cv_lock, [this]() -> bool {
      return is_set();
    });
  }

 private:
  /* Atomic unique pointer to event data */
  atomic_unique_ptr<T> event_data_;
  /* Event mutex */
  std::mutex mtx_;
  /* Event condition variable */
  std::condition_variable cv_;
};

}
}

#endif //JIFFY_EVENT_H
