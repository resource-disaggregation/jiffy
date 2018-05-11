#ifndef MMUX_BLOCKING_QUEUE_H
#define MMUX_BLOCKING_QUEUE_H

#include <chrono>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>

using namespace std::chrono_literals;

namespace mmux {
namespace storage {

template<typename T>
class blocking_queue {
 public:
  T pop(int64_t timeout_ms = -1) {
    std::unique_lock<std::mutex> mlock(mutex_);
    while (queue_.empty()) {
      if (timeout_ms != -1) {
        if (cond_.wait_for(mlock, timeout_ms * 1ms) == std::cv_status::timeout) {
          throw std::out_of_range("Timed out waiting for value");
        }
      } else {
        cond_.wait(mlock);
      }
    }
    auto item = queue_.front();
    queue_.pop();
    return item;
  }

  void push(const T &item) {
    std::unique_lock<std::mutex> mlock(mutex_);
    queue_.push(item);
    mlock.unlock();
    cond_.notify_one();
  }

  void push(T &&item) {
    std::unique_lock<std::mutex> mlock(mutex_);
    queue_.push(std::move(item));
    mlock.unlock();
    cond_.notify_one();
  }

 private:
  std::queue<T> queue_;
  std::mutex mutex_;
  std::condition_variable cond_;
};

}
}

#endif //MMUX_BLOCKING_QUEUE_H
