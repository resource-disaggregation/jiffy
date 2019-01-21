#ifndef JIFFY_BLOCKING_QUEUE_H
#define JIFFY_BLOCKING_QUEUE_H

#include <chrono>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>

using namespace std::chrono_literals;

namespace jiffy {
namespace storage {


template<typename T>
/* A blocking queue class template
 * Each push and pop argument can only be done once at a time.
 * Push can be done immediately when it gets the lock.
 * Pull can be done only when queue is not empty.
 * If empty and given timeout time, release lock and wait for given time
 * If empty and given timeout time is -1, wait for condition variable
 */
class blocking_queue {
 public:

  /**
   * @brief Pop element out of queue
   * @param timeout_ms timeout
   * @return Oldest element in the queue
   */

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

  /**
   * @brief Push item in the queue using lvalue reference
   * @param item Item to be pushed
   */

  void push(const T &item) {
    std::unique_lock<std::mutex> mlock(mutex_);
    queue_.push(item);
    mlock.unlock();
    cond_.notify_one();
  }

  /**
   * @brief Push item in the queue using rvalue reference
   * @param item Item to be pushed
   */

  void push(T &&item) {
    std::unique_lock<std::mutex> mlock(mutex_);
    queue_.push(std::move(item));
    mlock.unlock();
    cond_.notify_one();
  }

 private:
  /* Queue */
  std::queue<T> queue_;
  /* Operation mutex */
  std::mutex mutex_;
  /* Conditional variable */
  std::condition_variable cond_;
};

}
}

#endif //JIFFY_BLOCKING_QUEUE_H
