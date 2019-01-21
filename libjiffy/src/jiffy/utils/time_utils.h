#ifndef JIFFY_TIME_UTILS_H
#define JIFFY_TIME_UTILS_H

#include <cstdio>
#include <ctime>
#include <chrono>
#include <string>

namespace jiffy {
namespace utils {
/* Time utility class */
class time_utils {
 public:
  /**
   * @brief Fetch current data time
   * @return Current data time
   */

  static std::string current_date_time() {
    std::time_t rawtime;
    std::tm *timeinfo;
    char buffer[100];

    std::time(&rawtime);
    timeinfo = std::localtime(&rawtime);
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %X", timeinfo);
    return std::string(buffer);
  }

  /**
   * @brief Fetch current time in nanoseconds
   * @return Time
   */

  static uint64_t now_ns() {
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count());
  }

  /**
   * @brief Fetch current time in microseconds
   * @return Time
   */

  static uint64_t now_us() {
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count());
  }

  /**
   * @brief Fetch current time in milliseconds
   * @return Time
   */

  static uint64_t now_ms() {
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count());
  }

  /**
   * @brief Fetch current time in seconds
   * @return Time
   */

  static uint64_t now_s() {
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count());
  }

  template<typename F, typename ...Args>
  static uint64_t time_function_ns(F &&f, Args &&... args) {
    std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();
    f(std::forward<Args>(args)...);
    std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
  }
};

}
}

#endif //JIFFY_TIME_UTILS_H
