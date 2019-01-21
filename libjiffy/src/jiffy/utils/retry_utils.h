#ifndef JIFFY_RETRY_UTILS_H
#define JIFFY_RETRY_UTILS_H

#include <type_traits>
#include <utility>
#include "logger.h"

namespace jiffy {
namespace utils {
/* Retry utility class */
class retry_utils {
 public:
  template<typename F, typename... Args>
  static typename std::result_of_t<F(Args...)> retry(std::size_t n, F &&f, Args... args) {
    std::exception_ptr ex;
    while (n > 0) {
      LOG(log_level::trace) << "Attempt #" << n << "...";
      try {
        return f(std::forward<Args>(args)...);
      } catch (...) {
        LOG(log_level::trace) << "Attempt #" << n << " failed";
        n--;
        ex = std::current_exception();
      }
    }
    LOG(log_level::trace) << "All attempts failed";
    std::rethrow_exception(ex);
  }
};

}
}

#endif //JIFFY_RETRY_UTILS_H
