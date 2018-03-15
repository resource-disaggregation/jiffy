#ifndef ELASTICMEM_LOGGER_H
#define ELASTICMEM_LOGGER_H

#include <sstream>
#include <string>
#include <cstdio>
#include "time_utils.h"

namespace elasticmem {
namespace utils {

enum log_level {
  ALL = 0,
  TRACE = 1,
  DEBUG = 2,
  INFO = 3,
  WARN = 4,
  ERROR = 5,
  FATAL = 6,
  OFF = 7
};

static log_level LOG_LEVEL = log_level::INFO;

static void configure_logging(log_level level) {
  LOG_LEVEL = level;
}

class logger {
 public:
  explicit logger(log_level level) : opened_(false), msg_level_(level) {
    os_ << time_utils::current_date_time();
    os_ << " " << to_string(level) << ": ";
  }

  virtual ~logger() {
    if (opened_) {
      os_ << std::endl;
      fprintf(stdout, "%s", os_.str().c_str());
      fflush(stderr);
    }
    opened_ = false;
  }

  template<typename T>
  logger &operator<<(const T &msg) {
    if (msg_level_ >= LOG_LEVEL) {
      os_ << msg;
      opened_ = true;
    }
    return *this;
  }

 private:
  std::string to_string(const log_level level) {
    switch (level) {
      case log_level::TRACE:return "TRACE";
      case log_level::DEBUG:return "DEBUG";
      case log_level::INFO:return "INFO";
      case log_level::WARN:return "WARN";
      case log_level::ERROR:return "ERROR";
      case log_level::FATAL:return "FATAL";
      default:return "";
    }
  }

  bool opened_;
  std::ostringstream os_;
  log_level msg_level_;
};

}
}

#endif //ELASTICMEM_LOGGER_H
