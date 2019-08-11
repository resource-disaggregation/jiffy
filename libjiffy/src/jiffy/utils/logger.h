#ifndef JIFFY_LOGGER_H
#define JIFFY_LOGGER_H

#include <sstream>
#include <string>
#include <cstdio>
#include "time_utils.h"

namespace jiffy {
namespace utils {

enum log_level {
  all = 0,
  trace = 1,
  debug = 2,
  info = 3,
  warn = 4,
  error = 5,
  fatal = 6,
  off = 7
};

#ifdef __GNUG__
#define __COMPACT_PRETTY_FUNCTION__ log_utils::compute_method_name(__FUNCTION__, __PRETTY_FUNCTION__)
#define LOG(level) logger(level, __COMPACT_PRETTY_FUNCTION__)
#else
#define LOG(level) logger(level, __func__)
#endif
/* Logger class */
class logger {
 public:
  static log_level LOG_LEVEL;

  /**
   * @brief Constructor
   * @param level Log level
   * @param fname Log function name
   */

  explicit logger(log_level level, const std::string &fname) : opened_(false), msg_level_(level) {
    os_ << time_utils::current_date_time();
    os_ << " " << to_string(level);
    os_ << " " << fname << " ";
  }

  /**
   * @brief Virtual destructor
   */

  virtual ~logger() {
    if (opened_) {
      os_ << std::endl;
      fprintf(stderr, "%s", os_.str().c_str());
      fflush(stderr);
    }
    opened_ = false;
  }

  template<typename T>
  /**
   * @brief << operator
   * @tparam T Log message type
   * @param msg Log message
   * @return Logger
   */
  logger &operator<<(const T &msg) {
    if (msg_level_ >= LOG_LEVEL) {
      os_ << msg;
      opened_ = true;
    }
    return *this;
  }

 private:
  /**
   * @brief Convert log level to string
   * @param level Log level
   * @return String
   */

  static std::string to_string(const log_level level) {
    switch (level) {
      case log_level::trace:return "TRACE";
      case log_level::debug:return "DEBUG";
      case log_level::info:return "INFO";
      case log_level::warn:return "WARN";
      case log_level::error:return "ERROR";
      case log_level::fatal:return "FATAL";
      default:return "";
    }
  }
  /* Bool value, true if logger is opened */
  bool opened_;
  /* String stream */
  std::ostringstream os_;
  /* Log message level */
  log_level msg_level_;
};
/* Log utility class */
class log_utils {
 public:
  /**
   * @brief Log thrift message
   * @param msg Thrift message
   */

  static void log_thrift_msg(const char *msg) {
    logger(info, "Thrift") << msg;
  }

  /**
   * @brief Configure log level
   * @param level Log level
   */

  static void configure_log_level(log_level level) {
    logger::LOG_LEVEL = level;
  }

#ifdef __GNUG__
  static std::string compute_method_name(const std::string &function, const std::string &pretty_function) {
    size_t function_name_loc = pretty_function.find(function);
    size_t begin = pretty_function.rfind(' ', function_name_loc) + 1;
    size_t end = pretty_function.find('(', function_name_loc + function.length());
    if (pretty_function[end + 1] == ')')
      return (pretty_function.substr(begin, end - begin) + "()");
    else
      return (pretty_function.substr(begin, end - begin) + "(...)");
  }
#endif
};

}
}

#endif //JIFFY_LOGGER_H
