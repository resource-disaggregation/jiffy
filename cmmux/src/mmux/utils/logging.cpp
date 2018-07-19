#include "logging.h"
#include "mmux/utils/logger.h"
#include <thrift/TOutput.h>

int configure_logging(int log_level) {
  try {
    mmux::utils::log_utils::configure_log_level(static_cast<mmux::utils::log_level>(log_level));
    apache::thrift::GlobalOutput.setOutputFunction(mmux::utils::log_utils::log_thrift_msg);
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}
