#include "auto_scaling_service_handler.h"
#include "jiffy/utils/logger.h"

namespace jiffy {
namespace auto_scaling {

using namespace jiffy::utils;

auto_scaling_service_handler::auto_scaling_service_handler(std::vector<std::shared_ptr<block>> &blocks)
    : blocks_(blocks) {}

auto_scaling_exception auto_scaling_service_handler::make_exception(std::exception &e) {
  auto_scaling_exception ex;
  ex.msg = e.what();
  return ex;
}

auto_scaling_exception auto_scaling_service_handler::make_exception(const std::string &msg) {
  auto_scaling_exception ex;
  ex.msg = msg;
  return ex;
}

}
}
