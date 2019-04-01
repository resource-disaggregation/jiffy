#include "auto_scaling_service_handler.h"
#include "jiffy/utils/logger.h"

namespace jiffy {
namespace auto_scaling {

using namespace jiffy::utils;

auto_scaling_service_handler::auto_scaling_service_handler(const std::string directory_host, int directory_port) : directory_host_(directory_host), directory_port_(directory_port) {}

void auto_scaling_service_handler::auto_scaling(std::string& _return, const std::vector<std::string> & current_replica_chain, const std::string& path, const std::map<std::string, std::string> & conf) {

}

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
