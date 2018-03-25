#include <sstream>
#include <tuple>
#include <iostream>
#include "block_name_parser.h"

namespace elasticmem {
namespace storage {

block_id block_name_parser::parse(const std::string &name) {
  std::string host, service_port, management_port, notification_port, block_id;
  std::istringstream in(name);
  std::getline(in, host, ':');
  std::getline(in, service_port, ':');
  std::getline(in, management_port, ':');
  std::getline(in, notification_port, ':');
  std::getline(in, block_id, ':');
  return {host, std::stoi(service_port), std::stoi(management_port), std::stoi(notification_port), std::stoi(block_id)};
}

std::string block_name_parser::make(const std::string &host,
                                    int32_t service_port,
                                    int32_t management_port,
                                    int32_t notification_port,
                                    int32_t id) {
  return host + ":" + std::to_string(service_port) + ":" + std::to_string(management_port) + ":"
      + std::to_string(notification_port) + ":" + std::to_string(id);
}

}
}
