#include <sstream>
#include <tuple>
#include <iostream>
#include "block_name_parser.h"

namespace mmux {
namespace storage {

block_id block_name_parser::parse(const std::string &name) {
  std::string host, service_port, management_port, notification_port, chain_port, block_id;
  try {
    std::istringstream in(name);
    std::getline(in, host, ':');
    std::getline(in, service_port, ':');
    std::getline(in, management_port, ':');
    std::getline(in, notification_port, ':');
    std::getline(in, chain_port, ':');
    std::getline(in, block_id, ':');
    return {host, std::stoi(service_port), std::stoi(management_port), std::stoi(notification_port),
            std::stoi(chain_port), std::stoi(block_id)};
  } catch (std::exception &e) {
    throw std::invalid_argument("Could not parse block name: " + name + "; " + e.what());
  }
}

std::string block_name_parser::make(const std::string &host,
                                    int32_t service_port,
                                    int32_t management_port,
                                    int32_t notification_port,
                                    int32_t chain_port,
                                    int32_t id) {
  return host + ":" + std::to_string(service_port) + ":" + std::to_string(management_port) + ":"
      + std::to_string(notification_port) + ":" + std::to_string(chain_port) + ":" + std::to_string(id);
}

}
}
