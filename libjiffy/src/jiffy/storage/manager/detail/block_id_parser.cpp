#include <sstream>
#include <tuple>
#include <iostream>
#include "block_id_parser.h"

namespace jiffy {
namespace storage {

block_id block_id_parser::parse(const std::string &name) {
  std::string host, service_port, management_port, block_id, block_seq_no;
  try {
    std::istringstream in(name);
    std::getline(in, host, ':');
    std::getline(in, service_port, ':');
    std::getline(in, management_port, ':');
    std::getline(in, block_id, ':');
    std::getline(in, block_seq_no, ':');
    return {host, std::stoi(service_port), std::stoi(management_port), std::stoi(block_id), (block_seq_no != "")?(std::stoi(block_seq_no)):(0)};
  } catch (std::exception &e) {
    throw std::invalid_argument("Could not parse partition name: " + name + "; " + e.what());
  }
}

std::string block_id_parser::make(const std::string &host, int32_t service_port, int32_t management_port, int32_t id) {
  return host + ":" + std::to_string(service_port) + ":" + std::to_string(management_port) + ":" + std::to_string(id);
}

}
}
