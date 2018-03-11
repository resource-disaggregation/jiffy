#include <sstream>
#include <tuple>
#include "block_name_parser.h"

namespace elasticmem {
namespace kv {

std::tuple<std::string, int, int> block_name_parser::parse(const std::string &name) {
  std::string host, port, block_id;
  std::istringstream in(name);
  std::getline(in, host, ':');
  std::getline(in, port, ':');
  std::getline(in, block_id, ':');
  return std::make_tuple(host, std::stoi(port), std::stoi(block_id));
}

std::string block_name_parser::make_block_name(const std::tuple<std::string, int, int> &t) {
  return std::get<0>(t) + ":" + std::to_string(std::get<1>(t)) + ":" + std::to_string(std::get<2>(t));
}

}
}
