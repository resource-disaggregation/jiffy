#include "kv_manager.h"

#include "block_name_parser.h"
#include "../rpc/kv_management_client.h"

namespace elasticmem {
namespace kv {

void kv_manager::load(const std::string &block_name,
                      const std::string &persistent_path_prefix,
                      const std::string &path) {
  auto t = block_name_parser::parse(block_name);
  kv_management_client client(std::get<0>(t), std::get<1>(t));
  client.load(std::get<2>(t), persistent_path_prefix, path);
}

void kv_manager::flush(const std::string &block_name,
                       const std::string &persistent_path_prefix,
                       const std::string &path) {
  auto t = block_name_parser::parse(block_name);
  kv_management_client client(std::get<0>(t), std::get<1>(t));
  client.flush(std::get<2>(t), persistent_path_prefix, path);
}

void kv_manager::clear(const std::string &block_name) {
  auto t = block_name_parser::parse(block_name);
  kv_management_client client(std::get<0>(t), std::get<1>(t));
  client.clear(std::get<2>(t));
}

std::size_t kv_manager::storage_capacity(const std::string &block_name) {
  auto t = block_name_parser::parse(block_name);
  kv_management_client client(std::get<0>(t), std::get<1>(t));
  return static_cast<std::size_t>(client.storage_capacity(std::get<2>(t)));
}

std::size_t kv_manager::storage_size(const std::string &block_name) {
  auto t = block_name_parser::parse(block_name);
  kv_management_client client(std::get<0>(t), std::get<1>(t));
  return static_cast<std::size_t>(client.storage_size(std::get<2>(t)));
}

}
}