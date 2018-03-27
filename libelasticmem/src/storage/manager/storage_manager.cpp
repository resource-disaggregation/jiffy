#include "storage_manager.h"

#include "detail/block_name_parser.h"
#include "storage_management_client.h"

namespace elasticmem {
namespace storage {

void storage_manager::setup_block(const std::string &block_name,
                                  const std::string &path,
                                  int32_t role,
                                  const std::string &next_block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  client.setup_block(bid.id, path, role, next_block_name);
}

std::string storage_manager::path(const std::string &block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  return client.path(bid.id);
}

void storage_manager::load(const std::string &block_name,
                           const std::string &persistent_path_prefix,
                           const std::string &path) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  client.load(bid.id, persistent_path_prefix, path);
}

void storage_manager::flush(const std::string &block_name,
                            const std::string &persistent_path_prefix,
                            const std::string &path) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  client.flush(bid.id, persistent_path_prefix, path);
}

void storage_manager::reset(const std::string &block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  client.reset(bid.id);
}

std::size_t storage_manager::storage_capacity(const std::string &block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  return static_cast<std::size_t>(client.storage_capacity(bid.id));
}

std::size_t storage_manager::storage_size(const std::string &block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  return static_cast<std::size_t>(client.storage_size(bid.id));
}

void storage_manager::resend_pending(const std::string &block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  client.resend_pending(bid.id);
}

}
}