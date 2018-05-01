#include "storage_manager.h"

#include "detail/block_name_parser.h"
#include "storage_management_client.h"
#include "../../utils/logger.h"

namespace elasticmem {
namespace storage {

using namespace utils;

void storage_manager::setup_block(const std::string &block_name,
                                  const std::string &path,
                                  int32_t slot_begin,
                                  int32_t slot_end,
                                  const std::vector<std::string> &chain,
                                  int32_t role,
                                  const std::string &next_block_name) {
  LOG(log_level::info) << "Setting up block " << block_name << " with path=" << path << ", slot_range=(" << slot_begin
                       << ", " << slot_end << "), role=" << role << ", next_block=" << next_block_name;
  auto bid = block_name_parser::parse(block_name);
  try {
    storage_management_client client(bid.host, bid.management_port);
    client.setup_block(bid.id, path, slot_begin, slot_end, chain, role, next_block_name);
  } catch (std::exception& e) {
    LOG(log_level::error) << "Error setting up block on " << bid.host << ":" << bid.management_port;
    throw e;
  }
}

void storage_manager::set_exporting(const std::string &block_name,
                                    const std::vector<std::string> &target_block,
                                    int32_t slot_begin,
                                    int32_t slot_end) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  client.set_exporting(bid.id, target_block, slot_begin, slot_end);
}

void storage_manager::set_importing(const std::string &block_name, int32_t slot_begin, int32_t slot_end) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  client.set_importing(bid.id, slot_begin, slot_end);
}

void storage_manager::setup_and_set_importing(const std::string &block_name,
                                              const std::string &path,
                                              int32_t slot_begin,
                                              int32_t slot_end,
                                              const std::vector<std::string> &chain,
                                              int32_t role,
                                              const std::string &next_block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  client.setup_and_set_importing(bid.id, path, slot_begin, slot_end, chain, role, next_block_name);
}

void storage_manager::export_slots(const std::string &block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  client.export_slots(bid.id);
}

void storage_manager::set_regular(const std::string &block_name, int32_t slot_begin, int32_t slot_end) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  client.set_regular(bid.id, slot_begin, slot_end);
}

std::string storage_manager::path(const std::string &block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  return client.path(bid.id);
}

std::pair<int32_t, int32_t> storage_manager::slot_range(const std::string &block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  return client.slot_range(bid.id);
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

void storage_manager::forward_all(const std::string &block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  client.forward_all(bid.id);
}

}
}