#include "storage_manager.h"

#include "detail/block_name_parser.h"
#include "storage_management_client.h"
#include "../../utils/logger.h"

namespace jiffy {
namespace storage {

using namespace utils;

void storage_manager::setup_block(const std::string &block_name,
                                  const std::string &path,
                                  int32_t slot_begin,
                                  int32_t slot_end,
                                  const std::vector<std::string> &chain,
                                  bool auto_scale,
                                  int32_t role,
                                  const std::string &next_block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::trace) << "setup block on " << bid.host << ":" << bid.management_port;
  client.setup_block(bid.id, path, slot_begin, slot_end, chain, auto_scale, role, next_block_name);
}

void storage_manager::set_exporting(const std::string &block_name,
                                    const std::vector<std::string> &target_block,
                                    int32_t slot_begin,
                                    int32_t slot_end) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::trace) << "set exporting on " << bid.host << ":" << bid.management_port;
  client.set_exporting(bid.id, target_block, slot_begin, slot_end);
}

void storage_manager::set_importing(const std::string &block_name, int32_t slot_begin, int32_t slot_end) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::trace) << "set importing on " << bid.host << ":" << bid.management_port;
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
  LOG(log_level::trace) << "setup and set exporting on " << bid.host << ":" << bid.management_port;
  client.setup_and_set_importing(bid.id, path, slot_begin, slot_end, chain, role, next_block_name);
}

void storage_manager::export_slots(const std::string &block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::trace) << "export slots on " << bid.host << ":" << bid.management_port;
  client.export_slots(bid.id);
}

void storage_manager::set_regular(const std::string &block_name, int32_t slot_begin, int32_t slot_end) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::trace) << "set regular on " << bid.host << ":" << bid.management_port;
  client.set_regular(bid.id, slot_begin, slot_end);
}

std::string storage_manager::path(const std::string &block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::trace) << "path on " << bid.host << ":" << bid.management_port;
  return client.path(bid.id);
}

std::pair<int32_t, int32_t> storage_manager::slot_range(const std::string &block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::trace) << "slot range on " << bid.host << ":" << bid.management_port;
  return client.slot_range(bid.id);
}

void storage_manager::load(const std::string &block_name, const std::string &backing_path) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::trace) << "load on " << bid.host << ":" << bid.management_port;
  client.load(bid.id, backing_path);
}

void storage_manager::sync(const std::string &block_name, const std::string &backing_path) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::trace) << "sync on " << bid.host << ":" << bid.management_port;
  client.sync(bid.id, backing_path);
}

void storage_manager::dump(const std::string &block_name, const std::string &backing_path) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::trace) << "dump on " << bid.host << ":" << bid.management_port;
  client.dump(bid.id, backing_path);
}

void storage_manager::reset(const std::string &block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::trace) << "reset on " << bid.host << ":" << bid.management_port;
  client.reset(bid.id);
}

std::size_t storage_manager::storage_capacity(const std::string &block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::trace) << "storage capacity on " << bid.host << ":" << bid.management_port;
  return static_cast<std::size_t>(client.storage_capacity(bid.id));
}

std::size_t storage_manager::storage_size(const std::string &block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::trace) << "storage size on " << bid.host << ":" << bid.management_port;
  return static_cast<std::size_t>(client.storage_size(bid.id));
}

void storage_manager::resend_pending(const std::string &block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::trace) << "resend pending on " << bid.host << ":" << bid.management_port;
  client.resend_pending(bid.id);
}

void storage_manager::forward_all(const std::string &block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::trace) << "forward all on " << bid.host << ":" << bid.management_port;
  client.forward_all(bid.id);
}

}
}
