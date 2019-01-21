#include "storage_manager.h"

#include "detail/block_name_parser.h"
#include "storage_management_client.h"
#include "../../utils/logger.h"

namespace jiffy {
namespace storage {

using namespace utils;

void storage_manager::setup_block(const std::string &block_name,
                                  const std::string &path,
                                  const std::string &partition_type,
                                  const std::string &partition_name,
                                  const std::string &partition_metadata,
                                  const std::vector<std::string> &chain,
                                  int32_t role,
                                  const std::string &next_block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::trace) << "setup partition on " << bid.host << ":" << bid.management_port;
  client.setup_block(bid.id, path, partition_type, partition_name, partition_metadata, chain, role, next_block_name);
}

std::string storage_manager::path(const std::string &block_name) {
  auto bid = block_name_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::trace) << "path on " << bid.host << ":" << bid.management_port;
  return client.path(bid.id);
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
