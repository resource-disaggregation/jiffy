#include "storage_manager.h"

#include "jiffy/storage/manager/detail/block_id_parser.h"
#include "storage_management_client.h"
#include "../../utils/logger.h"

namespace jiffy {
namespace storage {

using namespace utils;

void storage_manager::create_partition(const std::string &block_id,
                                       const std::string &type,
                                       const std::string &name,
                                       const std::string &metadata,
                                       const std::map<std::string, std::string> &conf) {
  auto bid = block_id_parser::parse(block_id);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::info) << "Creating partition name : " << name << " on " << bid.host << ":" << bid.management_port;
  client.create_partition(bid.id, type, name, metadata, conf, bid.block_seq_no);
}

void storage_manager::setup_chain(const std::string &block_id,
                                  const std::string &path,
                                  const std::vector<std::string> &chain,
                                  int32_t role,
                                  const std::string &next_block_id) {
  auto bid = block_id_parser::parse(block_id);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::info) << "setup partition on " << bid.host << ":" << bid.management_port;
  client.setup_chain(bid.id, path, chain, role, next_block_id, bid.block_seq_no);
}

void storage_manager::destroy_partition(const std::string &block_name) {
  auto bid = block_id_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::info) << "reset on " << bid.host << ":" << bid.management_port;
  client.destroy_partition(bid.id, bid.block_seq_no);
}

std::string storage_manager::path(const std::string &block_name) {
  auto bid = block_id_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::info) << "path on " << bid.host << ":" << bid.management_port;
  return client.path(bid.id, bid.block_seq_no);
}

void storage_manager::load(const std::string &block_name, const std::string &backing_path) {
  auto bid = block_id_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::info) << "load on " << bid.host << ":" << bid.management_port;
  client.load(bid.id, backing_path, bid.block_seq_no);
}

void storage_manager::sync(const std::string &block_name, const std::string &backing_path) {
  auto bid = block_id_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::info) << "sync on " << bid.host << ":" << bid.management_port;
  client.sync(bid.id, backing_path, bid.block_seq_no);
}

void storage_manager::dump(const std::string &block_name, const std::string &backing_path) {
  auto bid = block_id_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::info) << "dump on " << bid.host << ":" << bid.management_port;
  client.dump(bid.id, backing_path, bid.block_seq_no);
}

std::size_t storage_manager::storage_capacity(const std::string &block_name) {
  auto bid = block_id_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  //LOG(log_level::info) << "storage capacity on " << bid.host << ":" << bid.management_port;
  return static_cast<std::size_t>(client.storage_capacity(bid.id, bid.block_seq_no));
}

std::size_t storage_manager::storage_size(const std::string &block_name) {
  auto bid = block_id_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::info) << "storage size on " << bid.host << ":" << bid.management_port;
  return static_cast<std::size_t>(client.storage_size(bid.id, bid.block_seq_no));
}

void storage_manager::resend_pending(const std::string &block_name) {
  auto bid = block_id_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::info) << "resend pending on " << bid.host << ":" << bid.management_port;
  client.resend_pending(bid.id, bid.block_seq_no);
}

void storage_manager::forward_all(const std::string &block_name) {
  auto bid = block_id_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  LOG(log_level::info) << "forward all on " << bid.host << ":" << bid.management_port;
  client.forward_all(bid.id, bid.block_seq_no);
}

void storage_manager::update_partition(const std::string &block_name,
                                       const std::string &partition_name,
                                       const std::string &partition_metadata) {
  auto bid = block_id_parser::parse(block_name);
  storage_management_client client(bid.host, bid.management_port);
  client.update_partition(bid.id, partition_name, partition_metadata, bid.block_seq_no);
}

}
}
