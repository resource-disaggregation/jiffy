#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include "directory_client.h"
#include "../fs/directory_type_conversions.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

namespace mmux {
namespace directory {

directory_client::directory_client(const std::string &host, int port) {
  connect(host, port);
}

directory_client::~directory_client() {
  if (transport_ != nullptr)
    disconnect();
}

void directory_client::connect(const std::string &host, int port) {
  socket_ = std::make_shared<TSocket>(host, port);
  transport_ = std::shared_ptr<TTransport>(new TBufferedTransport(socket_));
  protocol_ = std::shared_ptr<TProtocol>(new TBinaryProtocol(transport_));
  client_ = std::make_shared<thrift_client>(protocol_);
  transport_->open();
}

void directory_client::disconnect() {
  if (transport_->isOpen()) {
    transport_->close();
  }
}

void directory_client::create_directory(const std::string &path) {
  client_->create_directory(path);
}

void directory_client::create_directories(const std::string &path) {
  client_->create_directories(path);
}

data_status directory_client::open(const std::string &path) {
  rpc_data_status s;
  client_->open(s, path);
  return directory_type_conversions::from_rpc(s);
}

data_status directory_client::create(const std::string &path,
                                     const std::string &backing_path,
                                     std::size_t num_blocks,
                                     std::size_t chain_length,
                                     std::int32_t flags) {
  rpc_data_status s;
  client_->create(s,
                  path,
                  backing_path,
                  static_cast<const int32_t>(num_blocks),
                  static_cast<const int32_t>(chain_length),
                  flags);
  return directory_type_conversions::from_rpc(s);
}

data_status directory_client::open_or_create(const std::string &path,
                                             const std::string &backing_path,
                                             std::size_t num_blocks,
                                             std::size_t chain_length,
                                             std::int32_t flags) {
  rpc_data_status s;
  client_->open_or_create(s,
                          path,
                          backing_path,
                          static_cast<const int32_t>(num_blocks),
                          static_cast<const int32_t>(chain_length),
                          flags);
  return directory_type_conversions::from_rpc(s);
}

bool directory_client::exists(const std::string &path) const {
  return client_->exists(path);
}

std::uint64_t directory_client::last_write_time(const std::string &path) const {
  return static_cast<uint64_t>(client_->last_write_time(path));
}

perms directory_client::permissions(const std::string &path) {
  return perms(static_cast<uint16_t>(client_->get_permissions(path)));
}

void directory_client::permissions(const std::string &path, const perms &prms, perm_options opts) {
  client_->set_permissions(path, prms(), static_cast<const rpc_perm_options>(opts));
}

void directory_client::remove(const std::string &path) {
  client_->remove(path);
}

void directory_client::remove_all(const std::string &path) {
  client_->remove_all(path);
}

void directory_client::sync(const std::string &path, const std::string &backing_path) {
  client_->sync(path, backing_path);
}

void directory_client::dump(const std::string &path, const std::string &backing_path) {
  client_->dump(path, backing_path);
}

void directory_client::load(const std::string &path, const std::string &backing_path) {
  client_->load(path, backing_path);
}

void directory_client::rename(const std::string &old_path, const std::string &new_path) {
  client_->rename(old_path, new_path);
}

file_status directory_client::status(const std::string &path) const {
  rpc_file_status s;
  client_->status(s, path);
  return directory_type_conversions::from_rpc(s);
}

std::vector<directory_entry> directory_client::directory_entries(const std::string &path) {
  std::vector<rpc_dir_entry> entries;
  client_->directory_entries(entries, path);
  std::vector<directory_entry> out;
  for (const auto &e: entries) {
    out.push_back(directory_type_conversions::from_rpc(e));
  }
  return out;
}

std::vector<directory_entry> directory_client::recursive_directory_entries(const std::string &path) {
  std::vector<rpc_dir_entry> entries;
  client_->recursive_directory_entries(entries, path);
  std::vector<directory_entry> out;
  for (const auto &e: entries) {
    out.push_back(directory_type_conversions::from_rpc(e));
  }
  return out;
}

data_status directory_client::dstatus(const std::string &path) {
  rpc_data_status s;
  client_->dstatus(s, path);
  return directory_type_conversions::from_rpc(s);
}

bool directory_client::is_regular_file(const std::string &path) {
  return client_->is_regular_file(path);
}

bool directory_client::is_directory(const std::string &path) {
  return client_->is_directory(path);
}

replica_chain directory_client::resolve_failures(const std::string &path, const replica_chain &chain) {
  rpc_replica_chain in, out;
  in = directory_type_conversions::to_rpc(chain);
  client_->reslove_failures(out, path, in);
  return directory_type_conversions::from_rpc(out);
}

replica_chain directory_client::add_replica_to_chain(const std::string &path, const replica_chain &chain) {
  rpc_replica_chain in, out;
  in = directory_type_conversions::to_rpc(chain);
  client_->add_replica_to_chain(out, path, in);
  return directory_type_conversions::from_rpc(out);
}

void directory_client::add_block_to_file(const std::string &path) {
  client_->add_block_to_file(path);
}

void directory_client::split_slot_range(const std::string &path, int32_t slot_begin, int32_t slot_end) {
  client_->split_slot_range(path, slot_begin, slot_end);
}

void directory_client::merge_slot_range(const std::string &path, int32_t slot_begin, int32_t slot_end) {
  client_->merge_slot_range(path, slot_begin, slot_end);
}

void directory_client::touch(const std::string &) {
  throw directory_ops_exception("Unsupported operation");
}

void directory_client::handle_lease_expiry(const std::string &) {
  throw directory_ops_exception("Unsupported operation");
}

}
}