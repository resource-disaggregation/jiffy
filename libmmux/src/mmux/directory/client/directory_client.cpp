#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include "directory_client.h"
#include "../fs/directory_type_conversions.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

namespace mmux {
namespace directory {

/**
 * @brief Construction
 * @param host service host
 * @param port service port number
 */

directory_client::directory_client(const std::string &host, int port) {
  connect(host, port);
}

/**
 * @brief Deconstruction
 */

directory_client::~directory_client() {
  if (transport_ != nullptr)
    disconnect();
}

/**
 * @brief Connect service
 * @param host service host
 * @param port service port
 */

void directory_client::connect(const std::string &host, int port) {
  socket_ = std::make_shared<TSocket>(host, port);
  transport_ = std::shared_ptr<TTransport>(new TBufferedTransport(socket_));
  protocol_ = std::shared_ptr<TProtocol>(new TBinaryProtocol(transport_));
  client_ = std::make_shared<thrift_client>(protocol_);
  transport_->open();
}

/**
 * @brief Disconnect service
 */
void directory_client::disconnect() {
  if (transport_->isOpen()) {
    transport_->close();
  }
}

/**
 * @brief Create directory
 * @param path directory path
 */

void directory_client::create_directory(const std::string &path) {
  client_->create_directory(path);
}

/**
 * @brief Create directories
 * @param path directory paths
 */

void directory_client::create_directories(const std::string &path) {
  client_->create_directories(path);
}

/**
 * @brief Open file
 * @param path file path
 * @return data status
 */

data_status directory_client::open(const std::string &path) {
  rpc_data_status s;
  client_->open(s, path);
  return directory_type_conversions::from_rpc(s);
}

/**
 * @brief Create a file
 * @param path file path
 * @param backing_path file backing path
 * @param num_blocks number of blocks
 * @param chain_length replication chain length
 * @param flags flag arguments
 * @param permissions permissions
 * @param tags tag arguments
 * @return data status
 */

data_status directory_client::create(const std::string &path,
                                     const std::string &backing_path,
                                     std::size_t num_blocks,
                                     std::size_t chain_length,
                                     std::int32_t flags,
                                     std::int32_t permissions,
                                     const std::map<std::string, std::string> &tags) {
  rpc_data_status s;
  client_->create(s,
                  path,
                  backing_path,
                  static_cast<const int32_t>(num_blocks),
                  static_cast<const int32_t>(chain_length),
                  flags,
                  permissions,
                  tags);
  return directory_type_conversions::from_rpc(s);
}

/**
 * @brief Open if exist, otherwise create
 * @param path file path
 * @param backing_path backing_path
 * @param num_blocks number of blocks
 * @param chain_length replication chain length
 * @param flags flag arguments
 * @param permissions permissions
 * @param tags tag arguments
 * @return data status
 */

data_status directory_client::open_or_create(const std::string &path,
                                             const std::string &backing_path,
                                             std::size_t num_blocks,
                                             std::size_t chain_length,
                                             std::int32_t flags,
                                             std::int32_t permissions,
                                             const std::map<std::string, std::string> &tags) {
  rpc_data_status s;
  client_->open_or_create(s,
                          path,
                          backing_path,
                          static_cast<const int32_t>(num_blocks),
                          static_cast<const int32_t>(chain_length),
                          flags,
                          permissions,
                          tags);
  return directory_type_conversions::from_rpc(s);
}

/**
 * @brief Check if the file exists
 * @param path file path
 * @return bool value
 */

bool directory_client::exists(const std::string &path) const {
  return client_->exists(path);
}

/**
 * @brief Fetch last write time of file
 * @param path file path
 * @return last write time
 */

std::uint64_t directory_client::last_write_time(const std::string &path) const {
  return static_cast<uint64_t>(client_->last_write_time(path));
}

/**
 * @brief Fetch permission of a file
 * @param path file path
 * @return permission
 */

perms directory_client::permissions(const std::string &path) {
  return perms(static_cast<uint16_t>(client_->get_permissions(path)));
}

/**
 * @brief Set permissions
 * @param path
 * @param prms
 * @param opts
 */

void directory_client::permissions(const std::string &path, const perms &prms, perm_options opts) {
  client_->set_permissions(path, prms(), static_cast<const rpc_perm_options>(opts));
}

/**
 * @brief Remove file
 * @param path file path
 */

void directory_client::remove(const std::string &path) {
  client_->remove(path);
}

/**
 * @brief Remove all files under given directory
 * @param path directory path
 */

void directory_client::remove_all(const std::string &path) {
  client_->remove_all(path);
}

/**
 * @brief Write all dirty blocks back to persistent storage
 * @param path file path
 * @param backing_path file backing path
 */

void directory_client::sync(const std::string &path, const std::string &backing_path) {
  client_->sync(path, backing_path);
}

/**
 * @brief Write all dirty blocks back to persistent storage and clear the block
 * @param path file path
 * @param backing_path file backing path
 */

void directory_client::dump(const std::string &path, const std::string &backing_path) {
  client_->dump(path, backing_path);
}

/**
 * @brief Load blocks from persistent storage
 * @param path
 * @param backing_path
 */

void directory_client::load(const std::string &path, const std::string &backing_path) {
  client_->load(path, backing_path);
}

/**
 * @brief Rename a file
 * @param old_path original file path
 * @param new_path new file path
 */

void directory_client::rename(const std::string &old_path, const std::string &new_path) {
  client_->rename(old_path, new_path);
}

/**
 * @brief Fetch file status
 * @param path file path
 * @return file status
 */

file_status directory_client::status(const std::string &path) const {
  rpc_file_status s;
  client_->status(s, path);
  return directory_type_conversions::from_rpc(s);
}

/**
 * @brief Collect all entries of files in the directory
 * @param path directory path
 * @return directory entries
 */

std::vector<directory_entry> directory_client::directory_entries(const std::string &path) {
  std::vector<rpc_dir_entry> entries;
  client_->directory_entries(entries, path);
  std::vector<directory_entry> out;
  for (const auto &e: entries) {
    out.push_back(directory_type_conversions::from_rpc(e));
  }
  return out;
}

/**
 * @brief Collect all entries of files in the directory recursively
 * @param path directory path
 * @return directory entries
 */

std::vector<directory_entry> directory_client::recursive_directory_entries(const std::string &path) {
  std::vector<rpc_dir_entry> entries;
  client_->recursive_directory_entries(entries, path);
  std::vector<directory_entry> out;
  for (const auto &e: entries) {
    out.push_back(directory_type_conversions::from_rpc(e));
  }
  return out;
}

/**
 * @brief Collect data status
 * @param path file path
 * @return data status
 */

data_status directory_client::dstatus(const std::string &path) {
  rpc_data_status s;
  client_->dstatus(s, path);
  return directory_type_conversions::from_rpc(s);
}

/**
 * @brief Add tags to the file status
 * @param path file path
 * @param tags tags
 */

void directory_client::add_tags(const std::string &path, const std::map<std::string, std::string> &tags) {
  client_->add_tags(path, tags);
}

/**
 * @brief Check if path is regular file
 * @param path file path
 * @return bool value
 */

bool directory_client::is_regular_file(const std::string &path) {
  return client_->is_regular_file(path);
}

/**
 * @brief Check if path is directory
 * @param path directory path
 * @return bool value
 */

bool directory_client::is_directory(const std::string &path) {
  return client_->is_directory(path);
}

/**
 * @brief Resolve failures using replication chain
 * @param path file path
 * @param chain replication chain
 * @return replication chain
 */

replica_chain directory_client::resolve_failures(const std::string &path, const replica_chain &chain) {
  rpc_replica_chain in, out;
  in = directory_type_conversions::to_rpc(chain);
  client_->reslove_failures(out, path, in);
  return directory_type_conversions::from_rpc(out);
}

/**
 * @brief Add a new replication to chain
 * @param path file path
 * @param chain replication chain
 * @return replication chain
 */

replica_chain directory_client::add_replica_to_chain(const std::string &path, const replica_chain &chain) {
  rpc_replica_chain in, out;
  in = directory_type_conversions::to_rpc(chain);
  client_->add_replica_to_chain(out, path, in);
  return directory_type_conversions::from_rpc(out);
}

/**
 * @brief Add block to file
 * @param path file path
 */

void directory_client::add_block_to_file(const std::string &path) {
  client_->add_block_to_file(path);
}

/**
 * @brief Split block hash range
 * @param path file path
 * @param slot_begin split begin range
 * @param slot_end split end range
 */

void directory_client::split_slot_range(const std::string &path, int32_t slot_begin, int32_t slot_end) {
  client_->split_slot_range(path, slot_begin, slot_end);
}

/**
 * @brief Merge slot hash range
 * @param path file path
 * @param slot_begin merge begin range
 * @param slot_end merge end range
 */

void directory_client::merge_slot_range(const std::string &path, int32_t slot_begin, int32_t slot_end) {
  client_->merge_slot_range(path, slot_begin, slot_end);
}

/**
 * @brief touch -> unsupported operation
 */
void directory_client::touch(const std::string &) {
  throw directory_ops_exception("Unsupported operation");
}

/**
 * @brief handle lease expiry -> unsupported operation
 */

void directory_client::handle_lease_expiry(const std::string &) {
  throw directory_ops_exception("Unsupported operation");
}

}
}