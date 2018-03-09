#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include "directory_client.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

namespace elasticmem {
namespace directory {

directory_client::directory_client(const std::string &host, int port) {
  connect(host, port);
}

directory_client::~directory_client() {
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

void directory_client::create_file(const std::string &path) {
  client_->create_file(path);
}

bool directory_client::exists(const std::string &path) const {
  return client_->exists(path);
}

std::size_t directory_client::file_size(const std::string &path) const {
  return static_cast<size_t>(client_->file_size(path));
}

std::time_t directory_client::last_write_time(const std::string &path) const {
  return client_->last_write_time(path);
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

void directory_client::rename(const std::string &old_path, const std::string &new_path) {
  client_->rename(old_path, new_path);
}

file_status directory_client::status(const std::string &path) const {
  rpc_file_status s;
  client_->status(s, path);
  return file_status(static_cast<file_type>(s.type), perms(static_cast<uint16_t>(s.permissions)), s.last_write_time);;
}

std::vector<directory_entry> directory_client::directory_entries(const std::string &path) {
  std::vector<rpc_dir_entry> entries;
  client_->directory_entries(entries, path);
  std::vector<directory_entry> out;
  for (const auto& e: entries) {
    out.emplace_back(e.name,
                     file_status(static_cast<file_type>(e.status.type),
                                 perms(static_cast<uint16_t>(e.status.permissions)),
                                 e.status.last_write_time));
  }
  return out;
}

std::vector<directory_entry> directory_client::recursive_directory_entries(const std::string &path) {
  std::vector<rpc_dir_entry> entries;
  client_->recursive_directory_entries(entries, path);
  std::vector<directory_entry> out;
  for (const auto& e: entries) {
    out.emplace_back(e.name,
                     file_status(static_cast<file_type>(e.status.type),
                                 perms(static_cast<uint16_t>(e.status.permissions)),
                                 e.status.last_write_time));
  }
  return out;
}

data_status directory_client::dstatus(const std::string &path) {
  rpc_data_status s;
  client_->dstatus(s, path);
  return data_status(static_cast<storage_mode>(s.storage_mode), s.data_blocks);
}

storage_mode directory_client::mode(const std::string &path) {
  return static_cast<storage_mode>(client_->mode(path));
}

std::vector<std::string> directory_client::data_blocks(const std::string &path) {
  std::vector<std::string> out;
  client_->data_blocks(out, path);
  return out;
}

bool directory_client::is_regular_file(const std::string &path) {
  return client_->is_regular_file(path);
}

bool directory_client::is_directory(const std::string &path) {
  return client_->is_directory(path);
}

}
}