#include "jiffy_client.h"

namespace jiffy {
namespace client {

using namespace directory;

jiffy_client::jiffy_client(const std::string &host, int dir_port, int lease_port)
    : fs_(std::make_shared<directory_client>(host, dir_port)),
      lease_worker_(host, lease_port) {
  lease_worker_.start();
}

std::shared_ptr<directory::directory_client> jiffy_client::fs() {
  return fs_;
}

directory::lease_renewal_worker &jiffy_client::lease_worker() {
  return lease_worker_;
}

void jiffy_client::begin_scope(const std::string &path) {
  lease_worker_.add_path(path);
}

void jiffy_client::end_scope(const std::string &path) {
  lease_worker_.remove_path(path);
}

std::shared_ptr<storage::hash_table_client> jiffy_client::create_hash_table(const std::string &path,
                                                                            const std::string &backing_path,
                                                                            int32_t num_blocks,
                                                                            int32_t chain_length,
                                                                            int32_t flags,
                                                                            int32_t permissions,
                                                                            const std::map<std::string,
                                                                                           std::string> &tags) {
  std::vector<std::string> block_names;
  std::vector<std::string> block_metadata;
  int32_t slot_range = storage::hash_table_partition::SLOT_MAX / num_blocks;
  for (int32_t i = 0; i < num_blocks; ++i) {
    int32_t begin = i * slot_range;
    int32_t end = (i == num_blocks - 1) ? storage::hash_table_partition::SLOT_MAX : (i + 1) * slot_range;
    block_names.push_back(std::to_string(begin) + "_" + std::to_string(end));
    block_metadata.push_back("regular");
  }
  auto s = fs_->create(path, "hashtable", backing_path, num_blocks, chain_length, flags, permissions, block_names,
                       block_metadata, tags);
  begin_scope(path);
  return std::make_shared<storage::hash_table_client>(fs_, path, s);
}

std::shared_ptr<storage::hash_table_client> jiffy_client::open(const std::string &path) {
  auto s = fs_->open(path);
  begin_scope(path);
  return std::make_shared<storage::hash_table_client>(fs_, path, s);
}

std::shared_ptr<storage::hash_table_client> jiffy_client::open_or_create_hash_table(const std::string &path,
                                                                                    const std::string &backing_path,
                                                                                    int32_t num_blocks,
                                                                                    int32_t chain_length,
                                                                                    int32_t flags,
                                                                                    int32_t permissions,
                                                                                    const std::map<std::string,
                                                                                                   std::string> &tags) {
  std::vector<std::string> block_names;
  std::vector<std::string> block_metadata;
  int32_t slot_range = storage::hash_table_partition::SLOT_MAX / num_blocks;
  for (int32_t i = 0; i < num_blocks; ++i) {
    int32_t begin = i * slot_range;
    int32_t end = (i == num_blocks - 1) ? storage::hash_table_partition::SLOT_MAX : (i + 1) * slot_range;
    block_names.push_back(std::to_string(begin) + "_" + std::to_string(end));
    block_metadata.push_back("regular");
  }
  auto s = fs_->open_or_create(path, "hashtable", backing_path, num_blocks, chain_length, flags, permissions,
                               block_names, block_metadata, tags);
  begin_scope(path);
  return std::make_shared<storage::hash_table_client>(fs_, path, s);
}

std::shared_ptr<storage::hash_table_listener> jiffy_client::listen(const std::string &path) {
  auto s = fs_->open(path);
  begin_scope(path);
  return std::make_shared<storage::hash_table_listener>(path, s);
}

void jiffy_client::remove(const std::string &path) {
  end_scope(path);
  fs_->remove(path);
}

void jiffy_client::sync(const std::string &path, const std::string &dest) {
  fs_->sync(path, dest);
}

void jiffy_client::dump(const std::string &path, const std::string &dest) {
  fs_->dump(path, dest);
}

void jiffy_client::load(const std::string &path, const std::string &dest) {
  fs_->load(path, dest);
}

void jiffy_client::close(const std::string &path) {
  end_scope(path);
}

}
}
