#ifndef JIFFY_TEST_UTILS_H
#define JIFFY_TEST_UTILS_H

#include <string>
#include <vector>
#include <iostream>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>
#include "jiffy/storage/storage_management_ops.h"
#include "jiffy/directory/block/block_allocator.h"
#include "jiffy/storage/block.h"
#include "jiffy/storage/hashtable/hash_table_partition.h"
#include "jiffy/storage/notification/subscription_map.h"
#include "jiffy/utils/logger.h"
#include "jiffy/directory/directory_ops.h"

class dummy_storage_manager : public jiffy::storage::storage_management_ops {
 public:
  dummy_storage_manager() = default;
  virtual ~dummy_storage_manager() = default;

  void create_partition(const std::string &block_id,
                        const std::string &type,
                        const std::string &name,
                        const std::string &metadata,
                        const std::map<std::string, std::string> &conf) override {
    std::string conf_str;
    for (auto it = conf.begin(); it != conf.end(); ++it) {
      conf_str += it->first + "_" + it->second;
    }
    COMMANDS.push_back("create_partition:" + block_id + ":" + type + ":" + name + ":" + metadata);
  }

  void setup_chain(const std::string &block_id, const std::string &path, const std::vector<std::string> &chain,
                   int32_t role, const std::string &next_block_id) override {
    std::string chain_str;
    for (const auto &block: chain) {
      chain_str += ":" + block;
    }
    COMMANDS.push_back("setup_chain:" + block_id + ":" + path + ":" + std::to_string(role) + ":" + next_block_id);
  }

  void destroy_partition(const std::string &block_name) override {
    COMMANDS.push_back("destroy_partition:" + block_name);
  }

  std::string path(const std::string &block_id) override {
    COMMANDS.push_back("path:" + block_id);
    return "";
  }

  void load(const std::string &block_id, const std::string &backing_path) override {
    COMMANDS.push_back("load:" + block_id + ":" + backing_path);
  }

  void sync(const std::string &block_id, const std::string &backing_path) override {
    COMMANDS.push_back("sync:" + block_id + ":" + backing_path);
  }

  void dump(const std::string &block_id, const std::string &backing_path) override {
    COMMANDS.push_back("dump:" + block_id + ":" + backing_path);
  }

  std::size_t storage_capacity(const std::string &block_id) override {
    COMMANDS.push_back("storage_capacity:" + block_id);
    return 0;
  }

  std::size_t storage_size(const std::string &block_id) override {
    COMMANDS.push_back("storage_size:" + block_id);
    return 0;
  }

  void resend_pending(const std::string &block_id) override {
    COMMANDS.push_back("resend_pending:" + block_id);
  }

  void forward_all(const std::string &block_id) override {
    COMMANDS.push_back("forward_all:" + block_id);
  }
  void update_partition(const std::string &block_id,
                        const std::string &partition_name,
                        const std::string &partition_metadata) override {
    COMMANDS.push_back("update_partition:" + block_id + ":" + partition_name + ":" + partition_metadata);
  }

  std::vector<std::string> COMMANDS{};
};
using namespace jiffy::utils;
class sequential_block_allocator : public jiffy::directory::block_allocator {
 public:
  sequential_block_allocator() {}

  virtual ~sequential_block_allocator() = default;

  std::vector<std::string> allocate(std::size_t count, const std::vector<std::string> &, const std::string &/*tenant_id*/) override {
    std::vector<std::string> allocated;
    for (std::size_t i = 0; i < count; i++) {
      allocated.push_back(free_.front());
      alloc_.push_back(free_.front());
      free_.erase(free_.begin());
    }
    return allocated;
  }
  void free(const std::vector<std::string> &block_names, const std::string &/*tenant_id*/) override {
    free_.insert(free_.end(), block_names.begin(), block_names.end());
    for (const auto &block_name: block_names) {
      alloc_.erase(std::remove(alloc_.begin(), alloc_.end(), block_name), alloc_.end());
    }
  }
  void add_blocks(const std::vector<std::string> &block_names) override {
    free_.insert(free_.end(), block_names.begin(), block_names.end());
  }
  void remove_blocks(const std::vector<std::string> &block_names) override {
    for (const auto &block_name: block_names) {
      free_.erase(std::remove(free_.begin(), free_.end(), block_name), free_.end());
    }
  }
  std::size_t num_free_blocks() override {
    return free_.size();
  }
  std::size_t num_allocated_blocks() override {
    return alloc_.size();
  }
  std::size_t num_total_blocks() override {
    return alloc_.size() + free_.size();
  }

 private:
  std::vector<std::string> free_;
  std::vector<std::string> alloc_;
};

class dummy_block_allocator : public jiffy::directory::block_allocator {
 public:
  explicit dummy_block_allocator(std::size_t num_blocks) : num_free_(num_blocks) {}

  virtual ~dummy_block_allocator() = default;

  std::vector<std::string> allocate(std::size_t count, const std::vector<std::string> &, const std::string &/*tenant_id*/) override {
    if (num_free_ == 0) {
      throw std::out_of_range("Cannot allocate since nothing is free");
    }
    std::vector<std::string> ret;
    for (std::size_t i = 0; i < count; ++i) {
      ret.push_back(std::to_string(num_alloc_ + i));
    }
    num_alloc_ += count;
    num_free_ -= count;
    return ret;
  }

  void free(const std::vector<std::string> &blocks, const std::string &/*tenant_id*/) override {
    if (num_alloc_ == 0 && !blocks.empty()) {
      throw std::out_of_range("Cannot free since nothing is allocated");
    }
    num_free_ += blocks.size();
    num_alloc_ -= blocks.size();
  }

  void add_blocks(const std::vector<std::string> &blocks) override {
    num_free_ += blocks.size();
  }

  void remove_blocks(const std::vector<std::string> &blocks) override {
    if (num_free_ < blocks.size()) {
      throw std::out_of_range("Cannot remove: not enough blocks");
    }
    num_free_ -= blocks.size();
  }

  std::size_t num_free_blocks() override {
    return num_free_;
  }

  std::size_t num_allocated_blocks() override {
    return num_alloc_;
  }

  std::size_t num_total_blocks() override {
    return num_alloc_ + num_free_;
  }

 private:
  std::vector<std::string> empty_;
  std::size_t num_alloc_{};
  std::size_t num_free_{};
};

class test_utils {
 public:
  static void wait_till_server_ready(const std::string &host, int port) {
    bool check = true;
    while (check) {
      using namespace apache::thrift::transport;
      try {
        auto transport = std::shared_ptr<TTransport>(new TFramedTransport(std::make_shared<TSocket>(host, port)));
        transport->open();
        transport->close();
        check = false;
      } catch (TTransportException &e) {
        usleep(100000);
      }
    }
    using namespace jiffy::utils;
    LOG(log_level::info) << "Server @ " << host << ":" << port << " is live";
  }

  static std::vector<std::string> init_block_names(size_t num_blocks, int32_t service_port, int32_t management_port) {
    std::vector<std::string> block_names;
    for (size_t i = 0; i < num_blocks; ++i) {
      std::string block_name = jiffy::storage::block_id_parser::make("127.0.0.1",
                                                                     service_port,
                                                                     management_port,
                                                                     static_cast<int32_t>(i));
      block_names.push_back(block_name);
    }
    return block_names;
  }

  static std::vector<std::shared_ptr<jiffy::storage::block>> init_hash_table_blocks(size_t num_blocks,
                                                                                    int32_t service_port,
                                                                                    int32_t management_port) {
    using namespace jiffy::storage;
    std::vector<std::shared_ptr<block>> blks;
    blks.resize(num_blocks);
    for (size_t i = 0; i < num_blocks; ++i) {
      std::string id = block_id_parser::make("127.0.0.1", service_port, management_port,
                                             static_cast<int32_t>(i));
      blks[i] = std::make_shared<block>(id);
      blks[i]->setup("hashtable", "0_65536", "regular", {});
      std::dynamic_pointer_cast<hash_table_partition>(blks[i]->impl())->slot_range(0, 65536);
    }
    return blks;
  }

  static std::vector<std::shared_ptr<jiffy::storage::block>> init_hash_table_blocks(const std::vector<std::string> &block_ids,
                                                                                    size_t block_capacity = 134217728,
                                                                                    double threshold_lo = 0.05,
                                                                                    double threshold_hi = 0.95,
                                                                                    const std::string &dir_host = "127.0.0.1",
                                                                                    int dir_port = 9090) {
    jiffy::utils::property_map conf;
    conf.set("hashtable.capacity_threshold_lo", std::to_string(threshold_lo));
    conf.set("hashtable.capacity_threshold_hi", std::to_string(threshold_hi));
    conf.set("directory.host", dir_host);
    conf.set("directory.port", std::to_string(dir_port));
    std::vector<std::shared_ptr<jiffy::storage::block>> blks;
    blks.resize(block_ids.size());
    for (size_t i = 0; i < block_ids.size(); ++i) {
      blks[i] = std::make_shared<jiffy::storage::block>(block_ids[i], block_capacity);
      blks[i]->setup("hashtable", "0_65536", "regular", conf);
    }
    return blks;
  }

  static std::vector<std::shared_ptr<jiffy::storage::block>> init_file_blocks(size_t num_blocks,
                                                                              int32_t service_port,
                                                                              int32_t management_port) {
    using namespace jiffy::storage;
    std::vector<std::shared_ptr<block>> blks;
    blks.resize(num_blocks);
    for (size_t i = 0; i < num_blocks; ++i) {
      std::string id = block_id_parser::make("127.0.0.1", service_port, management_port,
                                             static_cast<int32_t>(i));
      blks[i] = std::make_shared<block>(id);
      blks[i]->setup("file", "", "regular", {});
    }
    return blks;
  }

  static std::vector<std::shared_ptr<jiffy::storage::block>> init_file_blocks(const std::vector<std::string> &block_ids,
                                                                              size_t block_capacity = 134217728,
                                                                              double threshold_lo = 0.05,
                                                                              double threshold_hi = 0.95,
                                                                              const std::string &dir_host = "127.0.0.1",
                                                                              int dir_port = 9090) {
    jiffy::utils::property_map conf;
    conf.set("file.capacity_threshold_lo", std::to_string(threshold_lo));
    conf.set("file.capacity_threshold_hi", std::to_string(threshold_hi));
    conf.set("directory.host", dir_host);
    conf.set("directory.port", std::to_string(dir_port));
    std::vector<std::shared_ptr<jiffy::storage::block>> blks;
    blks.resize(block_ids.size());
    for (size_t i = 0; i < block_ids.size(); ++i) {
      blks[i] = std::make_shared<jiffy::storage::block>(block_ids[i], block_capacity);
      blks[i]->setup("file", "", "regular", conf);
    }
    return blks;
  }

  static std::vector<std::shared_ptr<jiffy::storage::block>> init_fifo_queue_blocks(size_t num_blocks,
                                                                                    int32_t service_port,
                                                                                    int32_t management_port) {
    using namespace jiffy::storage;
    std::vector<std::shared_ptr<block>> blks;
    blks.resize(num_blocks);
    for (size_t i = 0; i < num_blocks; ++i) {
      std::string id = block_id_parser::make("127.0.0.1", service_port, management_port,
                                             static_cast<int32_t>(i));
      blks[i] = std::make_shared<block>(id);
      blks[i]->setup("fifoqueue", "", "regular", {});
    }
    return blks;
  }

  static std::vector<std::shared_ptr<jiffy::storage::block>> init_fifo_queue_blocks(const std::vector<std::string> &block_ids,
                                                                                    size_t block_capacity = 134217728,
                                                                                    double threshold_lo = 0.05,
                                                                                    double threshold_hi = 0.95,
                                                                                    const std::string &dir_host = "127.0.0.1",
                                                                                    int dir_port = 9090) {
    jiffy::utils::property_map conf;
    conf.set("fifoqueue.capacity_threshold_lo", std::to_string(threshold_lo));
    conf.set("fifoqueue.capacity_threshold_hi", std::to_string(threshold_hi));
    conf.set("directory.host", dir_host);
    conf.set("directory.port", std::to_string(dir_port));
    std::vector<std::shared_ptr<jiffy::storage::block>> blks;
    blks.resize(block_ids.size());
    for (size_t i = 0; i < block_ids.size(); ++i) {
      blks[i] = std::make_shared<jiffy::storage::block>(block_ids[i], block_capacity);
      blks[i]->setup("fifoqueue", "", "regular", conf);
    }
    return blks;
  }
};

#endif //JIFFY_TEST_UTILS_H
