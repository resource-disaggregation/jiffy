#ifndef ELASTICMEM_TEST_UTILS_H
#define ELASTICMEM_TEST_UTILS_H

#include <string>
#include <vector>
#include <iostream>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>
#include "../src/storage/storage_management_ops.h"
#include "../src/directory/block/block_allocator.h"
#include "../src/storage/kv/kv_block.h"
#include "../src/storage/notification/subscription_map.h"

class dummy_storage_manager : public elasticmem::storage::storage_management_ops {
 public:
  dummy_storage_manager() = default;

  void setup_block(const std::string &block_name,
                   const std::string &path,
                   int32_t slot_begin,
                   int32_t slot_end,
                   const std::vector<std::string> &chain,
                   int32_t role,
                   const std::string &next_block_name) override {
    std::string chain_str;
    for (const auto &block: chain) {
      chain_str += ":" + block;
    }
    COMMANDS.push_back(
        "setup_block:" + block_name + ":" + path + ":" + std::to_string(slot_begin) + ":" + std::to_string(slot_end)
            + chain_str + ":" + std::to_string(role) + ":" + next_block_name);
  }

  std::pair<int32_t, int32_t> slot_range(const std::string &block_name) override {
    COMMANDS.push_back("slot_range:" + block_name);
    return std::pair<int32_t, int32_t>(0, elasticmem::storage::block::SLOT_MAX);
  };

  std::string path(const std::string &block_name) override {
    COMMANDS.push_back("get_path:" + block_name);
    return "";
  }

  void set_exporting(const std::string &block_name,
                     const std::vector<std::string> &target_block,
                     int32_t slot_begin,
                     int32_t slot_end) override {
    std::string block_str;
    for (const auto &block: target_block) {
      block_str += ":" + block;
    }
    COMMANDS.push_back("set_exporting:" + block_name + block_str + ":" + std::to_string(slot_begin) + ":"
                           + std::to_string(slot_end));
  }

  void set_importing(const std::string &block_name,
                     const std::string &path,
                     int32_t slot_begin,
                     int32_t slot_end,
                     const std::vector<std::string> &chain,
                     int32_t role,
                     const std::string &next_block_name) override {
    std::string chain_str;
    for (const auto &block: chain) {
      chain_str += ":" + block;
    }
    COMMANDS.push_back(
        "set_importing:" + block_name + ":" + path + ":" + std::to_string(slot_begin) + ":" + std::to_string(slot_end)
            + chain_str + ":" + std::to_string(role) + ":" + next_block_name);
  }

  void export_slots(const std::string &block_name) override {
    COMMANDS.push_back("export_slots:" + block_name);
  }

  void set_regular(const std::string &block_name, int32_t slot_begin, int32_t slot_end) override {
    COMMANDS.push_back("set_regular:" + block_name + ":" + std::to_string(slot_begin) + ":" + std::to_string(slot_end));
  }

  void load(const std::string &block_name,
            const std::string &persistent_path_prefix,
            const std::string &path) override {
    COMMANDS.push_back("load:" + block_name + ":" + persistent_path_prefix + ":" + path);
  }

  void flush(const std::string &block_name,
             const std::string &persistent_path_prefix,
             const std::string &path) override {
    COMMANDS.push_back("flush:" + block_name + ":" + persistent_path_prefix + ":" + path);
  }

  void reset(const std::string &block_name) override {
    COMMANDS.push_back("reset:" + block_name);
  }

  std::size_t storage_capacity(const std::string &block_name) override {
    COMMANDS.push_back("storage_capacity:" + block_name);
    return 0;
  }

  std::size_t storage_size(const std::string &block_name) override {
    COMMANDS.push_back("storage_size:" + block_name);
    return 0;
  }

  void resend_pending(const std::string &block_name) override {
    COMMANDS.push_back("resend_pending:" + block_name);
  }

  void forward_all(const std::string &block_name) override {
    COMMANDS.push_back("forward_all:" + block_name);
  }

  std::vector<std::string> COMMANDS{};
};

class sequential_block_allocator : public elasticmem::directory::block_allocator {
 public:
  sequential_block_allocator() : cur_idx_{0} {}

  std::vector<std::string> allocate(std::size_t count, const std::vector<std::string> &) override {
    std::vector<std::string> allocated;
    for (std::size_t i = 0; i < count; i++) {
      allocated.push_back(free_.front());
      alloc_.push_back(free_.front());
      free_.erase(free_.begin());
    }
    return allocated;
  }
  void free(const std::vector<std::string> &block_names) override {
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
  std::size_t cur_idx_;
  std::vector<std::string> free_;
  std::vector<std::string> alloc_;
};

class dummy_block_allocator : public elasticmem::directory::block_allocator {
 public:
  explicit dummy_block_allocator(std::size_t num_blocks) : num_free_(num_blocks) {}

  std::vector<std::string> allocate(std::size_t count, const std::vector<std::string> &) override {
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

  void free(const std::vector<std::string> &blocks) override {
    if (num_alloc_ == 0) {
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
      try {
        auto transport =
            std::shared_ptr<apache::thrift::transport::TTransport>(new apache::thrift::transport::TBufferedTransport(
                std::make_shared<apache::thrift::transport::TSocket>(host, port)));
        transport->open();
        transport->close();
        check = false;
      } catch (apache::thrift::transport::TTransportException &e) {
        usleep(100000);
      }
    }
  }

  static std::vector<std::string> init_block_names(size_t num_blocks,
                                                   int32_t service_port,
                                                   int32_t management_port,
                                                   int32_t notification_port,
                                                   int32_t chain_port) {
    std::vector<std::string> block_names;
    for (size_t i = 0; i < num_blocks; ++i) {
      std::string block_name = elasticmem::storage::block_name_parser::make("127.0.0.1",
                                                                            service_port,
                                                                            management_port,
                                                                            notification_port,
                                                                            chain_port,
                                                                            static_cast<int32_t>(i));
      block_names.push_back(block_name);
    }
    return block_names;
  }

  static std::vector<std::shared_ptr<elasticmem::storage::chain_module>> init_kv_blocks(size_t num_blocks,
                                                                                        int32_t service_port,
                                                                                        int32_t management_port,
                                                                                        int32_t notification_port) {
    std::vector<std::shared_ptr<elasticmem::storage::chain_module>> blks;
    blks.resize(num_blocks);
    for (size_t i = 0; i < num_blocks; ++i) {
      std::string block_name = elasticmem::storage::block_name_parser::make("127.0.0.1",
                                                                            service_port,
                                                                            management_port,
                                                                            notification_port,
                                                                            0,
                                                                            static_cast<int32_t>(i));
      blks[i] = std::make_shared<elasticmem::storage::kv_block>(block_name);
      blks[i]->slot_range(0, elasticmem::storage::block::SLOT_MAX);
    }
    return blks;
  }

  static std::vector<std::shared_ptr<elasticmem::storage::chain_module>> init_kv_blocks(const std::vector<std::string> &block_names,
                                                                                        size_t block_capacity = 134217728,
                                                                                        const std::string &dir_host = "127.0.0.1",
                                                                                        int dir_port = 9090) {
    std::vector<std::shared_ptr<elasticmem::storage::chain_module>> blks;
    blks.resize(block_names.size());
    for (size_t i = 0; i < block_names.size(); ++i) {
      blks[i] = std::make_shared<elasticmem::storage::kv_block>(block_names[i], block_capacity, dir_host, dir_port);
    }
    return blks;
  }
};

#endif //ELASTICMEM_TEST_UTILS_H
