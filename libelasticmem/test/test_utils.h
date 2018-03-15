#ifndef ELASTICMEM_TEST_UTILS_H
#define ELASTICMEM_TEST_UTILS_H

#include <string>
#include <vector>
#include "../src/storage/storage_management_service.h"

class dummy_kv_manager : public elasticmem::storage::storage_management_service {
 public:
  dummy_kv_manager() = default;

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

  void clear(const std::string &block_name) override {
    COMMANDS.push_back("clear:" + block_name);
  }

  size_t storage_capacity(const std::string &block_name) override {
    COMMANDS.push_back("storage_capacity:" + block_name);
    return 0;
  }

  size_t storage_size(const std::string &block_name) override {
    COMMANDS.push_back("storage_size:" + block_name);
    return 0;
  }

  std::vector<std::string> COMMANDS{};
};

class dummy_block_allocator : public elasticmem::directory::block_allocator {
 public:
  explicit dummy_block_allocator(std::size_t num_blocks) : num_free_(num_blocks) {}

  std::string allocate(const std::string &) override {
    std::string ret = std::to_string(num_alloc_);
    ++num_alloc_;
    --num_free_;
    return ret;
  }

  void free(const std::string &) override {
    ++num_free_;
    --num_alloc_;
  }

  void add_blocks(const std::vector<std::string> &) override {
    throw std::out_of_range("Cannot add blocks");
  }

  void remove_blocks(const std::vector<std::string> &) override {
    throw std::out_of_range("Cannot remove blocks");
  }

  size_t num_free_blocks() override {
    return num_free_;
  }

  size_t num_allocated_blocks() override {
    return num_alloc_;
  }

  size_t num_total_blocks() override {
    return num_alloc_ + num_free_;
  }

 private:
  std::size_t num_alloc_{};
  std::size_t num_free_{};
};

#endif //ELASTICMEM_TEST_UTILS_H
