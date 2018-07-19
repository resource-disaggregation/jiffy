#ifndef MMUX_RANDOM_BLOCK_ALLOCATOR_H
#define MMUX_RANDOM_BLOCK_ALLOCATOR_H

#include <iostream>
#include <shared_mutex>
#include <set>
#include <map>
#include "block_allocator.h"
#include "../../utils/rand_utils.h"
#include "../../utils/logger.h"

namespace mmux {
namespace directory {

class random_block_allocator : public block_allocator {
 public:
  random_block_allocator() = default;
  std::vector<std::string> allocate(std::size_t count, const std::vector<std::string> &exclude_list) override;
  void free(const std::vector<std::string> &block_name) override;
  void add_blocks(const std::vector<std::string> &block_names) override;
  void remove_blocks(const std::vector<std::string> &block_names) override;

  std::size_t num_free_blocks() override;
  std::size_t num_allocated_blocks() override;
  std::size_t num_total_blocks() override;
 private:
  std::shared_mutex mtx_;
  std::set<std::string> allocated_blocks_;
  std::set<std::string> free_blocks_;
};

}
}

#endif //MMUX_RANDOM_BLOCK_ALLOCATOR_H
