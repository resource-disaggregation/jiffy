#ifndef ELASTICMEM_RANDOM_BLOCK_ALLOCATOR_H
#define ELASTICMEM_RANDOM_BLOCK_ALLOCATOR_H

#include <shared_mutex>
#include "block_allocator.h"

namespace elasticmem {
namespace directory {

class random_block_allocator : public block_allocator {
 public:
  random_block_allocator() = default;
  explicit random_block_allocator(const std::vector<std::string> &blocks);
  std::string allocate(const std::string &hint = "") override;
  void free(const std::string &block_name) override;
  void add_blocks(const std::vector<std::string> &block_names) override;
  void remove_blocks(const std::vector<std::string> &block_names) override;

  std::size_t num_free_blocks() override;
  std::size_t num_allocated_blocks() override;
  std::size_t num_total_blocks() override;
 private:
  std::shared_mutex mtx_;
  std::vector<std::string> allocated_blocks_;
  std::vector<std::string> free_blocks_;
};

}
}

#endif //ELASTICMEM_RANDOM_BLOCK_ALLOCATOR_H
