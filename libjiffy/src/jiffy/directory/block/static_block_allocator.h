#ifndef JIFFY_STATIC_BLOCK_ALLOCATOR_H
#define JIFFY_STATIC_BLOCK_ALLOCATOR_H

#include <iostream>
#include <shared_mutex>
#include <set>
#include <map>
#include <unordered_map>
#include <thread>
#include "block_allocator.h"
#include "../../utils/rand_utils.h"
#include "../../utils/logger.h"

namespace jiffy {
namespace directory {
/* Random block allocator class, inherited from block allocator */
class static_block_allocator : public block_allocator {
 public:
  static_block_allocator(uint32_t num_tenants, uint32_t interval_ms);

  virtual ~static_block_allocator() = default;

  /**
   * @brief Allocate blocks in different prefixes
   * @param count Number of blocks
   * @return Block names
   */

  std::vector<std::string> allocate(std::size_t count, const std::vector<std::string> &exclude_list, const std::string &tenant_id) override;

  /**
   * @brief Free blocks
   * @param blocks Block names
   */
  void free(const std::vector<std::string> &block_name, const std::string &tenant_id) override;

  /**
   * @brief Add blocks to free block list
   * @param block_names Block names
   */

  void add_blocks(const std::vector<std::string> &block_names) override;

  /**
   * @brief Remove blocks from free block list
   * @param block_names Block names
   */

  void remove_blocks(const std::vector<std::string> &block_names) override;

  /**
   * @brief Fetch number of free blocks
   * @return Number of free blocks
   */

  std::size_t num_free_blocks() override;

  /**
   * @brief Fetch number of allocated blocks
   * @return Number of allocated blocks
   */

  std::size_t num_allocated_blocks() override;

  /**
   * @brief Fetch number of total blocks
   * @return Number of total blocks
   */

  std::size_t num_total_blocks() override;
 private:

 std::size_t num_allocated_blocks_unsafe();
 
 void register_tenant(std::string tenant_id);

 void thread_run(uint32_t interval_ms);

 void compute_allocations();

  /*
   * Fetch prefix of block name
   */

  std::string prefix(const std::string &block_name) const {
    auto pos = block_name.find_last_of(':');
    if (pos == std::string::npos) {
      throw std::logic_error("Malformed block name [" + block_name + "]");
    }
    return block_name.substr(0, pos);
  }
  /* Operation mutex */
  std::mutex mtx_;

  /* Free blocks */
  std::set<std::string> free_blocks_;
  uint32_t num_tenants_;
  std::size_t total_blocks_;

  /* Allocated blocks per-tenant */
  std::unordered_map<std::string, std::set<std::string> > allocated_blocks_;

  /* Stats thread*/
  std::thread thread_;

  std::unordered_map<std::string, bool> used_bitmap_;
  std::unordered_map<std::string, bool> temp_used_bitmap_;
};

}
}

#endif //JIFFY_STATIC_BLOCK_ALLOCATOR_H
