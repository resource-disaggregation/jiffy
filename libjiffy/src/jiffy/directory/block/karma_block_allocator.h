#ifndef JIFFY_KARMA_BLOCK_ALLOCATOR_H
#define JIFFY_KARMA_BLOCK_ALLOCATOR_H

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
/* Max-min fairness block allocator class, inherited from block allocator */
class karma_block_allocator : public block_allocator {
 public:
  karma_block_allocator(uint32_t num_tenants, uint64_t init_credits, uint32_t interval_ms, uint32_t public_blocks);

  virtual ~karma_block_allocator() = default;

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

  void update_demand(const std::string &tenant_id, uint32_t demand, uint32_t oracle_demand) override;


 void compute_allocations();

  private:

 // Must be called with lock
 // Updates allocations, credits, rates
 void karma_algorithm(std::unordered_map<std::string, uint32_t> &demands);

// HACK: making these public for testing
public:

// Optimized karma algorithm
 void karma_algorithm_fast(std::unordered_map<std::string, uint32_t> &demands);
 void borrow_from_poorest_fast(std::unordered_map<std::string, uint32_t> &demands, std::vector<std::string>& donors, std::vector<std::string>& borrowers);
 void give_to_richest_fast(std::unordered_map<std::string, uint32_t> &demands, std::vector<std::string>& donors, std::vector<std::string>& borrowers);

private:

 void thread_run(uint32_t interval_ms);
 void stats_thread_run(uint32_t interval_ms);

 void log_stats();

 std::size_t num_allocated_blocks_unsafe();

public:
 void register_tenant(std::string tenant_id);

private:

 std::vector<std::string> append_seq_nos(const std::vector<std::string> &blocks);

 std::vector<std::string> strip_seq_nos(const std::vector<std::string> &blocks);

  /* Operation mutex */
  std::mutex mtx_;
  /* Allocated blocks per-tenant */
  std::unordered_map<std::string, std::set<std::string> > active_blocks_;
  /* Free blocks */
  std::set<std::string> free_blocks_;
  /*Fair share of each tenant*/
//   std::size_t fair_share_;
  /*Block sequence numbers*/
  std::unordered_map<std::string, int32_t> block_seq_no_;
  /*Last tenant that has used this block*/
  std::unordered_map<std::string, std::string> last_tenant_;
  public:
  std::size_t total_blocks_;
  private:
  uint32_t num_tenants_;
  uint64_t init_credits_;

public:
  std::unordered_map<std::string, uint32_t> demands_;
  std::unordered_map<std::string, uint32_t> oracle_demands_;
  std::unordered_map<std::string, uint64_t> credits_;
  std::unordered_map<std::string, int32_t> rate_;
  std::unordered_map<std::string, uint32_t> allocations_;

private:

  std::thread thread_;
  std::thread stats_thread_;

  std::unordered_map<std::string, bool> used_bitmap_;
  std::unordered_map<std::string, bool> temp_used_bitmap_;
  
  uint32_t public_blocks_;

};

}
}

#endif //JIFFY_KARMA_BLOCK_ALLOCATOR_H
