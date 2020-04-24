#ifndef JIFFY_FREE_BLOCK_ALLOCATOR_H
#define JIFFY_FREE_BLOCK_ALLOCATOR_H


#include <shared_mutex>
#include <set>
#include <unordered_map>
#include <list>
#include "jiffy/directory/block/block_allocator.h"


namespace jiffy {
  namespace maestro {

    class free_block_allocator : public directory::block_allocator {

    public:
      free_block_allocator() = default;

      virtual ~free_block_allocator() = default;

      /**
       * @brief Allocate blocks
       * @param count Number of blocks
       * @return Block names
       */

      std::vector<std::string> allocate(std::size_t count, const std::vector<std::string> &exclude_list) override;

      /**
       * @brief Add blocks to free block list
       * @param blocks Block names
       */
      void free(const std::vector<std::string> &block_name) override;

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

      /* Operation mutex */
      std::mutex mtx_;
      /* Free blocks per storage server*/
      std::unordered_map<std::string, std::set<std::string>> free_blocks_by_server_;

    };

  }
}

#endif //JIFFY_FREE_BLOCK_ALLOCATOR_H
