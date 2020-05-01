#ifndef JIFFY_USAGE_TRACKER_H
#define JIFFY_USAGE_TRACKER_H

#include <string>
#include <unordered_map>
#include <mutex>
#include "free_block_allocator.h"

namespace jiffy {
  namespace maestro {

    class usage_tracker {

    public:

      explicit usage_tracker(std::shared_ptr<free_block_allocator> free_pool);

      void add_blocks(const std::string& tenant_id, std::size_t num_blocks);

      void remove_blocks(const std::string& tenant_id, std::size_t num_blocks);

//      void update_fair_distribution(std::unordered_map<std::string, std::size_t> &distribution_by_tenant);

    private:
      std::shared_ptr<free_block_allocator> free_block_allocator_;

      /* Operation mutex */
      std::mutex mtx_;

      /* Number of blocks currently being used by each tenant*/
      std::unordered_map<std::string, std::size_t> num_assigned_blocks_by_tenant_;


      /* Capacity distribution priority */
      std::unordered_map<std::string, std::size_t> distribution_by_tenant_;

    };
  }
}


#endif //JIFFY_USAGE_TRACKER_H
