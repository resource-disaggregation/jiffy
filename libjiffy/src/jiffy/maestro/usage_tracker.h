#ifndef JIFFY_USAGE_TRACKER_H
#define JIFFY_USAGE_TRACKER_H

#include <string>
#include <unordered_map>
#include <mutex>
#include "free_block_allocator.h"

namespace jiffy {
  namespace maestro {

    struct tenant {
      std::string id;
      int32_t num_block{ 0 };
      int32_t num_over_provisioned_blocks{ 0 };
    };

    class usage_tracker {

    public:

      explicit usage_tracker(std::shared_ptr<free_block_allocator> free_pool,
                             std::unordered_map<std::string, float> &distribution_by_tenant);

      void add_blocks(const std::string& tenant_id, std::int32_t num_blocks);

      void remove_blocks(const std::string& tenant_id, std::int32_t num_blocks);

      int32_t num_under_provisioned_blocks(const std::string& tenant_ID);

      tenant get_max_borrowing_tenant();

      //      void update_fair_distribution(std::unordered_map<std::string, std::size_t> &distribution_by_tenant);

    private:
      std::shared_ptr<free_block_allocator> free_pool;

      /* Operation mutex */
      std::mutex mtx_;

      /* Number of blocks currently being used by each tenant*/
      std::unordered_map<std::string, std::size_t> num_assigned_blocks_by_tenant_;

      /* Capacity distribution in percentage by tenant */
      std::unordered_map<std::string, float> distribution_by_tenant_;

    };
  }
}


#endif //JIFFY_USAGE_TRACKER_H
