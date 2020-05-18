#ifndef JIFFY_ALLOCATOR_SERVICE_H
#define JIFFY_ALLOCATOR_SERVICE_H

#include <string>
#include <vector>
#include "jiffy/storage/manager/detail/block_id_parser.h"
#include "jiffy/maestro/free_block_allocator.h"
#include "jiffy/maestro/usage_tracker.h"
#include "jiffy/maestro/reclaim_service.h"

namespace jiffy {
  namespace maestro {

    class allocator_service {
    public:

      explicit allocator_service(std::shared_ptr<free_block_allocator> free_pool,
                                 std::shared_ptr<usage_tracker> usage_tracker,
                                 std::shared_ptr<reclaim_service> reclaim_service);

      std::vector<std::string> allocate(const std::string& tenant_ID, int64_t num_blocks);

      void deallocate(const std::string& tenant_ID, const std::vector<std::string>& blocks);

    private:
      std::shared_ptr<free_block_allocator> free_block_allocator_;
      std::shared_ptr<usage_tracker> usage_tracker_;
      std::shared_ptr<reclaim_service> reclaim_service_;
    };

  }
}
#endif //JIFFY_ALLOCATOR_SERVICE_H
