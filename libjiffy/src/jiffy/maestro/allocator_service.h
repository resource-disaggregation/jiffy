#ifndef JIFFY_ALLOCATOR_SERVICE_H
#define JIFFY_ALLOCATOR_SERVICE_H

#include <string>
#include <vector>
#include "jiffy/storage/manager/detail/block_id_parser.h"

namespace jiffy {
  namespace maestro {

    class allocator_service {
    public:
      std::vector<std::string> allocate(std::string tenantID, int64_t numBlocks);

      void deallocate(std::string tenantID, std::vector<std::string> blocks);
    };

  }
}
#endif //JIFFY_ALLOCATOR_SERVICE_H
