#include "allocator_service.h"
#include "jiffy/storage/manager/detail/block_id_parser.h"

namespace jiffy {
  namespace maestro {


    std::vector<std::string> allocator_service::allocate(std::string tenantID, int64_t numBlocks) {

      //todo complete
      std::vector<std::string> result;
      result.push_back("yes it works");
      return result;
    }

    void allocator_service::deallocate(std::string tenantID, std::vector<std::string> blocks) {

      //todo complete
    }
  }
}