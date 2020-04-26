#include "maestro_allocator_service_handler.h"
#include "../../utils/logger.h"
#include "jiffy/storage/manager/detail/block_id_parser.h"

namespace jiffy {
  namespace maestro {

    using namespace utils;

    maestro_allocator_service_handler::maestro_allocator_service_handler(std::shared_ptr<allocator_service> allocator)
      : allocator_(std::move(allocator)) {}

    void maestro_allocator_service_handler::allocate(std::vector<std::string> &_return,
                                                     const std::string &tenantID,
                                                     int64_t numBlocks) {
      LOG(log_level::info) << "Received allocation request for " << numBlocks << " blocks from " << tenantID;
      _return = allocator_->allocate(tenantID, numBlocks);
    }

    void maestro_allocator_service_handler::deallocate(const std::string &tenantID,
                                                       const std::vector<std::string> &blocks) {
      LOG(log_level::info) << "Received deallocate request for " << blocks.size() << " blocks from " << tenantID;
      allocator_->deallocate(tenantID, blocks);
    }

    // todo remove this exception if not required
    maestro_allocator_service_exception maestro_allocator_service_handler::make_exception(const std::string &ae) {
      return maestro_allocator_service_exception();
    }

  }
}


