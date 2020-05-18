#ifndef JIFFY_MAESTRO_ALLOCATOR_SERVICE_HANDLER_H
#define JIFFY_MAESTRO_ALLOCATOR_SERVICE_HANDLER_H

#include <jiffy/maestro/allocator_service.h>
#include "maestro_allocator_service.h"

namespace jiffy {
namespace maestro {

  class maestro_allocator_service_handler : public maestro_allocator_serviceIf {
  public:

    /**
     * @brief Constructor
     * @param allocator Maestro Allocator Service
     */

    explicit maestro_allocator_service_handler(std::shared_ptr<allocator_service> allocator);

    /**
     * @brief Allocate memory
     * @param _return Allocated blocks
     * @param tenantID Requesting tenant identifier
     * @param numBlocks number of blocks requested
     */
    void allocate(std::vector<std::string> &_return, const std::string &tenantID, const int64_t numBlocks) override;

    /**
     * @brief Deallocate memory
     * @param tenantID Requesting tenant identifier
     * @param blocks number of blocks requested
     */
    void deallocate(const std::string &tenentID, const std::vector<std::string> &blocks) override;

  private:

    maestro_allocator_service_exception make_exception(const std::string &ae);

    std::shared_ptr<allocator_service> allocator_;
  };
}
}

#endif //JIFFY_MAESTRO_ALLOCATOR_SERVICE_HANDLER_H
