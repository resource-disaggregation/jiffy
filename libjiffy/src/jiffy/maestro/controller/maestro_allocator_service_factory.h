
#ifndef JIFFY_MAESTRO_ALLOCATOR_SERVICE_FACTORY_H
#define JIFFY_MAESTRO_ALLOCATOR_SERVICE_FACTORY_H

#include <jiffy/maestro/allocator_service.h>
#include "maestro_allocator_service.h"

namespace jiffy {
  namespace maestro {
    class maestro_allocator_service_factory : public maestro_allocator_serviceIfFactory {
    public:

      explicit maestro_allocator_service_factory(std::shared_ptr<allocator_service> allocator);

    private:

      maestro_allocator_serviceIf* getHandler(const ::apache::thrift::TConnectionInfo& connInfo) override ;

      void releaseHandler(maestro_allocator_serviceIf *anIf) override;

      std::shared_ptr<allocator_service> allocator_;
    };
  }
}

#endif //JIFFY_MAESTRO_ALLOCATOR_SERVICE_FACTORY_H
