//
// Created by Akhil Raj Azhikodan on 01/05/20.
//

#ifndef JIFFY_RECLAIM_SERVICE_H
#define JIFFY_RECLAIM_SERVICE_H

#include <mutex>
#include <unordered_map>
#include "usage_tracker.h"

namespace jiffy {
  namespace maestro {

    class reclaim_service {

    public:

      explicit reclaim_service(std::shared_ptr<usage_tracker> usage_tracker);

      std::vector<std::string> reclaim(const std::string& tenant_id, std::size_t num_blocks);

    private:

      /* Operation mutex */
      std::mutex mtx_;

      std::shared_ptr<usage_tracker> usage_tracker_;

    };
  }
}



#endif //JIFFY_RECLAIM_SERVICE_H
