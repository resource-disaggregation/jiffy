//
// Created by Akhil Raj Azhikodan on 01/05/20.
//

#include "reclaim_service.h"

namespace jiffy {
  namespace maestro {


    reclaim_service::reclaim_service(std::shared_ptr<usage_tracker> usage_tracker)
        : usage_tracker_(std::move(usage_tracker)) {}

    std::vector<std::string> reclaim_service::reclaim(const std::string &tenant_id, std::size_t num_blocks) {

      std::vector<std::string> reclaimed_blocks;

      while(reclaimed_blocks.size() < num_blocks) {
        auto tenant = usage_tracker_->get_max_borrowing_tenant();
        if(tenant.num_over_provisioned_blocks == 0) {
          return reclaimed_blocks;
        }

        // make reclaim API call
        // append blocks to reclaimed blocks
        // update usage_tracker_
      }

    }

  }
}