#ifndef JIFFY_SHARED_LOG_OPS_H
#define JIFFY_SHARED_LOG_OPS_H

#include <vector>
#include <string>
#include "jiffy/storage/command.h"

namespace jiffy {
namespace storage {

extern command_map SHARED_LOG_OPS;

/**
 * @brief shared_log supported operations
 */

enum shared_log_cmd_id : uint32_t {
  shared_log_write = 0,
  shared_log_scan = 1,
  shared_log_trim = 2,
  shared_log_update_partition = 3,
  shared_log_add_blocks = 4,
  shared_log_get_storage_capacity = 5
};

}
}

#endif //JIFFY_SHARED_LOG_OPS_H