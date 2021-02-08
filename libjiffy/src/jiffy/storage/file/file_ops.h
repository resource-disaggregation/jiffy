#ifndef JIFFY_FILE_OPS_H
#define JIFFY_FILE_OPS_H

#include <vector>
#include <string>
#include "jiffy/storage/command.h"

namespace jiffy {
namespace storage {

extern command_map FILE_OPS;

/**
 * @brief File supported operations
 */

enum file_cmd_id : uint32_t {
  file_write = 0,
  file_read = 1,
  file_write_ls = 2,
  file_read_ls = 3,
  file_clear = 4,
  file_update_partition = 5,
  file_add_blocks = 6,
  file_get_storage_capacity = 7
};

}
}

#endif //JIFFY_FILE_OPS_H