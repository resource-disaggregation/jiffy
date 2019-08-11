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

enum file_cmd_id: uint32_t {
  file_write = 0,
  file_read = 1,
  file_seek = 2,
  file_clear = 3,
  file_update_partition = 4
};

}
}

#endif //JIFFY_FILE_OPS_H