#ifndef JIFFY_FILE_OPS_H
#define JIFFY_FILE_OPS_H

#include <vector>
#include <string>
#include "jiffy/storage/command.h"

namespace jiffy {
namespace storage {
extern std::vector<command> FILE_OPS;

/**
 * @brief File supported operations
 */

enum file_cmd_id {
  file_write = 0,
  file_read = 1,
  file_clear = 2,
  file_update_partition = 3,
  file_seek = 4
};

}
}

#endif //JIFFY_FILE_OPS_H