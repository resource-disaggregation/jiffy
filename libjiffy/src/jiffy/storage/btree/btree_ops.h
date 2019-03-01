#ifndef JIFFY_B_TREE_OPS_H
#define JIFFY_B_TREE_OPS_H

#include <vector>
#include <string>
#include "jiffy/storage/command.h"

namespace jiffy {
namespace storage {
extern std::vector<command> BTREE_OPS;

/**
 * @brief Btree supported operations
 */

enum class b_tree_cmd_id {
  exists = 0,
  get = 1,
  num_keys = 2,
  put = 3,
  range_lookup = 4,
  remove = 5,
  update = 6,
};



}
}

#endif //JIFFY_B_TREE_OPS_H