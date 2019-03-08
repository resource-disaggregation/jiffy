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

enum b_tree_cmd_id {
  bt_exists = 0,
  bt_get = 1,
  bt_num_keys = 2,
  bt_put = 3,
  bt_range_lookup = 4,
  bt_remove = 5,
  bt_update = 6,
  bt_range_count = 7
};

}
}

#endif //JIFFY_B_TREE_OPS_H