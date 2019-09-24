#ifndef JIFFY_HASH_TABLE_OPS_H
#define JIFFY_HASH_TABLE_OPS_H

#include <vector>
#include "jiffy/storage/command.h"

namespace jiffy {
namespace storage {

extern command_map HT_OPS;

/**
 * @brief Hash table supported operations
 */
enum hash_table_cmd_id: uint32_t {
  ht_exists = 0,
  ht_get = 1,
  ht_put = 2,
  ht_remove = 3,
  ht_update = 4,
  ht_upsert = 5,
  ht_update_partition = 6,
  ht_get_storage_size = 7,
  ht_get_metadata = 8,
  ht_get_range_data = 9,
  ht_scale_put = 10,
  ht_scale_remove = 11
};

}
}

#endif //JIFFY_HASH_TABLE_OPS_H
