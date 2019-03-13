#ifndef JIFFY_HASH_TABLE_OPS_H
#define JIFFY_HASH_TABLE_OPS_H

#include <vector>
#include "jiffy/storage/command.h"

namespace jiffy {
namespace storage {

extern std::vector<command> KV_OPS;

/**
 * @brief Hash table supported operations
 */
enum hash_table_cmd_id {
  ht_exists = 0,
  ht_get = 1,
  ht_keys = 2, // TODO: We should not support multi-key operations since we do not provide any guarantees
  ht_num_keys = 3, // TODO: We should not support multi-key operations since we do not provide any guarantees
  ht_put = 4,
  ht_remove = 5,
  ht_update = 6,
  ht_lock = 7,
  ht_unlock = 8,
  ht_locked_data_in_slot_range = 9,
  ht_locked_get = 10,
  ht_locked_put = 11,
  ht_locked_remove = 12,
  ht_locked_update = 13,
  ht_upsert = 14,
  ht_locked_upsert = 15,
  ht_update_partition = 16,
  ht_locked_update_partition = 17,
  ht_get_storage_size = 18,
  ht_locked_get_storage_size = 19,
  ht_get_metadata = 20,
  ht_locked_get_metadata = 21
};

}
}

#endif //JIFFY_HASH_TABLE_OPS_H
