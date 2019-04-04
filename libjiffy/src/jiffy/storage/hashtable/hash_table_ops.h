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
  ht_upsert = 7,
  ht_update_partition = 8,
  ht_get_storage_size = 9,
  ht_get_metadata = 10,
  ht_get_range_data = 11,
  ht_scale_remove = 12,
};

}
}

#endif //JIFFY_HASH_TABLE_OPS_H
