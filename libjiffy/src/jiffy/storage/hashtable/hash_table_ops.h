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
  ht_exists_ls = 6,
  ht_get_ls = 7,
  ht_put_ls = 8,
  ht_remove_ls = 9,
  ht_update_ls = 10,
  ht_upsert_ls = 11,
  ht_update_partition = 12,
  ht_get_storage_size = 13,
  ht_get_metadata = 14,
  ht_get_range_data = 15,
  ht_scale_put = 16,
  ht_scale_remove = 17
};

}
}

#endif //JIFFY_HASH_TABLE_OPS_H