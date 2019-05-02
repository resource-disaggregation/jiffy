#include "jiffy/storage/hashtable/hash_table_ops.h"

namespace jiffy {
namespace storage {

std::vector<command> KV_OPS = {command{command_type::accessor, "ht_exists"},
                               command{command_type::accessor, "ht_get"},
                               command{command_type::accessor, "ht_keys"},
                               command{command_type::accessor, "ht_num_keys"},
                               command{command_type::mutator, "ht_put"},
                               command{command_type::mutator, "ht_remove"},
                               command{command_type::mutator, "ht_update"},
                               command{command_type::mutator, "ht_upsert"},
                               command{command_type::mutator, "ht_update_partition"},
                               command{command_type::accessor, "ht_get_storage_size"},
                               command{command_type::accessor, "ht_get_metadata"},
                               command{command_type::accessor, "ht_get_range_data"},
                               command{command_type::mutator, "ht_scale_remove"}};
}
}