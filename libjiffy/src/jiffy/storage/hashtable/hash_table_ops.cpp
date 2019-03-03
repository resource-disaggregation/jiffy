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
                               command{command_type::mutator, "ht_lock"},
                               command{command_type::mutator, "ht_unlock"},
                               command{command_type::accessor, "ht_locked_get_data_in_slot_range"},
                               command{command_type::accessor, "ht_locked_get"},
                               command{command_type::mutator, "ht_locked_put"},
                               command{command_type::mutator, "ht_locked_remove"},
                               command{command_type::mutator, "ht_locked_update"},
                               command{command_type::mutator, "ht_upsert"},
                               command{command_type::mutator, "ht_locked_upsert"},
                               command{command_type::mutator, "ht_update_partition"},
                               command{command_type::mutator, "ht_locked_update_partition"},
                               command{command_type::mutator, "ht_get_storage_size"},
                               command{command_type::mutator, "ht_locked_get_storage_size"},
                               command{command_type::mutator, "ht_get_metadata"},
                               command{command_type::mutator, "ht_locked_get_metadata"}};

}
}