#include "jiffy/storage/btree/btree_ops.h"

namespace jiffy {
namespace storage {

std::vector<command> BTREE_OPS = {command{command_type::accessor, "bt_exists"},
                                  command{command_type::accessor, "bt_get"},
                                  command{command_type::accessor, "bt_num_keys"},
                                  command{command_type::mutator, "bt_put"},
                                  command{command_type::accessor, "bt_range_lookup"},
                                  command{command_type::mutator, "bt_remove"},
                                  command{command_type::mutator, "bt_update"},
                                  command{command_type::accessor, "bt_range_count"},
                                  command{command_type::mutator, "bt_update_partition"},
                                  command{command_type::accessor, "bt_range_lookup_batches"},
                                  command{command_type::mutator, "bt_scale_remove"},
                                  command{command_type::accessor, "bt_get_storage_size"}};
}
}