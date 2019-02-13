#include "jiffy/storage/hashtable/hash_table_ops.h"

namespace jiffy {
namespace storage {

std::vector<command> KV_OPS = {command{command_type::accessor, "exists"},
                               command{command_type::accessor, "get"},
                               command{command_type::accessor, "keys"},
                               command{command_type::accessor, "num_keys"},
                               command{command_type::mutator, "put"},
                               command{command_type::mutator, "remove"},
                               command{command_type::mutator, "update"},
                               command{command_type::mutator, "lock"},
                               command{command_type::mutator, "unlock"},
                               command{command_type::accessor, "locked_get_data_in_slot_range"},
                               command{command_type::accessor, "locked_get"},
                               command{command_type::mutator, "locked_put"},
                               command{command_type::mutator, "locked_remove"},
                               command{command_type::mutator, "locked_update"},
                               command{command_type::mutator, "upsert"},
                               command{command_type::mutator, "locked_upsert"}};

}
}