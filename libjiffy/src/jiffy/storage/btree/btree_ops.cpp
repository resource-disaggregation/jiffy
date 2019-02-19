#include "jiffy/storage/btree/btree_ops.h"

namespace jiffy {
namespace storage {

std::vector<command> BTREE_OPS = {command{command_type::accessor, "exists"},
                                  command{command_type::accessor, "get"},
                                  command{command_type::accessor, "num_keys"},
                                  command{command_type::mutator, "put"},
                                  command{command_type::accessor, "range_lookup"},
                                  command{command_type::mutator, "remove"},
                                  command{command_type::mutator, "update"}};
}
}