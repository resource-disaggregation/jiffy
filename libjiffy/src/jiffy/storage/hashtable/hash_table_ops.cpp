#include "jiffy/storage/hashtable/hash_table_ops.h"
#include <string>

namespace jiffy {
namespace storage {

command_map HT_OPS = {{"exists", {command_type::accessor, 0}},
                      {"get", {command_type::accessor, 1}},
                      {"put", {command_type::mutator, 2}},
                      {"remove", {command_type::mutator, 3}},
                      {"update", {command_type::mutator, 4}},
                      {"upsert", {command_type::mutator, 5}},
                      {"update_partition", {command_type::mutator, 6}},
                      {"get_storage_size", {command_type::accessor, 7}},
                      {"get_metadata", {command_type::accessor, 8}},
                      {"get_range_data", {command_type::accessor, 9}},
                      {"scale_put", {command_type::mutator, 10}},
                      {"scale_remove", {command_type::mutator, 11}}};
}
}