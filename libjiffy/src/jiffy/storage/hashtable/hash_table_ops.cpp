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
                      {"exists_ls", {command_type::accessor, 6}},
                      {"get_ls", {command_type::accessor, 7}},
                      {"put_ls", {command_type::mutator, 8}},
                      {"remove_ls", {command_type::mutator, 9}},
                      {"update_ls", {command_type::mutator, 10}},
                      {"upsert_ls", {command_type::mutator, 11}},
                      {"multi_get", {command_type::accessor, 12}},
                      {"multi_get_ls", {command_type::accessor, 13}},
                      {"update_partition", {command_type::mutator, 14}},
                      {"get_storage_size", {command_type::accessor, 15}},
                      {"get_metadata", {command_type::accessor, 16}},
                      {"get_range_data", {command_type::accessor, 17}},
                      {"scale_put", {command_type::mutator, 18}},
                      {"scale_remove", {command_type::mutator, 19}}};
}
}