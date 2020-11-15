#include "jiffy/storage/file/file_ops.h"

namespace jiffy {
namespace storage {

command_map FILE_OPS = {{"write", {command_type::mutator, file_cmd_id::file_write}},
                        {"read", {command_type::accessor, file_cmd_id::file_read}},
                        {"clear", {command_type::mutator, file_cmd_id::file_clear}},
                        {"update_partition", {command_type::mutator, file_cmd_id::file_update_partition}},
                        {"add_blocks", {command_type::accessor, file_cmd_id::file_add_blocks}},
                        {"get_storage_capacity", {command_type::accessor, file_cmd_id::file_get_storage_capacity}},
                        {"get_partition_size", {command_type::accessor, file_cmd_id::file_get_partition_size}}};
}
}