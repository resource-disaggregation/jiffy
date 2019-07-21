#include "jiffy/storage/file/file_ops.h"

namespace jiffy {
namespace storage {

command_map FILE_OPS = {{"write", {command_type::mutator, file_cmd_id::file_write}},
                        {"read", {command_type::accessor, file_cmd_id::file_read}},
                        {"seek", {command_type::accessor, file_cmd_id::file_seek}},
                        {"clear", {command_type::mutator, file_cmd_id::file_clear}},
                        {"update_partition", {command_type::mutator, file_cmd_id::file_update_partition}}};
}
}