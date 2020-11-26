#include "jiffy/storage/shared_log/shared_log_ops.h"

namespace jiffy {
namespace storage {

command_map SHARED_LOG_OPS = {{"write", {command_type::mutator, shared_log_cmd_id::shared_log_write}},
                        {"scan", {command_type::accessor, shared_log_cmd_id::shared_log_scan}},
                        {"trim", {command_type::mutator, shared_log_cmd_id::shared_log_trim}},
                        {"update_partition", {command_type::mutator, shared_log_cmd_id::shared_log_update_partition}},
                        {"add_blocks", {command_type::accessor, shared_log_cmd_id::shared_log_add_blocks}},
                        {"get_storage_capacity", {command_type::accessor, shared_log_cmd_id::shared_log_get_storage_capacity}}};
}
}