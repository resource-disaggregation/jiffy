#include "jiffy/storage/file/file_ops.h"

namespace jiffy {
namespace storage {

std::vector<command> FILE_OPS = {command{command_type::mutator, "file_write"},
                                 command{command_type::accessor, "file_read"},
                                 command{command_type::mutator, "file_clear"},
                                 command{command_type::mutator, "file_update_partition"}};
}
}