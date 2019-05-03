#ifndef JIFFY_FILE_H
#define JIFFY_FILE_H

#include <functional>
#include <deque>
#include "jiffy/storage/block_memory_allocator.h"
#include "jiffy/storage/types/binary.h"

namespace jiffy {
namespace storage {

// The default number of elements in a file
constexpr size_t FILE_DEFAULT_SIZE = 0;

// Key/Value definitions
typedef binary msg_type;

// Custom template arguments
typedef block_memory_allocator<msg_type> file_allocator_type;

// Msg queue definitions
typedef std::vector<msg_type, file_allocator_type> file_type;

}
}

#endif //JIFFY_FILE_H
