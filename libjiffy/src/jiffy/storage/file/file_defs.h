#ifndef JIFFY_FILE_H
#define JIFFY_FILE_H

#include <functional>
#include <vector>
#include "jiffy/storage/block_memory_allocator.h"
#include "jiffy/storage/types/binary.h"
#include "jiffy/storage/string_array.h"

namespace jiffy {
namespace storage {

// The default number of elements in a file
constexpr size_t FILE_DEFAULT_SIZE = 0;

// Key/Value definitions
typedef char msg_type;

// Custom template arguments
typedef block_memory_allocator<msg_type> file_allocator_type;

// Msg queue definitions
//typedef std::vector<msg_type, file_allocator_type> file_type;

// File definition
typedef string_array file_type;

}
}

#endif //JIFFY_FILE_H
