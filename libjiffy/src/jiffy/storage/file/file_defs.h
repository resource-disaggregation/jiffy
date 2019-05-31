#ifndef JIFFY_FILE_H
#define JIFFY_FILE_H

#include <functional>
#include "jiffy/storage/block_memory_allocator.h"
#include "jiffy/storage/types/binary.h"
#include "jiffy/storage/file/dummy_block.h"

namespace jiffy {
namespace storage {
// File definition
typedef dummy_block file_type;

}
}

#endif //JIFFY_FILE_H
