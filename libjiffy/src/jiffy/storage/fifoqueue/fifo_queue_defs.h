#ifndef JIFFY_FIFO_QUEUE_H
#define JIFFY_FIFO_QUEUE_H

#include <functional>
#include "jiffy/storage/block_memory_allocator.h"
#include "jiffy/storage/types/binary.h"
#include "string_array.h"

namespace jiffy {
namespace storage {

// Fifo queue definition
typedef string_array fifo_queue_type;

}
}

#endif //JIFFY_FIFO_QUEUE_H
