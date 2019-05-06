#ifndef JIFFY_FIFO_QUEUE_H
#define JIFFY_FIFO_QUEUE_H

#include <functional>
#include <deque>
#include "jiffy/storage/block_memory_allocator.h"
#include "jiffy/storage/types/binary.h"
#include "jiffy/storage/string_array.h"

namespace jiffy {
namespace storage {

// The default number of elements in a fifo queue
constexpr size_t FIFO_QUEUE_DEFAULT_SIZE = 0;

// Fifo queue element definitions
typedef std::string element_type;

// Custom template arguments
typedef block_memory_allocator<element_type> fq_allocator_type;

// Msg queue definitions
//typedef std::vector<element_type, fq_allocator_type> fifo_queue_type;

typedef string_array fifo_queue_type;

}
}

#endif //JIFFY_FIFO_QUEUE_H
