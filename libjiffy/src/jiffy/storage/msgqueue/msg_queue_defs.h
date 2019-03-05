#ifndef JIFFY_MSG_QUEUE_H
#define JIFFY_MSG_QUEUE_H

#include <functional>
#include <deque>
#include "jiffy/storage/block_memory_allocator.h"

namespace jiffy {
namespace storage {

// The default number of elements in a msg queue
constexpr size_t MSG_QUEUE_DEFAULT_SIZE = 0;



// Key/Value definitions
typedef std::string msg_type;

// Custom template arguments
typedef block_memory_allocator<msg_type> allocator_type;

// Btree definitions
typedef std::deque<msg_type , allocator_type> msg_queue_type;

}
}

#endif //JIFFY_MSG_QUEUE_H
