#ifndef JIFFY_SHARED_LOG_H
#define JIFFY_SHARED_LOG_H

#include <functional>
#include "jiffy/storage/block_memory_allocator.h"
#include "jiffy/storage/types/binary.h"
#include "jiffy/storage/shared_log/shared_log_block.h"

namespace jiffy {
namespace storage {
// shared_log definition
typedef shared_log_block shared_log_type;

}
}

#endif //JIFFY_SHARED_LOG_H
