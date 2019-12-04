#ifndef JIFFY_FIFO_QUEUE_OPS_H
#define JIFFY_FIFO_QUEUE_OPS_H

#include <vector>
#include <string>
#include <unordered_map>
#include "jiffy/storage/command.h"

namespace jiffy {
namespace storage {

extern command_map FQ_CMDS;

/**
 * @brief FIFO queue supported operations
 */
enum fifo_queue_cmd_id : uint32_t {
  fq_enqueue = 0,
  fq_dequeue = 1,
  fq_clear = 2,
  fq_update_partition = 3,
  fq_readnext = 4,
  fq_length = 5,
  fq_in_rate = 6,
  fq_out_rate = 7,
  fq_front = 8
};

}
}

#endif //JIFFY_FIFO_QUEUE_OPS_H
