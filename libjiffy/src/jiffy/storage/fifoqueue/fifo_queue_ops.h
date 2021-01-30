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
  fq_enqueue_ls = 2,
  fq_dequeue_ls = 3,
  fq_clear = 4,
  fq_update_partition = 5,
  fq_readnext = 6,
  fq_readnext_ls = 7,
  fq_length = 8,
  fq_in_rate = 9,
  fq_out_rate = 10,
  fq_front = 11
};

}
}

#endif //JIFFY_FIFO_QUEUE_OPS_H