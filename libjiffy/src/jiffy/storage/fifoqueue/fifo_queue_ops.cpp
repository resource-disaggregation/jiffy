#include "jiffy/storage/fifoqueue/fifo_queue_ops.h"

namespace jiffy {
namespace storage {

std::vector<command> FIFO_QUEUE_OPS = {command{command_type::mutator, "fq_enqueue"},
                                       command{command_type::mutator, "fq_dequeue"},
                                       command{command_type::mutator, "fq_clear"},
                                       command{command_type::mutator, "fq_update_partition"},
                                       command{command_type::accessor, "fq_readnext"}};
}
}