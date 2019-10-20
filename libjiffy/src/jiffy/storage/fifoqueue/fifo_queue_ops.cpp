#include "jiffy/storage/fifoqueue/fifo_queue_ops.h"

namespace jiffy {
namespace storage {

command_map FQ_CMDS = {{"enqueue", {command_type::mutator, 0}},
                       {"dequeue", {command_type::accessor, 1}},
                       {"clear", {command_type::mutator, 2}},
                       {"update_partition", {command_type::mutator, 3}},
                       {"read_next", {command_type::accessor, 4}},
                       {"qsize", {command_type::accessor, 5}},
                       {"fq_in_rate", {command_type::accessor, 6}},
                       {"fq_out_rate", {command_type::accessor, 7}}};
}
}