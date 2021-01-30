#include "jiffy/storage/fifoqueue/fifo_queue_ops.h"

namespace jiffy {
namespace storage {

command_map FQ_CMDS = {{"enqueue", {command_type::mutator, 0}},
                       {"dequeue", {command_type::mutator, 1}},
                       {"clear", {command_type::mutator, 2}},
                       {"update_partition", {command_type::mutator, 3}},
                       {"read_next", {command_type::accessor, 4}},
                       {"length", {command_type::accessor, 5}},
                       {"in_rate", {command_type::accessor, 6}},
                       {"out_rate", {command_type::accessor, 7}},
                       {"front", {command_type::accessor, 8}}};
}
}
