#include "jiffy/storage/fifoqueue/fifo_queue_ops.h"

namespace jiffy {
namespace storage {

command_map FQ_CMDS = {{"enqueue", {command_type::mutator, 0}},
                       {"dequeue", {command_type::mutator, 1}},
                       {"enqueue_ls", {command_type::mutator, 2}},
                       {"dequeue_ls", {command_type::mutator, 3}},
                       {"clear", {command_type::mutator, 4}},
                       {"update_partition", {command_type::mutator, 5}},
                       {"read_next", {command_type::accessor, 6}},
                       {"read_next_ls", {command_type::accessor, 7}},
                       {"length", {command_type::accessor, 8}},
                       {"length_ls", {command_type::accessor, 9}},
                       {"in_rate", {command_type::accessor, 10}},
                       {"out_rate", {command_type::accessor, 11}},
                       {"front", {command_type::accessor, 12}}};
}
}
