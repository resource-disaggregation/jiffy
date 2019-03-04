#include "jiffy/storage/msgqueue/msg_queue_ops.h"

namespace jiffy {
namespace storage {

std::vector<command> MSG_QUEUE_OPS = {command{command_type::accessor, "mq_receive"},
                                      command{command_type::mutator, "mq_send"}};
}
}