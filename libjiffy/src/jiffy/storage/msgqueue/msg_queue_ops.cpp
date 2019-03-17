#include "jiffy/storage/msgqueue/msg_queue_ops.h"

namespace jiffy {
namespace storage {

std::vector<command> MSG_QUEUE_OPS = {command{command_type::mutator, "mq_send"},
                                      command{command_type::accessor, "mq_read"},
                                      command{command_type::mutator, "mq_clear"},
                                      command{command_type::mutator, "mq_update_partition"}};
}
}