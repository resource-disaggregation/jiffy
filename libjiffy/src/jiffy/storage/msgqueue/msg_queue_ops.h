#ifndef JIFFY_MSG_QUEUE_OPS_H
#define JIFFY_MSG_QUEUE_OPS_H

#include <vector>
#include <string>
#include "jiffy/storage/command.h"

namespace jiffy {
namespace storage {
extern std::vector<command> MSG_QUEUE_OPS;

/**
 * @brief Message queue supported operations
 */

enum msg_queue_cmd_id {
  mq_send = 0,
  mq_receive = 1,
  mq_clear = 2,
};

}
}

#endif //JIFFY_B_TREE_OPS_H