#ifndef JIFFY_BTREE_CLIENT_H
#define JIFFY_BTREE_CLIENT_H

#include "jiffy/directory/client/directory_client.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/client_cache.h"

namespace jiffy {
namespace storage {

/* Redo when exception class
 * Redo whenever exception happens */
class redo_error : public std::exception {
 public:
  redo_error() = default;
};

class btree_client {

};

}
}

#endif //JIFFY_BTREE_CLIENT_H