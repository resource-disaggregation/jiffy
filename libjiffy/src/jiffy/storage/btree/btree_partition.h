#ifndef JIFFY_BTREE_SERVICE_SHARD_H
#define JIFFY_BTREE_SERVICE_SHARD_H

#include <string>
#include <jiffy/utils/property_map.h>
//#include "serde/serde.h"
//#include "serde/binary_serde.h"
#include "jiffy/storage/partition.h"
#include "jiffy/persistent/persistent_service.h"
#include "jiffy/storage/chain_module.h"
#include "btree_defs.h_defs.h"
//#include "serde/csv_serde.h"

namespace jiffy {
namespace storage {

class btree_partition : public chain_module {

};

}
}

#endif //JIFFY_BTREE_SERVICE_SHARD_H
