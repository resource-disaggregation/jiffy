#ifndef JIFFY_SERDE_H
#define JIFFY_SERDE_H

#include "jiffy/storage/hashtable/hash_table_defs.h"

namespace jiffy {
namespace storage {
/* Virtual class for Custom serializer/deserializer */
class serde {
 public:
  typedef locked_hash_table_type block_type;
  virtual ~serde() = default;
  virtual std::size_t serialize(const block_type &table, std::shared_ptr<std::ostream> out) = 0;
  virtual std::size_t deserialize(std::shared_ptr<std::istream> in, block_type &table) = 0;
};

}
}

#endif //JIFFY_SERDE_H
