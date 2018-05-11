#ifndef ELASTICMEM_SERDE_H
#define ELASTICMEM_SERDE_H

#include "../kv_hash.h"

namespace mmux {
namespace storage {

class serializer {
 public:
  typedef locked_hash_table_type block_type;
  virtual std::size_t serialize(const block_type &table, const std::string &path) = 0;
};

class deserializer {
 public:
  typedef locked_hash_table_type block_type;
  virtual std::size_t deserialize(const std::string &path, block_type &table) = 0;
};

}
}

#endif //ELASTICMEM_SERDE_H
