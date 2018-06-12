#ifndef MMUX_SERDE_H
#define MMUX_SERDE_H

#include "../kv_hash.h"

namespace mmux {
namespace storage {

class serde {
 public:
  typedef locked_hash_table_type block_type;
  virtual std::size_t serialize(const block_type &table, std::shared_ptr<std::ostream> out) = 0;
  virtual std::size_t deserialize(std::shared_ptr<std::istream> in, block_type &table) = 0;
};

}
}

#endif //MMUX_SERDE_H
