#ifndef JIFFY_B_TREE_H
#define JIFFY_B_TREE_H

#include <functional>
#include "btree/btree_map.h"
#include "jiffy/storage/block_memory_allocator.h"
#include "jiffy/storage/types/binary.h"

namespace jiffy {
namespace storage {

// The default number of btree target node size
const size_t NODE_SIZE = 256;

// The maximum key length
const size_t MAX_KEY_LENGTH = 1024;

// The min and max keys
const std::string MIN_KEY;
const std::string MAX_KEY(MAX_KEY_LENGTH, 0x7f);

// Key/Value definitions
typedef binary key_type;
typedef binary value_type;
typedef std::pair<const key_type, value_type> btree_pair_type;

// Custom template arguments
struct less_type {
  template<typename KeyType1, typename KeyType2>
  bool operator()(const KeyType1& lhs, const KeyType2& rhs) const {
    return strcmp(reinterpret_cast<const char*>(lhs.data()), reinterpret_cast<const char*>(rhs.data())) < 0;
  }
};
typedef block_memory_allocator<btree_pair_type> bt_allocator_type;

// Btree definitions
typedef btree::btree_map<key_type, value_type, less_type, bt_allocator_type, NODE_SIZE> btree_type;

}
}

#endif //JIFFY_B_TREE_H
