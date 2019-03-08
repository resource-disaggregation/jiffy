#ifndef JIFFY_B_TREE_H
#define JIFFY_B_TREE_H

#include <functional>
#include "btree/btree_map.h"
#include "jiffy/storage/block_memory_allocator.h"

namespace jiffy {
namespace storage {

// The default number of btree target node size
const size_t NODE_SIZE = 256;

// The maximum key length
const size_t MAX_KEY_LENGTH = 1024;

// The min and max keys
const std::string MIN_KEY = "";
const std::string MAX_KEY(MAX_KEY_LENGTH, 0x7f);

// Key/Value definitions
typedef std::string key_type;
typedef std::string value_type;
typedef std::pair<const key_type, value_type> btree_pair_type;

// Custom template arguments
typedef std::less<key_type> less_type;
typedef block_memory_allocator<btree_pair_type> bt_allocator_type;

// Btree definitions
typedef btree::btree_map<key_type, value_type, less_type, bt_allocator_type, NODE_SIZE> btree_type;

}
}

#endif //JIFFY_B_TREE_H
