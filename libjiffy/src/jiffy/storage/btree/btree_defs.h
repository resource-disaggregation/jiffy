#ifndef JIFFY_B_TREE_H
#define JIFFY_B_TREE_H

#include <functional>
#include "cpp-btree-1.0.1/btree_map.h" //TODO cannot find this header file
#include "jiffy/storage/block_memory_allocator.h"

namespace jiffy {
namespace storage {

// The default number of elements in an empty btree
constexpr size_t BTREE_DEFAULT_SIZE = 0;

// The default number of btree target node size
const size_t NodeSize = 256;

// Key/Value definitions
typedef std::string key_type;
typedef std::string value_type;
typedef std::pair<const key_type, value_type> btree_pair_type;

// Custom template arguments
typedef std::less<std::string> less_type;
typedef block_memory_allocator<kv_pair_type> allocator_type;

// Btree definitions
typedef btree_map<key_type, value_type, less_type, allocator_type, NodeSize> btree_type;

}
}

#endif //JIFFY_B_TREE_H
