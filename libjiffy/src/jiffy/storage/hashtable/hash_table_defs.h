#ifndef JIFFY_KV_HASH_H
#define JIFFY_KV_HASH_H

#include <functional>
#include "libcuckoo/cuckoohash_map.hh"
#include "jiffy/storage/block_memory_allocator.h"
#include "jiffy/storage/types/binary.h"
#include <unordered_map>

namespace jiffy {
namespace storage {

// The default number of elements in an empty hash table
constexpr size_t HASH_TABLE_DEFAULT_SIZE = 0;

// Hash table max key size
constexpr size_t HASH_TABLE_MAX_KEY_SIZE = 65536;

// Key/Value definitions
typedef binary key_type;
typedef binary value_type;
typedef std::pair<const key_type, value_type> kv_pair_type;

// Custom template arguments
struct hash_type {
  template<typename KeyType>
  std::size_t operator()(const KeyType &k) const {
    std::size_t result = 0;
    for (size_t i = 0; i < k.size(); i++) {
      result = k[i] + (result * 31);
    }
    return result;
  }
};

struct equal_type {
  template<typename KeyType1, typename KeyType2>
  bool operator()(const KeyType1 &lhs, const KeyType2 &rhs) const {
    return lhs.size() == rhs.size() && memcmp(lhs.data(), rhs.data(), lhs.size()) == 0;
  }
};

// Hash table definitions
typedef std::unordered_map<key_type, value_type, hash_type, equal_type> hash_table_type;

}
}

#endif //JIFFY_KV_HASH_H
