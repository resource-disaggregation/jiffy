#ifndef JIFFY_B_TREE_H
#define JIFFY_B_TREE_H

#include <functional>
#include <jiffy/utils/logger.h>
#include "btree/btree_map.h"
#include "jiffy/storage/block_memory_allocator.h"
#include "jiffy/storage/types/binary.h"
#include "jiffy/utils/logger.h"

namespace jiffy {
namespace storage {
using namespace utils;
// The default number of btree target node size
const size_t NODE_SIZE = 256;
/*
// The maximum key length
const size_t MAX_KEY_LENGTH = 1024;

// The min and max keys
const std::string MIN_KEY;
const std::string MAX_KEY(MAX_KEY_LENGTH, 0x7f);
*/

// TODO make it general
const size_t MAX_KEY_LENGTH = 32;
const std::string MIN_KEY = "a";
const std::string MAX_KEY(MAX_KEY_LENGTH, 'z');
const std::string default_name = MIN_KEY + "_" + MAX_KEY;
// Key/Value definitions
typedef binary key_type;
typedef binary value_type;
typedef std::pair<const key_type, value_type> btree_pair_type;

// Custom template arguments
struct less_type {
  template<typename KeyType1, typename KeyType2>
  bool operator()(const KeyType1& lhs, const KeyType2& rhs) const {
    /*
    LOG(log_level::info) << "Into the less type";
    char l[lhs.size() + 1];
    char r[rhs.size() + 1];
    l[lhs.size()] = '\0';
    r[rhs.size()] = '\0';
    memcpy(l, lhs.data(), lhs.size());
    std::string left(l);
    LOG(log_level::info) << "left string" << left;
    memcpy(r, rhs.data(), rhs.size());
    std::string right(r);
    LOG(log_level::info) << "right string" << right;
    LOG(log_level::info) << "Compare result" << (strcmp(l, r) < 0);
    return strcmp(l, r) < 0;
     */
    //LOG(log_level::info) << (strcmp(reinterpret_cast<const char*>(lhs.data()), reinterpret_cast<const char*>(rhs.data())) < 0);
    return strcmp(reinterpret_cast<const char*>(lhs.data()), reinterpret_cast<const char*>(rhs.data())) < 0;
  }
};
typedef block_memory_allocator<btree_pair_type> bt_allocator_type;

// Btree definitions
typedef btree::btree_map<key_type, value_type, less_type, bt_allocator_type, NODE_SIZE> btree_type;
//typedef btree::btree_map<key_type, value_type, less_type> btree_type;

}
}

#endif //JIFFY_B_TREE_H
