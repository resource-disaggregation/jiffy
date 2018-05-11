#ifndef MMUX_KV_HASH_H
#define MMUX_KV_HASH_H

#include <libcuckoo/cuckoohash_map.hh>

namespace mmux {
namespace storage {

typedef cuckoohash_map<std::string, std::string> hash_table_type;
typedef hash_table_type::locked_table locked_hash_table_type;

}
}

#endif //MMUX_KV_HASH_H
