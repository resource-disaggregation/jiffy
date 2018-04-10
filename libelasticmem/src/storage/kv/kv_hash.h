#ifndef ELASTICMEM_KV_HASH_H
#define ELASTICMEM_KV_HASH_H

#include <libcuckoo/cuckoohash_map.hh>

namespace elasticmem {
namespace storage {

typedef cuckoohash_map<std::string, std::string> hash_table_type;
typedef hash_table_type::locked_table locked_hash_table_type;

}
}

#endif //ELASTICMEM_KV_HASH_H
