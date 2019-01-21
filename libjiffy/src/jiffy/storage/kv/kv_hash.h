#ifndef JIFFY_KV_HASH_H
#define JIFFY_KV_HASH_H

#include <libcuckoo/cuckoohash_map.hh>

namespace jiffy {
namespace storage {

typedef cuckoohash_map<std::string, std::string> hash_table_type;
typedef hash_table_type::locked_table locked_hash_table_type;

}
}

#endif //JIFFY_KV_HASH_H
