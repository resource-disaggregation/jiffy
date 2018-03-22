#ifndef ELASTICMEM_SERDE_H
#define ELASTICMEM_SERDE_H

#include <libcuckoo/cuckoohash_map.hh>

namespace elasticmem {
namespace storage {

class serializer {
 public:
  typedef cuckoohash_map<std::string, std::string>::locked_table block_type;
  virtual std::size_t serialize(const block_type &table, const std::string &path) = 0;
};

class deserializer {
 public:
  typedef cuckoohash_map<std::string, std::string>::locked_table block_type;
  virtual std::size_t deserialize(const std::string &path, block_type &table) = 0;
};

}
}

#endif //ELASTICMEM_SERDE_H
