#ifndef ELASTICMEM_CSV_SERIALIZER_H
#define ELASTICMEM_CSV_SERIALIZER_H

#include <sstream>
#include "serde.h"

namespace elasticmem {
namespace kv {

class csv_serializer : public serializer {
 public:
  csv_serializer() = default;
  size_t serialize(const block_type &table, const std::string &path) override;

};

class csv_deserializer : public deserializer {
 public:
  csv_deserializer() = default;
  size_t deserialize(const std::string &path, block_type &table) override;

 private:
  inline std::vector<std::string> split(const std::string &s, char delim,
                                               size_t count) {
    std::stringstream ss(s);
    std::string item;
    std::vector<std::string> elems;
    size_t i = 0;
    while (std::getline(ss, item, delim) && i < count) {
      elems.push_back(std::move(item));
      i++;
    }
    while (std::getline(ss, item, delim))
      elems.back() += item;
    return elems;
  }

  inline std::vector<std::string> split(const std::string &s,
                                               char delim) {
    return split(s, delim, UINT64_MAX);
  }
};

}
}

#endif //ELASTICMEM_CSV_SERIALIZER_H