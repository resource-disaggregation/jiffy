#ifndef MMUX_CSV_SERIALIZER_H
#define MMUX_CSV_SERIALIZER_H

#include <sstream>
#include "serde.h"

namespace mmux {
namespace storage {

class csv_serde : public serde {
 public:
  csv_serde() = default;
  size_t serialize(const block_type &table, std::shared_ptr<std::ostream> path) override;
  size_t deserialize(std::shared_ptr<std::istream> in, block_type &table) override;

 private:
  inline std::vector<std::string> split(const std::string &s, char delim, size_t count) {
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

  inline std::vector<std::string> split(const std::string &s, char delim) {
    return split(s, delim, UINT64_MAX);
  }
};

}
}

#endif //MMUX_CSV_SERIALIZER_H
