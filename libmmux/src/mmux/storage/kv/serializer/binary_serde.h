#ifndef MMUX_BINARY_SERIALIZER_H
#define MMUX_BINARY_SERIALIZER_H

#include "serde.h"
namespace mmux {
namespace storage {

class binary_serializer : public serializer {
 public:
  binary_serializer() = default;
  size_t serialize(const block_type &table, const std::string &path) override;

};

class binary_deserializer : public deserializer {
 public:
  binary_deserializer() = default;
  size_t deserialize(const std::string &path, block_type &table) override;
};
}
}

#endif //MMUX_BINARY_SERIALIZER_H
