#ifndef MMUX_BINARY_SERIALIZER_H
#define MMUX_BINARY_SERIALIZER_H

#include "serde.h"

namespace mmux {
namespace storage {

class binary_serde : public serde {
 public:
  binary_serde() = default;
  size_t serialize(const block_type &table, std::shared_ptr<std::ostream> out) override;
  size_t deserialize(std::shared_ptr<std::istream> in, block_type &table) override;
};
}
}

#endif //MMUX_BINARY_SERIALIZER_H
