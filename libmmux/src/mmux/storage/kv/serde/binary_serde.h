#ifndef MMUX_BINARY_SERIALIZER_H
#define MMUX_BINARY_SERIALIZER_H

#include "serde.h"

namespace mmux {
namespace storage {
/* Binary serializer/deserializer class
 * Inherited from serde class */
class binary_serde : public serde {
 public:
  binary_serde() = default;
  /**
   * @brief Binary serialization
   * @param table Locked hash table
   * @param out Output stream
   * @return Output stream position
   */

  size_t serialize(const block_type &table, std::shared_ptr<std::ostream> out) override;

  /**
   * @brief Binary deserialization
   * @param in Input stream
   * @param table Locked hash table
   * @return Input stream position
   */

  size_t deserialize(std::shared_ptr<std::istream> in, block_type &table) override;
};
}
}

#endif //MMUX_BINARY_SERIALIZER_H