#ifndef MMUX_BINARY_SERIALIZER_H
#define MMUX_BINARY_SERIALIZER_H

#include "serde.h"

namespace mmux {
namespace storage {
/* Binary serialization and deserialization class
 * Inherited from serde */
class binary_serde : public serde {
 public:
  binary_serde() = default;
  /**
   * @brief Binary serialization
   * @param table Lock hash table
   * @param out Ostream
   * @return Ostream position
   */

  size_t serialize(const block_type &table, std::shared_ptr<std::ostream> out) override;

  /**
   * @brief Binary deserialization
   * @param in Istream
   * @param table Lock hash table
   * @return Istream position
   */

  size_t deserialize(std::shared_ptr<std::istream> in, block_type &table) override;
};
}
}

#endif //MMUX_BINARY_SERIALIZER_H
