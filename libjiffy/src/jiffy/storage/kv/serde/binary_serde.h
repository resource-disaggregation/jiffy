#ifndef JIFFY_BINARY_SERIALIZER_H
#define JIFFY_BINARY_SERIALIZER_H

#include "serde.h"

namespace jiffy {
namespace storage {
/* Binary serializer/deserializer class
 * Inherited from serde class */
class binary_serde : public serde {
 public:
  binary_serde() = default;

  virtual ~binary_serde() = default;
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

#endif //JIFFY_BINARY_SERIALIZER_H
