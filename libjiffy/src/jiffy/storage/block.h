#ifndef JIFFY_MEMORY_BLOCK_H
#define JIFFY_MEMORY_BLOCK_H

#include <string>
#include <vector>
#include <jiffy/utils/property_map.h>
#include "chain_module.h"
#include "partition.h"

namespace jiffy {
namespace storage {

class block {
 public:
  /**
   * @brief Constructor.
   * @param id The identifier for the block.
   * @param capacity The block memory capacity.
   */
  block(const std::string& id, const size_t capacity = 134217728);

  /**
   * @brief Get memory block identifier.
   * @return Memory block identifier.
   */
  const std::string& id() const;

  /**
   * @brief Get the underlying partition implementation.
   * @return The underlying partition implementation.
   */
  std::shared_ptr<chain_module> impl();

  /**
   * @brief Set the underlying partition implementation.
   * @param type The type of the partition.
   * @param conf Configuration parameters for constructing the partition.
   */
  void setup(const std::string &type,
             const std::string &name,
             const std::string &metadata,
             const utils::property_map &conf);

  /**
   * @brief Destroy the underlying implementation.
   */
  void destroy();

  /**
   * @brief Get the capacity of the block.
   * @return The capacity of the block.
   */
  size_t capacity() const;

  /**
   * @brief Get the used number of bytes of the block.
   * @return
   */
  size_t used() const;

 private:
  std::string id_;
  block_memory_manager manager_;
  std::shared_ptr<chain_module> impl_;
};

}
}

#endif //JIFFY_MEMORY_BLOCK_H
