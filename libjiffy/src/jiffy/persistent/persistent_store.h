#ifndef JIFFY_PERSISTENT_STORE_H
#define JIFFY_PERSISTENT_STORE_H

#include <memory>
#include "persistent_service.h"

namespace jiffy {
namespace persistent {

class persistent_store {
 public:

  /**
   * @brief Fetch persistent service instance
   * @param path Path
   * @param ser Custom serializer/deserializer
   * @return Persistent service
   */

  static std::shared_ptr<persistent_service> instance(const std::string &path, std::shared_ptr<storage::serde> ser);

  /**
   * @brief Decompose path
   * @param path Path
   * @return Pair of uri and key
   */

  static std::pair<std::string, std::string> decompose_path(const std::string &path);

};

}
}

#endif //JIFFY_PERSISTENT_STORE_H
