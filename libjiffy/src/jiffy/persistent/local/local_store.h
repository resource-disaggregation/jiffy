#ifndef JIFFY_LOCAL_STORE_H
#define JIFFY_LOCAL_STORE_H

#include "jiffy/persistent/persistent_service.h"

namespace jiffy {
namespace persistent {

/* Local store, inherited persistent_service */
class local_store_impl : public persistent_service {
 public:

  /**
   * @brief Constructor
   * @param ser Custom serializer/deserializer
   */

  local_store_impl(std::shared_ptr<storage::serde> ser);

 protected:
  /**
   * @brief Write data from hash table to persistent storage
   * @param table Hash table
   * @param out_path Output persistent storage path
   */
  template <typename Datatype>
  void write_impl(const Datatype &table, const std::string &out_path);

  /**
   * @brief Read data from persistent storage to hash table
   * @param in_path Input persistent storage path
   * @param table Hash table
   */
  template <typename Datatype>
  void read_impl(const std::string &in_path, Datatype &table);

 public:
  /**
   * @brief Fetch URI
   * @return URI string
   */

  std::string URI() override;
};

using local_store = derived_persistent<local_store_impl>();

}
}

#endif //JIFFY_LOCAL_STORE_H
