#ifndef JIFFY_LOCAL_STORE_H
#define JIFFY_LOCAL_STORE_H

#include "jiffy/persistent/persistent_service.h"
#include "jiffy/storage/hashtable/hash_table_defs.h"

namespace jiffy {
namespace persistent {

/* Local store, inherited persistent_service */
class local_store : public persistent_service {
 public:

  /**
   * @brief Constructor
   * @param ser Custom serializer/deserializer
   */

  local_store(std::shared_ptr<storage::serde> ser);

  /**
   * @brief Write data from hash table to persistent storage
   * @param table Hash table
   * @param out_path Output persistent storage path
   */

  void write(const storage::locked_hash_table_type &table, const std::string &out_path) override;

  /**
   * @brief Read data from persistent storage to hash table
   * @param in_path Input persistent storage path
   * @param table Hash table
   */

  void read(const std::string &in_path, storage::locked_hash_table_type &table) override;

  /**
   * @brief Fetch URI
   * @return URI string
   */

  std::string URI() override;
};

}
}

#endif //JIFFY_LOCAL_STORE_H
