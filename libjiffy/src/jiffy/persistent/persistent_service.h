#ifndef JIFFY_PERSISTENT_SERVICE_H
#define JIFFY_PERSISTENT_SERVICE_H

#include <string>
#include "jiffy/storage/hashtable/hash_table_defs.h"
#include "jiffy/storage/hashtable/serde/serde.h"

namespace jiffy {
namespace persistent {
/* Persistent service virtual class */
class persistent_service {
 public:
  persistent_service(std::shared_ptr<storage::serde> ser): ser_(std::move(ser)) {}

  virtual ~persistent_service() = default;

  virtual void write(const storage::locked_hash_table_type &table, const std::string &out_path) = 0;

  virtual void read(const std::string &in_path, storage::locked_hash_table_type &table) = 0;

  virtual std::string URI() = 0;

  /**
   * @brief  Fetch custom serializer/deserializer
   * @return Custom serializer/deserializer
   */
  std::shared_ptr<storage::serde> serde() {
    return ser_;
  }

 private:
  /* Custom serializer/deserializer */
  std::shared_ptr<storage::serde> ser_;
};

}
}

#endif //JIFFY_PERSISTENT_SERVICE_H
