#ifndef JIFFY_S3_STORE_H
#define JIFFY_S3_STORE_H

#include <aws/core/Aws.h>
#include "../persistent_service.h"

namespace jiffy {
namespace persistent {
/* s3_store class, inherited from persistent_service class */
class s3_store : public persistent_service {
 public:

  /**
   * @brief Constructor
   * @param ser Custom serializer/deserializer
   */

  s3_store(std::shared_ptr<storage::serde> ser);

  /**
   * @brief Destructor
   */

  ~s3_store();

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
 private:
  /**
   * @brief Extract path element
   * @param s3_path s3 path
   * @return Pair of bucket name and key
   */
  std::pair<std::string, std::string> extract_path_elements(const std::string &s3_path);
  /* AWS SDK options */
  Aws::SDKOptions options_;
};

}
}

#endif //JIFFY_S3_STORE_H
