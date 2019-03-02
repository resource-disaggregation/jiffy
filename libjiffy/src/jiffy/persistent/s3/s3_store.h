#ifndef JIFFY_S3_STORE_H
#define JIFFY_S3_STORE_H

#include <aws/core/Aws.h>
#include "jiffy/persistent/persistent_service.h"

namespace jiffy {
namespace persistent {
/* s3_store class, inherited from persistent_service class */
class s3_store_impl : public persistent_service {
 public:

  /**
   * @brief Destructor
   */

  ~s3_store_impl();
 protected:


  /**
   * @brief Constructor
   * @param ser Custom serializer/deserializer
   */

  s3_store_impl(std::shared_ptr<storage::serde> ser);


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

using s3_store = derived_persistent<s3_store_impl>;

}
}

#endif //JIFFY_S3_STORE_H
