#ifndef MMUX_S3_STORE_H
#define MMUX_S3_STORE_H

#include <aws/core/Aws.h>
#include "../persistent_service.h"

namespace mmux {
namespace persistent {

class s3_store : public persistent_service {
 public:
  s3_store(std::shared_ptr<storage::serde> ser);
  ~s3_store();
  void write(const storage::locked_hash_table_type &table, const std::string &out_path) override;
  void read(const std::string &in_path, storage::locked_hash_table_type &table) override;
  std::string URI() override;
 private:
  std::pair<std::string, std::string> extract_path_elements(const std::string &s3_path);

  Aws::SDKOptions options_;
};

}
}

#endif //MMUX_S3_STORE_H
