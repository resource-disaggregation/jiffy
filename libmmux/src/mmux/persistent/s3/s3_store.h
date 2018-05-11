#ifndef ELASTICMEM_S3_STORE_H
#define ELASTICMEM_S3_STORE_H

#include <aws/core/Aws.h>
#include "../persistent_service.h"

namespace mmux {
namespace persistent {

class s3_store : public persistent_service {
 public:
  s3_store();
  ~s3_store();
  void write(const std::string &local_path, const std::string &remote_path) override;
  void read(const std::string &remote_path, const std::string &local_path) override;
  void remove(const std::string &remote_path) override;

 private:
  std::pair<std::string, std::string> extract_s3_path_elements(const std::string& s3_path);

  Aws::SDKOptions options_;
};

}
}

#endif //ELASTICMEM_S3_STORE_H
