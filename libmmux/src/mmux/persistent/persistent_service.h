#ifndef MMUX_PERSISTENT_SERVICE_H
#define MMUX_PERSISTENT_SERVICE_H

#include <string>
#include "../storage/kv/kv_hash.h"
#include "../storage/kv/serde/serde.h"

namespace mmux {
namespace persistent {

class persistent_service {
 public:
  persistent_service(std::shared_ptr<storage::serde> ser): ser_(std::move(ser)) {}

  virtual ~persistent_service() = default;

  virtual void write(const storage::locked_hash_table_type &table, const std::string &out_path) = 0;

  virtual void read(const std::string &in_path, storage::locked_hash_table_type &table) = 0;

  virtual std::string URI() = 0;

  std::shared_ptr<storage::serde> serde() {
    return ser_;
  }

 private:
  std::shared_ptr<storage::serde> ser_;
};

}
}

#endif //MMUX_PERSISTENT_SERVICE_H
