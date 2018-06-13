#ifndef MMUX_LOCAL_STORE_H
#define MMUX_LOCAL_STORE_H

#include "../persistent_service.h"
#include "../../storage/kv/kv_hash.h"

namespace mmux {
namespace persistent {

class local_store : public persistent_service {
 public:
  local_store(std::shared_ptr<storage::serde> ser);

  void write(const storage::locked_hash_table_type &table, const std::string &out_path) override;

  void read(const std::string &in_path, storage::locked_hash_table_type &table) override;

  std::string URI() override;
};

}
}

#endif //MMUX_LOCAL_STORE_H
