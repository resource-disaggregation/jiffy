#include "local_store.h"
#include "../../utils/directory_utils.h"

namespace mmux {
namespace persistent {

using namespace utils;

local_store::local_store(const std::shared_ptr<storage::serde> &ser) : persistent_service(ser) {}

void local_store::write(const storage::locked_hash_table_type &table, const std::string &out_path) {
  auto out = std::make_shared<std::ofstream>(out_path);
  serde()->serialize(table, out);
}

void local_store::read(const std::string &in_path, storage::locked_hash_table_type &table) {
  auto in = std::make_shared<std::ifstream>(in_path);
  serde()->deserialize(in, table);
}

std::string local_store::URI() {
  return "lfs";
}

}
}
