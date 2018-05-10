#include "local_store.h"
#include "../../utils/directory_utils.h"

namespace mmux {
namespace persistent {

using namespace utils;
void local_store::write(const std::string &local_path, const std::string &remote_path) {
  directory_utils::copy_file(local_path, remote_path);
}

void local_store::read(const std::string &remote_path, const std::string &local_path) {
  directory_utils::copy_file(remote_path, local_path);
}

void local_store::remove(const std::string &remote_path) {
  std::remove(remote_path.c_str());
}

}
}
