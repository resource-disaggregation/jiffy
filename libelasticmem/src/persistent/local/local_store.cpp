#include "local_store.h"
#include <experimental/filesystem>

namespace elasticmem {
namespace persistent {

void local_store::write(const std::string &local_path, const std::string &remote_path) {
  namespace fs = std::experimental::filesystem;
  fs::path src(local_path);
  fs::path dst(remote_path);
  fs::copy(src, dst);
}

void local_store::read(const std::string &remote_path, const std::string &local_path) {
  namespace fs = std::experimental::filesystem;
  fs::path src(remote_path);
  fs::path dst(local_path);
  fs::copy(src, dst);
}

void local_store::remove(const std::string &remote_path) {
  namespace fs = std::experimental::filesystem;
  fs::path src(remote_path);
  fs::remove_all(src);
}

}
}
