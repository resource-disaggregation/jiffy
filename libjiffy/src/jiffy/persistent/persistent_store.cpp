#include "persistent_store.h"

namespace jiffy {
namespace persistent {

std::shared_ptr<persistent_service> persistent_store::instance(const std::string &path,
                                                               std::shared_ptr<storage::serde> ser) {
  auto uri = decompose_path(path).first;
  if (uri == "local") {
    return std::make_shared<local_store>(std::move(ser));
  } else if (uri == "s3") {
    return std::make_shared<s3_store>(std::move(ser));
  }
  return nullptr;
}

std::pair<std::string, std::string> persistent_store::decompose_path(const std::string &path) {
  std::string separator = ":/";
  std::size_t pos = path.find(separator);
  if (pos == std::string::npos) {
    throw std::invalid_argument("Invalid path format " + path);
  }
  std::string uri = path.substr(0, pos);
  std::size_t key_pos = pos + separator.length();
  std::size_t key_len = path.length() - separator.length() - uri.length();
  std::string key = path.substr(key_pos, key_len);
  return std::make_pair(uri, key);
}
}
}
