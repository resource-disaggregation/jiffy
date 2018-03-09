#ifndef ELASTICMEM_DIRECTORY_UTILS_H
#define ELASTICMEM_DIRECTORY_UTILS_H

#include <string>
#include <experimental/filesystem>

namespace elasticmem {
namespace directory {

class directory_utils {
 public:
  static std::string normalize_path(const std::string &path) {
    namespace fs = std::experimental::filesystem;
    std::string ret = path;
    while (ret.rbegin() != ret.rend() && *ret.rbegin() == fs::path::preferred_separator)
      ret.pop_back();
    return ret;
  }

  static std::vector<std::string> path_elements(const std::string &path) {
    namespace fs = std::experimental::filesystem;
    fs::path p(normalize_path(path));
    std::vector<std::string> out;
    for (auto &name: p.relative_path()) {
      out.emplace_back(name);
    }
    return out;
  }

  static void push_path_element(std::string &path, const std::string& element) {
    namespace fs = std::experimental::filesystem;
    path = path + fs::path::preferred_separator + element;
  }

  static std::string pop_path_element(std::string &path) {
    namespace fs = std::experimental::filesystem;
    path = normalize_path(path);
    fs::path p(path);
    auto element = p.filename().generic_string();
    path = p.remove_filename().generic_string();
    return element;
  }
};

}
}

#endif //ELASTICMEM_DIRECTORY_UTILS_H
