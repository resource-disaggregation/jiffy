#ifndef ELASTICMEM_DIRECTORY_UTILS_H
#define ELASTICMEM_DIRECTORY_UTILS_H

#include <string>
#include <vector>
#include <fstream>

namespace mmux {
namespace utils {

class directory_utils {
 public:
  static const char PATH_SEPARATOR = '/';

  static void copy_file(const std::string &source, const std::string &dest) {
    std::ifstream src(source, std::ios::binary);
    std::ofstream dst(dest, std::ios::binary);

    dst << src.rdbuf();

    src.close();
    dst.close();
  }

  static std::string get_filename(const std::string &path) {
    auto tmp = path;
    return pop_path_element(tmp);
  }

  static std::string get_parent_path(const std::string &path) {
    auto tmp = path;
    pop_path_element(tmp);
    return tmp;
  }

  static std::string normalize_path(const std::string &path) {
    std::string ret = path;
    while (ret.rbegin() != ret.rend() && *ret.rbegin() == PATH_SEPARATOR)
      ret.pop_back();
    return ret;
  }

  static std::vector<std::string> path_elements(const std::string &path) {
    std::vector<std::string> result;
    auto tmp = path;
    std::string elem;
    while (!tmp.empty())
      if (!(elem = pop_path_element(tmp)).empty())
        result.insert(result.begin(), elem);
    return result;
  }

  static void push_path_element(std::string &path, const std::string &element) {
    path = path + PATH_SEPARATOR + element;
  }

  static std::string pop_path_element(std::string &path) {
    path = normalize_path(path);

    if (path.empty())
      return "";

    auto i = path.length() - 1;
    while (path[i] != PATH_SEPARATOR && i > 0) {
      --i;
    }
    auto element = path.substr(i + 1, path.length() - i - 1);
    path = path.substr(0, i + 1);
    return element;
  }
};

}
}

#endif //ELASTICMEM_DIRECTORY_UTILS_H
