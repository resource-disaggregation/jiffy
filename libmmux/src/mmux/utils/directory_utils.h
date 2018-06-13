#ifndef MMUX_DIRECTORY_UTILS_H
#define MMUX_DIRECTORY_UTILS_H

#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <vector>
#include <fstream>
#include <regex>

namespace mmux {
namespace utils {

class directory_utils {
 public:
  static const char PATH_SEPARATOR = '/';

  static void check_path(const std::string &path) {
    if (!std::regex_match(path, std::regex("^(/[^/ ]*)+/?$"))) {
      throw std::invalid_argument("Malformed path: " + path);
    }
  }

  static void copy_file(const std::string &source, const std::string &dest) {
    std::ifstream src(source, std::ios::binary);
    std::ofstream dst(dest, std::ios::binary);

    dst << src.rdbuf();

    src.close();
    dst.close();
  }

  static void create_directory(const std::string &path) {
    char tmp[256];
    char *p = NULL;
    size_t len;
    snprintf(tmp, sizeof(tmp), "%s", path.c_str());
    len = strlen(tmp);
    if (tmp[len - 1] == '/')
      tmp[len - 1] = 0;
    for (p = tmp + 1; *p; p++)
      if (*p == '/') {
        *p = 0;
        mkdir(tmp, 0755);
        *p = '/';
      }
    mkdir(tmp, 0755);
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
    check_path(path);
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

#endif //MMUX_DIRECTORY_UTILS_H
