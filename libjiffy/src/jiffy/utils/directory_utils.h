#ifndef JIFFY_DIRECTORY_UTILS_H
#define JIFFY_DIRECTORY_UTILS_H

#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <vector>
#include <fstream>
#include <regex>

namespace jiffy {
namespace utils {
/* Directory utility class */
class directory_utils {
 public:
  static const char PATH_SEPARATOR = '/';

  /**
   * @brief Check if path is in correct format
   * @param path Path
   */

  static void check_path(const std::string &path) {
    if (!std::regex_match(path, std::regex("^(/[^/ ]*)+/?$"))) {
      throw std::invalid_argument("Malformed path: " + path);
    }
  }

  /**
   * @brief Copy file from source to destination
   * @param source Source file
   * @param dest Destination file
   */

  static void copy_file(const std::string &source, const std::string &dest) {
    std::ifstream src(source, std::ios::binary);
    std::ofstream dst(dest, std::ios::binary);

    dst << src.rdbuf();

    src.close();
    dst.close();
  }

  /**
   * @brief Create a directory under path
   * @param path Path
   */

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

  /**
   * Fetch file name
   * @param path File path
   * @return File name
   */

  static std::string get_filename(const std::string &path) {
    auto tmp = path;
    return pop_path_element(tmp);
  }

  /**
   * Fetch parent path
   * @param path File path
   * @return Parent path
   */

  static std::string get_parent_path(const std::string &path) {
    auto tmp = path;
    pop_path_element(tmp);
    return tmp;
  }

  /**
   * @brief Normalize path
   * @param path Path
   * @return Normalized Path
   */

  static std::string normalize_path(const std::string &path) {
    std::string ret = path;
    while (ret.rbegin() != ret.rend() && *ret.rbegin() == PATH_SEPARATOR)
      ret.pop_back();
    return ret;
  }

  /**
   * @brief Fetch path elements
   * @param path Path
   * @return Path elements
   */

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

  /**
   * @brief Add element to path
   * @param path Path
   * @param element Element
   */

  static void push_path_element(std::string &path, const std::string &element) {
    path = path + PATH_SEPARATOR + element;
  }

  /**
   * Pop file name
   * Path modified to parent path
   * @param path File path
   * @return File name
   */

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

  // Get root element of path
  static std::string get_root(const std::string &path) {
    auto elems = path_elements(path);
    if(elems.size() == 0) {
      return "";
    }
    return elems[0];
  }
};

}
}

#endif //JIFFY_DIRECTORY_UTILS_H
