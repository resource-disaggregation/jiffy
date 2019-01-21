#ifndef JIFFY_STRING_UTILS_H
#define JIFFY_STRING_UTILS_H

#include <algorithm>
#include <string>
#include <sstream>
#include <iostream>
#include <vector>
#include <typeinfo>

namespace jiffy {
namespace utils {
/* String utility class */
class string_utils {
 public:
  /**
   * @brief Split string
   * @param s String
   * @param delim Separation symbol
   * @param count Total parts
   * @return Separated strings
   */

  inline static std::vector<std::string> split(const std::string &s, char delim,
                                               size_t count) {
    std::stringstream ss(s);
    std::string item;
    std::vector<std::string> elems;
    size_t i = 0;
    while (std::getline(ss, item, delim) && i < count) {
      elems.push_back(std::move(item));
      i++;
    }
    while (std::getline(ss, item, delim))
      elems.back() += item;
    return elems;
  }

  /**
   * @brief Split string with default count
   * @param s String
   * @param delim Separation symbol
   * @return Separated strings
   */

  inline static std::vector<std::string> split(const std::string &s,
                                               char delim) {
    return split(s, delim, UINT64_MAX);
  }

  /**
   * @brief Combine multiple strings to one string
   * @param v Strings
   * @param delim Separation symbol
   * @return String
   */

  inline static std::string mk_string(const std::vector<std::string> &v,
                                      const std::string &delim) {
    std::string str = "";
    size_t i = 0;
    for (; i < v.size() - 1; i++) {
      str += v[i] + delim;
    }
    return str + v[i];
  }

  template<typename functor>
  /**
   * @brief Transform string according to transformation function
   * @tparam functor Transformation function type
   * @param str String
   * @param f Transformation function
   * @return String after transform
   */
  inline static std::string transform(const std::string &str, functor f) {
    std::string out;
    out.resize(str.length());
    std::transform(str.begin(), str.end(), out.begin(), f);
    return out;
  }

  /**
   * @brief Transform string to upper characters
   * @param str String
   * @return String after transformation
   */

  inline static std::string to_upper(const std::string &str) {
    return transform(str, ::toupper);
  }

  /**
   * @brief Transform string to lower characters
   * @param str String
   * @return String after transformation
   */

  inline static std::string to_lower(const std::string &str) {
    return transform(str, ::tolower);
  }

  template<typename T>
  /**
   * @brief Lexical cast of string
   * @tparam T Cast type
   * @param s String
   * @return String after cast
   */

  inline static T lexical_cast(const std::string &s) {
    std::stringstream ss(s);

    T result;
    if ((ss >> result).fail() || !(ss >> std::ws).eof()) {
      throw std::bad_cast();
    }

    return result;
  }
};

template<>
/**
 * @brief Lexical cast of string to bool
 * @param s String
 * @return Bool after cast
 */

inline bool string_utils::lexical_cast<bool>(const std::string &s) {
  std::stringstream ss(to_lower(s));

  bool result;
  if ((ss >> std::boolalpha >> result).fail() || !(ss >> std::ws).eof()) {
    throw std::bad_cast();
  }

  return result;
}

}
}

#endif //JIFFY_STRING_UTILS_H
