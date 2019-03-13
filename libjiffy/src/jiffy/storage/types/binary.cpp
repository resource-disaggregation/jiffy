#include "jiffy/storage/types/binary.h"

namespace jiffy {
namespace storage {

bool operator<(const binary &a, const std::string &b) {
  return strcmp(reinterpret_cast<const char*>(a.data()), b.c_str()) < 0;
}

bool operator<=(const binary &a, const std::string &b) {
  return strcmp(reinterpret_cast<const char*>(a.data()), b.c_str()) <= 0;
}

bool operator>(const binary &a, const std::string &b) {
  return strcmp(reinterpret_cast<const char*>(a.data()), b.c_str()) > 0;
}

bool operator>=(const binary &a, const std::string &b) {
  return strcmp(reinterpret_cast<const char*>(a.data()), b.c_str()) >= 0;
}

bool operator<(const std::string &a, const binary &b) {
  return strcmp(a.c_str(), reinterpret_cast<const char*>(b.data())) < 0;
}

bool operator<=(const std::string &a, const binary &b) {
  return strcmp(a.c_str(), reinterpret_cast<const char*>(b.data())) <= 0;
}

bool operator>(const std::string &a, const binary &b) {
  return strcmp(a.c_str(), reinterpret_cast<const char*>(b.data())) > 0;
}

bool operator>=(const std::string &a, const binary &b) {
  return strcmp(a.c_str(), reinterpret_cast<const char*>(b.data())) >= 0;
}

bool operator==(const binary &a, const std::string &b) {
  return strcmp(reinterpret_cast<const char*>(a.data()), b.c_str()) == 0;
}

bool operator==(const std::string &a, const binary &b) {
  return strcmp(a.c_str(), reinterpret_cast<const char*>(b.data())) == 0;
}

std::string to_string(const binary &bin) {
  return std::string(reinterpret_cast<const char *>(bin.data()), bin.size());
}

}
}