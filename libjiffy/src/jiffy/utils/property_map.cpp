#include "property_map.h"

namespace jiffy {
namespace utils {

property_map::property_map(std::map<std::string, std::string> properties)
    : properties_(properties) {}

void property_map::set(const std::string &key, const std::string &value) {
  properties_[key] = value;
}

std::string property_map::get(const std::string &key, const std::string default_value) const {
  auto it = properties_.find(key);
  if (it == properties_.end()) {
    return default_value;
  }
  return it->second;
}

}
}
