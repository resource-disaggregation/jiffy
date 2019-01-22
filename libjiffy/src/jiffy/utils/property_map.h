#ifndef JIFFY_PROPERTY_MAP_H
#define JIFFY_PROPERTY_MAP_H

#include <string>
#include <map>
#include "string_utils.h"

namespace jiffy {
namespace utils {

/* Property map class */
class property_map {
 public:
  /**
   * @brief Constructor.
   * @param properties Initial properties.
   */
  property_map(std::map<std::string, std::string> properties = {});

  /**
   * @brief Set the property value for specified key.
   * @param key Property key.
   * @param value Property value.
   */
  void set(const std::string& key, const std::string& value);

  /**
   * @brief Get the property value for specified key.
   * @param key Property key.
   * @param default_value Default property value.
   * @return Property value if it exists, otherwise the default property value.
   */
  std::string get(const std::string& key, const std::string default_value = "") const;

  /**
   * @brief Get property value as specified type.
   * @tparam T Property type.
   * @param key Property key.
   * @param default_value Default property value.
   * @return Property value if it exists, otherwise the default property value.
   */
  template<typename T>
  T get_as(const std::string& key, const T& default_value = T()) const {
    auto it = properties_.find(key);
    if (it == properties_.end()) {
      return default_value;
    }
    return string_utils::lexical_cast<T>(it->second);
  }

 private:
  /* The internal storage for the property map */
  std::map<std::string, std::string> properties_;
};

}
}
#endif //JIFFY_PROPERTY_MAP_H
