#ifndef JIFFY_COMMAND_H
#define JIFFY_COMMAND_H

#include <cstdint>
#include <cstring>
#include <unordered_map>

namespace jiffy {
namespace storage {

/**
 * Command type
 * Mutator can be read and written
 * Accessor can only be read
 */
enum command_type : uint8_t {
  accessor = 0,
  mutator = 1
};

/**
 * Command structure
 */
struct command_info {
  command_type type;
  uint32_t id;

  /**
   * @brief Checks if the command is an accessor.
   * @return True if the command is an accessor, false otherwise.
   */
  bool is_accessor() const;

  /**
   * @brief Checks if the command is a mutator.
   * @return True if the command is an accessor, false otherwise.
   */
  bool is_mutator() const;
};

typedef std::unordered_map<std::string, command_info> command_map;

}

}

#endif //JIFFY_COMMAND_H
