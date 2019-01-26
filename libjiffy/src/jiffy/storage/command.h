#ifndef JIFFY_COMMAND_H
#define JIFFY_COMMAND_H

#include <cstdint>
#include <cstring>

#define MAX_BLOCK_OP_NAME_SIZE 63

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
struct command {
  command_type type;
  char name[MAX_BLOCK_OP_NAME_SIZE];

  /**
   * @brief Operator < to check if name is smaller in Lexicographical order
   * @param other Other block operation
   * @return Bool value
   */

  bool operator<(const command &other) const;
};

}

}

#endif //JIFFY_COMMAND_H
