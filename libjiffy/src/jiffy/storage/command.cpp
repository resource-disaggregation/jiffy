#include "command.h"

namespace jiffy {
namespace storage {

bool command::operator<(const command &other) const {
  return std::strcmp(name, other.name) < 0;
}

}
}