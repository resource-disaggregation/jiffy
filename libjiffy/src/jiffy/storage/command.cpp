#include "command.h"

namespace jiffy {
namespace storage {

bool command_info::is_accessor() const {
  return type == command_type::accessor;
}

bool command_info::is_mutator() const {
  return type == command_type::mutator;
}

}
}