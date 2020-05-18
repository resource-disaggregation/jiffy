#include "maestro_ops_exception.h"

namespace jiffy {
  namespace maestro {

    maestro_ops_exception::maestro_ops_exception(std::string msg) : msg_(std::move(msg)) {}

    char const *maestro_ops_exception::what() const noexcept {
      return msg_.c_str();
    }

  }
}