#include "local_store.h"

namespace elasticmem {
namespace kv {

void local_store::write(const std::string &, const std::string &) {
  // No-op
}

void local_store::read(const std::string &, const std::string &) {
  // No-op
}

void local_store::remove(const std::string &) {
  // No-op
}

}
}
