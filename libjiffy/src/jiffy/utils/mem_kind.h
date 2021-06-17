#ifndef JIFFY_MEM_KIND_H
#define JIFFY_MEM_KIND_H

#include <cstdlib>
#include <string>

namespace jiffy {
namespace utils {

class mem_kind {
 public:
  static std::string mmap_dir;
  static size_t max_arena_size;
  static size_t num_arenas;

  static void init(std::string dir, size_t max_size, size_t n_arenas);

  static void destroy();

  static std::string path(unsigned arena_idx);

  static void *map(unsigned int arena_idx, void *addr, size_t size);

  static int unmap(unsigned int arena_idx, void *addr, size_t size);
};

}
}

#endif //JIFFY_MEM_KIND_H
