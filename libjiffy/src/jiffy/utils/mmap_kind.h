#ifndef JIFFY_MMAP_KIND_H
#define JIFFY_MMAP_KIND_H

#include <vector>
#include <string>
#include <map>

#define MALLOCX_ARENA_MAX 0xffe
#define INVALID_ARENA_IDX unsigned(-1)

namespace jiffy {
namespace utils {

class mmap_kind {
 public:
  struct mmap_arena_ctx {
    unsigned idx = INVALID_ARENA_IDX;
    int fd = -1;
    size_t cur_size = 0;
    size_t offset = 0;
  };

  static std::string mmap_dir;
  static size_t max_arena_size;
  static size_t num_arenas;
  static mmap_arena_ctx registry[MALLOCX_ARENA_MAX];

  static void init(std::string dir, size_t max_size, size_t n_arenas);

  static void destroy();

  static std::string path(unsigned arena_idx);

  static void *map(unsigned int arena_idx, void *addr, size_t size);

  static int unmap(unsigned int arena_idx, void *addr, size_t size);
};

}
}

#endif //JIFFY_MMAP_KIND_H
