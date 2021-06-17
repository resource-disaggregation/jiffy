#ifndef JIFFY_MEM_UTILS_H
#define JIFFY_MEM_UTILS_H

#include <string>
#ifdef MEMKIND_IN_USE
  #include <memkind.h>
#else
#include "mmap_kind.h"
#endif

namespace jiffy {
namespace utils {

class mem_utils {
 public:
  static void *init_kind(const std::string &memory_mode,
                         const std::string &mem_path,
                         size_t num_arenas,
                         size_t max_arena_size) {
    #ifdef MEMKIND_IN_USE
      struct memkind *pmem_kind = nullptr;
      if (memory_mode == "PMEM") {
        size_t err = memkind_create_pmem(pmem_path.c_str(),0,&pmem_kind);
        if (err) {
            char error_message[MEMKIND_ERROR_MESSAGE_SIZE];
            memkind_error_message(err, error_message, MEMKIND_ERROR_MESSAGE_SIZE);
            fprintf(stderr, "%s\n", error_message);
        }
      }
      return pmem_kind;
    #else
      mmap_kind::init(mem_path, block_size, num_blocks);
    #endif
    return nullptr;
  }

  static void destroy_kind(struct memkind* pmem_kind) {
    #ifdef MEMKIND_IN_USE
      int err = memkind_destroy_kind(pmem_kind);
      if(err) {
        char error_message[MEMKIND_ERROR_MESSAGE_SIZE];
        memkind_error_message(err, error_message, MEMKIND_ERROR_MESSAGE_SIZE);
        fprintf(stderr, "%s\n", error_message);
      }
    #else
      mmap_kind::destroy();
    #endif
  }
};

}
}

#endif //JIFFY_MEM_UTILS_H
