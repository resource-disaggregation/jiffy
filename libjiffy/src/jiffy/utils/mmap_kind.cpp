#include "mmap_kind.h"
#include "logger.h"
#include <jemalloc/jemalloc.h>
#include <fcntl.h>

#include <utility>
#include <unistd.h>
#include <sys/mman.h>

namespace jiffy {
namespace utils {

std::string mmap_kind::mmap_dir;
size_t mmap_kind::max_arena_size = 0;
size_t mmap_kind::num_arenas = 0;
mmap_kind::mmap_arena_ctx mmap_kind::registry[MALLOCX_ARENA_MAX];

#if defined(__APPLE__)
static int posix_fallocate(int fd, off_t offset, off_t len) {
  off_t c_test;
  int ret;
  if (!__builtin_saddll_overflow(offset, len, &c_test)) {
    fstore_t store = {F_ALLOCATECONTIG, F_PEOFPOSMODE, 0, offset + len};
    // Try to get a continuous chunk of disk space
    ret = fcntl(fd, F_PREALLOCATE, &store);
    if (ret < 0) {
      // OK, perhaps we are too fragmented, allocate non-continuous
      store.fst_flags = F_ALLOCATEALL;
      ret = fcntl(fd, F_PREALLOCATE, &store);
      if (ret < 0) {
        return ret;
      }
    }
    ret = ftruncate(fd, offset + len);
  } else {
    // offset+len would overflow.
    ret = -1;
  }
  return ret;
}
#else
#include <fcntl.h>
#endif

void *mmap_extent_alloc(extent_hooks_t *,
                        void *new_addr,
                        size_t size,
                        size_t,
                        bool *zero,
                        bool *commit,
                        unsigned arena_ind) {
  void *addr = nullptr;

  if (new_addr != nullptr)
    return addr;

  addr = mmap_kind::map(arena_ind, new_addr, size);

  if (addr != MAP_FAILED) {
    *zero = true;
    *commit = true;
    /* XXX - check alignment */
  } else {
    addr = nullptr;
  }
  return addr;
}

bool mmap_extent_dalloc(extent_hooks_t *, void *addr, size_t size, bool, unsigned arena_ind) {
  bool result = true;
  if (mmap_kind::unmap(arena_ind, addr, size) == -1) {
    LOG(log_level::error) << "munmap failed!";
  } else {
    result = false;
  }
  return result;
}

bool mmap_extent_commit(extent_hooks_t *, void *, size_t, size_t, size_t, unsigned) {
  /* do nothing - report success */
  return false;
}

bool mmap_extent_decommit(extent_hooks_t *, void *, size_t, size_t, size_t, unsigned) {
  /* do nothing - report failure (opt-out) */
  return true;
}

bool mmap_extent_purge(extent_hooks_t *, void *, size_t, size_t, size_t, unsigned) {
  /* do nothing - report failure (opt-out) */
  return true;
}

bool mmap_extent_split(extent_hooks_t *, void *, size_t, size_t, size_t, bool, unsigned) {
  /* do nothing - report success */
  return false;
}

bool mmap_extent_merge(extent_hooks_t *, void *, size_t, void *, size_t, bool, unsigned) {
  /* do nothing - report success */
  return false;
}

void mmap_extent_destroy(extent_hooks_t *, void *addr, size_t size, bool, unsigned arena_ind) {
  if (mmap_kind::unmap(arena_ind, addr, size) == -1) {
    LOG(log_level::error) << "munmap failed!";
  }
}

static extent_hooks_t mmap_extent_hooks = {
    .alloc = mmap_extent_alloc,
    .dalloc = mmap_extent_dalloc,
    .destroy = mmap_extent_destroy,
    .commit = mmap_extent_commit,
    .decommit = mmap_extent_decommit,
    .purge_lazy = mmap_extent_purge,
    .split = mmap_extent_split,
    .merge = mmap_extent_merge};

void mmap_kind::init(std::string dir, size_t max_size, size_t n_arenas) {
  mmap_dir = std::move(dir);
  max_arena_size = max_size;
  num_arenas = n_arenas;
  char cmd[64];
  for (size_t i = 0; i < num_arenas; i++) {
    unsigned arena_idx = -1;
    size_t sz = sizeof(unsigned int);
    int err = mallctl("arenas.create", (void *) &arena_idx, &sz, nullptr, 0);
    if (err)
      throw std::runtime_error("Could not create arena");

    int fd = open(path(arena_idx).c_str(), O_CREAT | O_RDWR, 0666);
    registry[i] = {arena_idx, fd, 0, 0};

    snprintf(cmd, sizeof(cmd), "arena.%u.extent_hooks", arena_idx);
    err = mallctl(cmd, nullptr, nullptr, (void *) &mmap_extent_hooks, sizeof(extent_hooks_t *));
    if (err)
      throw std::runtime_error("Could not register extent hooks");
  }
}

void mmap_kind::destroy() {
  char cmd[128];
  for (size_t i = 0; i < num_arenas; i++) {
    snprintf(cmd, sizeof(cmd), "arena.%u.destroy", registry[i].idx);
    mallctl(cmd, nullptr, nullptr, nullptr, 0);
    close(registry[i].fd);
    registry[i] = {INVALID_ARENA_IDX, -1, 0, 0};
  }
}

std::string mmap_kind::path(unsigned arena_idx) {
  return mmap_dir + "/arena_" + std::to_string(arena_idx);
}

void *mmap_kind::map(unsigned int arena_idx, void *, size_t size) {
  mmap_arena_ctx ctx = registry[arena_idx];
  void *result;

  if (ctx.cur_size + size > max_arena_size) {
    return MAP_FAILED;
  }

  if (posix_fallocate(ctx.fd, ctx.offset, size != 0)) {
    return MAP_FAILED;
  }

  result = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, ctx.fd, ctx.offset);

  if (result != MAP_FAILED) {
    ctx.offset += size;
    ctx.cur_size += size;
  }

  return result;
}

int mmap_kind::unmap(unsigned int, void *addr, size_t size) {
  return munmap(addr, size);
}

}
}

