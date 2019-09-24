#ifndef JIFFY_THREAD_UTILS_H
#define JIFFY_THREAD_UTILS_H

#include <thread>
#include <sched.h>
#include <unistd.h>
#include <pthread.h>

namespace jiffy {
namespace utils {

class thread_utils {
 public:
  static inline int set_self_core_affinity(int core_id) {
    return set_core_affinity(pthread_self(), core_id);
  }

  static inline int set_core_affinity(std::thread &t, int core_id) {
    return set_core_affinity(t.native_handle(), core_id);
  }

  static inline int set_core_affinity(pthread_t thread_handle, int core_id) {
    int ret = 0;
#ifdef _GNU_SOURCE
    int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
    if (core_id < 0 || core_id >= num_cores) {
      return EINVAL;
    }

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    ret = pthread_setaffinity_np(thread_handle, sizeof(cpu_set_t), &cpuset);
#else
    (void) thread_handle;
    (void) core_id;
#endif
    return ret;
  }
};

}
}

#endif /* JIFFY_THREAD_UTILS_H */