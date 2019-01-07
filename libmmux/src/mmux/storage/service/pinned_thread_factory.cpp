#include "pinned_thread_factory.h"
#include "../../utils/rand_utils.h"
#include "../../utils/logger.h"
#include <thrift/concurrency/Monitor.h>

#ifdef __APPLE__
#include <sys/sysctl.h>
#include <mach/mach_types.h>
#include <mach/thread_act.h>

typedef struct cpu_set {
  uint32_t count;
} cpu_set_t;

static inline void CPU_ZERO(cpu_set_t *cs) { cs->count = 0; }

static inline void CPU_SET(int num, cpu_set_t *cs) { cs->count |= (1 << num); }

static inline int CPU_ISSET(int num, cpu_set_t *cs) { return (cs->count & (1 << num)); }

int pthread_setaffinity_np(pthread_t thread, size_t cpu_size, cpu_set_t *cpu_set) {
  thread_port_t mach_thread;
  int core = 0;

  for (core = 0; core < static_cast<int>(8 * cpu_size); core++) {
    if (CPU_ISSET(core, cpu_set)) break;
  }
  thread_affinity_policy_data_t policy = {core};
  mach_thread = pthread_mach_thread_np(thread);
  thread_policy_set(mach_thread, THREAD_AFFINITY_POLICY, (thread_policy_t) &policy, 1);
  return 0;
}
#endif

using namespace ::apache::thrift::concurrency;
using namespace ::apache::thrift;
using namespace ::mmux::utils;

namespace mmux {
namespace storage {

class pinnable_thread : public Thread {
 public:
  enum STATE { uninitialized, starting, started, stopping, stopped };

  static const int MB = 1024 * 1024;

  static void *threadMain(void *arg);

 private:
  pthread_t pthread_;
  Monitor monitor_;        // guard to protect state_ and also notification
  STATE state_;         // to protect proper thread start behavior
  int policy_;
  int priority_;
  int stackSize_;
  stdcxx::weak_ptr<pinnable_thread> self_;
  bool detached_;
  int core_;

 public:
  pinnable_thread(int policy,
                  int priority,
                  int stackSize,
                  bool detached,
                  int core,
                  stdcxx::shared_ptr<Runnable> runnable)
      : pthread_(0),
        state_(uninitialized),
        policy_(policy),
        priority_(priority),
        stackSize_(stackSize),
        detached_(detached),
        core_(core) {
    this->Thread::runnable(runnable);
  }

  ~pinnable_thread() {
    /* Nothing references this thread, if is is not detached, do a join
       now, otherwise the thread-id and, possibly, other resources will
       be leaked. */
    if (!detached_) {
      try {
        join();
      } catch (...) {
        // We're really hosed.
      }
    }
  }

  STATE getState() const {
    Synchronized sync(monitor_);
    return state_;
  }

  void setState(STATE newState) {
    Synchronized sync(monitor_);
    state_ = newState;

    // unblock start() with the knowledge that the thread has actually
    // started running, which avoids a race in detached threads.
    if (newState == started) {
      monitor_.notify();
    }
  }

  void start() {
    if (getState() != uninitialized) {
      return;
    }

    pthread_attr_t thread_attr;
    if (pthread_attr_init(&thread_attr) != 0) {
      throw SystemResourceException("pthread_attr_init failed");
    }

    if (pthread_attr_setdetachstate(&thread_attr, detached_ ? PTHREAD_CREATE_DETACHED : PTHREAD_CREATE_JOINABLE)
        != 0) {
      throw SystemResourceException("pthread_attr_setdetachstate failed");
    }

    // Set thread stack size
    if (pthread_attr_setstacksize(&thread_attr, static_cast<size_t>(MB * stackSize_)) != 0) {
      throw SystemResourceException("pthread_attr_setstacksize failed");
    }

#if _POSIX_THREAD_PRIORITY_SCHEDULING > 0
    if (pthread_attr_setschedpolicy(&thread_attr, policy_) != 0) {
      throw SystemResourceException("pthread_attr_setschedpolicy failed");
    }
#endif

    struct sched_param sched_param;
    sched_param.sched_priority = priority_;

    // Set thread priority
    if (pthread_attr_setschedparam(&thread_attr, &sched_param) != 0) {
      throw SystemResourceException("pthread_attr_setschedparam failed");
    }

    // Create reference
    stdcxx::shared_ptr<pinnable_thread> *selfRef = new stdcxx::shared_ptr<pinnable_thread>();
    *selfRef = self_.lock();

    setState(starting);

    Synchronized sync(monitor_);

    if (pthread_create(&pthread_, &thread_attr, threadMain, (void *) selfRef) != 0) {
      throw SystemResourceException("pthread_create failed");
    }

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_, &cpuset);
    int ret;
    if ((ret = pthread_setaffinity_np(pthread_, sizeof(cpu_set_t), &cpuset)) != 0) {
      LOG(log_level::warn) << "Could not pin thread to core#" << core_ << ": " << ret;
    } else {
      LOG(log_level::info) << "Pinned thread to core#" << core_;
    }

    // The caller may not choose to guarantee the scope of the Runnable
    // being used in the thread, so we must actually wait until the thread
    // starts before we return.  If we do not wait, it would be possible
    // for the caller to start destructing the Runnable and the Thread,
    // and we would end up in a race.  This was identified with valgrind.
    monitor_.wait();
  }

  void join() {
    if (!detached_ && getState() != uninitialized) {
      void *ignore;
      /* XXX
         If join fails it is most likely due to the fact
         that the last reference was the thread itself and cannot
         join.  This results in leaked threads and will eventually
         cause the process to run out of thread resources.
         We're beyond the point of throwing an exception.  Not clear how
         best to handle this. */
      int res = pthread_join(pthread_, &ignore);
      detached_ = (res == 0);
      if (res != 0) {
        GlobalOutput.printf("pinnable_thread::join(): fail with code %d", res);
      }
    }
  }

  Thread::id_t getId() {
    return (Thread::id_t) pthread_;
  }

  stdcxx::shared_ptr<Runnable> runnable() const { return Thread::runnable(); }

  void runnable(stdcxx::shared_ptr<Runnable> value) { Thread::runnable(value); }

  void weakRef(stdcxx::shared_ptr<pinnable_thread> self) {
    assert(self.get() == this);
    self_ = stdcxx::weak_ptr<pinnable_thread>(self);
  }
};

void *pinnable_thread::threadMain(void *arg) {
  stdcxx::shared_ptr<pinnable_thread> thread = *(stdcxx::shared_ptr<pinnable_thread> *) arg;
  delete reinterpret_cast<stdcxx::shared_ptr<pinnable_thread> *>(arg);

  thread->setState(started);
  thread->runnable()->run();

  STATE _s = thread->getState();
  if (_s != stopping && _s != stopped) {
    thread->setState(stopping);
  }

  return (void *) 0;
}

/**
 * Converts generic posix thread schedule policy enums into pthread
 * API values.
 */
static int toPthreadPolicy(pinned_thread_factory::POLICY policy) {
  switch (policy) {
    case pinned_thread_factory::OTHER:return SCHED_OTHER;
    case pinned_thread_factory::FIFO:return SCHED_FIFO;
    case pinned_thread_factory::ROUND_ROBIN:return SCHED_RR;
  }
  return SCHED_OTHER;
}

/**
 * Converts relative thread priorities to absolute value based on posix
 * thread scheduler policy
 *
 *  The idea is simply to divide up the priority range for the given policy
 * into the correpsonding relative priority level (lowest..highest) and
 * then pro-rate accordingly.
 */
static int toPthreadPriority(pinned_thread_factory::POLICY policy, pinned_thread_factory::PRIORITY priority) {
  int pthread_policy = toPthreadPolicy(policy);
  int min_priority = 0;
  int max_priority = 0;
#ifdef HAVE_SCHED_GET_PRIORITY_MIN
  min_priority = sched_get_priority_min(pthread_policy);
#endif
#ifdef HAVE_SCHED_GET_PRIORITY_MAX
  max_priority = sched_get_priority_max(pthread_policy);
#endif
  int quanta = (pinned_thread_factory::HIGHEST - pinned_thread_factory::LOWEST) + 1;
  float stepsperquanta = (float) (max_priority - min_priority) / quanta;

  if (priority <= pinned_thread_factory::HIGHEST) {
    return (int) (min_priority + stepsperquanta * priority);
  } else {
    // should never get here for priority increments.
    assert(false);
    return (int) (min_priority + stepsperquanta * pinned_thread_factory::NORMAL);
  }
}

pinned_thread_factory::pinned_thread_factory(pinned_thread_factory::POLICY policy,
                                             pinned_thread_factory::PRIORITY priority,
                                             int stackSize,
                                             bool detached,
                                             int num_cores)
    : ThreadFactory(detached), policy_(policy), priority_(priority), stackSize_(stackSize), num_cores_(num_cores) {
}

pinned_thread_factory::pinned_thread_factory(bool detached)
    : ThreadFactory(detached),
      policy_(ROUND_ROBIN),
      priority_(NORMAL),
      stackSize_(1),
      num_cores_(std::thread::hardware_concurrency()) {
}

stdcxx::shared_ptr<Thread> pinned_thread_factory::newThread(stdcxx::shared_ptr<Runnable> runnable) const {
  stdcxx::shared_ptr<pinnable_thread> result
      = stdcxx::shared_ptr<pinnable_thread>(new pinnable_thread(toPthreadPolicy(policy_),
                                                                toPthreadPriority(policy_, priority_),
                                                                stackSize_,
                                                                isDetached(),
                                                                utils::rand_utils::rand_int32(num_cores_ - 1),
                                                                runnable));
  result->weakRef(result);
  runnable->thread(result);
  return result;
}

::apache::thrift::concurrency::Thread::id_t pinned_thread_factory::getCurrentThreadId() const {
  return (Thread::id_t) pthread_self();
}

int pinned_thread_factory::getStackSize() const {
  return stackSize_;
}

void pinned_thread_factory::setStackSize(int value) {
  stackSize_ = value;
}

pinned_thread_factory::PRIORITY pinned_thread_factory::getPriority() const {
  return priority_;
}

void pinned_thread_factory::setPriority(pinned_thread_factory::PRIORITY value) {
  priority_ = value;
}

}
}