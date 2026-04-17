#include "common/thread_name.h"

#if defined(__APPLE__) || defined(__linux__)
#include <pthread.h>
#endif

namespace minikv {

void SetCurrentThreadName(const std::string& name) {
#if defined(__APPLE__)
  pthread_setname_np(name.substr(0, 63).c_str());
#elif defined(__linux__)
  pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
#else
  (void)name;
#endif
}

}  // namespace minikv
