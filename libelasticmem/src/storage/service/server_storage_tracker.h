#ifndef ELASTICMEM_SERVER_STORAGE_TRACKER_H
#define ELASTICMEM_SERVER_STORAGE_TRACKER_H

#include <fstream>
#include <atomic>
#include <chrono>
#include <thread>
#include "../chain_module.h"

namespace elasticmem {
namespace storage {

class server_storage_tracker {
 public:
  server_storage_tracker(std::vector<std::shared_ptr<chain_module>> &blocks,
                         uint64_t periodicity_ms,
                         const std::string &output_file);

  ~server_storage_tracker();

  void start();

  void stop();

 private:
  void report_file_sizes(std::ofstream &out);

  std::chrono::milliseconds periodicity_ms_;
  std::atomic_bool stop_{false};
  std::thread worker_;
  std::string output_file_;
  std::vector<std::shared_ptr<chain_module>> &blocks_;

};
}
}

#endif //ELASTICMEM_SERVER_STORAGE_TRACKER_H
