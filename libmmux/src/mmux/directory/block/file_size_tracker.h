#ifndef MMUX_BLOCK_SIZE_TRACKER_H
#define MMUX_BLOCK_SIZE_TRACKER_H

#include <memory>
#include <thread>
#include <atomic>
#include "block_allocator.h"
#include "../../storage/manager/storage_manager.h"
#include "../fs/directory_tree.h"

namespace mmux {
namespace directory {

class file_size_tracker {
 public:
  file_size_tracker(std::shared_ptr<directory_tree> tree,
                      uint64_t periodicity_ms,
                      const std::string &output_file);

  ~file_size_tracker();

  void start();

  void stop();

 private:
  void report_file_sizes(std::ofstream &out);

  void report_file_sizes(std::ofstream &out,
                         std::shared_ptr<ds_node> node,
                         const std::string &parent_path,
                         uint64_t epoch);

  std::chrono::milliseconds periodicity_ms_;
  std::atomic_bool stop_{false};
  std::thread worker_;
  std::string output_file_;
  std::shared_ptr<directory_tree> tree_;
  std::shared_ptr<storage::storage_management_ops> storage_;
};

}
}

#endif //MMUX_BLOCK_SIZE_TRACKER_H
