#ifndef JIFFY_BLOCK_SIZE_TRACKER_H
#define JIFFY_BLOCK_SIZE_TRACKER_H

#include <memory>
#include <thread>
#include <atomic>
#include "block_allocator.h"
#include "../../storage/manager/storage_manager.h"
#include "../fs/directory_tree.h"

namespace jiffy {
namespace directory {
/* File size tracker class */
class file_size_tracker {
 public:

  /**
   * @brief Constructor
   * @param tree Directory tree
   * @param periodicity_ms Working period
   * @param output_file Output file name
   */

  file_size_tracker(std::shared_ptr<directory_tree> tree,
                    uint64_t periodicity_ms,
                    const std::string &output_file);

  /**
   * @brief Destructor
   */

  ~file_size_tracker();

  /**
   * @brief Start worker
   * Report file size and sleep until next period
   */

  void start();

  /**
   * @brief Stop worker
   */

  void stop();

 private:

  /**
   * @brief Report file size starting from root
   * @param out Output file stream
   */

  void report_file_sizes(std::ofstream &out);

  /**
   * @brief Report file size recursively
   * @param out Output file stream
   * @param node File node
   * @param parent_path Parent path
   * @param epoch Time epoch
   */

  void report_file_sizes(std::ofstream &out,
                         std::shared_ptr<ds_node> node,
                         const std::string &parent_path,
                         uint64_t epoch);
  /* Working period */
  std::chrono::milliseconds periodicity_ms_;
  /* Stop bool */
  std::atomic_bool stop_{false};
  /* Worker thread */
  std::thread worker_;
  /* Output file name */
  std::string output_file_;
  /* Directory tree */
  std::shared_ptr<directory_tree> tree_;
  /* Storage */
  std::shared_ptr<storage::storage_management_ops> storage_;

};

}
}

#endif //JIFFY_BLOCK_SIZE_TRACKER_H
