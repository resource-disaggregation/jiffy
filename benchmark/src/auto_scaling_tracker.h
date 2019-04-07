#ifndef JIFFY_SERVER_STORAGE_TRACKER_H
#define JIFFY_SERVER_STORAGE_TRACKER_H

#include <fstream>
#include <atomic>
#include <chrono>
#include <thread>
#include <jiffy/storage/block.h>

namespace jiffy {
namespace storage {

/* Server storage tracker class*/
class server_storage_tracker {
 public:
  /**
   * @brief Constructor
   * @param blocks Data blocks
   * @param periodicity_ms Periodicity
   * @param output_file Output file name
   */

  server_storage_tracker(std::vector<std::shared_ptr<block>> &blocks,
                         uint64_t periodicity_ms,
                         const std::string &output_file);
  /**
   * @brief Destructor
   */

  ~server_storage_tracker();

  /**
   * @brief Start worker thread and periodically check file size
   */

  void start();

  /**
   * @brief Set stop bit and stop worker thread
   */

  void stop();

 private:
  /**
   * @brief Report file size to output file stream
   * @param out output file stream
   */
  void report_file_sizes(std::ofstream &out);
  /* Data blocks */
  std::vector<std::shared_ptr<block>> &blocks_;
  /* Periodicity */
  std::chrono::milliseconds periodicity_ms_;
  /* Atomic stop bool */
  std::atomic_bool stop_{false};
  /* Worker thread */
  std::thread worker_;
  /* Output file name */
  std::string output_file_;
};
}
}

#endif //JIFFY_SERVER_STORAGE_TRACKER_H
