#include "server_storage_tracker.h"
#include <jiffy/utils/logger.h>

namespace jiffy {
namespace storage {

using namespace utils;

server_storage_tracker::server_storage_tracker(std::vector<std::shared_ptr<block>> &blocks,
                                               uint64_t periodicity_ms,
                                               const std::string &output_file)
    : blocks_(blocks), periodicity_ms_(periodicity_ms), output_file_(output_file) {}

server_storage_tracker::~server_storage_tracker() {
  stop();
}

void server_storage_tracker::start() {
  worker_ = std::thread([&] {
    std::ofstream out(output_file_);
    while (!stop_.load()) {
      auto start = std::chrono::steady_clock::now();
      try {
        report_file_sizes(out);
      } catch (std::exception &e) {
        LOG(log_level::error) << "Exception: " << e.what();
      }
      auto end = std::chrono::steady_clock::now();
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

      auto time_to_wait = std::chrono::duration_cast<std::chrono::milliseconds>(periodicity_ms_ - elapsed);
      if (time_to_wait > std::chrono::milliseconds::zero()) {
        std::this_thread::sleep_for(time_to_wait);
      }
    }
    out.close();
  });
}

void server_storage_tracker::stop() {
  stop_.store(true);
  if (worker_.joinable())
    worker_.join();
}

void server_storage_tracker::report_file_sizes(std::ofstream &out) {
  namespace ts = std::chrono;
  auto cur_epoch = ts::duration_cast<ts::milliseconds>(ts::system_clock::now().time_since_epoch()).count();
  out << cur_epoch;
  for (const auto &block: blocks_) {
    out << "\t" << block->used();
  }
  out << std::endl;
}

}
}
