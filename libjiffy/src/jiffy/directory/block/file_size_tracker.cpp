#include "file_size_tracker.h"
#include "../../utils/logger.h"
#include "../../utils/directory_utils.h"
#include <utility>
#include <iostream>

namespace jiffy {
namespace directory {

using namespace utils;

file_size_tracker::file_size_tracker(std::shared_ptr<directory_tree> tree,
                                     uint64_t periodicity_ms,
                                     const std::string &output_file)
    : periodicity_ms_(periodicity_ms),
      output_file_(output_file),
      tree_(std::move(tree)),
      storage_(tree_->get_storage_manager()) {}

file_size_tracker::~file_size_tracker() {
  stop_.store(true);
  if (worker_.joinable())
    worker_.join();
}

void file_size_tracker::start() {
  worker_ = std::thread([&] {
    std::ofstream out(output_file_);
    while (!stop_.load()) {
      auto start = std::chrono::steady_clock::now();
      try {
        report_file_sizes(out);
      } catch (std::exception &e) {
        LOG(error) << "Exception: " << e.what();
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

void file_size_tracker::stop() {
  stop_.store(true);
}

void file_size_tracker::report_file_sizes(std::ofstream &out) {
  namespace ts = std::chrono;
  auto cur_epoch = ts::duration_cast<ts::milliseconds>(ts::system_clock::now().time_since_epoch()).count();
  out << cur_epoch << "\t" << tree_->get_allocator()->num_allocated_blocks() << std::endl;
}

}
}
