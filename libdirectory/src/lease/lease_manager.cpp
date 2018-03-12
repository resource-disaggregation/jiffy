#include <iostream>
#include "lease_manager.h"

namespace elasticmem {
namespace directory {

lease_manager::lease_manager(std::shared_ptr<directory_tree> tree,
                             std::uint64_t lease_period_s,
                             std::uint64_t grace_period_s)
    : lease_period_ms_(lease_period_s), grace_period_ms_(grace_period_s), tree_(std::move(tree)), stop_(false) {
}

lease_manager::~lease_manager() {
  stop_.store(true);
  if (worker_.joinable())
    worker_.join();
}

void lease_manager::start() {
  worker_ = std::move(std::thread([&] {
    while (!stop_.load()) {
      auto start = std::chrono::steady_clock::now();
      try {
        remove_expired_leases();
      } catch (std::exception& e) {
        std::cout << "Exception: " << e.what() << std::endl;
      }
      auto end = std::chrono::steady_clock::now();
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

      auto time_to_wait = std::chrono::duration_cast<std::chrono::milliseconds>(lease_period_ms_ - elapsed);
      if (time_to_wait > std::chrono::milliseconds::zero()) {
        std::this_thread::sleep_for(time_to_wait);
      }
    }
  }));
}

void lease_manager::stop() {
  stop_.store(true);
}

void lease_manager::remove_expired_leases() {
  namespace ts = std::chrono;
  auto cur_epoch = ts::duration_cast<ts::milliseconds>(ts::system_clock::now().time_since_epoch()).count();
  auto node = std::dynamic_pointer_cast<ds_dir_node>(tree_->root_);
  for (const auto &entry: *node) {
    remove_expired_nodes(node, entry.first, cur_epoch);
  }
}

void lease_manager::remove_expired_nodes(std::shared_ptr<ds_dir_node> parent,
                                         const std::string &child_name,
                                         std::uint64_t epoch) {
  auto child = parent->get_child(child_name);
  auto extended_lease_duration = static_cast<uint64_t>(lease_period_ms_.count() + grace_period_ms_.count());
  if (epoch - child->last_write_time() >= extended_lease_duration) {
    // Remove child since its lease has expired
    parent->remove_child(child_name);
  } else if (child->is_directory()) {
    auto node = std::dynamic_pointer_cast<ds_dir_node>(child);
    for (const auto &entry: *node) {
      remove_expired_nodes(node, entry.first, epoch);
    }
  }
}

}
}