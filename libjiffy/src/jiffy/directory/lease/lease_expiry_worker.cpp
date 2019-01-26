#include <iostream>
#include <chrono>
#include "lease_expiry_worker.h"

namespace jiffy {
namespace directory {

using namespace utils;

lease_expiry_worker::lease_expiry_worker(std::shared_ptr<directory_tree> tree,
                                         std::uint64_t lease_period_ms,
                                         std::uint64_t grace_period_ms)
    : lease_period_ms_(lease_period_ms),
      grace_period_ms_(grace_period_ms),
      tree_(std::move(tree)),
      stop_(false),
      num_epochs_(0) {
}

lease_expiry_worker::~lease_expiry_worker() {
  stop();
}

void lease_expiry_worker::start() {
  worker_ = std::thread([&] {
    while (!stop_.load()) {
      LOG(trace) << "Looking for expired leases...";
      auto start = std::chrono::steady_clock::now();
      try {
        remove_expired_leases();
      } catch (std::exception &e) {
        LOG(error) << "Exception: " << e.what();
      }
      ++num_epochs_;
      auto end = std::chrono::steady_clock::now();
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

      auto time_to_wait = std::chrono::duration_cast<std::chrono::milliseconds>(lease_period_ms_ - elapsed);
      if (time_to_wait > std::chrono::milliseconds::zero()) {
        std::this_thread::sleep_for(time_to_wait);
      }
    }
  });
}

void lease_expiry_worker::stop() {
  stop_.store(true);
  if (worker_.joinable())
    worker_.join();
}

void lease_expiry_worker::remove_expired_leases() {
  namespace ts = std::chrono;
  auto cur_epoch = ts::duration_cast<ts::milliseconds>(ts::system_clock::now().time_since_epoch()).count();
  auto node = std::dynamic_pointer_cast<ds_dir_node>(tree_->root_);
  std::string parent_path;
  for (const auto &cname: node->child_names()) {
    remove_expired_nodes(node, parent_path, cname, static_cast<uint64_t>(cur_epoch));
  }
}

void lease_expiry_worker::remove_expired_nodes(std::shared_ptr<ds_dir_node> parent,
                                               const std::string &parent_path,
                                               const std::string &child_name,
                                               std::uint64_t epoch) {
  auto child_path = parent_path;
  directory_utils::push_path_element(child_path, child_name);
  auto child = parent->get_child(child_name);
  if (child == nullptr) {
    return;
  }
  auto time_since_last_renewal = epoch - child->last_write_time();
  auto lease_duration = static_cast<uint64_t>(lease_period_ms_.count());
  auto extended_lease_duration = lease_duration + static_cast<uint64_t>(grace_period_ms_.count());
  if (time_since_last_renewal >= extended_lease_duration) {
    if (child->is_regular_file() && std::dynamic_pointer_cast<ds_file_node>(child)->is_pinned()) {
      return;
    }
    // Remove child since its lease has expired
    LOG(warn) << "Lease expired for " << child_path << "...";
    tree_->handle_lease_expiry(child_path);
  } else {
    if (time_since_last_renewal >= lease_duration && child->is_regular_file()) {
      auto node = std::dynamic_pointer_cast<ds_file_node>(child);
      if (!node->is_pinned()) {
        LOG(warn) << "Lease in grace period for " << child_path;
        node->mode(storage_mode::in_memory_grace);
      }
    }
    if (child->is_directory()) {
      auto node = std::dynamic_pointer_cast<ds_dir_node>(child);
      for (const auto &cname: node->child_names()) {
        remove_expired_nodes(node, child_path, cname, epoch);
      }
    }
  }
}

size_t lease_expiry_worker::num_epochs() const {
  return num_epochs_.load();
}

}
}
