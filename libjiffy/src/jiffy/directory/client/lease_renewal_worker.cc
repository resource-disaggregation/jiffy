#include "lease_renewal_worker.h"
#include "../../utils/logger.h"

namespace jiffy {
namespace directory {

using namespace jiffy::utils;

lease_renewal_worker::lease_renewal_worker(const std::string &host, int port)
    : stop_(false), ls_(host, port) {
}

lease_renewal_worker::~lease_renewal_worker() {
  stop_.store(true);
  if (worker_.joinable())
    worker_.join();
}

void lease_renewal_worker::stop() {
  stop_.store(true);
}

void lease_renewal_worker::start() {
  worker_ = std::thread([&] {
    rpc_lease_ack ack;
    while (!stop_.load()) {
      LOG(trace) << "Looking for expired leases...";
      auto start = std::chrono::steady_clock::now();
      try {
        std::unique_lock<std::mutex> lock(metadata_mtx_);
        if (!to_renew_.empty()) {
          ack = ls_.renew_leases(to_renew_);
        }
      } catch (std::exception &e) {
        LOG(error) << "Exception: " << e.what();
      }
      auto end = std::chrono::steady_clock::now();
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
      auto time_to_wait = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::milliseconds(ack.lease_period_ms) - elapsed);
      if (time_to_wait > std::chrono::milliseconds::zero()) {
        std::this_thread::sleep_for(time_to_wait);
      }
    }
  });
}

void lease_renewal_worker::add_path(const std::string &path) {
  std::unique_lock<std::mutex> lock(metadata_mtx_);
  if (std::find(to_renew_.begin(), to_renew_.end(), path) == to_renew_.end()) {
    to_renew_.push_back(path);
  }
}

void lease_renewal_worker::remove_path(const std::string &path) {
  std::unique_lock<std::mutex> lock(metadata_mtx_);
  std::vector<std::string>::iterator it;
  if ((it = std::find(to_renew_.begin(), to_renew_.end(), path)) != to_renew_.end()) {
    to_renew_.erase(it);
  }
}

bool lease_renewal_worker::has_path(const std::string &path) {
  std::unique_lock<std::mutex> lock(metadata_mtx_);
  return std::find(to_renew_.begin(), to_renew_.end(), path) != to_renew_.end();
}

}
}
