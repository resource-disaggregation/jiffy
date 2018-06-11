#include <iostream>
#include <thrift/transport/TTransportException.h>
#include "notification_worker.h"
#include "../../utils/logger.h"

using namespace ::apache::thrift::transport;
using namespace ::mmux::utils;

namespace mmux {
namespace storage {

notification_worker::notification_worker(mailbox_t &notifications, mailbox_t &controls)
    : notifications_(notifications), controls_(controls), stop_(false) {
  processor_ = std::make_shared<processor_t>(std::make_shared<notification_handler>(notifications_, controls_));
}

notification_worker::~notification_worker() {
  stop();
}

void notification_worker::add_protocol(notification_worker::protocol_ptr_t protocol) {
  protocols_.push_back(protocol);
}

void notification_worker::start() {
  worker_ = std::move(std::thread([&] {
    while (!stop_.load()) {
      try {
        for (auto protocol: protocols_) {
          if (!processor_->process(protocol, protocol, nullptr)) {
            return;
          }
        }
      } catch (TTransportException &e) {
        LOG(log_level::info) << "Connection no longer active: " << e.what();
        return;
      } catch (std::exception &e) {
        LOG(log_level::warn) << "Encountered exception: " << e.what();
        return;
      }
    }
  }));
  LOG(log_level::info) << "Notification Worker " << worker_.get_id() << " started";
}

void notification_worker::stop() {
  bool expected = false;
  if (stop_.compare_exchange_strong(expected, true) && worker_.joinable()) {
    LOG(log_level::info) << "Notification Worker " << worker_.get_id() << " terminating";
    worker_.join();
    LOG(log_level::info) << "Notification Worker terminated";
  }
}

}
}
