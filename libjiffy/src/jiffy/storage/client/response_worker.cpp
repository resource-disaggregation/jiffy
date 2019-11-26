#include <thrift/transport/TTransportException.h>
#include "jiffy/storage/client/response_worker.h"
#include "jiffy/storage/client/response_handler.h"
#include "jiffy/utils/logger.h"

using namespace ::apache::thrift::transport;
using namespace ::jiffy::utils;

namespace jiffy {
namespace storage {

response_worker::response_worker(std::vector<std::shared_ptr<mailbox_t>> &responses)
    : responses_(responses), stop_(false) {
  processor_ = std::make_shared<processor_t>(std::make_shared<response_handler>(responses_));
}

response_worker::~response_worker() {
  stop();
}

void response_worker::add_protocol(response_worker::protocol_ptr_t protocol) {
  LOG(log_level::info) << "Adding this protocol";
  protocols_.push_back(protocol);
}

void response_worker::start() {
  worker_ = std::thread([&] {
    while (!stop_.load()) {
      try {
        for (const auto &protocol: protocols_) {
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
  });
  LOG(log_level::info) << "Response Worker " << worker_.get_id() << " started";
}

void response_worker::stop() {
  bool expected = false;
  if (stop_.compare_exchange_strong(expected, true) && worker_.joinable()) {
    LOG(log_level::info) << "Response Worker " << worker_.get_id() << " terminating";
    worker_.join();
    LOG(log_level::info) << "Response Worker terminated";
  }
}

}
}
