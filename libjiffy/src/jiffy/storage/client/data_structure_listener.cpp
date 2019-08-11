#include <thrift/transport/TTransportException.h>
#include "jiffy/storage/client/data_structure_listener.h"
#include "jiffy/storage/manager/detail/block_id_parser.h"
#include "jiffy/utils/logger.h"

using namespace apache::thrift::transport;
using namespace jiffy::utils;

namespace jiffy {
namespace storage {

data_structure_listener::data_structure_listener(const std::string &path, const directory::data_status &status)
    : path_(path), status_(status), worker_(notifications_, controls_) {
  for (const auto &block: status_.data_blocks()) {
    auto t = block_id_parser::parse(block.block_ids.back());
    block_ids_.push_back(t.id);
    listeners_.push_back(std::make_shared<block_listener>(t.host, t.service_port, controls_));
    worker_.add_protocol(listeners_.back()->protocol());
  }
  worker_.start();
}

data_structure_listener::~data_structure_listener() {
  try {
    for (auto &listener: listeners_) {
      listener->disconnect();
    }
    worker_.stop();
  } catch (TTransportException &e) {
    LOG(log_level::info) << "Could not destruct: " << e.what();
  }
}

void data_structure_listener::subscribe(const std::vector<std::string> &ops) {
  for (size_t i = 0; i < listeners_.size(); i++) {
    listeners_[i]->subscribe(block_ids_[i], ops);
  }
}

void data_structure_listener::unsubscribe(const std::vector<std::string> &ops) {
  for (size_t i = 0; i < listeners_.size(); i++) {
    listeners_[i]->unsubscribe(block_ids_[i], ops);
  }
}

data_structure_listener::notification_t data_structure_listener::get_notification(int64_t timeout_ms) {
  auto notification = notifications_.pop(timeout_ms);
  if (notification.first == "error" && notification.second == "!block_moved") {
    // TODO: refresh connections
  }
  return notification;
}

}
}
