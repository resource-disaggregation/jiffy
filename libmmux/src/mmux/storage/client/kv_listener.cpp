#include <thrift/transport/TTransportException.h>
#include "kv_listener.h"
#include "../manager/detail/block_name_parser.h"
#include "../../utils/logger.h"

using namespace apache::thrift::transport;
using namespace mmux::utils;

namespace mmux {
namespace storage {

kv_listener::kv_listener(const std::string &path, const directory::data_status &status)
    : path_(path), status_(status), worker_(notifications_, controls_) {
  for (const auto &block: status_.data_blocks()) {
    auto t = block_name_parser::parse(block.block_names.back());
    block_ids_.push_back(t.id);
    listeners_.push_back(std::make_shared<block_listener>(t.host, t.notification_port, notifications_, controls_));
    worker_.add_protocol(listeners_.back()->protocol());
  }
  worker_.start();
}

kv_listener::~kv_listener() {
  try {
    for (auto &listener: listeners_) {
      listener->disconnect();
    }
    worker_.stop();
  } catch (TTransportException &e) {
    LOG(log_level::info) << "Could not destruct: " << e.what();
  }
}

void kv_listener::subscribe(const std::vector<std::string> &ops) {
  for (size_t i = 0; i < listeners_.size(); i++) {
    listeners_[i]->subscribe(block_ids_[i], ops);
  }
}

void kv_listener::unsubscribe(const std::vector<std::string> &ops) {
  for (size_t i = 0; i < listeners_.size(); i++) {
    listeners_[i]->unsubscribe(block_ids_[i], ops);
  }
}

kv_listener::notification_t kv_listener::get_notification(int64_t timeout_ms) {
  return notifications_.pop(timeout_ms);
}

}
}