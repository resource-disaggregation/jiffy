#include <thrift/transport/TTransportException.h>
#include "hash_table_listener.h"
#include "jiffy/storage/manager/detail/block_id_parser.h"
#include "../../utils/logger.h"

using namespace apache::thrift::transport;
using namespace jiffy::utils;

namespace jiffy {
namespace storage {

hash_table_listener::hash_table_listener(const std::string &path, const directory::data_status &status)
    : path_(path), status_(status), worker_(notifications_, controls_) {
  for (const auto &block: status_.data_blocks()) {
    auto t = block_id_parser::parse(block.block_names.back());
    block_ids_.push_back(t.id);
    listeners_.push_back(std::make_shared<block_listener>(t.host, t.notification_port, controls_));
    worker_.add_protocol(listeners_.back()->protocol());
  }
  worker_.start();
}

hash_table_listener::~hash_table_listener() {
  try {
    for (auto &listener: listeners_) {
      listener->disconnect();
    }
    worker_.stop();
  } catch (TTransportException &e) {
    LOG(log_level::info) << "Could not destruct: " << e.what();
  }
}

void hash_table_listener::subscribe(const std::vector<std::string> &ops) {
  for (size_t i = 0; i < listeners_.size(); i++) {
    listeners_[i]->subscribe(block_ids_[i], ops);
  }
}

void hash_table_listener::unsubscribe(const std::vector<std::string> &ops) {
  for (size_t i = 0; i < listeners_.size(); i++) {
    listeners_[i]->unsubscribe(block_ids_[i], ops);
  }
}

hash_table_listener::notification_t hash_table_listener::get_notification(int64_t timeout_ms) {
  return notifications_.pop(timeout_ms);
}

}
}
