#include <jiffy/utils/string_utils.h>
#include "msg_queue_partition.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/logger.h"
#include "jiffy/persistent/persistent_store.h"
#include "jiffy/storage/partition_manager.h"
#include "jiffy/storage/msgqueue/msg_queue_ops.h"
#include "jiffy/directory/client/directory_client.h"
#include "jiffy/directory/directory_ops.h"

namespace jiffy {
namespace storage {

using namespace utils;

msg_queue_partition::msg_queue_partition(block_memory_manager *manager,
                                         const std::string &name,
                                         const std::string &metadata,
                                         const utils::property_map &conf,
                                         const std::string &directory_host,
                                         const int directory_port)
    : chain_module(manager, name, metadata, MSG_QUEUE_OPS),
      partition_(build_allocator<msg_type>()),
      splitting_(false),
      merging_(false),
      dirty_(false),
      directory_host_(directory_host),
      directory_port_(directory_port) {
  auto ser = conf.get("msgqueue.serializer", "csv");
  if (ser == "binary") {
    ser_ = std::make_shared<csv_serde>();
  } else if (ser == "csv") {
    ser_ = std::make_shared<binary_serde>();
  } else {
    throw std::invalid_argument("No such serializer/deserializer " + ser);
  }
  threshold_hi_ = conf.get_as<double>("msgqueue.capacity_threshold_hi", 0.95);
  threshold_lo_ = conf.get_as<double>("msgqueue.capacity_threshold_lo", 0.00);
  auto_scale_ = conf.get_as<bool>("msgqueue.auto_scale", true);
  LOG(log_level::info) << "Partition name: " << name_;
}

std::string msg_queue_partition::send(const msg_type &message, bool indirect) {
  //LOG(log_level::info) << "Sending new message = " << message;
  std::unique_lock<std::shared_mutex> lock(operation_mtx_);
  partition_.push_back(message);
  return "!ok";
}

msg_type msg_queue_partition::receive(std::string position, bool indirect) {
  auto pos = std::stoi(position);
  if (pos < 0) throw std::invalid_argument("receive position invalid");
  if (pos < size()) {
    std::unique_lock<std::shared_mutex> lock(operation_mtx_);
    return partition_[pos];
  }
  return "!key_not_found";

}

std::string msg_queue_partition::clear() {
  std::unique_lock<std::shared_mutex> lock(operation_mtx_);
  partition_.clear();
  return "!ok";
}

void msg_queue_partition::run_command(std::vector<std::string> &_return,
                                      int32_t cmd_id,
                                      const std::vector<std::string> &args) {
  bool redirect = !args.empty() && args.back() == "!redirected";
  size_t nargs = redirect ? args.size() - 1 : args.size();
  switch (cmd_id) {
    case msg_queue_cmd_id::mq_send:
      for (const msg_type &msg: args)
        _return.emplace_back(send(msg, redirect));
      break;
    case msg_queue_cmd_id::mq_receive:
      for (const auto &pos: args)
        _return.emplace_back(receive(pos, redirect));
      break;
    case msg_queue_cmd_id::mq_clear:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(clear());
      }
      break;
    default:throw std::invalid_argument("No such operation id " + std::to_string(cmd_id));
  }
  if (is_mutator(cmd_id)) {
    dirty_ = true;
  }
  bool expected = false;
  if (auto_scale_.load() && is_mutator(cmd_id) && overload() && metadata_ != "exporting"
      && metadata_ != "importing" && is_tail()
      && splitting_.compare_exchange_strong(expected, true)) {
    LOG(log_level::info) << "Overloaded partition; storage = " << bytes_.load() << " capacity = "
                         << manager_->mb_capacity();
    try {

      splitting_ = false;
      LOG(log_level::info) << "Not supporting auto_scaling currently";

    } catch (std::exception &e) {
      splitting_ = false;
      LOG(log_level::warn) << "Split slot range failed: " << e.what();
    }
    LOG(log_level::info) << "After split storage: " << manager_->mb_used() << " capacity: " << manager_->mb_capacity();
  }
  expected = false;
  if (auto_scale_.load() && underload() && metadata_ != "exporting"
      && metadata_ != "importing" && is_tail()
      && merging_.compare_exchange_strong(expected, true)) {
    LOG(log_level::info) << "Underloaded partition; storage = " << bytes_.load() << " capacity = "
                         << manager_->mb_capacity();
    try {
      merging_ = false;
      LOG(log_level::info) << "Currently does not support auto_scaling";

    } catch (std::exception &e) {
      merging_ = false;
      LOG(log_level::warn) << "Merge slot range failed: " << e.what();
    }
  }
}

std::size_t msg_queue_partition::size() const {
  return partition_.size();
}

bool msg_queue_partition::empty() const {
  return partition_.empty();
}

bool msg_queue_partition::is_dirty() const {
  return dirty_.load();
}

void msg_queue_partition::load(const std::string &path) {
  auto remote = persistent::persistent_store::instance(path, ser_);
  auto decomposed = persistent::persistent_store::decompose_path(path);
  remote->read<msg_queue_type>(decomposed.second,
                               partition_);// TODO fix this, currently template function only supports hash tables
}

bool msg_queue_partition::sync(const std::string &path) {
  bool expected = true;
  if (dirty_.compare_exchange_strong(expected, false)) {
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    remote->write<msg_queue_type>(partition_,
                                  decomposed.second);// TODO fix this, currently template function only supports hash tables
    return true;
  }
  return false;
}

bool msg_queue_partition::dump(const std::string &path) {
  std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
  bool expected = true;
  bool flushed = false;
  if (dirty_.compare_exchange_strong(expected, false)) {
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    remote->write<msg_queue_type>(partition_,
                                  decomposed.second);// TODO fix this, currently template function only supports hash tables
    flushed = true;
  }
  partition_.clear();
  next_->reset("nil");
  path_ = "";
  // clients().clear();
  sub_map_.clear();
  chain_ = {};
  role_ = singleton;
  splitting_ = false;
  merging_ = false;
  dirty_ = false;
  return flushed;
}

void msg_queue_partition::forward_all() {
  int64_t i = 0;
  for (auto it = partition_.begin(); it != partition_.end(); it++) {
    std::vector<std::string> result;
    run_command_on_next(result, msg_queue_cmd_id::mq_send, {*it});
    ++i;
  }
}

bool msg_queue_partition::overload() {
  return manager_->mb_used() > static_cast<size_t>(static_cast<double>(manager_->mb_capacity()) * threshold_hi_);
}

bool msg_queue_partition::underload() {
  return manager_->mb_used() < static_cast<size_t>(static_cast<double>(manager_->mb_capacity()) * threshold_lo_);
}

REGISTER_IMPLEMENTATION("msgqueue", msg_queue_partition);

}
}