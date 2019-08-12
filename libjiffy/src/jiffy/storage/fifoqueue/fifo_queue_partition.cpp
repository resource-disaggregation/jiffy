#include "fifo_queue_partition.h"
#include "jiffy/persistent/persistent_store.h"
#include "jiffy/storage/partition_manager.h"
#include "jiffy/storage/fifoqueue/fifo_queue_ops.h"

namespace jiffy {
namespace storage {

using namespace utils;

fifo_queue_partition::fifo_queue_partition(block_memory_manager *manager,
                                           const std::string &name,
                                           const std::string &metadata,
                                           const utils::property_map &conf,
                                           const std::string &directory_host,
                                           int directory_port,
                                           const std::string &auto_scaling_host,
                                           int auto_scaling_port)
    : chain_module(manager, name, metadata, FQ_CMDS),
      partition_(manager->mb_capacity(), build_allocator<char>()),
      scaling_up_(false),
      scaling_down_(false),
      dirty_(false),
      full_(false),
      empty_(true),
      directory_host_(directory_host),
      directory_port_(directory_port),
      auto_scaling_host_(auto_scaling_host),
      auto_scaling_port_(auto_scaling_port) {
  auto ser = conf.get("fifoqueue.serializer", "csv");
  if (ser == "binary") {
    ser_ = std::make_shared<csv_serde>(binary_allocator_);
  } else if (ser == "csv") {
    ser_ = std::make_shared<binary_serde>(binary_allocator_);
  } else {
    throw std::invalid_argument("No such serializer/deserializer " + ser);
  }
  threshold_hi_ = conf.get_as<double>("fifoqueue.capacity_threshold_hi", 0.95);
}

void fifo_queue_partition::enqueue(response &_return, const arg_list &args) {
  if (args.size() != 2) {
    RETURN_ERR("!args_error");
  }
  if (!partition_.enqueue(args[1])) {
    full_ = true;
    RETURN_ERR("!full");
  }
  empty_ = false;
  RETURN_OK();
}

void fifo_queue_partition::dequeue(response &_return, const arg_list &args) {
  if (args.size() != 1) {
    RETURN_ERR("!args_error");
  }
  std::string item;
  if (!partition_.dequeue(item)) {
    empty_ = true;
    RETURN_ERR("!empty");
  }
  full_ = false;
  RETURN_OK(item);
}

void fifo_queue_partition::clear(response &_return, const arg_list &args) {
  if (args.size() != 1) {
    RETURN_ERR("!args_error");
  }
  partition_.clear();
  scaling_up_ = false;
  scaling_down_ = false;
  dirty_ = false;
  RETURN_OK();
}

void fifo_queue_partition::update_partition(response &_return, const arg_list &args) {
  next_target(args[1]);
  RETURN_OK();
}

void fifo_queue_partition::run_command(response &_return, const arg_list &args) {
  auto cmd_name = args[0];
  switch (command_id(cmd_name)) {
    case fifo_queue_cmd_id::fq_enqueue:enqueue(_return, args);
      break;
    case fifo_queue_cmd_id::fq_dequeue:dequeue(_return, args);
      break;
    case fifo_queue_cmd_id::fq_clear:clear(_return, args);
      break;
    case fifo_queue_cmd_id::fq_update_partition:update_partition(_return, args);
      break;
    default: {
      _return.emplace_back("!no_such_command");
      return;
    }
  }
  if (is_mutator(cmd_name)) {
    dirty_ = true;
  }
  // Do not support scaling for now
}

std::size_t fifo_queue_partition::size() const {
  return partition_.size();
}

bool fifo_queue_partition::empty() const {
  return partition_.empty();
}

bool fifo_queue_partition::is_dirty() const {
  return dirty_;
}

void fifo_queue_partition::load(const std::string &path) {
  auto remote = persistent::persistent_store::instance(path, ser_);
  auto decomposed = persistent::persistent_store::decompose_path(path);
  remote->read<fifo_queue_type>(decomposed.second, partition_);
}

bool fifo_queue_partition::sync(const std::string &path) {
  if (dirty_) {
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    remote->write<fifo_queue_type>(partition_, decomposed.second);
    dirty_ = false;
    return true;
  }
  return false;
}

bool fifo_queue_partition::dump(const std::string &path) {
  bool flushed = false;
  if (dirty_) {
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    remote->write<fifo_queue_type>(partition_, decomposed.second);
    flushed = true;
  }
  partition_.clear();
  next_->reset("nil");
  path_ = "";
  sub_map_.clear();
  chain_ = {};
  role_ = singleton;
  scaling_up_ = false;
  dirty_ = false;
  return flushed;
}

void fifo_queue_partition::forward_all() {
  for (auto it = partition_.begin(); it != partition_.end(); it++) {
    std::vector<std::string> ignore;
    run_command_on_next(ignore, {"enqueue", *it}); // TODO: Could be more efficient
  }
}

bool fifo_queue_partition::overload() {
  return partition_.size() >= static_cast<size_t>(static_cast<double>(partition_.capacity()) * threshold_hi_);
}

REGISTER_IMPLEMENTATION("fifoqueue", fifo_queue_partition);

}
}