#include "fifo_queue_partition.h"
#include "jiffy/persistent/persistent_store.h"
#include "jiffy/storage/partition_manager.h"
#include "jiffy/storage/fifoqueue/fifo_queue_ops.h"
#include "jiffy/auto_scaling/auto_scaling_client.h"

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
  head_ = 0;
  threshold_hi_ = conf.get_as<double>("fifoqueue.capacity_threshold_hi", 0.95);
  auto_scale_ = conf.get_as<bool>("fifoqueue.auto_scale", true);
}

void fifo_queue_partition::enqueue(response &_return, const arg_list &args) {
  if (args.size() != 2) {
    RETURN_ERR("!args_error");
  }
  auto ret = partition_.push_back(args[1]);
  if (!ret.first) {
    if (!auto_scale_) {
      RETURN_ERR("!split_enqueue", std::to_string(ret.second.size()));
    } else if (!next_target_str_.empty()) {
      RETURN_ERR("!split_enqueue", std::to_string(ret.second.size()), next_target_str_);
    } else {
      partition_.recover(args[1].size() - ret.second.size());
      RETURN_ERR("!redo");
    }
  }
  RETURN_OK();
}

void fifo_queue_partition::dequeue(response &_return, const arg_list &args) {
  if (args.size() != 1) {
    RETURN_ERR("!args_error");
  }
  auto ret = partition_.at(head_);
  if (ret.first) {
    head_ += (string_array::METADATA_LEN + ret.second.size());
    RETURN_OK(ret.second);
  }
  if (ret.second == "!not_available") {
    RETURN_ERR("!msg_not_found");
  }
  head_ += (string_array::METADATA_LEN + ret.second.size());
  if (!auto_scale_) {
    RETURN_ERR("!split_dequeue", ret.second);
  }
  if (!next_target_str_.empty()) {
    RETURN_ERR("!split_dequeue", ret.second, next_target_str_);
  }
  head_ -= (string_array::METADATA_LEN + ret.second.size());
  RETURN_ERR("!redo");
}

void fifo_queue_partition::read_next(response &_return, const arg_list &args) {
  if (args.size() != 2) {
    RETURN_ERR("!args_error");
  }
  auto ret = partition_.at(std::stoi(args[1]));
  if (ret.first) {
    RETURN_OK(ret.second);
  }
  if (ret.second == "!not_available") {
    RETURN_ERR("!msg_not_found");
  }
  if (!auto_scale_) {
    RETURN_ERR("!split_readnext", ret.second);
  }
  if (!next_target_str_.empty()) {
    RETURN_ERR("!split_readnext", ret.second, next_target_str_);
  }
  RETURN_ERR("!redo");
}

void fifo_queue_partition::clear(response &_return, const arg_list &args) {
  if (args.size() != 1) {
    RETURN_ERR("!args_error");
  }
  partition_.clear();
  head_ = 0;
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
    case fifo_queue_cmd_id::fq_enqueue:
      enqueue(_return, args);
      break;
    case fifo_queue_cmd_id::fq_dequeue:
      dequeue(_return, args);
      break;
    case fifo_queue_cmd_id::fq_readnext:
      read_next(_return, args);
      break;
    case fifo_queue_cmd_id::fq_clear:
      clear(_return, args);
      break;
    case fifo_queue_cmd_id::fq_update_partition:
      update_partition(_return, args);
      break;
    default: {
      _return.emplace_back("!no_such_command");
      return;
    }
  }
  if (is_mutator(cmd_name)) {
    dirty_ = true;
  }
  if (auto_scale_ && is_mutator(cmd_name) && overload() && is_tail() && !scaling_up_) {
    LOG(log_level::info) << "Overloaded partition: " << name() << " storage = " << storage_size() << " capacity = "
                         << storage_capacity() << " partition size = " << size() << "partition capacity "
                         << partition_.capacity();
    try {
      scaling_up_ = true;
      std::string dst_partition_name = std::to_string(std::stoi(name_) + 1);
      std::map<std::string, std::string> scale_conf;
      scale_conf.emplace(std::make_pair(std::string("type"), std::string("fifo_queue_add")));
      scale_conf.emplace(std::make_pair(std::string("next_partition_name"), dst_partition_name));
      auto scale = std::make_shared<auto_scaling::auto_scaling_client>(auto_scaling_host_, auto_scaling_port_);
      scale->auto_scaling(chain(), path(), scale_conf);
    } catch (std::exception &e) {
      scaling_up_ = false;
      LOG(log_level::warn) << "Adding new message queue partition failed: " << e.what();
    }
  }
  if (auto_scale_ && cmd_name == "dequeue" && head_ > partition_.capacity() && is_tail() && !scaling_down_
      && !next_target_str_.empty()) {
    try {
      LOG(log_level::info) << "Underloaded partition: " << name() << " storage = " << storage_size() << " capacity = "
                           << storage_capacity() << " partition size = " << size() << "partition capacity "
                           << partition_.capacity();
      scaling_down_ = true;
      std::string dst_partition_name = std::to_string(std::stoi(name_) + 1);
      std::map<std::string, std::string> scale_conf;
      scale_conf.emplace(std::make_pair(std::string("type"), std::string("fifo_queue_delete")));
      scale_conf.emplace(std::make_pair(std::string("current_partition_name"), name()));
      auto scale = std::make_shared<auto_scaling::auto_scaling_client>(auto_scaling_host_, auto_scaling_port_);
      scale->auto_scaling(chain(), path(), scale_conf);
    } catch (std::exception &e) {
      scaling_down_ = false;
      LOG(log_level::warn) << "Adding new message queue partition failed: " << e.what();
    }
  }
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