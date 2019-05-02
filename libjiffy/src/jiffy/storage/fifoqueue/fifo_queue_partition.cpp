#include <jiffy/utils/string_utils.h>
#include "fifo_queue_partition.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/logger.h"
#include "jiffy/persistent/persistent_store.h"
#include "jiffy/storage/partition_manager.h"
#include "jiffy/storage/fifoqueue/fifo_queue_ops.h"
#include "jiffy/directory/client/directory_client.h"
#include "jiffy/auto_scaling/auto_scaling_client.h"
#include <thread>

namespace jiffy {
namespace storage {

using namespace utils;

fifo_queue_partition::fifo_queue_partition(block_memory_manager *manager,
                                         const std::string &name,
                                         const std::string &metadata,
                                         const utils::property_map &conf,
                                         const std::string &directory_host,
                                         const int directory_port,
                                         const std::string &auto_scaling_host,
                                         const int auto_scaling_port)
    : chain_module(manager, name, metadata, FIFO_QUEUE_OPS),
      partition_(build_allocator<element_type>()),
      overload_(false),
      underload_(false),
      dirty_(false),
      directory_host_(directory_host),
      directory_port_(directory_port),
      auto_scaling_host_(auto_scaling_host),
      auto_scaling_port_(auto_scaling_port){
  auto ser = conf.get("fifoqueue.serializer", "csv");
  if (ser == "binary") {
    ser_ = std::make_shared<csv_serde>(binary_allocator_);
  } else if (ser == "csv") {
    ser_ = std::make_shared<binary_serde>(binary_allocator_);
  } else {
    throw std::invalid_argument("No such serializer/deserializer " + ser);
  }
  head_ = 0;
  new_block_available_ = false;
  threshold_hi_ = conf.get_as<double>("fifoqueue.capacity_threshold_hi", 0.95);
  threshold_lo_ = conf.get_as<double>("fifoqueue.capacity_threshold_lo", 0.00);
  auto_scale_ = conf.get_as<bool>("fifoqueue.auto_scale", true);
}

std::string fifo_queue_partition::enqueue(const std::string &message) {
  //LOG(log_level::info) << "Enqueue ";
  //LOG(log_level::info) << " Storage size " << storage_size() << " Storage capacity "
  //                     << storage_capacity();
  //LOG(log_level::info) << "partition size " << partition_.size() << " partition capacity " << partition_.capacity();
  if (storage_size() * 2 >= storage_capacity() && partition_.size() >= partition_.capacity()) {
    if (!next_target_str().empty()) {
      new_block_available_ = true;
      return "!full!" + next_target_str();
    } else {
      new_block_available_ = false;
      return "!redo";
    }
  }
  //LOG(log_level::info) << "Come here 1";
  partition_.push_back(make_binary(message));
  //LOG(log_level::info) << "Come here 2";
  return "!ok";
}

std::string fifo_queue_partition::dequeue() {
  //LOG(log_level::info) << "Dequeueing " << head_;
  if (head_ < partition_.size()) {
    return to_string(partition_[head_++]);
  }
  if(storage_size() * 2 >= storage_capacity() && partition_.size() >= partition_.capacity()) {
      if (!next_target_str().empty()) {
        head_++;
        new_block_available_ = true;
        return "!msg_not_in_partition!" + next_target_str();
      } else {
        return "!redo";
      }
  }
  return "!msg_not_found";
}

std::string fifo_queue_partition::clear() {
  partition_.clear();
  return "!ok";
}

std::string fifo_queue_partition::update_partition(const std::string &next) {
  next_target(next);
  return "!ok";
}

void fifo_queue_partition::run_command(std::vector<std::string> &_return,
                                      int32_t cmd_id,
                                      const std::vector<std::string> &args) {
  size_t nargs = args.size();
  switch (cmd_id) {
    case fifo_queue_cmd_id::fq_enqueue:
      for (const std::string &msg: args)
        _return.emplace_back(enqueue(msg));
      break;
    case fifo_queue_cmd_id::fq_dequeue:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(dequeue());
      }
      break;
    case fifo_queue_cmd_id::fq_clear:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(clear());
      }
      break;
    case fifo_queue_cmd_id::fq_update_partition:
      if (nargs != 1) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(update_partition(args[0]));
      }
      break;
    default:throw std::invalid_argument("No such operation id " + std::to_string(cmd_id));
  }
  if (is_mutator(cmd_id)) {
    dirty_ = true;
  }
  bool expected = false;
  if (auto_scale_.load() && is_mutator(cmd_id) && overload() && is_tail()
      && overload_.compare_exchange_strong(expected, true)) {
    //LOG(log_level::info) << "Overloaded partition; storage = " << storage_size() << " capacity = "
    //                     << storage_capacity() << " partition size = " << size() << "partition capacity "
    //                     << partition_.capacity();
    try {
      overload_ = true;
      std::string dst_partition_name = std::to_string(std::stoi(name_) + 1);
      std::map<std::string, std::string> scale_conf;
      scale_conf.emplace(std::make_pair(std::string("type"), std::string("fifo_queue_add")));
      scale_conf.emplace(std::make_pair(std::string("next_partition_name"), dst_partition_name));
      auto scale = std::make_shared<auto_scaling::auto_scaling_client>(auto_scaling_host_, auto_scaling_port_);
      scale->auto_scaling(chain(), path(), scale_conf);
    } catch (std::exception &e) {
      overload_ = false;
      LOG(log_level::warn) << "Adding new message queue partition failed: " << e.what();
    }
  }
  expected = false;
  if (auto_scale_.load() && cmd_id == fifo_queue_cmd_id::fq_dequeue && head_ > partition_.capacity() && is_tail()
      && underload_.compare_exchange_strong(expected, true) && new_block_available_ == true) {
    try {
      //LOG(log_level::info) << "Underloaded partition; storage = " << storage_size() << " capacity = "
      //                     << storage_capacity() << " partition size = " << size() << "partition capacity "
      //                     << partition_.capacity();
      underload_ = true;
      std::string dst_partition_name = std::to_string(std::stoi(name_) + 1);
      std::map<std::string, std::string> scale_conf;
      scale_conf.emplace(std::make_pair(std::string("type"), std::string("fifo_queue_delete")));
      scale_conf.emplace(std::make_pair(std::string("current_partition_name"), name()));
      auto scale = std::make_shared<auto_scaling::auto_scaling_client>(auto_scaling_host_, auto_scaling_port_);
      scale->auto_scaling(chain(), path(), scale_conf);
    } catch (std::exception &e) {
      underload_ = false;
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
  return dirty_.load();
}

void fifo_queue_partition::load(const std::string &path) {
  auto remote = persistent::persistent_store::instance(path, ser_);
  auto decomposed = persistent::persistent_store::decompose_path(path);
  remote->read<fifo_queue_type>(decomposed.second, partition_);
}

bool fifo_queue_partition::sync(const std::string &path) {
  bool expected = true;
  if (dirty_.compare_exchange_strong(expected, false)) {
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    remote->write<fifo_queue_type>(partition_, decomposed.second);
    return true;
  }
  return false;
}

bool fifo_queue_partition::dump(const std::string &path) {
  bool expected = true;
  bool flushed = false;
  if (dirty_.compare_exchange_strong(expected, false)) {
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    remote->write<fifo_queue_type>(partition_, decomposed.second);
    flushed = true;
  }
  partition_.clear();
  next_->reset("nil");
  path_ = "";
  // clients().clear();
  sub_map_.clear();
  chain_ = {};
  role_ = singleton;
  overload_ = false;
  dirty_ = false;
  return flushed;
}

void fifo_queue_partition::forward_all() {
  int64_t i = 0;
  for (auto it = partition_.begin(); it != partition_.end(); it++) {
    std::vector<std::string> result;
    run_command_on_next(result, fifo_queue_cmd_id::fq_enqueue, {to_string(*it)});
    ++i;
  }
}

bool fifo_queue_partition::overload() {
  //if (storage_size() < storage_capacity())
  //return false;
  //return partition_.size() > static_cast<size_t>(static_cast<double>(partition_.capacity()) * threshold_hi_);
  //TODO this is a hot fix
  return partition_.size() > static_cast<size_t>(static_cast<double>(1024) * threshold_hi_);
  //return storage_size() > static_cast<size_t>(static_cast<double>(storage_capacity()) * threshold_hi_);
}

REGISTER_IMPLEMENTATION("fifoqueue", fifo_queue_partition);

}
}