#include <jiffy/utils/string_utils.h>
#include "msg_queue_partition.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/logger.h"
#include "jiffy/persistent/persistent_store.h"
#include "jiffy/storage/partition_manager.h"
#include "jiffy/storage/msgqueue/msg_queue_ops.h"
#include "jiffy/directory/client/directory_client.h"
#include <thread>

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
      overload_(false),
      full_(false),
      dirty_(false),
      directory_host_(directory_host),
      directory_port_(directory_port) {
  auto ser = conf.get("msgqueue.serializer", "csv");
  if (ser == "binary") {
    ser_ = std::make_shared<csv_serde>(binary_allocator_);
  } else if (ser == "csv") {
    ser_ = std::make_shared<binary_serde>(binary_allocator_);
  } else {
    throw std::invalid_argument("No such serializer/deserializer " + ser);
  }
  threshold_hi_ = conf.get_as<double>("msgqueue.capacity_threshold_hi", 0.95);
  threshold_lo_ = conf.get_as<double>("msgqueue.capacity_threshold_lo", 0.00);
  auto_scale_ = conf.get_as<bool>("msgqueue.auto_scale", true);
  LOG(log_level::info) << "Partition name: " << name_;
}

std::string msg_queue_partition::send(const std::string &message) {
  LOG(log_level::info) << "sending message: " << message << " full " << full_;
  LOG(log_level::info) << "partition size is " << partition_.size();
  LOG(log_level::info) << "partition capacity is " << partition_.capacity();
  std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
  //bool expected = false;
  if (storage_size() >=  storage_capacity() && partition_.size() >= partition_.capacity()) {
    LOG(log_level::info) << "I'm in this loop ";
    if (!next_target_str().empty()) {
      return "!full!" + next_target_str();
    } else {
      throw std::logic_error("The message queue should already allocate next partition when overload");
    }
  }
  partition_.push_back(make_binary(message));
  LOG(log_level::info) << "Message sent: " << message;
  return "!ok";
}

std::string msg_queue_partition::read(std::string position) {
  LOG(log_level::info) << "Reading at this position " << position << "with full size" << size();
  std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
  auto pos = std::stoi(position);
  if (pos < 0) throw std::invalid_argument("read position invalid");
  if (static_cast<std::size_t>(pos) < size()) {
    return to_string(partition_[pos]);
  }
  if (!next_target_str().empty())
    return "!msg_not_in_partition!" + next_target_str();
  else return "!msg_not_found";
}

std::string msg_queue_partition::clear() {
  std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
  partition_.clear();
  return "!ok";
}

std::string msg_queue_partition::update_partition(const std::string &next) {
  next_target(next);
  return "!ok";
}

void msg_queue_partition::run_command(std::vector<std::string> &_return,
                                      int32_t cmd_id,
                                      const std::vector<std::string> &args) {
  /*
  std::string chain_test;
  chain_to_string(chain_test);
  LOG(log_level::info) << "chain is::::" << chain_test;
  auto fs = std::make_shared<directory::directory_client>(directory_host_, directory_port_);
  LOG(log_level::info) << "host " << directory_host_ << " port " << directory_port_;
  auto src = std::make_shared<replica_chain_client>(fs, path(), chain());
  LOG(log_level::info) << "Successfully connect to this current replica chain client";
*/

  LOG(log_level::info) << "Storage = " << storage_size() << " capacity = " << storage_capacity();
  size_t nargs = args.size();
  switch (cmd_id) {
    case msg_queue_cmd_id::mq_send:
      for (const std::string &msg: args)
        _return.emplace_back(send(msg));
      break;
    case msg_queue_cmd_id::mq_read:
      for (const auto &pos: args)
        _return.emplace_back(read(pos));
      break;
    case msg_queue_cmd_id::mq_clear:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(clear());
      }
      break;
    case msg_queue_cmd_id::mq_update_partition:
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
    LOG(log_level::info) << "Overloaded partition; storage = " << storage_size() << " capacity = "
                         << storage_capacity();
    try {
      overload_ = true;
      LOG(log_level::info) << "Requested new message queue partition";
      std::string dst_partition_name = std::to_string(std::stoi(name()) + 1);
      auto fs = std::make_shared<directory::directory_client>(directory_host_, directory_port_);
      LOG(log_level::info) << "host " << directory_host_ << " port " << directory_port_;
      //TODO will we use the other stuff in replica chain?
      auto dst_replica_chain =
          fs->add_block(path(), dst_partition_name, "regular");
      LOG(log_level::info) << "New replica chain added";
      next_target(dst_replica_chain.block_ids);
      std::string chain_test;
      chain_to_string(chain_test);
      LOG(log_level::info) << "Trying to connect to current replica chain client path: " << path() << "chain " << chain_test;
      auto path_current = path();
      auto chain_current = chain();
      std::vector<std::string> src_before_args;
      src_before_args.push_back(next_target_str());
      //std::thread update_worker([fs, path_current,chain_current,src_before_args]() {
      //  auto src = std::make_shared<replica_chain_client>(fs, path_current, chain_current);
      //  src->send_command(msg_queue_cmd_id::mq_update_partition, src_before_args);
      //  src->recv_response();
      //});
      //update_worker.join();
      LOG(log_level::info) << "Trying to connect to the currently replica chain";
      //auto src = std::make_shared<replica_chain_client>(fs, path(), directory::replica_chain(dst_replica_chain));
      LOG(log_level::info) << "Replica chain client connected";
      update_partition(src_before_args.front());
      LOG(log_level::info) << "Sending update partition";
      //src->send_command(msg_queue_cmd_id::mq_update_partition, src_before_args);
      LOG(log_level::info) << "Receiving response";
      //src->recv_response();
      LOG(log_level::info) << "Finish adding new queue";
    } catch (std::exception &e) {
      overload_ = false;
      LOG(log_level::warn) << "Split slot range failed: " << e.what();
    }
    LOG(log_level::info) << "After split storage: " << storage_size() << " capacity: " << storage_capacity();
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
  remote->read<msg_queue_type>(decomposed.second, partition_);
}

bool msg_queue_partition::sync(const std::string &path) {
  bool expected = true;
  if (dirty_.compare_exchange_strong(expected, false)) {
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    remote->write<msg_queue_type>(partition_, decomposed.second);
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
    remote->write<msg_queue_type>(partition_, decomposed.second);
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
  full_ = false;
  dirty_ = false;
  return flushed;
}

void msg_queue_partition::forward_all() {
  int64_t i = 0;
  for (auto it = partition_.begin(); it != partition_.end(); it++) {
    std::vector<std::string> result;
    run_command_on_next(result, msg_queue_cmd_id::mq_send, {to_string(*it)});
    ++i;
  }
}

bool msg_queue_partition::overload() {
  if(storage_size() < storage_capacity())
    return false;
  return partition_.size() > static_cast<size_t>(static_cast<double>(partition_.capacity()) * threshold_hi_);
}

REGISTER_IMPLEMENTATION("msgqueue", msg_queue_partition);

}
}