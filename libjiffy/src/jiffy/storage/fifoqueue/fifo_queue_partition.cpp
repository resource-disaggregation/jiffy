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
                                           const std::string &auto_scaling_host,
                                           int auto_scaling_port)
    : chain_module(manager, name, metadata, FQ_CMDS),
      partition_(manager->mb_capacity(), build_allocator<char>()),
      scaling_up_(false),
      scaling_down_(false),
      dirty_(false),
      auto_scaling_host_(auto_scaling_host),
      auto_scaling_port_(auto_scaling_port),
      head_(0),
      read_head_(0),
      enqueue_redirected_(false),
      dequeue_redirected_(false),
      readnext_redirected_(false),
      enqueue_num_elements_(0),
      dequeue_num_elements_(0),
      prev_num_elements_(0) {
  auto ser = conf.get("fifoqueue.serializer", "csv");
  if (ser == "binary") {
    ser_ = std::make_shared<csv_serde>(binary_allocator_);
  } else if (ser == "csv") {
    ser_ = std::make_shared<binary_serde>(binary_allocator_);
  } else {
    throw std::invalid_argument("No such serializer/deserializer " + ser);
  }
  threshold_hi_ = conf.get_as<double>("fifoqueue.capacity_threshold_hi", 0.95);
  auto_scale_ = conf.get_as<bool>("fifoqueue.auto_scale", true);
}

void fifo_queue_partition::enqueue(response &_return, const arg_list &args) {
  if (!(args.size() == 2 || (args.size() == 4 && args[3] == "!redirected"))) {
    RETURN_ERR("!args_error");
  }
  if (prev_num_elements_ == 0 && (args.size() == 4 && args[3] == "!redirected")) {
    enqueue_num_elements_ += std::stoul(args[2]);
    prev_num_elements_ = std::stoul(args[2]);
  }
  auto ret = partition_.push_back(args[1]);
  if (!ret.first) {
    if (!auto_scale_) {
      enqueue_redirected_ = true;
      RETURN_ERR("!split_enqueue", std::to_string(ret.second.size()), std::to_string(enqueue_num_elements_));
    } else if (!next_target_str_.empty()) {
      enqueue_redirected_ = true;
      RETURN_ERR("!split_enqueue",
                 std::to_string(ret.second.size()),
                 next_target_str_,
                 std::to_string(enqueue_num_elements_));
    } else {
      RETURN_ERR("!redo");
    }
  }
  enqueue_num_elements_++;
  RETURN_OK();
}

void fifo_queue_partition::dequeue(response &_return, const arg_list &args) {
  if (!(args.size() == 1 || (args.size() == 2 && args[1] == "!redirected"))) {
    RETURN_ERR("!args_error");
  }
  if (args.size() == 2 && args[1] == "!redirected" && dequeue_num_elements_ == 0) {
    dequeue_num_elements_ += prev_num_elements_;
  }
  auto ret = partition_.at(head_);
  if (ret.first) {
    head_ += (string_array::METADATA_LEN + ret.second.size());
    update_read_head();
    dequeue_num_elements_++;
    RETURN_OK(ret.second);
  }
  if (ret.second == "!not_available") {
    RETURN_ERR("!msg_not_found");
  }
  if (!auto_scale_) {
    dequeue_redirected_ = true;
    RETURN_ERR("!split_dequeue", ret.second);
  }
  if (!next_target_str_.empty()) {
    dequeue_redirected_ = true;
    RETURN_ERR("!split_dequeue", ret.second, next_target_str_);
  }
  RETURN_ERR("!redo");
}

void fifo_queue_partition::read_next(response &_return, const arg_list &args) {
  if (args.size() != 1) {
    RETURN_ERR("!args_error");
  }
  auto ret = partition_.at(read_head_);
  if (ret.first) {
    read_head_ += (string_array::METADATA_LEN + ret.second.size());
    RETURN_OK(ret.second);
  }
  if (ret.second == "!not_available") {
    RETURN_ERR("!msg_not_found");
  }
  if (!auto_scale_) {
    RETURN_ERR("!split_readnext", ret.second);
  }
  if (!next_target_str_.empty()) {
    readnext_redirected_ = true;
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
  read_head_ = 0;
  enqueue_redirected_ = false;
  dequeue_redirected_ = false;
  readnext_redirected_ = false;
  enqueue_num_elements_ = 0;
  dequeue_num_elements_ = 0;
  prev_num_elements_ = 0;
  RETURN_OK();
}

void fifo_queue_partition::update_partition(response &_return, const arg_list &args) {
  next_target(args[1]);
  RETURN_OK();
}

void fifo_queue_partition::qsize(response &_return, const arg_list &args) {
  switch (std::stoi(args[1])) {
    case fifo_queue_size_type::head_size:
      if (overload() && enqueue_redirected_) {
        RETURN_ERR("!split_qsize", next_target_str_);
      } else {
        RETURN_OK(std::to_string(enqueue_num_elements_));
      }
    case fifo_queue_size_type::tail_size:
      if (underload() && dequeue_redirected_) {
        RETURN_ERR("!split_qsize", next_target_str_);
      } else {
        RETURN_OK(std::to_string(dequeue_num_elements_));
      }
    default:throw std::logic_error("Undefined type for qsize operation");
  }
}

void fifo_queue_partition::in_rate(response &_return, const arg_list &args) {
  if(!rate_set_)
    RETURN_ERR("!redo")
  RETURN_OK(std::to_string(in_rate_));
}

void fifo_queue_partition::out_rate(response &_return, const arg_list &args) {
  if(!rate_set_)
    RETURN_ERR("!redo")
  RETURN_OK(std::to_string(out_rate_));
}

void fifo_queue_partition::run_command(response &_return, const arg_list &args) {
  auto cmd_name = args[0];
  switch (command_id(cmd_name)) {
    case fifo_queue_cmd_id::fq_enqueue:enqueue(_return, args);
      break;
    case fifo_queue_cmd_id::fq_dequeue:dequeue(_return, args);
      break;
    case fifo_queue_cmd_id::fq_readnext:read_next(_return, args);
      break;
    case fifo_queue_cmd_id::fq_clear:clear(_return, args);
      break;
    case fifo_queue_cmd_id::fq_update_partition:update_partition(_return, args);
      break;
    case fifo_queue_cmd_id::fq_qsize:qsize(_return, args);
      break;
    default: {
      _return.emplace_back("!no_such_command");
      return;
    }
  }
  if (is_mutator(cmd_name)) {
    dirty_ = true;
  }
  if (auto_scale_ && is_mutator(cmd_name) && overload() && is_tail() && !scaling_up_ && !scaling_down_) {
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
  if (auto_scale_ && cmd_name == "dequeue" && underload() && is_tail() && !scaling_down_
      && (enqueue_redirected_ || dequeue_redirected_ || readnext_redirected_)
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
  head_ = 0;
  read_head_ = 0;
  enqueue_redirected_ = false;
  dequeue_redirected_ = false;
  readnext_redirected_ = false;
  enqueue_num_elements_ = 0;
  dequeue_num_elements_ = 0;
  prev_num_elements_ = 0;
  return flushed;
}

void fifo_queue_partition::forward_all() {
  for (auto it = partition_.begin(); it != partition_.end(); it++) {
    std::vector<std::string> ignore;
    run_command_on_next(ignore, {"enqueue", *it}); // TODO: Could be more efficient
  }
}

bool fifo_queue_partition::overload() {
  return partition_.full();
}

bool fifo_queue_partition::underload() {
  return head_ > partition_.last_element_offset() && partition_.full();
}

REGISTER_IMPLEMENTATION("fifoqueue", fifo_queue_partition);

}
}
