#include "shared_log_partition.h"
#include "jiffy/persistent/persistent_store.h"
#include "jiffy/storage/partition_manager.h"
#include "jiffy/storage/shared_log/shared_log_ops.h"
#include "jiffy/auto_scaling/auto_scaling_client.h"
#include <jiffy/utils/directory_utils.h>
#include <thread>
#include <iostream>

namespace jiffy {
namespace storage {

using namespace utils;

#define min(a, b) ((a) < (b) ? (a) : (b))

shared_log_partition::shared_log_partition(block_memory_manager *manager,
                                           const std::string &backing_path,
                                           const std::string &name,
                                           const std::string &metadata,
                                           const utils::property_map &conf,
                                           const std::string &auto_scaling_host,
                                           int auto_scaling_port)
    : chain_module(manager, backing_path, name, metadata, SHARED_LOG_OPS),
      partition_(manager->mb_capacity(), build_allocator<char>()),
      scaling_up_(false),
      dirty_(false),
      block_allocated_(false),
      auto_scaling_host_(auto_scaling_host),
      auto_scaling_port_(auto_scaling_port) {
  auto ser_name_ = conf.get("shared_log.serializer", "binary");
  if (ser_name_ == "binary") {
    ser_ = std::make_shared<binary_serde>(binary_allocator_);
  } else {
    throw std::invalid_argument("No such serializer/deserializer " + ser_name_);
  }
  auto_scale_ = conf.get_as<bool>("shared_log.auto_scale", true);
}

void shared_log_partition::write(response &_return, const arg_list &args) {
  if (args.size() < 4) {
    RETURN_ERR("!args_error");
  }
  auto position = std::stoi(args[1]);
  if (log_info_.size() == 0) {
    seq_no_ = position;
  }
  auto data = args[2];
  std::string logical_stream = "";
  std::vector<int> info_set;
  info_set.push_back(int(starting_offset_));
  info_set.push_back(int(data.size()));
  for (std::size_t i = 3; i < args.size(); i++) {
    logical_stream += args[i];
    info_set.push_back(int(args[i].size()));
  }
  log_info_.push_back(info_set);
  std::string writing_content = logical_stream + data;
  auto ret = partition_.write(writing_content, starting_offset_);
  if (!ret.first) {
    throw std::logic_error("Write failed");
  }
  starting_offset_ += writing_content.size();
  RETURN_OK();

}

void shared_log_partition::scan(response &_return, const arg_list &args) {
  if (args.size() < 4) {
    RETURN_ERR("!args_error");
  }
  auto start_pos = std::stoi(args[1]) - seq_no_;
  auto end_pos = std::stoi(args[2]) - seq_no_;
  if (static_cast<size_t>(end_pos) > log_info_.size()) end_pos = log_info_.size() - 1;
  std::vector<std::string> logical_streams = {};
  for (std::size_t i = 3; i < args.size(); i++) {
    logical_streams.push_back(args[i]);
  }
  std::vector<std::string> ret = {"!ok"};
  if (log_info_.size() == 0) {
    _return = ret;
    return;
  }
  if (start_pos < 0 || static_cast<size_t>(start_pos) >= log_info_.size() || end_pos < 0 || end_pos < start_pos)
    throw std::invalid_argument("scan position invalid");
  for (int i = start_pos; i <= end_pos; i++) {
    auto info_set = log_info_[i];
    if (info_set[0] == -1) continue;
    int temp_offset = info_set[0];
    int data_size = info_set[1];
    int stream_size = 0;
    for (std::size_t j = 2; j < info_set.size(); j++) {
      stream_size += info_set[j];
    }
    for (std::size_t j = 2; j < info_set.size(); j++) {
      auto
          stream = partition_.read(static_cast<std::size_t>(temp_offset), static_cast<std::size_t>(info_set[j])).second;
      temp_offset += info_set[j];
      std::vector<std::string>::iterator it;
      it = find(logical_streams.begin(), logical_streams.end(), stream);
      if (it != logical_streams.end()) {
        auto data = partition_.read(static_cast<std::size_t>(info_set[0] + stream_size),
                                    static_cast<std::size_t>(data_size)).second;
        ret.push_back(data);
        break;
      }
    }
  }
  _return = ret;

}

void shared_log_partition::trim(response &_return, const arg_list &args) {
  if (args.size() != 3) {
    RETURN_ERR("!args_error");
  }
  if (log_info_.size() == 0) {
    RETURN_OK(0);
  }
  auto start_pos = std::stoi(args[1]) - seq_no_;
  auto end_pos = std::stoi(args[2]) - seq_no_;
  if (start_pos < 0 || static_cast<size_t>(start_pos) >= log_info_.size() || end_pos < 0 || end_pos < start_pos)
    throw std::invalid_argument("trim position invalid");
  if (static_cast<size_t>(end_pos) > log_info_.size()) end_pos = log_info_.size() - 1;
  int trimmed_length = 0;
  for (int i = start_pos; i <= end_pos; i++) {
    auto info_set = log_info_[i];
    if (info_set[0] == -1) continue;
    log_info_[i][0] = -1; // make the log entry invalid
    for (std::size_t j = 1; j < info_set.size(); j++) {
      trimmed_length += info_set[j];
    }
  }

  RETURN_OK(std::to_string(trimmed_length));
}

void shared_log_partition::update_partition(response &_return, const arg_list &args) {
  if (args.size() >= 2) {
    block_allocated_ = true;
    allocated_blocks_.insert(allocated_blocks_.end(), args.begin() + 1, args.end());
  }
  RETURN_OK();
}

void shared_log_partition::add_blocks(response &_return, const arg_list &args) {
  if (args.size() != 3) {
    RETURN_ERR("!args_error");
  }
  if (!scaling_up_) {
    scaling_up_ = true;
    std::string dst_partition_name = std::to_string(std::stoi(args[1]) + 1);
    std::map<std::string, std::string>
        scale_conf{{"type", "shared_log"}, {"next_partition_name", dst_partition_name}, {"partition_num", args[2]}};
    auto scale = std::make_shared<auto_scaling::auto_scaling_client>(auto_scaling_host_, auto_scaling_port_);
    scale->auto_scaling(chain(), path(), scale_conf);
  }
  if (block_allocated_) {
    block_allocated_ = false;
    scaling_up_ = false;
    _return.push_back("!block_allocated");
    _return.insert(_return.end(), allocated_blocks_.begin(), allocated_blocks_.end());
    allocated_blocks_.clear();
    return;
  }
  RETURN_ERR("!blocks_not_ready");
}

void shared_log_partition::get_storage_capacity(response &_return, const arg_list &args) {
  if (args.size() != 1) {
    RETURN_ERR("!args_error");
  }
  RETURN_OK(std::to_string(manager_->mb_capacity()));
}

void shared_log_partition::run_command(response &_return, const arg_list &args) {
  auto cmd_name = args[0];
  switch (command_id(cmd_name)) {
    case shared_log_cmd_id::shared_log_write:write(_return, args);
      break;
    case shared_log_cmd_id::shared_log_scan:scan(_return, args);
      break;
    case shared_log_cmd_id::shared_log_trim:trim(_return, args);
      break;
    case shared_log_cmd_id::shared_log_update_partition:update_partition(_return, args);
      break;
    case shared_log_cmd_id::shared_log_add_blocks:add_blocks(_return, args);
      break;
    case shared_log_cmd_id::shared_log_get_storage_capacity:get_storage_capacity(_return, args);
      break;
    default: {
      _return.emplace_back("!no_such_command");
      return;
    }
  }

  if (is_mutator(cmd_name)) {
    dirty_ = true;
  }
}

std::size_t shared_log_partition::size() const {
  return partition_.size();
}

bool shared_log_partition::is_dirty() const {
  return dirty_;
}

void shared_log_partition::load(const std::string &path) {
  auto remote = persistent::persistent_store::instance(path, ser_);
  auto decomposed = persistent::persistent_store::decompose_path(path);
  shared_log_serde_type triple = {&partition_, log_info_, seq_no_};
  remote->read<shared_log_serde_type>(decomposed.second, triple);
}

bool shared_log_partition::sync(const std::string &path) {
  if (dirty_) {
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    shared_log_serde_type triple = {&partition_, log_info_, seq_no_};
    remote->write<shared_log_serde_type>(triple, decomposed.second);
    dirty_ = false;
    return true;
  }
  return false;
}

bool shared_log_partition::dump(const std::string &path) {
  bool flushed = false;
  if (dirty_) {
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    shared_log_serde_type triple = {&partition_, log_info_, seq_no_};
    remote->write<shared_log_serde_type>(triple, decomposed.second);
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

void shared_log_partition::forward_all() {
  std::vector<std::string> result;
  run_command_on_next(result, {"write", std::string(partition_.data(), partition_.size())});
}

REGISTER_IMPLEMENTATION("shared_log", shared_log_partition);

}
}
