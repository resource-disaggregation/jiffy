#include <jiffy/utils/string_utils.h>
#include "file_partition.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/logger.h"
#include "jiffy/persistent/persistent_store.h"
#include "jiffy/storage/partition_manager.h"
#include "jiffy/storage/file/file_ops.h"
#include "jiffy/directory/client/directory_client.h"
#include "jiffy/auto_scaling/auto_scaling_client.h"
#include <thread>

namespace jiffy {
namespace storage {

using namespace utils;

file_partition::file_partition(block_memory_manager *manager,
                               const std::string &name,
                               const std::string &metadata,
                               const utils::property_map &conf,
                               const std::string &directory_host,
                               int directory_port,
                               const std::string &auto_scaling_host,
                               int auto_scaling_port)
    : chain_module(manager, name, metadata, FILE_OPS),
      partition_(manager->mb_capacity(), build_allocator<char>()),
      overload_(false),
      dirty_(false),
      directory_host_(directory_host),
      directory_port_(directory_port),
      auto_scaling_host_(auto_scaling_host),
      auto_scaling_port_(auto_scaling_port) {
  (void) directory_host_;
  (void) directory_port_;
  auto ser = conf.get("file.serializer", "csv");
  if (ser == "binary") {
    ser_ = std::make_shared<csv_serde>(binary_allocator_);
  } else if (ser == "csv") {
    ser_ = std::make_shared<binary_serde>(binary_allocator_);
  } else {
    throw std::invalid_argument("No such serializer/deserializer " + ser);
  }
  threshold_hi_ = conf.get_as<double>("file.capacity_threshold_hi", 0.95);
  auto_scale_ = conf.get_as<bool>("file.auto_scale", true);
}

std::string file_partition::write(const std::string &message) {
  if (partition_.size() > partition_.capacity()) {
    if (!next_target_str().empty()) {
      return "!full!" + next_target_str();
    } else if (!auto_scale_) {
      return "!next_partition";
    }
    return "!redo";
  }
  auto ret = partition_.push_back(message);
  if (!ret.first) {
    if (!auto_scale_) {
      return "!direct_split_write!" + std::to_string(ret.second.size());
    }
    return "!split_write!" + next_target_str() + "!" + std::to_string(ret.second.size());
  }
  return "!ok";
}

std::string file_partition::read(std::string position, std::string size) {
  auto pos = std::stoi(position);
  auto read_size = std::stoi(size);
  if (pos < 0) throw std::invalid_argument("read position invalid");
  auto ret = partition_.read(static_cast<std::size_t>(pos), static_cast<std::size_t>(read_size));
  if (ret.first) {
    return ret.second;
  } else if (ret.second == "!reach_end") {
    if (!next_target_str().empty() || !auto_scale_)
      return "!msg_not_in_partition";
    else {
      return "!redo";
    }
  } else if (ret.second == "!not_available") {
    return "!msg_not_found";
  } else {
    return "!split_read!" + ret.second;
  }
}

void file_partition::seek(std::vector<std::string> &ret) {
  ret.emplace_back(std::to_string(partition_.size()));
  ret.emplace_back(std::to_string(partition_.capacity()));
}

std::string file_partition::clear() {
  partition_.clear();
  overload_ = false;
  dirty_ = false;
  return "!ok";
}

std::string file_partition::update_partition(const std::string &next) {
  next_target(next);
  return "!ok";
}

void file_partition::run_command(std::vector<std::string> &_return,
                                 int32_t cmd_id,
                                 const std::vector<std::string> &args) {
  size_t nargs = args.size();
  switch (cmd_id) {
    case file_cmd_id::file_write:
      for (const std::string &msg: args)
        _return.emplace_back(write(msg));
      break;
    case file_cmd_id::file_read:
      if (nargs % 2 != 0) {
        _return.emplace_back("!args_error");
      }
      for (size_t i = 0; i < nargs; i += 2)
        _return.emplace_back(read(args[i], args[i + 1]));
      break;
    case file_cmd_id::file_clear:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(clear());
      }
      break;
    case file_cmd_id::file_update_partition:
      if (nargs != 1) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(update_partition(args[0]));
      }
      break;
    case file_cmd_id::file_seek:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        std::vector<std::string> ret;
        seek(ret);
        _return.emplace_back(ret[0]);
        _return.emplace_back(ret[1]);
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
                         << storage_capacity() << " partition size = " << size() << "partition capacity "
                         << partition_.capacity();
    try {
      overload_ = true;
      std::string dst_partition_name = std::to_string(std::stoi(name_) + 1);
      std::map<std::string, std::string> scale_conf;
      scale_conf.emplace(std::make_pair(std::string("type"), std::string("file")));
      scale_conf.emplace(std::make_pair(std::string("next_partition_name"), dst_partition_name));
      auto scale = std::make_shared<auto_scaling::auto_scaling_client>(auto_scaling_host_, auto_scaling_port_);
      scale->auto_scaling(chain(), path(), scale_conf);
    } catch (std::exception &e) {
      overload_ = false;
      LOG(log_level::warn) << "Adding new message queue partition failed: " << e.what();
    }
  }
}

std::size_t file_partition::size() const {
  return partition_.size();
}

bool file_partition::empty() const {
  return partition_.empty();
}

bool file_partition::is_dirty() const {
  return dirty_.load();
}

void file_partition::load(const std::string &path) {
  auto remote = persistent::persistent_store::instance(path, ser_);
  auto decomposed = persistent::persistent_store::decompose_path(path);
  remote->read<file_type>(decomposed.second, partition_);
}

bool file_partition::sync(const std::string &path) {
  bool expected = true;
  if (dirty_.compare_exchange_strong(expected, false)) {
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    remote->write<file_type>(partition_, decomposed.second);
    return true;
  }
  return false;
}

bool file_partition::dump(const std::string &path) {
  std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
  bool expected = true;
  bool flushed = false;
  if (dirty_.compare_exchange_strong(expected, false)) {
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    remote->write<file_type>(partition_, decomposed.second);
    flushed = true;
  }
  partition_.clear();
  next_->reset("nil");
  path_ = "";
  sub_map_.clear();
  chain_ = {};
  role_ = singleton;
  overload_ = false;
  dirty_ = false;
  return flushed;
}

void file_partition::forward_all() {
  std::vector<std::string> result;
  run_command_on_next(result, file_cmd_id::file_write, {std::string(partition_.data(), partition_.size())});
}

bool file_partition::overload() {
  return partition_.size() >= static_cast<size_t>(static_cast<double>(partition_.capacity()) * threshold_hi_);
}

REGISTER_IMPLEMENTATION("file", file_partition);

}
}