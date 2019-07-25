#include "file_partition.h"
#include "jiffy/persistent/persistent_store.h"
#include "jiffy/storage/partition_manager.h"
#include "jiffy/storage/file/file_ops.h"
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
      scaling_up_(false),
      dirty_(false),
      directory_host_(directory_host),
      directory_port_(directory_port),
      auto_scaling_host_(auto_scaling_host),
      auto_scaling_port_(auto_scaling_port) {
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

void file_partition::write(response& _return, const arg_list &args) {
  if (args.size() != 3) {
    RETURN_ERR("!args_error");
  }
  auto off = std::stoi(args[2]);
  auto ret = partition_.write(args[1], off);
  if (!ret.first) {
    if (!auto_scale_) {
      RETURN_ERR("!split_write", std::to_string(ret.second.size()));
    }
    if (!next_target().empty()) {
      RETURN_ERR("!split_write", std::to_string(ret.second.size()), next_target_str_);
    } else {
      RETURN_ERR("!redo");
    }
  }
  RETURN_OK();
}

void file_partition::read(response& _return, const arg_list &args) {
  if (args.size() != 3) {
    RETURN_ERR("!args_error");
  }
  auto pos = std::stoi(args[1]);
  auto size = std::stoi(args[2]);
  if (pos < 0) throw std::invalid_argument("read position invalid");
  auto ret = partition_.read(static_cast<std::size_t>(pos), static_cast<std::size_t>(size));
  if (ret.first) {
    RETURN_OK(ret.second);
  }
  if (ret.second == "!not_available") {
    RETURN_ERR("!msg_not_found");
  } else {
    if (!auto_scale_) {
      RETURN_ERR("!split_read", ret.second);
    }
    if (!next_target().empty()) {
      RETURN_ERR("!split_read", ret.second, next_target_str_);
    } else {
      RETURN_ERR("!redo");
    }
  }
}

void file_partition::seek(response& _return, const arg_list &args) {
  if (args.size() != 1) {
    RETURN_ERR("!args_error");
  }
  RETURN_OK(std::to_string(partition_.size()), std::to_string(partition_.capacity()));
}

void file_partition::clear(response& _return, const arg_list &args) {
  if (args.size() != 1) {
    RETURN_ERR("!args_error");
  }
  partition_.clear();
  scaling_up_ = false;
  dirty_ = false;
  RETURN_OK();
}

void file_partition::update_partition(response& _return, const arg_list &args) {
  if (args.size() != 2) {
    RETURN_ERR("!args_error");
  }
  next_target(args[1]);
  RETURN_OK();
}

void file_partition::run_command(response &_return, const arg_list &args) {
  auto cmd_name = args[0];
  switch (command_id(cmd_name)) {
    case file_cmd_id::file_write:
      write(_return, args);
      break;
    case file_cmd_id::file_read:
      read(_return, args);
      break;
    case file_cmd_id::file_clear:
      clear(_return, args);
      break;
    case file_cmd_id::file_update_partition:
      update_partition(_return, args);
      break;
    case file_cmd_id::file_seek:
      seek(_return, args);
      break;
    default: {
      _return.emplace_back("!no_such_command");
      return;
    }
  }
  if (is_mutator(cmd_name)) {
    dirty_ = true;
  }
  if (auto_scale_ && is_mutator(cmd_name) && overload() && is_tail() && !scaling_up_ && next_target().empty()) {
    LOG(log_level::info) << "Overloaded partition; storage = " << storage_size() << " capacity = "
                         << storage_capacity() << " partition size = " << size() << " partition capacity = "
                         << partition_.capacity();
    try {
      scaling_up_ = true;
      std::string dst_partition_name = std::to_string(std::stoi(name_) + 1);
      std::map<std::string, std::string> scale_conf{{"type", "file"}, {"next_partition_name", dst_partition_name}};
      auto scale = std::make_shared<auto_scaling::auto_scaling_client>(auto_scaling_host_, auto_scaling_port_);
      scale->auto_scaling(chain(), path(), scale_conf);
    } catch (std::exception &e) {
      scaling_up_ = false;
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
  return dirty_;
}

void file_partition::load(const std::string &path) {
  auto remote = persistent::persistent_store::instance(path, ser_);
  auto decomposed = persistent::persistent_store::decompose_path(path);
  remote->read<file_type>(decomposed.second, partition_);
}

bool file_partition::sync(const std::string &path) {
  if (dirty_) {
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    remote->write<file_type>(partition_, decomposed.second);
    dirty_ = false;
    return true;
  }
  return false;
}

bool file_partition::dump(const std::string &path) {
  bool flushed = false;
  if (dirty_) {
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
  scaling_up_ = false;
  dirty_ = false;
  return flushed;
}

void file_partition::forward_all() {
  std::vector<std::string> result;
  run_command_on_next(result, {"write", std::string(partition_.data(), partition_.size())});
}

bool file_partition::overload() {
  return partition_.size() >= static_cast<size_t>(static_cast<double>(partition_.capacity()) * threshold_hi_);
}

REGISTER_IMPLEMENTATION("file", file_partition);

}
}
