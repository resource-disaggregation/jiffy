#include "file_partition.h"
#include "jiffy/persistent/persistent_store.h"
#include "jiffy/storage/partition_manager.h"
#include "jiffy/storage/file/file_ops.h"
#include "jiffy/auto_scaling/auto_scaling_client.h"
#include <jiffy/utils/directory_utils.h>
#include <thread>
#include <iostream>

namespace jiffy {
namespace storage {

using namespace utils;

#define min(a,b) ((a) < (b) ? (a) : (b))

file_partition::file_partition(block_memory_manager *manager,
                               const std::string &backing_path,
                               const std::string &name,
                               const std::string &metadata,
                               const utils::property_map &conf,
                               const std::string &auto_scaling_host,
                               int auto_scaling_port)
    : chain_module(manager, backing_path, name, metadata, FILE_OPS),
      partition_(manager->mb_capacity(), build_allocator<char>()),
      scaling_up_(false),
      dirty_(false),
      block_allocated_(false),
      auto_scaling_host_(auto_scaling_host),
      auto_scaling_port_(auto_scaling_port) {
  auto ser = conf.get("file.serializer", "csv");
  if (ser == "binary") {
    ser_ = std::make_shared<binary_serde>(binary_allocator_);
  } else if (ser == "csv") {
    ser_ = std::make_shared<csv_serde>(binary_allocator_);
  } else {
    throw std::invalid_argument("No such serializer/deserializer " + ser);
  }
  auto_scale_ = conf.get_as<bool>("file.auto_scale", true);
}

void file_partition::write(response &_return, const arg_list &args) {
  if (args.size() != 5 && args.size() != 3) {
    RETURN_ERR("!args_error");
  }
  auto off = std::stoi(args[2]);
  auto ret = partition_.write(args[1], off);
  if (!ret.first) {
    throw std::logic_error("Write failed");
  }
  if (args.size() == 5) {
    int cache_block_size = std::stoi(args[3]);
    int last_offset = std::stoi(args[4]) + args[1].size();
    int start_offset = (int(off)) / cache_block_size * cache_block_size;
    int end_offset = (int(off) + args[1].size() - 1) / cache_block_size * cache_block_size;
    int num_of_blocks = (end_offset - start_offset) / cache_block_size + 1;
    auto full_block_data = partition_.read(static_cast<std::size_t>(start_offset), static_cast<std::size_t>(min(last_offset - start_offset, cache_block_size * num_of_blocks)));
    if (full_block_data.first) {
      RETURN_OK(full_block_data.second);
    }
  }
  RETURN_OK();
  
}

void file_partition::read(response &_return, const arg_list &args) {
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

}

void file_partition::write_ls(response &_return, const arg_list &args) {
  if (args.size() != 3 && args.size() != 5) {
    RETURN_ERR("!args_error");
  }
  std::string file_path, line, data;
  int pos = std::stoi(args[2]);
  file_path = directory_utils::decompose_path(backing_path());
  directory_utils::push_path_element(file_path,name());
  std::ofstream out(file_path,std::ios::in|std::ios::out);
  if (out) {
    data = args[1];
    out.seekp(pos, std::ios::beg);
    out << data;
    out.close();
  }
  else {
    RETURN_ERR("!file_does_not_exist");
  }
  if (args.size() == 5) {
    std::ifstream in(file_path,std::ios::in);
    int cache_block_size = std::stoi(args[3]);
    int last_offset = std::stoi(args[4]) + args[1].size();
    int start_offset = (int(off)) / cache_block_size * cache_block_size;
    int end_offset = (int(off) + args[1].size() - 1) / cache_block_size * cache_block_size;
    int num_of_blocks = (end_offset - start_offset) / cache_block_size + 1;
    if (in) {
      in.seekg(0, std::ios::end);
      in.seekg(start_offset, std::ios::beg);
      int read_size = min(last_offset - start_offset, cache_block_size * num_of_blocks);
      char *ret = new char[read_size];
      in.read(ret, read_size);
      std::string ret_str = ret;
      memset(ret, 0, read_size);
      delete[] ret;
      RETURN_OK(ret_str);
    }
  }
  RETURN_OK();
  
}

void file_partition::read_ls(response &_return, const arg_list &args) {
  if (args.size() != 3) {
    RETURN_ERR("!args_error");
  }
  std::string file_path, line, ret_str;
  auto pos = std::stoi(args[1]);
  auto size = std::stoi(args[2]);
  file_path = directory_utils::decompose_path(backing_path());
  directory_utils::push_path_element(file_path,name());
  std::ifstream in(file_path,std::ios::in);
  if (in) {
    if (pos < 0) throw std::invalid_argument("read position invalid");
    in.seekg(0, std::ios::end);
    in.seekg(pos, std::ios::beg);
    char *ret = new char[size];
    in.read(ret,size);
    ret_str = ret;
    memset(ret,0,size);
    delete[] ret;
    RETURN_OK(ret_str);
  }
  else {
    RETURN_ERR("file_does_not_exist");
  }

}

void file_partition::clear(response &_return, const arg_list &args) {
  if (args.size() != 1) {
    RETURN_ERR("!args_error");
  }
  partition_.clear();
  scaling_up_ = false;
  dirty_ = false;
  RETURN_OK();
}

void file_partition::update_partition(response &_return, const arg_list &args) {
  if (args.size() >= 2) {
    block_allocated_ = true;
    allocated_blocks_.insert(allocated_blocks_.end(), args.begin() + 1, args.end());
  }
  RETURN_OK();
}

void file_partition::add_blocks(response &_return, const arg_list &args) {
  if (args.size() != 3) {
    RETURN_ERR("!args_error");
  }
  if (!scaling_up_) {
    scaling_up_ = true;
    std::string dst_partition_name = std::to_string(std::stoi(args[1]) + 1);
    std::map<std::string, std::string>
        scale_conf{{"type", "file"}, {"next_partition_name", dst_partition_name}, {"partition_num", args[2]}};
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

void file_partition::get_storage_capacity(response &_return, const arg_list &args) {
  if (args.size() != 1) {
    RETURN_ERR("!args_error");
  }
  RETURN_OK(std::to_string(manager_->mb_capacity()));
}

void file_partition::run_command(response &_return, const arg_list &args) {
  auto cmd_name = args[0];
  switch (command_id(cmd_name)) {
    case file_cmd_id::file_write:write(_return, args);
      break;
    case file_cmd_id::file_read:read(_return, args);
      break;
    case file_cmd_id::file_write_ls:write_ls(_return, args);
      break;
    case file_cmd_id::file_read_ls:read_ls(_return, args);
      break;
    case file_cmd_id::file_clear:clear(_return, args);
      break;
    case file_cmd_id::file_update_partition:update_partition(_return, args);
      break;
    case file_cmd_id::file_add_blocks:add_blocks(_return, args);
      break;
    case file_cmd_id::file_get_storage_capacity:get_storage_capacity(_return, args);
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

std::size_t file_partition::size() const {
  return partition_.size();
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
    std::cout<<"previous:"<<partition_.data()<<"\n";
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    remote->write<file_type>(partition_, decomposed.second);
    std::cout<<"after:"<<partition_.data()<<"\n";
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

REGISTER_IMPLEMENTATION("file", file_partition);

}
}
