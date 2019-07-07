#include "file_client.h"
#include "jiffy/utils/logger.h"
#include "jiffy/utils/string_utils.h"
#include <algorithm>
#include <thread>

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

file_client::file_client(std::shared_ptr<directory::directory_interface> fs,
                         const std::string &path,
                         const directory::data_status &status,
                         int timeout_ms)
    : data_structure_client(fs, path, status, FILE_OPS, timeout_ms) {
  read_offset_ = 0;
  read_partition_ = 0;
  write_partition_ = 0;
  write_offset_ = 0;
  for (const auto &block: status.data_blocks()) {
    blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, FILE_OPS, timeout_ms_));
  }
}

void file_client::refresh() {
  status_ = fs_->dstatus(path_);
  blocks_.clear();
  for (const auto &block: status_.data_blocks()) {
    blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, FILE_OPS, timeout_ms_));
  }
}

std::string file_client::write(const std::string &msg) {
  std::string _return;
  std::vector<std::string> args{msg};
  args.push_back(std::to_string(write_offset_));
  bool redo;
  do {
    try {
      _return = blocks_[block_id(file_cmd_id::file_write)]->run_command(file_cmd_id::file_write, args).front();
      handle_redirect(file_cmd_id::file_write, args, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;

}

std::string file_client::read(const std::size_t size) {
  std::string _return;
  std::vector<std::string> args;
  args.push_back(std::to_string(read_offset_));
  args.push_back(std::to_string(size));
  bool redo;
  do {
    try {
      _return = blocks_[block_id(file_cmd_id::file_read)]->run_command(file_cmd_id::file_read, args).front();
      handle_redirect(file_cmd_id::file_read, args, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

bool file_client::seek(const std::size_t offset) {
  std::vector<std::string> ret;
  auto seek_partition = block_id(file_cmd_id::file_seek);
  ret = blocks_[seek_partition]->run_command(file_cmd_id::file_seek, {});
  auto size = static_cast<std::size_t>(std::stoi(ret[0]));
  auto cap = static_cast<std::size_t>(std::stoi(ret[1]));
  if (offset >= seek_partition * cap + size) {
    return false;
  } else {
    read_partition_ = offset / cap;
    read_offset_ = offset % cap;
    write_partition_ = offset / cap;
    write_offset_ = offset % cap;
    return true;
  }
}

bool file_client::need_chain(const file_cmd_id &op) const {
  if(op == file_cmd_id::file_write) {
    return write_partition_ >= blocks_.size() - 1; 
  } else if (op == file_cmd_id::file_read){
    return read_partition_ >= blocks_.size() - 1;
  } else {
    throw std::logic_error("Adding chain should only happen in write and read");
  }
}

std::size_t file_client::block_id(const file_cmd_id &op) const {
  switch (op) {
    case file_cmd_id::file_write:
      if (!is_valid(write_partition_)) {
        throw std::logic_error("Blocks are insufficient, need to add more");
      }
      return write_partition_;
    case file_cmd_id::file_read:
      if (!is_valid(read_partition_)) {
        throw std::logic_error("Blocks are insufficient, need to add more");
      }
      return read_partition_;
    case file_cmd_id::file_seek:
      if (read_partition_ > write_partition_) {
        return read_partition_;
      } else {
        return write_partition_;
      }
    default:throw std::invalid_argument("Incorrect operation of message queue");
  }
}

void file_client::handle_redirect(int32_t cmd_id, const std::vector<std::string> &args, std::string &response) {
  bool read_flag = true;
  bool write_flag = true;
  typedef std::vector<std::string> list_t;
  if (response == "!redo") {
    throw redo_error();
  }
  if (response.substr(0, 12) == "!split_write") {
    do {
      auto parts = string_utils::split(response, '!');
      auto remain_string_length = std::stoi(*(parts.end() - 1));
      auto msg = args.front();
      auto
          remain_string = std::vector<std::string>{msg.substr(msg.size() - remain_string_length, remain_string_length)};
      if(need_chain(static_cast<file_cmd_id>(cmd_id))) {
        auto chain = list_t(parts.begin() + 2, parts.end() - 1);
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_,
                                                                 path_,
                                                                 directory::replica_chain(chain),
                                                                 FILE_OPS));
      }
      write_partition_++;
      write_offset_ = 0;
      response = blocks_[block_id(static_cast<file_cmd_id >(cmd_id))]->run_command(cmd_id, remain_string).front();
    } while (response.substr(0, 12) == "!split_write");
  }
  if (response.substr(0, 11) == "!split_read") {
    do {
      auto parts = string_utils::split(response, '!');
      auto first_part_string = *(parts.end() - 1);
      if(need_chain(static_cast<file_cmd_id>(cmd_id))) {
          auto chain = list_t(parts.begin() + 2, parts.end() - 1);
          blocks_.push_back(std::make_shared<replica_chain_client>(fs_,
                                                                   path_,
                                                                   directory::replica_chain(chain),
                                                                   FILE_OPS));
      }
      read_partition_++;
      read_offset_ = 0;
      std::vector<std::string> modified_args;
      modified_args.push_back(std::to_string(read_offset_));
      modified_args.push_back(std::to_string(std::stoi(args[1]) - first_part_string.size()));
      auto second_part_string =
          blocks_[block_id(static_cast<file_cmd_id >(cmd_id))]->run_command(cmd_id, modified_args).front();
      if (second_part_string != "!msg_not_found") {
        read_offset_ += second_part_string.size();
        read_flag = false;
        response = first_part_string + second_part_string;
      } else {
        response = first_part_string;
        read_flag = false;
      }
    } while (response.substr(0, 11) == "!split_read");
  }
  if (response != "!msg_not_found" && cmd_id == static_cast<int32_t>(file_cmd_id::file_read) && read_flag) {
    read_offset_ += response.size();
  }
  if (cmd_id == static_cast<int32_t>(file_cmd_id::file_write) && write_flag) {
    write_offset_ += args[0].size();
  }
}

void file_client::handle_redirects(int32_t,
                                   const std::vector<std::string> &,
                                   std::vector<std::string> &) {
}

}
}
