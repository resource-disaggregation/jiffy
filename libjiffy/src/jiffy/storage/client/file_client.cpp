#include "file_client.h"
#include "jiffy/utils/logger.h"
#include "jiffy/utils/string_utils.h"
#include <algorithm>
#include <thread>
#include <jiffy/storage/string_array.h>

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

std::string file_client::read() {
  std::string _return;
  std::vector<std::string> args;
  args.push_back(std::to_string(read_offset_));
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
/*
std::vector<std::string> file_client::write(const std::vector<std::string> &msgs) {
  if (msgs.size() == 0) {
    throw std::invalid_argument("Incorrect number of arguments");
  }
  std::vector<std::string> _return;
  bool redo;
  do {
    try {
      _return = blocks_[block_id(file_cmd_id::file_write)]->run_command(file_cmd_id::file_write, msgs);
      handle_redirects(file_cmd_id::file_write, msgs, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::vector<std::string> file_client::read(std::size_t num_msg) {
  std::vector<std::string> args;
  std::vector<std::string> _return;
  for (std::size_t i = 0; i < num_msg; i++) {
    args.push_back(get_inc_read_pos());
  }
  bool redo;
  do {
    try {
      _return = blocks_[block_id(file_cmd_id::file_read)]->run_command(file_cmd_id::file_read, args);
      handle_redirects(file_cmd_id::file_read, args, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}
*/
std::size_t file_client::block_id(const file_cmd_id &op) {
  if (op == file_cmd_id::file_write) {
    return write_partition_;
  } else if (op == file_cmd_id::file_read) {
    return read_partition_;
  } else {
    throw std::invalid_argument("Incorrect operation of message queue");
  }
}

void file_client::handle_redirect(int32_t cmd_id, const std::vector<std::string> &args, std::string &response) {
  bool read_flag = true;
  typedef std::vector<std::string> list_t;
  if (response == "!redo") {
    throw redo_error();
  }
  // TODO Maybe this code could be simplified
  if (response.substr(0, 5) == "!full") {
    do {
      auto parts = string_utils::split(response, '!');
      auto chain = list_t(parts.begin() + 2, parts.end());
      blocks_.push_back(std::make_shared<replica_chain_client>(fs_,
                                                               path_,
                                                               directory::replica_chain(chain),
                                                               FILE_OPS));
      write_partition_++;
      response = blocks_[block_id(static_cast<file_cmd_id >(cmd_id))]->run_command(cmd_id, args).front();
    } while (response.substr(0, 5) == "!full");
  }
  if (response.substr(0, 21) == "!msg_not_in_partition") {
    do {
      read_partition_++;
      read_offset_ = 0;
      std::vector<std::string> modified_args;
      modified_args.push_back(std::to_string(read_offset_));
      response = blocks_[block_id(static_cast<file_cmd_id >(cmd_id))]->run_command(cmd_id, modified_args).front();
      if(response != "!msg_not_found") {
        read_offset_ += (response.size() + metadata_length);
        read_flag = false;
      }
    } while (response.substr(0, 21) == "!msg_not_in_partition");
  }
  if (response.substr(0, 12) == "!split_write") {
    do {
      auto parts = string_utils::split(response, '!');
      auto chain = list_t(parts.begin() + 2, parts.end() - 1);
      auto remain_string_length = std::stoi(list_t(parts.end() - 1, parts.end()).front());
      auto msg = args.front();
      auto
          remain_string = std::vector<std::string>{msg.substr(msg.size() - remain_string_length, remain_string_length)};
      blocks_.push_back(std::make_shared<replica_chain_client>(fs_,
                                                               path_,
                                                               directory::replica_chain(chain),
                                                               FILE_OPS));
      write_partition_++;
      response = blocks_[block_id(static_cast<file_cmd_id >(cmd_id))]->run_command(cmd_id, remain_string).front();
    } while (response.substr(0, 12) == "!split_write");
  }
  if (response.substr(0, 11) == "!split_read") {
    do {
      auto parts = string_utils::split(response, '!', 3);
      auto first_part_string = parts[2];
      read_partition_++;
      read_offset_ = 0;
      std::vector<std::string> modified_args;
      modified_args.push_back(std::to_string(read_offset_));
      auto second_part_string =
          blocks_[block_id(static_cast<file_cmd_id >(cmd_id))]->run_command(cmd_id, modified_args).front();
      response = first_part_string + second_part_string;
      if(response != "!msg_not_found") {
        read_offset_ += (second_part_string.size() + metadata_length);
        read_flag = false;
      }
    } while (response.substr(0, 11) == "!split_write");
  }
  if (response != "!msg_not_found" && cmd_id == static_cast<int32_t>(file_cmd_id::file_read) && read_flag) {
    read_offset_ += (response.size() + metadata_length);
  }
}

void file_client::handle_redirects(int32_t cmd_id,
                                   const std::vector<std::string> &args,
                                   std::vector<std::string> &responses) {
  (void)cmd_id;
  (void)args;
  (void)responses;
  /*
  std::vector<std::string> modified_args = args;
  typedef std::vector<std::string> list_t;
  size_t n_ops = responses.size();
  size_t n_op_args = args.size() / n_ops;
  bool write_flag_all = false;
  bool read_flag_all = false;
  for (size_t i = 0; i < responses.size(); i++) {
    auto &response = responses[i];
    if (response == "!redo")
      throw redo_error();
    if (response.substr(0, 5) == "!full") {
      list_t op_args(modified_args.begin() + i * n_op_args, modified_args.begin() + (i + 1) * n_op_args);
      bool write_flag = true;
      do {
        auto parts = string_utils::split(response, '!');
        auto chain = list_t(parts.begin() + 2, parts.end());
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_,
                                                                 path_,
                                                                 directory::replica_chain(chain),
                                                                 FILE_OPS,
                                                                 0));
        if (!write_flag_all || !write_flag) {
          write_partition_++;
        }
        write_flag = false;
        response = blocks_[block_id(static_cast<file_cmd_id >(cmd_id))]->run_command(cmd_id, op_args).front();
      } while (response.substr(0, 5) == "!full");
      write_flag_all = true;
    }
    if (response.substr(0, 5) == "!msg_not_in_partition") {
      list_t op_args(modified_args.begin() + i * n_op_args, modified_args.begin() + (i + 1) * n_op_args);
      bool read_flag = true;
      do {
        auto parts = string_utils::split(response, '!');
        auto chain = list_t(parts.begin() + 2, parts.end());
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_,
                                                                 path_,
                                                                 directory::replica_chain(chain),
                                                                 FILE_OPS,
                                                                 0));
        if (!read_flag_all || !read_flag) {
          read_partition_++;
          auto old_value = std::stoi(op_args.front());
          for (auto it = modified_args.begin() + i; it != modified_args.end(); it++) {
            *it = std::to_string(std::stoi(*it) - old_value);
          }
          op_args.clear();
          op_args.push_back("0");
        }
        read_flag = false;
        response = blocks_[block_id(static_cast<file_cmd_id >(cmd_id))]->run_command(cmd_id, op_args).front();
      } while (response.substr(0, 5) == "!msg_not_in_partition");
      read_flag_all = true;
    }
    if (response == "!msg_not_found") {
      list_t op_args(modified_args.begin() + i * n_op_args, modified_args.begin() + (i + 1) * n_op_args);
      read_offset_ = static_cast<std::size_t>(std::stoi(op_args.front()));
      break;
    }
  }
   */
}

}
}
