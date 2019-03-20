#include "msg_queue_client.h"
#include "jiffy/utils/logger.h"
#include "jiffy/utils/string_utils.h"
#include <algorithm>

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

msg_queue_client::msg_queue_client(std::shared_ptr<directory::directory_interface> fs,
                                   const std::string &path,
                                   const directory::data_status &status,
                                   int timeout_ms)
    : data_structure_client(fs, path, status, timeout_ms) {
  read_offset_ = 0;
  read_partition_ = 0;
  send_partition_ = 0;
}

void msg_queue_client::refresh() {
  status_ = fs_->dstatus(path_);
  LOG(log_level::info) << "Refreshing partition mappings to " << status_.to_string();
  blocks_.clear();
  for (const auto &block: status_.data_blocks()) {
    blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, timeout_ms_));
  }
}

std::string msg_queue_client::send(const std::string &msg) {
  std::string _return;
  std::vector<std::string> args{msg};
  bool redo;
  do {
    try {
      _return = blocks_[block_id(msg_queue_cmd_id::mq_send)]->run_command(msg_queue_cmd_id::mq_send, args).front();
      handle_redirect(msg_queue_cmd_id::mq_send, args, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;

}

std::string msg_queue_client::read() {
  std::string _return;
  std::vector<std::string> args;
  args.push_back(get_inc_read_pos());
  bool redo;
  do {
    try {
      _return = blocks_[block_id(msg_queue_cmd_id::mq_read)]->run_command(msg_queue_cmd_id::mq_read, args).front();
      handle_redirect(msg_queue_cmd_id::mq_read, args, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::vector<std::string> msg_queue_client::send(const std::vector<std::string> &msgs) {
  if (msgs.size() == 0) {
    throw std::invalid_argument("Incorrect number of arguments");
  }
  std::vector<std::string> _return;
  bool redo;
  do {
    try {
      _return = blocks_[block_id(msg_queue_cmd_id::mq_send)]->run_command(msg_queue_cmd_id::mq_send, msgs);
      handle_redirects(msg_queue_cmd_id::mq_send, msgs, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::vector<std::string> msg_queue_client::read(std::size_t num_msg) {
  std::vector<std::string> args;
  std::vector<std::string> _return;
  for (std::size_t i = 0; i < num_msg; i++) {
    args.push_back(get_inc_read_pos());
  }
  bool redo;
  do {
    try {
      _return = blocks_[block_id(msg_queue_cmd_id::mq_read)]->run_command(msg_queue_cmd_id::mq_read, args);
      handle_redirects(msg_queue_cmd_id::mq_read, args, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::size_t msg_queue_client::block_id(const msg_queue_cmd_id &op) {
  if (op == msg_queue_cmd_id::mq_send) {
    return send_partition_;
  } else if (op == msg_queue_cmd_id::mq_read) {
    return read_partition_;
  } else {
    throw std::invalid_argument("Incorrect operation of message queue");
  }
}

void msg_queue_client::handle_redirect(int32_t cmd_id, const std::vector<std::string> &args, std::string &response) {
  if (response.substr(0, 5) == "!full") {
    typedef std::vector<std::string> list_t;
    do {
      auto parts = string_utils::split(response, '!');
      auto chain = list_t(parts.begin() + 2, parts.end());
      blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, directory::replica_chain(chain), 0));
      send_partition_++;
      response = blocks_[block_id(static_cast<msg_queue_cmd_id >(cmd_id))]->run_command(cmd_id, args).front();
    } while (response.substr(0, 5) == "!full");
  }
  if (response.substr(0, 21) == "!msg_not_in_partition") {
    typedef std::vector<std::string> list_t;
    do {
      auto parts = string_utils::split(response, '!');
      auto chain = list_t(parts.begin() + 2, parts.end());
      blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, directory::replica_chain(chain)));
      read_partition_++;
      read_offset_ = 0;
      std::vector<std::string> modified_args;
      modified_args.push_back(get_inc_read_pos());
      response = blocks_[block_id(static_cast<msg_queue_cmd_id >(cmd_id))]->run_command(cmd_id, modified_args).front();
    } while (response.substr(0, 21) == "!msg_not_in_partition");
  }
  if (response == "!msg_not_found") {
    read_offset_--;
  }
}

void msg_queue_client::handle_redirects(int32_t cmd_id,
                                        const std::vector<std::string> &args,
                                        std::vector<std::string> &responses) {
  std::vector<std::string> modified_args = args;
  typedef std::vector<std::string> list_t;
  size_t n_ops = responses.size();
  size_t n_op_args = args.size() / n_ops;
  bool send_flag_all = false;
  bool read_flag_all = false;
  for (size_t i = 0; i < responses.size(); i++) {
    auto &response = responses[i];
    if (response.substr(0, 5) == "!full") {
      list_t op_args(modified_args.begin() + i * n_op_args, modified_args.begin() + (i + 1) * n_op_args);
      bool send_flag = true;
      do {
        auto parts = string_utils::split(response, '!');
        auto chain = list_t(parts.begin() + 2, parts.end());
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, directory::replica_chain(chain), 0));
        if (!send_flag_all || !send_flag) {
          send_partition_++;
        }
        send_flag = false;
        response = blocks_[block_id(static_cast<msg_queue_cmd_id >(cmd_id))]->run_command(cmd_id, op_args).front();
      } while (response.substr(0, 5) == "!full");
      send_flag_all = true;
    }
    if (response.substr(0, 5) == "!msg_not_in_partition") {
      list_t op_args(modified_args.begin() + i * n_op_args, modified_args.begin() + (i + 1) * n_op_args);
      bool read_flag = true;
      do {
        auto parts = string_utils::split(response, '!');
        auto chain = list_t(parts.begin() + 2, parts.end());
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, directory::replica_chain(chain), 0));
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
        response = blocks_[block_id(static_cast<msg_queue_cmd_id >(cmd_id))]->run_command(cmd_id, op_args).front();
      } while (response.substr(0, 5) == "!msg_not_in_partition");
      read_flag_all = true;
    }
    if (response == "!msg_not_found") {
      list_t op_args(modified_args.begin() + i * n_op_args, modified_args.begin() + (i + 1) * n_op_args);
      read_offset_ = static_cast<std::size_t>(std::stoi(op_args.front()));
      break;
    }
  }
}

}
}
