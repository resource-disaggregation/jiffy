#include "fifo_queue_client.h"
#include "jiffy/utils/logger.h"
#include "jiffy/utils/string_utils.h"
#include "../fifoqueue/fifo_queue_ops.h"
#include <algorithm>
#include <thread>

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

fifo_queue_client::fifo_queue_client(std::shared_ptr<directory::directory_interface> fs,
                                   const std::string &path,
                                   const directory::data_status &status,
                                   int timeout_ms)
    : data_structure_client(fs, path, status, FIFO_QUEUE_OPS, timeout_ms) {
  dequeue_partition_ = 0;
  enqueue_partition_ = 0;
}

void fifo_queue_client::refresh() {
  status_ = fs_->dstatus(path_);
  blocks_.clear();
  for (const auto &block: status_.data_blocks()) {
    blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, FIFO_QUEUE_OPS, timeout_ms_));
  }
}

std::string fifo_queue_client::enqueue(const std::string &msg) {
  std::string _return;
  std::vector<std::string> args{msg};
  bool redo;
  do {
    try {
      _return = blocks_[block_id(fifo_queue_cmd_id::fq_enqueue)]->run_command(fifo_queue_cmd_id::fq_enqueue, args).front();
      handle_redirect(fifo_queue_cmd_id::fq_enqueue, args, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;

}

std::string fifo_queue_client::dequeue() {
  std::string _return;
  bool redo;
  do {
    try {
      _return = blocks_[block_id(fifo_queue_cmd_id::fq_dequeue)]->run_command(fifo_queue_cmd_id::fq_dequeue, {}).front();
      handle_redirect(fifo_queue_cmd_id::fq_dequeue, {}, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}
/*
std::vector<std::string> fifo_queue_client::enqueue(const std::vector<std::string> &msgs) {
  if (msgs.size() == 0) {
    throw std::invalid_argument("Incorrect number of arguments");
  }
  std::vector<std::string> _return;
  bool redo;
  do {
    try {
      _return = blocks_[block_id(fifo_queue_cmd_id::fq_enqueue)]->run_command(fifo_queue_cmd_id::fq_enqueue, msgs);
      handle_redirects(fifo_queue_cmd_id::fq_enqueue, msgs, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}
// TODO fix this function
std::vector<std::string> fifo_queue_client::dequeue(std::size_t num_msg) {
  std::vector<std::string> args;
  std::vector<std::string> _return;
  bool redo;
  do {
    try {
      _return = blocks_[block_id(fifo_queue_cmd_id::fq_dequeue)]->run_command(fifo_queue_cmd_id::fq_dequeue, args);
      handle_redirects(fifo_queue_cmd_id::fq_dequeue, args, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}
 */

std::size_t fifo_queue_client::block_id(const fifo_queue_cmd_id &op) {
  if (op == fifo_queue_cmd_id::fq_enqueue) {
    return enqueue_partition_;
  } else if (op == fifo_queue_cmd_id::fq_dequeue) {
    return dequeue_partition_;
  } else {
    throw std::invalid_argument("Incorrect operation of message queue");
  }
}

void fifo_queue_client::handle_redirect(int32_t cmd_id, const std::vector<std::string> &args, std::string &response) {
  typedef std::vector<std::string> list_t;
  if (response == "!redo") {
    throw redo_error();
  }
  if (response.substr(0, 5) == "!full") {
    do {
      auto parts = string_utils::split(response, '!');
      auto chain = list_t(parts.begin() + 2, parts.end());
      blocks_.push_back(std::make_shared<replica_chain_client>(fs_,
                                                               path_,
                                                               directory::replica_chain(chain),
                                                               FIFO_QUEUE_OPS));
      enqueue_partition_++;
      response = blocks_[block_id(static_cast<fifo_queue_cmd_id>(cmd_id))]->run_command(cmd_id, args).front();
    } while (response.substr(0, 5) == "!full");
  }
  if (response.substr(0, 21) == "!msg_not_in_partition") {
    do {
      dequeue_partition_++;
      response = blocks_[block_id(static_cast<fifo_queue_cmd_id >(cmd_id))]->run_command(cmd_id, {}).front();
    } while (response.substr(0, 21) == "!msg_not_in_partition");
  }
  if (response.substr(0, 14) == "!split_enqueue") {
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
                                                               FIFO_QUEUE_OPS));
      enqueue_partition_++;
      response = blocks_[block_id(static_cast<fifo_queue_cmd_id >(cmd_id))]->run_command(cmd_id, remain_string).front();
    } while (response.substr(0, 12) == "!split_enqueue");
  }
  if (response.substr(0, 14) == "!split_dequeue") {
    do {
      auto parts = string_utils::split(response, '!', 3);
      auto first_part_string = parts[2];
      dequeue_partition_++;
      auto second_part_string =
          blocks_[block_id(static_cast<fifo_queue_cmd_id >(cmd_id))]->run_command(cmd_id, {}).front();
      response = first_part_string + second_part_string;
    } while (response.substr(0, 14) == "!split_dequeue");
  }
}


// Remove batch commands

void fifo_queue_client::handle_redirects(int32_t cmd_id,
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
  bool send_flag_all = false;
  bool read_flag_all = false;
  for (size_t i = 0; i < responses.size(); i++) {
    auto &response = responses[i];
    if (response == "!redo")
      throw redo_error();
    if (response.substr(0, 5) == "!full") {
      list_t op_args(modified_args.begin() + i * n_op_args, modified_args.begin() + (i + 1) * n_op_args);
      bool send_flag = true;
      do {
        auto parts = string_utils::split(response, '!');
        auto chain = list_t(parts.begin() + 2, parts.end());
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_,
                                                                 path_,
                                                                 directory::replica_chain(chain),
                                                                 FIFO_QUEUE_OPS,
                                                                 0));
        if (!send_flag_all || !send_flag) {
          send_partition_++;
        }
        send_flag = false;
        response = blocks_[block_id(static_cast<fifo_queue_cmd_id >(cmd_id))]->run_command(cmd_id, op_args).front();
      } while (response.substr(0, 5) == "!full");
      send_flag_all = true;
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
                                                                 FIFO_QUEUE_OPS,
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
        response = blocks_[block_id(static_cast<fifo_queue_cmd_id >(cmd_id))]->run_command(cmd_id, op_args).front();
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
