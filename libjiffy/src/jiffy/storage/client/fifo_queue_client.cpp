#include "fifo_queue_client.h"
#include "jiffy/utils/string_utils.h"
#include <algorithm>
#include <thread>

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

fifo_queue_client::fifo_queue_client(std::shared_ptr<directory::directory_interface> fs,
                                     const std::string &path,
                                     const directory::data_status &status,
                                     int timeout_ms)
    : data_structure_client(fs, path, status, timeout_ms) {
  dequeue_partition_ = 0;
  enqueue_partition_ = 0;
  read_partition_ = 0;
  read_offset_ = 0;
  for (const auto &block: status.data_blocks()) {
    blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, FQ_CMDS, timeout_ms_));
  }
}

void fifo_queue_client::refresh() {
  status_ = fs_->dstatus(path_);
  blocks_.clear();
  for (const auto &block: status_.data_blocks()) {
    blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, FQ_CMDS, timeout_ms_));
  }
}

std::string fifo_queue_client::enqueue(const std::string &msg) {
  std::vector<std::string> _return;
  std::vector<std::string> args{"enqueue", msg};
  bool redo;
  do {
    try {
      _return = blocks_[enqueue_partition_]->run_command(args);
      handle_redirect(args, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return[0];
}

std::string fifo_queue_client::dequeue() {
  std::vector<std::string> _return;
  std::vector<std::string> args{"dequeue"};
  bool redo;
  do {
    try {
      _return = blocks_[dequeue_partition_]->run_command(args);
      handle_redirect(args, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return[0];
}

std::string fifo_queue_client::read_next() {
  std::vector<std::string> _return;
  std::vector<std::string> args{"read_next", std::to_string(read_offset_)};
  bool redo;
  do {
    try {
      _return = blocks_[read_partition_]->run_command(args);
      handle_redirect(args, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return[0];
}

std::size_t fifo_queue_client::block_id(const fifo_queue_cmd_id &op) const {
  switch (op) {
    case fifo_queue_cmd_id::fq_enqueue:
      if (!is_valid(enqueue_partition_)) {
        throw std::logic_error("Blocks are insufficient, need to add more");
      }
      return enqueue_partition_;
    case fifo_queue_cmd_id::fq_dequeue:
      if (!is_valid(dequeue_partition_)) {
        throw std::logic_error("Blocks are insufficient, need to add more");
      }
      return dequeue_partition_;
    case fifo_queue_cmd_id::fq_readnext:
      if (!is_valid(read_partition_)) {
        throw std::logic_error("Blocks are insufficient, need to add more");
      }
      return read_partition_;
    default:throw std::invalid_argument("Incorrect operation of message queue");
  }
}

void fifo_queue_client::handle_redirect(const std::vector<std::string> &args, std::vector<std::string> &response) {
  auto cmd_name = args.front();
  bool read_flag = true;

  if (response[0] == "!redo") throw redo_error();

  if (response[0] == "!split_enqueue") {
    do {
      auto remaining_data_len = std::stoi(response[1]);
      auto data = args[1];
      auto remaining_data = data.substr(data.size() - remaining_data_len, remaining_data_len);

      if (enqueue_partition_ >= blocks_.size() - 1) {
        auto chain = string_utils::split(response[2], '!');
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FQ_CMDS));
      }
      enqueue_partition_++;
      do {
        response = blocks_[enqueue_partition_]->run_command({"enqueue", remaining_data});
      } while (response[0] == "!redo");
    } while (response[0] == "!split_enqueue");
  }
  if (response[0] == "!split_dequeue") {
    std::string result;
    do {
      auto data_part = response[1];
      result += data_part;
      if (dequeue_partition_ >= blocks_.size() - 1) {
        auto chain = string_utils::split(response[2], '!');
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FQ_CMDS));
      }
      dequeue_partition_++;
      response = blocks_[dequeue_partition_]->run_command({"dequeue"});
      if (response[0] != "!msg_not_found") {
        if (response[0] == "!split_dequeue") continue;
        result += response[0];
      }
    } while (response[0] == "!split_dequeue");
    response[0] = result;
  }
  if (response[0] == "!split_readnext") {
    std::string result;
    do {
      auto data_part = response[1];
      result += data_part;
      if (read_partition_ >= blocks_.size() - 1) {
        auto chain = string_utils::split(response[2], '!');
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FQ_CMDS));
      }
      read_partition_++;
      read_offset_ = 0;
      response = blocks_[read_partition_]->run_command({"read_next", std::to_string(0)});
      if (response[0] != "!msg_not_found") {
        if (response[0] == "!split_readnext")
          continue;
        read_offset_ += (string_array::METADATA_LEN + response[0].size());
        result += response[0];
      }
    } while (response[0] == "!split_readnext");
    response[0] = result;
    read_flag = false;
  }
  if (read_flag && cmd_name == "read_next" && response[0] != "!msg_not_found") {
    read_offset_ += (string_array::METADATA_LEN + response[0].size());
  }
}

}
}
