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
  read_partition_ = 0;
  read_offset_ = 0;
  for (const auto &block: status.data_blocks()) {
    blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, FIFO_QUEUE_OPS, timeout_ms_));
  }
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
      _return =
          blocks_[block_id(fifo_queue_cmd_id::fq_enqueue)]->run_command(fifo_queue_cmd_id::fq_enqueue, args).front();
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
      _return =
          blocks_[block_id(fifo_queue_cmd_id::fq_dequeue)]->run_command(fifo_queue_cmd_id::fq_dequeue, {}).front();
      handle_redirect(fifo_queue_cmd_id::fq_dequeue, {}, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::string fifo_queue_client::readnext() {
  std::string _return;
  std::vector<std::string> args{std::to_string(read_offset_)};
  bool redo;
  do {
    try {
      _return =
          blocks_[block_id(fifo_queue_cmd_id::fq_readnext)]->run_command(fifo_queue_cmd_id::fq_readnext, args).front();
      handle_redirect(fifo_queue_cmd_id::fq_readnext, {}, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

bool fifo_queue_client::need_chain(const fifo_queue_cmd_id &op) const {
  switch(op) {
    case fifo_queue_cmd_id::fq_enqueue:
      return enqueue_partition_ >= blocks_.size() - 1;
    case fifo_queue_cmd_id::fq_dequeue:
      return dequeue_partition_ >= blocks_.size() - 1;
    case fifo_queue_cmd_id::fq_readnext:
      return read_partition_ >= blocks_.size() - 1;
    default:
      throw std::logic_error("Adding chain should only happen in the three operations");    }
}

std::size_t fifo_queue_client::block_id(const fifo_queue_cmd_id &op) const {
  switch(op) {
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
    default:
      throw std::invalid_argument("Incorrect operation of message queue");
  }
}

void fifo_queue_client::handle_redirect(int32_t cmd_id, const std::vector<std::string> &args, std::string &response) {
  typedef std::vector<std::string> list_t;
  bool read_flag = true;
  if (response == "!redo") {
    throw redo_error();
  }
  if (response.substr(0, 14) == "!split_enqueue") {
    do {
      auto parts = string_utils::split(response, '!');
      auto remain_string_length = std::stoi(*(parts.end() - 1));
      auto msg = args.front();
      auto
          remain_string = std::vector<std::string>{msg.substr(msg.size() - remain_string_length, remain_string_length)};
      if(need_chain(static_cast<fifo_queue_cmd_id>(cmd_id))) {
        auto chain = list_t(parts.begin() + 2, parts.end() - 1);
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_,
                                                                 path_,
                                                                 directory::replica_chain(chain),
                                                                 FIFO_QUEUE_OPS));
      }
      enqueue_partition_++;
      do {
        response = blocks_[block_id(static_cast<fifo_queue_cmd_id >(cmd_id))]->run_command(cmd_id, remain_string).front();
      } while(response == "!redo");
    } while (response.substr(0, 14) == "!split_enqueue");
  }
  if (response.substr(0, 14) == "!split_dequeue") {
    std::string result;
    do {
      auto parts = string_utils::split(response, '!');
      if(response[response.size() - 1] == '!')
        parts.push_back(std::string(""));
      auto first_part_string = *(parts.end() - 1);
      result += first_part_string;
      if(need_chain(static_cast<fifo_queue_cmd_id>(cmd_id))) {
        auto chain = list_t(parts.begin() + 2, parts.end() - 1);
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_,
                                                                 path_,
                                                                 directory::replica_chain(chain),
                                                                 FIFO_QUEUE_OPS));
      }
      dequeue_partition_++;
      response =
          blocks_[block_id(static_cast<fifo_queue_cmd_id >(cmd_id))]->run_command(cmd_id, {}).front();
      if(response != "!msg_not_found") {
        if(response.substr(0, 14) == "!split_dequeue") {
          continue;
        }
        result += response;
      }
    } while (response.substr(0, 14) == "!split_dequeue");
    response = result;
  }
  if (response.substr(0, 15) == "!split_readnext") {
    std::string result;
    do {
      auto parts = string_utils::split(response, '!');
      if(response[response.size() - 1] == '!')
	      parts.push_back(std::string(""));
      auto first_part_string = *(parts.end() - 1);
      result += first_part_string;
      if(need_chain(static_cast<fifo_queue_cmd_id>(cmd_id))) {
        auto chain = list_t(parts.begin() + 2, parts.end() - 1);
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_,
                                                                 path_,
                                                                 directory::replica_chain(chain),
                                                                 FIFO_QUEUE_OPS));
      }
      read_partition_++;
      read_offset_ = 0;
      response =
          blocks_[block_id(static_cast<fifo_queue_cmd_id >(cmd_id))]->run_command(cmd_id, {std::to_string(0)}).front();
      if(response != "!msg_not_found") {
        if(response.substr(0, 15) == "!split_readnext")
          continue;
        read_offset_ += (metadata_length + response.size());
        read_flag = false;
        result += response;
      } 
    } while (response.substr(0, 15) == "!split_readnext");
    response = result;
    read_flag = false;
  }
  if(read_flag && cmd_id == fifo_queue_cmd_id::fq_readnext && response != "!msg_not_found") {
    read_offset_ += (metadata_length + response.size());
  } 
}

void fifo_queue_client::handle_redirects(int32_t,
                                         const std::vector<std::string> &,
                                         std::vector<std::string> &) {
}

}
}
