#include "fifo_queue_client.h"
#include "jiffy/utils/string_utils.h"
#include "jiffy/utils/logger.h"
#include <algorithm>
#include <thread>
#include <utility>
#include <jiffy/storage/fifoqueue/fifo_queue_partition.h>

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

fifo_queue_client::fifo_queue_client(std::shared_ptr<directory::directory_interface> fs,
                                     const std::string &path,
                                     const directory::data_status &status,
                                     int timeout_ms)
    : data_structure_client(std::move(fs), path, status, timeout_ms) {
  dequeue_partition_ = 0;
  enqueue_partition_ = 0;
  read_partition_ = 0;
  start_ = 0;
  for (const auto &block: status.data_blocks()) {
    blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, FQ_CMDS, timeout_ms_));
  }
}

void fifo_queue_client::refresh() {
  status_ = fs_->dstatus(path_);
  blocks_.clear();
  bool flag = true;
  for (const auto &block: status_.data_blocks()) {
    try {
      blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, FQ_CMDS, timeout_ms_));
      if (flag) {
        flag = false;
        start_ = std::stoul(block.name);
      }
    } catch (std::exception &e) {}
  }
  // Restore pointers after refreshing
  enqueue_partition_ = blocks_.size() - 1;
  dequeue_partition_ = 0;
  if (read_partition_ < start_) {
    read_partition_ = start_;
  }
}

void fifo_queue_client::enqueue(const std::string &item) {
  std::vector<std::string> _return;
  std::vector<std::string> args{"enqueue", item};
  bool redo;
  do {
    try {
      _return = blocks_[block_id(fifo_queue_cmd_id::fq_enqueue)]->run_command(args);
      handle_redirect(_return, args);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  THROW_IF_NOT_OK(_return);
}

std::string fifo_queue_client::dequeue() {
  std::vector<std::string> _return;
  std::vector<std::string> args{"dequeue"};
  bool redo;
  do {
    try {
      _return = blocks_[block_id(fifo_queue_cmd_id::fq_dequeue)]->run_command(args);
      handle_redirect(_return, args);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  THROW_IF_NOT_OK(_return);
  return _return[1];
}

std::string fifo_queue_client::read_next() {
  std::vector<std::string> _return;
  std::vector<std::string> args{"read_next"};
  bool redo;
  do {
    try {
      // Use partition name instead of offset for read_next to avoid refreshing
      _return = blocks_[block_id(fifo_queue_cmd_id::fq_readnext)]->run_command(args);
      handle_redirect(_return, args);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  THROW_IF_NOT_OK(_return);
  return _return[1];
}

std::size_t fifo_queue_client::qsize() {
  std::vector<std::string> _head, _tail;
  std::vector<std::string> head_args{"qsize", std::to_string(fifo_queue_size_type::head_size)};
  std::vector<std::string> tail_args{"qsize", std::to_string(fifo_queue_size_type::tail_size)};
  bool redo;
  do {
    try {
      _tail = blocks_[block_id(fifo_queue_cmd_id::fq_dequeue)]->run_command(tail_args);
      handle_redirect(_tail, tail_args);
      _head = blocks_[block_id(fifo_queue_cmd_id::fq_enqueue)]->run_command(head_args);
      handle_redirect(_head, head_args);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  THROW_IF_NOT_OK(_head);
  THROW_IF_NOT_OK(_tail);
  return std::stoul(_head[1]) - std::stoul(_tail[1]);
}

double fifo_queue_client::in_rate() {

  return 0;
}

double fifo_queue_client::out_rate() {
  return 0;
}

void fifo_queue_client::handle_redirect(std::vector<std::string> &_return, const std::vector<std::string> &args) {
  auto cmd_name = args.front();
  if (_return[0] == "!redo") {
    throw redo_error();
  } else if (_return[0] == "!split_enqueue") {
    do {
      if (enqueue_partition_ >= blocks_.size() - 1) {
        if(_return.size() == 4) {
          auto chain = string_utils::split(_return[2], '!');
          blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FQ_CMDS));
        } else {
          throw std::logic_error("Insufficient blocks");
        }
      }
      enqueue_partition_++;
      do {
        auto args_copy = args;
        args_copy.emplace_back(_return.back());
        _return = blocks_[block_id(fifo_queue_cmd_id::fq_enqueue)]->run_command_redirected(args_copy);
      } while (_return[0] == "!redo");
    } while (_return[0] == "!split_enqueue");
  } else if (_return[0] == "!split_dequeue") {
    std::string result;
    result += _return[1];
    do {
      if (dequeue_partition_ >= blocks_.size() - 1) {
        auto chain = string_utils::split(_return[2], '!');
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FQ_CMDS));
      }
      dequeue_partition_++;
      if(dequeue_partition_ > enqueue_partition_) {
        enqueue_partition_ = dequeue_partition_;
      }
      auto dequeue_partition_name = dequeue_partition_ + start_;
      if (dequeue_partition_name > read_partition_)
        read_partition_ = dequeue_partition_name;
      do {
        _return = blocks_[block_id(fifo_queue_cmd_id::fq_dequeue)]->run_command_redirected({"dequeue"});
      } while (_return[0] == "!redo");
      if (_return[0] == "!ok" || _return[0] == "!split_dequeue")
        result += _return[1];
    } while (_return[0] == "!split_dequeue");
    if (_return.size() >= 2)
      _return[1] = result;
  } else if (_return[0] == "!split_readnext") {
    std::string result;
    result += _return[1];
    do {
      if (read_partition_ - start_ >= blocks_.size() - 1) {
        auto chain = string_utils::split(_return[2], '!');
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FQ_CMDS));
      }
      read_partition_++;
      if (read_partition_ - start_ > enqueue_partition_) {
        enqueue_partition_ = read_partition_ - start_;
      }
      do {
        _return = blocks_[block_id(fifo_queue_cmd_id::fq_readnext)]->run_command({"read_next"});
      } while (_return[0] == "!redo");
      if (_return[0] != "!msg_not_found")
        result += _return[1];
    } while (_return[0] == "!split_readnext");
    _return[1] = result;
  } else if (_return[0] == "!split_qsize") {
    do {
      if (std::stoi(args[1]) == fifo_queue_size_type::head_size) {
        if(enqueue_partition_ >= blocks_.size() - 1) {
          auto chain = string_utils::split(_return[1], '!');
          blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FQ_CMDS));
        }
        enqueue_partition_++;
        _return = blocks_[block_id(fifo_queue_cmd_id::fq_enqueue)]->run_command(args);
      } else {
        if(dequeue_partition_ >= blocks_.size() - 1) {
          auto chain = string_utils::split(_return[1], '!');
          blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FQ_CMDS));
        }
        dequeue_partition_++;
        _return = blocks_[block_id(fifo_queue_cmd_id::fq_dequeue)]->run_command(args);
      }
    } while (_return[0] == "!split_qsize");
  }
  if (_return[0] == "!block_moved") {
    refresh();
    throw redo_error();
  }
}

std::size_t fifo_queue_client::block_id(fifo_queue_cmd_id cmd_id) {
  switch (cmd_id) {
    case fifo_queue_cmd_id::fq_enqueue:return enqueue_partition_;
    case fifo_queue_cmd_id::fq_dequeue:return dequeue_partition_;
    case fifo_queue_cmd_id::fq_readnext:return read_partition_ - start_;
    default: {
      throw std::logic_error("Fifo queue command does not exists");
    }
  }
}

}
}
