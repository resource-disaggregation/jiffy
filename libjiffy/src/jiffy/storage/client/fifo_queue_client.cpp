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
  try {
    auto_scaling_ = (status.get_tag("fifoqueue.auto_scale") == "true");
  } catch (directory::directory_ops_exception &e) {
    auto_scaling_ = true;
  }
}

void fifo_queue_client::refresh() {
  status_ = fs_->dstatus(path_);
  blocks_.clear();
  bool redo;
  do {
    try {
      for (const auto &block: status_.data_blocks()) {
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, FQ_CMDS, timeout_ms_));
      }
      redo = false;
    } catch (std::exception &e) {
      redo = true;
    }
  } while (redo);
  // Restore pointers after refreshing
  start_ = std::stoul(status_.data_blocks()[0].name);
  enqueue_partition_ = blocks_.size() - 1;
  dequeue_partition_ = 0;
  if (read_partition_ < start_) {
    read_partition_ = start_;
  }
}

void fifo_queue_client::enqueue(const std::string &item) {
  std::vector<std::string> _return;
  std::vector<std::string> args{"enqueue", item};
  run_repeated(_return, args);
}

std::string fifo_queue_client::dequeue() {
  std::vector<std::string> _return;
  std::vector<std::string> args{"dequeue"};
  run_repeated(_return, args);
  return _return[1];
}

std::string fifo_queue_client::read_next() {
  std::vector<std::string> _return;
  std::vector<std::string> args{"read_next"};
  run_repeated(_return, args);
  return _return[1];
}

std::size_t fifo_queue_client::length() {
  std::vector<std::string> _head, _tail;
  std::vector<std::string> tail_args{"length", std::to_string(fifo_queue_size_type::tail_size)};
  run_repeated(_tail, tail_args);
  std::vector<std::string> head_args{"length", std::to_string(fifo_queue_size_type::head_size)};
  run_repeated(_head, head_args);
  return std::stoul(_head[1]) - std::stoul(_tail[1]);
}

double fifo_queue_client::in_rate() {
  std::vector<std::string> _return;
  std::vector<std::string> args{"in_rate"};
  run_repeated(_return, args);
  return std::stod(_return[1]);
}

double fifo_queue_client::out_rate() {
  std::vector<std::string> _return;
  std::vector<std::string> args{"out_rate"};
  run_repeated(_return, args);
  return std::stod(_return[1]);
}

void fifo_queue_client::handle_redirect(std::vector<std::string> &_return, const std::vector<std::string> &args) {
  auto cmd_name = args.front();
  if (_return[0] == "!redo") {
    throw redo_error();
  }
  FIFO_QUEUE_HANDLE_REDIRECT(enqueue);
  FIFO_QUEUE_HANDLE_REDIRECT(dequeue);
  FIFO_QUEUE_HANDLE_REDIRECT(readnext);
  FIFO_QUEUE_HANDLE_REDIRECT(length);
  FIFO_QUEUE_HANDLE_REDIRECT(rate);
  if (_return[0] == "!block_moved") {
    refresh();
    throw redo_error();
  }
}

std::size_t fifo_queue_client::block_id(const std::vector<std::string> &args) {
  switch (FQ_CMDS[args[0]].id) {
    case fifo_queue_cmd_id::fq_enqueue:return enqueue_partition_;
    case fifo_queue_cmd_id::fq_dequeue:return dequeue_partition_;
    case fifo_queue_cmd_id::fq_readnext:return read_partition_ - start_;
    case fifo_queue_cmd_id::fq_length:
      if (std::stoi(args[1]) == fifo_queue_size_type::head_size)
        return enqueue_partition_;
      else
        return dequeue_partition_;
    case fifo_queue_cmd_id::fq_in_rate:return enqueue_partition_;
    case fifo_queue_cmd_id::fq_out_rate:return dequeue_partition_;
    default: {
      throw std::logic_error("Fifo queue command does not exists");
    }
  }
}

void fifo_queue_client::handle_partition_id(const std::vector<std::string> &args) {
  auto cmd = FQ_CMDS[args[0]].id;
  if (cmd == fifo_queue_cmd_id::fq_enqueue
      || (cmd == fifo_queue_cmd_id::fq_length && std::stoi(args[1]) == fifo_queue_size_type::head_size)
      || (cmd == fifo_queue_cmd_id::fq_in_rate)) {
    enqueue_partition_++;
  } else if (cmd == fifo_queue_cmd_id::fq_dequeue
      || (cmd == fifo_queue_cmd_id::fq_length && std::stoi(args[1]) == fifo_queue_size_type::tail_size)
      || (cmd == fifo_queue_cmd_id::fq_out_rate)) {
    dequeue_partition_++;
    if (dequeue_partition_ > enqueue_partition_) {
      enqueue_partition_ = dequeue_partition_;
    }
    auto dequeue_partition_name = dequeue_partition_ + start_;
    if (dequeue_partition_name > read_partition_)
      read_partition_ = dequeue_partition_name;
  } else if (cmd == fifo_queue_cmd_id::fq_readnext) {
    read_partition_++;
    if (read_partition_ - start_ > enqueue_partition_) {
      enqueue_partition_ = read_partition_ - start_;
    }
  } else {
    throw std::logic_error("Wrong command or argument");
  }
}

void fifo_queue_client::run_repeated(std::vector<std::string> &_return, const std::vector<std::string> &args) {
  bool redo;
  do {
    try {
      _return = blocks_[block_id(args)]->run_command(args);
      handle_redirect(_return, args);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  THROW_IF_NOT_OK(_return);
}

void fifo_queue_client::add_blocks(const std::vector<std::string> &_return, const std::vector<std::string> &args) {
  if (block_id(args) >= blocks_.size() - 1) {
    if (auto_scaling_) {
      auto chain = string_utils::split(_return[1], '!');
      blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FQ_CMDS));
    } else {
      throw std::logic_error("Insufficient blocks");
    }
  }
}

redirect_operations fifo_queue_client::redirect_type(std::string &type) {
  if (type == "!redirected_enqueue")
    return redirect_operations::redirected_enqueue;
  if (type == "!redirected_dequeue")
    return redirect_operations::redirected_dequeue;
  if (type == "!redirected_readnext")
    return redirect_operations::redirected_readnext;
  if (type == "!redirected_length")
    return redirect_operations::redirected_length;
  if (type == "!redirected_rate")
    return redirect_operations::redirected_rate;
  return redirect_operations::non_type;
}

}
}
