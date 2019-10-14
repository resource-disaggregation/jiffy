#include "fifo_queue_client.h"
#include "jiffy/utils/string_utils.h"
#include "jiffy/utils/logger.h"
#include <algorithm>
#include <thread>
#include <utility>

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
  for (const auto &block: status_.data_blocks()) {
    blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, FQ_CMDS, timeout_ms_));
  }
  // Restore pointers after refreshing
  start_ = std::stoul(status_.data_blocks().front().name);
  enqueue_partition_ = blocks_.size() - 1;
  dequeue_partition_ = 0;
  if(read_partition_ < start_) {
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
      _return = blocks_[fifo_queue_cmd_id::fq_dequeue]->run_command(args);
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
      _return = blocks_[fifo_queue_cmd_id::fq_readnext]->run_command(args);
      handle_redirect(_return, args);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  THROW_IF_NOT_OK(_return);
  return _return[1];
}

void fifo_queue_client::handle_redirect(std::vector<std::string> &_return, const std::vector<std::string> &args) {
  auto cmd_name = args.front();
  if (_return[0] == "!redo") {
    throw redo_error();
  } else if (_return[0] == "!split_enqueue") {
    do {
      auto remaining_data_len = std::stoi(_return[1]);
      auto data = args[1];
      auto remaining_data = data.substr(data.size() - remaining_data_len, remaining_data_len);

      if (enqueue_partition_ >= blocks_.size() - 1) {
        auto chain = string_utils::split(_return[2], '!');
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FQ_CMDS));
      }
      enqueue_partition_++;
      do {
        _return = blocks_[enqueue_partition_]->run_command({"enqueue", remaining_data});
      } while (_return[0] == "!redo");
    } while (_return[0] == "!split_enqueue");
  } else if (_return[0] == "!split_dequeue") {
    std::string result;
    do {
      auto data_part = _return[1];
      result += data_part;
      if (dequeue_partition_ >= blocks_.size() - 1) {
        auto chain = string_utils::split(_return[2], '!');
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FQ_CMDS));
      }
      dequeue_partition_++;
      auto dequeue_partition_name = dequeue_partition_ + start_;
      if(dequeue_partition_name > read_partition_)
        read_partition_ = dequeue_partition_name;
      _return = blocks_[dequeue_partition_]->run_command({"dequeue"});
      if (_return[0] == "!ok") {
        result += _return[1];
      }
    } while (_return[0] == "!split_dequeue");
    _return[1] = result;
  } else if (_return[0] == "!split_readnext") {
    std::string result;
    do {
      auto data_part = _return[1];
      result += data_part;
      if (read_partition_ >= blocks_.size() - 1) {
        auto chain = string_utils::split(_return[2], '!');
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FQ_CMDS));
      }
      read_partition_++;
      _return = blocks_[read_partition_ - start_]->run_command({"read_next"});
      if (_return[0] == "!ok") {
        result += _return[1];
      }
    } while (_return[0] == "!split_readnext");
    _return[1] = result;
  }
  if (_return[0] == "!block_moved") {
    refresh();
    throw redo_error();
  }
}
std::size_t fifo_queue_client::block_id(fifo_queue_cmd_id cmd) {
  switch (cmd) {
    case fifo_queue_cmd_id::fq_enqueue: return enqueue_partition_;
    case fifo_queue_cmd_id::fq_dequeue: return dequeue_partition_;
    case fifo_queue_cmd_id::fq_readnext: return read_partition_ - start_;
    default: {
      throw std::logic_error("Undefined fifo queue operation");
    }
  }
}

}
}
