#include "fifo_queue_client.h"
#include "jiffy/utils/string_utils.h"
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

void fifo_queue_client::enqueue(const std::string &item) {
  std::vector<std::string> _return;
  std::vector<std::string> args{"enqueue", item};
  bool redo;
  do {
    try {
      _return = blocks_[enqueue_partition_]->run_command(args);
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
      _return = blocks_[dequeue_partition_]->run_command(args);
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
  std::vector<std::string> args{"read_next", std::to_string(read_offset_)};
  bool redo;
  do {
    try {
      _return = blocks_[read_partition_]->run_command(args);
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

  if (_return[0] == "!ok") {
    if (cmd_name == "read_next") read_offset_ += (string_array::METADATA_LEN + _return[1].size());
    return;
  } else if (_return[0] == "!redo") {
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
      read_offset_ = 0;
      _return = blocks_[read_partition_]->run_command({"read_next", std::to_string(0)});
      if (_return[0] == "!ok") {
        read_offset_ += (string_array::METADATA_LEN + _return[1].size());
        result += _return[1];
      }
    } while (_return[0] == "!split_readnext");
    _return[1] = result;
  }
}

}
}
