#include "file_writer.h"
#include "jiffy/utils/string_utils.h"

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

file_writer::file_writer(std::shared_ptr<jiffy::directory::directory_interface> fs,
                         const std::string &path,
                         const jiffy::directory::data_status &status,
                         int timeout_ms) : file_client(fs, path, status, timeout_ms) {}

void file_writer::write(const std::string &data) {
  std::vector<std::string> _return;
  std::size_t file_size = last_partition() * block_size_;
  int remain_size = file_size - cur_partition_ * block_size_ - cur_offset_;
  int num_chain_needed = 0;
  if(remain_size < data.size()) {
    num_chain_needed = (remain_size - data.size()) / block_size_ + 1;
  }
// First write should be able to allocate new blocks
  bool redo;
  int init_size;
  if(block_size_ - cur_offset == 0) {
    cur_partition_++;
    cur_offset_ = 0;
    init_size = MIN(data_to_write, block_size_);
  } else {
    init_size = block_size - cur_offset_;
  }
  std::vector<std::string> args{"write", data.substr(0, init_size), std::to_string(cur_offset_)};
  do {
    try {
      _return = blocks_[block_id()]->run_command(args);
      handle_redirect(_return, args);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  THROW_IF_NOT_OK(_return);
  int has_more = data.size() - init_size;
  while(has_more > 0) {
    bool redo;
    std::string data_to_write = data.substr(data.size() - has_more, MIN(has_more, block_size_ - cur_offset_));
    std::vector<std::string> args{"write", data_to_write, std::to_string(cur_offset_)};
    do {
      try {
        _return = blocks_[block_id()]->run_command(args);
        handle_redirect(_return, args);
        if(_return[0] == "!ok") {
          has_more -= data_to_write.size();
        }
        redo = false;
      } catch (redo_error &e) {
        redo = true;
      }
    }while (redo);
    THROW_IF_NOT_OK(_return);
  }
}

void file_writer::handle_redirect(std::vector<std::string> &_return, const std::vector<std::string> &args) {
  auto data = args[1];
  if (_return[0] == "!ok") {
    cur_offset_ += data.size();
    return;
  } else if (_return[0] == "!redo") {
    throw redo_error();
  } else if (_return[0] == "!split_write") {
    do {
      auto remaining_data_len = std::stoi(_return[1]);
      auto remaining_data = data.substr(data.size() - remaining_data_len, remaining_data_len);

      if (need_chain()) {
        auto chain = string_utils::split(_return[2], '!');
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FILE_OPS));
      }
      cur_partition_++;
      update_last_partition(cur_partition_, current_offset_);
      cur_offset_ = 0;
      std::vector<std::string> new_args{"write", remaining_data, std::to_string(cur_offset_)};
      do {
        _return = blocks_[block_id()]->run_command(new_args);
      } while (_return[0] == "!redo");
      cur_offset_ += remaining_data.size();
    } while (_return[0] == "!split_write");
  }
}

}
}
