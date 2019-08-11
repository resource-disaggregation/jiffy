#include "file_reader.h"

#include <utility>
#include "jiffy/utils/string_utils.h"

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

file_reader::file_reader(std::shared_ptr<jiffy::directory::directory_interface> fs,
                         const std::string &path,
                         const jiffy::directory::data_status &status,
                         int timeout_ms) : file_client(std::move(fs), path, status, timeout_ms) {
}

std::string file_reader::read(const std::size_t size) {
  std::vector<std::string> _return;
  std::vector<std::string> args = {"read", std::to_string(cur_offset_), std::to_string(size)};
  bool redo;
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
  return _return[1];
}

void file_reader::handle_redirect(std::vector<std::string> &_return, const std::vector<std::string> &args) {
  if (_return[0] == "!ok") {
    cur_offset_ += _return[1].size();
    return;
  } else if (_return[0] == "!redo") {
    throw redo_error();
  } else if (_return[0] == "!split_read") {
    std::string result;
    do {
      auto data_part = _return[1];
      auto chain = string_utils::split(_return[2], '!');
      result += data_part;
      if (need_chain()) {
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FILE_OPS));
      }
      cur_partition_++;
      update_last_partition(cur_partition_);
      cur_offset_ = 0;
      std::vector<std::string> new_args{"read", std::to_string(cur_offset_),
                                        std::to_string(std::stoi(args[2]) - result.size())};
      _return = blocks_[block_id()]->run_command(new_args);
      if (_return[0] == "!ok") {
        cur_offset_ += _return[1].size();
        result += _return[1];
      }
    } while (_return[0] == "!split_read");
    _return[1] = result;
  }
}

}
}