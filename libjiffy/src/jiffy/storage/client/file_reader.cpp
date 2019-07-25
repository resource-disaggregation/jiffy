#include "file_reader.h"
#include "jiffy/utils/string_utils.h"

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

file_reader::file_reader(std::shared_ptr<jiffy::directory::directory_interface> fs,
                         const std::string &path,
                         const jiffy::directory::data_status &status,
                         int timeout_ms) : file_client(fs, path, status, timeout_ms) {
}

std::string file_reader::read(const std::size_t size) {
  std::vector<std::string> _return;
  std::vector<std::string> args = {"read", std::to_string(cur_offset_), std::to_string(size)};
  bool redo;
  do {
    try {
      _return = blocks_[block_id()]->run_command(args);
      handle_redirect(args, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return[0];
}

void file_reader::handle_redirect(const std::vector<std::string> &args, std::vector<std::string> &response) {
  bool read_flag = true;
  if (response[0] == "!redo") throw redo_error();

  if (response[0] == "!split_read") {
    std::string result;
    do {
      auto data_part = response[1];
      auto chain = string_utils::split(response[2], '!');
      result += data_part;
      if (need_chain()) {
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FILE_OPS));
      }
      cur_partition_++;
      update_last_partition(cur_partition_);
      cur_offset_ = 0;
      std::vector<std::string> new_args{"read", std::to_string(cur_offset_),
                                        std::to_string(std::stoi(args[2]) - result.size())};
      response = blocks_[block_id()]->run_command(new_args);
      if (response[0] != "!msg_not_found") {
        if (response[0] == "!split_read")
          continue;
        cur_offset_ += response[0].size();
        result += response[0];
      }
    } while (response[0] == "!split_read");
    response[0] = result;
    read_flag = false;
  }
  if (response[0] != "!msg_not_found" && read_flag) {
    cur_offset_ += response[0].size();
  }
}

}
}