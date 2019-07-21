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
  std::string _return;
  std::vector<std::string> args = {"read", std::to_string(cur_offset_), std::to_string(size)};
  bool redo;
  do {
    try {
      _return = blocks_[block_id()]->run_command(args).front();
      handle_redirect(args, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

void file_reader::handle_redirect(const std::vector<std::string> &args, std::string &response) {
  bool read_flag = true;
  typedef std::vector<std::string> list_t;
  if (response == "!redo") throw redo_error();

  if (response.substr(0, 11) == "!split_read") {
    std::string result;
    do {
      auto parts = string_utils::split(response, '!');
      auto data_part = *(parts.end() - 1);
      result += data_part;
      if (need_chain()) {
        auto chain = directory::replica_chain(list_t(parts.begin() + 2, parts.end() - 1));
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FILE_OPS));
      }
      cur_partition_++;
      update_last_partition(cur_partition_);
      cur_offset_ = 0;
      std::vector<std::string> new_args{"read", std::to_string(cur_offset_),
                                        std::to_string(std::stoi(args[2]) - result.size())};
      response = blocks_[block_id()]->run_command(new_args).front();
      if (response != "!msg_not_found") {
        if (response.substr(0, 11) == "!split_read")
          continue;
        cur_offset_ += response.size();
        result += response;
      }
    } while (response.substr(0, 11) == "!split_read");
    response = result;
    read_flag = false;
  }
  if (response != "!msg_not_found" && read_flag) {
    cur_offset_ += response.size();
  }
}

}
}