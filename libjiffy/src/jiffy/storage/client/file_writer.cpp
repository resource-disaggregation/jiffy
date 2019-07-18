#include "file_writer.h"
#include "jiffy/utils/string_utils.h"

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

file_writer::file_writer(std::shared_ptr<jiffy::directory::directory_interface> fs,
                         const std::string &path,
                         const jiffy::directory::data_status &status,
                         int timeout_ms) : file_client(fs, path, status, timeout_ms) {}

std::string file_writer::write(const std::string &data) {
  std::string _return;
  std::vector<std::string> args{data};
  args.push_back(std::to_string(cur_offset_));
  bool redo;
  do {
    try {
      _return = blocks_[block_id()]->run_command(file_cmd_id::file_write, args).front();
      handle_redirect(file_cmd_id::file_write, args, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

void file_writer::handle_redirect(int32_t cmd_id,
                                  const std::vector<std::string> &args,
                                  std::string &response) {
  bool write_flag = true;
  typedef std::vector<std::string> list_t;
  if (response == "!redo") {
    throw redo_error();
  }
  if (response.substr(0, 12) == "!split_write") {
    do {
      auto parts = string_utils::split(response, '!');
      auto remaining_data_len = std::stoi(*(parts.end() - 1));
      auto data = args.front();
      auto remaining_data = data.substr(data.size() - remaining_data_len, remaining_data_len);
      auto new_args = std::vector<std::string>{remaining_data};
      if (need_chain()) {
        auto chain = list_t(parts.begin() + 2, parts.end() - 1);
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_,
                                                                 path_,
                                                                 directory::replica_chain(chain),
                                                                 FILE_OPS));
      }
      cur_partition_++;
      update_last_partition(cur_partition_);
      cur_offset_ = 0;
      new_args.push_back(std::to_string(cur_offset_));
      do {
        response = blocks_[block_id()]->run_command(cmd_id, new_args).front();
      } while (response == "!redo");
      cur_offset_ += new_args[0].size();
      write_flag = false;
    } while (response.substr(0, 12) == "!split_write");
  }

  if (cmd_id == static_cast<int32_t>(file_cmd_id::file_write) && write_flag) {
    cur_offset_ += args[0].size();
  }
}

void file_writer::handle_redirects(int32_t,
                                   const std::vector<std::string> &,
                                   std::vector<std::string> &) {
  // Not supported yet
}

}
}
