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
  std::vector<std::string> _return;
  std::vector<std::string> args{"write", data, std::to_string(cur_offset_)};
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

void file_writer::handle_redirect(const std::vector<std::string> &args, std::vector<std::string> &response) {
  bool write_flag = true;
  auto data = args[1];

  if (response[0] == "!redo") throw redo_error();

  if (response[0] == "!split_write") {
    do {
      auto remaining_data_len = std::stoi(response[1]);
      auto chain = string_utils::split(response[2], '!');

      auto remaining_data = data.substr(data.size() - remaining_data_len, remaining_data_len);

      if (need_chain()) {
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FILE_OPS));
      }
      cur_partition_++;
      update_last_partition(cur_partition_);
      cur_offset_ = 0;
      std::vector<std::string> new_args{"write", remaining_data, std::to_string(cur_offset_)};
      do {
        response = blocks_[block_id()]->run_command(new_args);
      } while (response[0] == "!redo");
      cur_offset_ += remaining_data.size();
      write_flag = false;
    } while (response[0] == "!split_write");
  }

  if (write_flag) {
    cur_offset_ += data.size();
  }
}

}
}
