#include "file_writer.h"
#include "jiffy/utils/string_utils.h"
#include "jiffy/utils/logger.h"
#include <thread>

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

file_writer::file_writer(std::shared_ptr<jiffy::directory::directory_interface> fs,
                         const std::string &path,
                         const jiffy::directory::data_status &status,
                         int timeout_ms) : file_client(fs, path, status, timeout_ms) {}

void file_writer::write(const std::string &data) {
  LOG(log_level::info) << "Write " << data;
  std::vector<std::string> _return;
  std::size_t file_size = (last_partition_ + 1) * block_size_;
  std::size_t remain_size = file_size - cur_partition_ * block_size_ - cur_offset_;
  std::size_t num_chain_needed = 0;
  if (remain_size < data.size()) {
    num_chain_needed = (data.size() - remain_size) / block_size_ + ((data.size() - remain_size) % block_size_ != 0);
  }
  LOG(log_level::info) << "Num chains needed " << num_chain_needed << " " << file_size << " " << remain_size;
  std::size_t init_size;
  init_size = MIN(data.size(), block_size_ - cur_offset_);
  LOG(log_level::info) << "Writing data " << init_size;
  /**
   * @brief First write here, success if no need to auto_scaling, wait in loop if need more chain and chain are not correctly allocated
   */
  std::vector<std::string> init_args
      {"write", data.substr(0, init_size), std::to_string(cur_offset_), std::to_string(num_chain_needed),
       std::to_string(last_partition_)};
  while (num_chain_needed != 0) {
    _return = blocks_[block_id()]->run_command(init_args);
    if (_return[0] == "!block_allocated") {
      LOG(log_level::info) << "Fuck this code";
      last_partition_ += num_chain_needed;
      num_chain_needed = 0;
      for (auto x = _return.begin() + 1; x < _return.end(); x++) {
        LOG(log_level::info) << *x;
        auto chain = string_utils::split(*x, '!');
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FILE_OPS));
      }
      cur_partition_++;
      cur_offset_ = 0;
      update_last_partition(cur_partition_);
    }
  }
  /**
   * @brief Parallel write here, assuming all the partitions are ready after the above step
   */
  std::size_t has_more = data.size() - init_size;
  LOG(log_level::info) << "Has more " << data.size() << " " << init_size << " " << has_more;
  std::size_t start_partition = block_id();
  std::size_t count = 0;
  while (has_more > 0) {
    count++;
    std::string data_to_write = data.substr(data.size() - has_more, MIN(has_more, block_size_ - cur_offset_));
    std::vector<std::string>
        args{"write", data_to_write, std::to_string(cur_offset_), std::to_string(0), std::to_string(0)};
    LOG(log_level::info) << "Data to write " << data_to_write.size() << " " << has_more << " " << cur_partition_;
    blocks_[block_id()]->send_command(args);
    has_more -= data_to_write.size();
    cur_offset_ += data_to_write.size();
    if (cur_offset_ == block_size_ && cur_partition_ != last_partition_) {
      cur_offset_ = 0;
      cur_partition_++;
      update_last_partition(cur_partition_);
    }
  }

  for(std::size_t i = 0; i < count; i++) {
    blocks_[start_partition + i]->recv_response();
  }

}

void file_writer::handle_redirect(std::vector<std::string> &_return, const std::vector<std::string> &args) {
  /*
  auto data = args[1];
  if (_return[0] == "!ok") {
    cur_offset_ += data.size();
    return;
  } else if (_return[0] == "!redo") {
    LOG(log_level::info) << "redo";
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    throw redo_error();
  } else if (_return[0] == "!split_write") {
    do {
      LOG(log_level::info) << "Split writing ";
      auto remaining_data_len = std::stoul(_return[1]);
      auto remaining_data = data.substr(data.size() - remaining_data_len, remaining_data_len);

      if (need_chain()) {
        auto chain = string_utils::split(_return[2], '!');
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FILE_OPS));
      }
      cur_partition_++;
      update_last_partition(cur_partition_);
      cur_offset_ = 0;
      if (remaining_data_len == 0) {
        _return.clear();
        _return.emplace_back("!ok");
        return;
      }
      std::vector<std::string>
          new_args{"write", remaining_data, std::to_string(cur_offset_), std::to_string(0), std::to_string(0)};
      do {
        _return = blocks_[block_id()]->run_command(new_args);
      } while (_return[0] == "!redo");
      cur_offset_ += remaining_data.size();
    } while (_return[0] == "!split_write");
  }
   */
}

}
}
