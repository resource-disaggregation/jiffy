#include "shared_log_client.h"
#include "jiffy/utils/logger.h"
#include "jiffy/utils/string_utils.h"
#include "jiffy/directory/directory_ops.h"
#include <algorithm>
#include <thread>
#include <utility>

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

shared_log_client::shared_log_client(std::shared_ptr<directory::directory_interface> fs,
                         const std::string &path,
                         const directory::data_status &status,
                         int timeout_ms)
    : data_structure_client(std::move(fs), path, status, timeout_ms),
      cur_partition_(0),
      cur_offset_(0),
      last_partition_(0),
      last_offset_(0) {
  for (const auto &block: status.data_blocks()) {
    blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, SHARED_LOG_OPS, timeout_ms_));
  }
  last_partition_ = status.data_blocks().size() - 1;
  std::vector<std::string> get_storage_capacity_args{"get_storage_capacity"};
  auto ret = blocks_[block_id()]->run_command(get_storage_capacity_args);
  THROW_IF_NOT_OK(ret);
  block_size_ = std::stoul(ret[1]);
  try {
    auto_scaling_ = (status.get_tag("shared_log.auto_scale") == "true");
  } catch (directory::directory_ops_exception &e) {
    auto_scaling_ = true;
  }
}

int shared_log_client::scan(std::vector<std::string> &buf, const std::string &start_pos, const std::string &end_pos, const std::vector<std::string> &logical_streams) {
  // Parallel scan here
  std::size_t start_partition = block_id();
  std::size_t count = 0;
  while (start_partition + count < blocks_.size()) {
    count++;
    std::vector<std::string>
        args{"scan", start_pos, end_pos};
    for (int i = 0; i < logical_streams.size(); i++){
      args.push_back(logical_streams[i]);
    }
    blocks_[block_id()]->send_command(args);
    cur_partition_ ++;
  }
  auto previous_size = buf.size();
  for (std::size_t k = 0; k < count; k++) {
    std::vector<std::string> resp = blocks_[start_partition + k]->recv_response();
    for (std::size_t i = 0; i < resp.size(); i++){
      buf.push_back(resp[i]);
    }
  }
  auto after_size = buf.size();
  return static_cast<int>(after_size - previous_size);
}

int shared_log_client::write(const std::string &position, const std::string &data_, const std::vector<std::string> &logical_streams) {
  std::size_t file_size = (last_partition_ + 1) * block_size_;
  std::vector<std::string> _return;

  std::size_t remain_size;
  std::size_t num_chain_needed = 0;

  std::string data = "";
  for (int i = 0; i < logical_streams.size(); i++){
    data += logical_streams[i];
  }
  data += data_;

  if (cur_partition_ * block_size_ + cur_offset_ > file_size) {
    num_chain_needed = cur_partition_ - last_partition_;
    file_size = (cur_partition_ + 1) * block_size_;
    remain_size = file_size - cur_partition_ * block_size_ - cur_offset_;
    num_chain_needed += (data.size() - remain_size) / block_size_ + ((data.size() - remain_size) % block_size_ != 0);
  } else {
    remain_size = file_size - cur_partition_ * block_size_ - cur_offset_;
    if (remain_size < data.size()) {
      num_chain_needed = (data.size() - remain_size) / block_size_ + ((data.size() - remain_size) % block_size_ != 0);
    }
  }

  if (num_chain_needed && !auto_scaling_) {
    return -1;
  }

  // First allocate new blocks if needed
  std::vector<std::string> add_block_args
      {"add_blocks", std::to_string(last_partition_), std::to_string(num_chain_needed)};

  while (num_chain_needed != 0) {
    _return = blocks_[last_partition_]->run_command(add_block_args);
    if (_return[0] == "!block_allocated") {
      last_partition_ += num_chain_needed;
      last_offset_ = 0;
      num_chain_needed = 0;
      try {
        for (auto x = _return.begin() + 1; x < _return.end(); x++) {
          auto chain = string_utils::split(*x, '!');
          blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, SHARED_LOG_OPS));
        }
      } catch (std::exception &e) {
        return -1;
      }
    }
  }
  if (block_size_ - cur_offset_ == 0) {
    cur_partition_++;
    cur_offset_ = 0;
  }
  // Parallel write
  std::size_t remaining_data = data.size();
  std::size_t start_partition = block_id();
  std::size_t count = 0;

  while (remaining_data > 0) {
    count++;
    std::string
        data_to_write = data.substr(data.size() - remaining_data, std::min(remaining_data, block_size_ - cur_offset_));
    std::vector<std::string>
        args{"write", data_to_write, std::to_string(cur_offset_)};
    for (int i = 0; i < logical_streams.size(); i++){
      args.push_back(logical_streams[i]);
    }
    blocks_[block_id()]->send_command(args);
    remaining_data -= data_to_write.size();
    cur_offset_ += data_to_write.size();
    update_last_offset();
    if (cur_offset_ == block_size_ && cur_partition_ != last_partition_) {
      cur_offset_ = 0;
      cur_partition_++;
      update_last_partition();
    }
  }

  for (std::size_t i = 0; i < count; i++) {
    blocks_[start_partition + i]->recv_response();
  }
  return data.size();

}

bool shared_log_client::trim(const std::string &start_pos, const std::string &end_pos) {
  // Parallel trim here
  std::size_t start_partition = block_id();
  std::size_t count = 0;
  while (start_partition + count < blocks_.size()) {
    count++;
    std::vector<std::string>
        args{"trim", start_pos, end_pos};
    blocks_[block_id()]->send_command(args);
    cur_partition_ ++;
  }
  return true;
}

void shared_log_client::refresh() {
  bool redo;
  do {
    status_ = fs_->dstatus(path_);
    blocks_.clear();
    try {
      for (const auto &block: status_.data_blocks()) {
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, SHARED_LOG_OPS, timeout_ms_));
      }
      redo = false;
    } catch (std::exception &e) {
      redo = true;
    }
  } while (redo);
}



bool shared_log_client::need_chain() const {
  return cur_partition_ >= blocks_.size() - 1;
}

std::size_t shared_log_client::block_id() const {
  if (cur_partition_ >= blocks_.size()) {
    throw std::logic_error("Blocks are insufficient, need to add more");
  }
  return cur_partition_;
}

void shared_log_client::handle_redirect(std::vector<std::string> &, const std::vector<std::string> &) {

}

}
}
