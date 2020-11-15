#include "file_client.h"
#include "jiffy/utils/logger.h"
#include "jiffy/utils/string_utils.h"
#include "jiffy/directory/directory_ops.h"
#include <algorithm>
#include <thread>
#include <utility>

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

file_client::file_client(std::shared_ptr<directory::directory_interface> fs,
                         const std::string &path,
                         const directory::data_status &status,
                         int timeout_ms)
    : data_structure_client(std::move(fs), path, status, timeout_ms),
      cur_partition_(0),
      cur_offset_(0),
      last_partition_(0),
      last_offset_(0) {
  for (const auto &block: status.data_blocks()) {
    blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, FILE_OPS, timeout_ms_));
  }
  last_partition_ = status.data_blocks().size() - 1;
  std::vector<std::string> get_storage_capacity_args{"get_storage_capacity"};
  auto ret = blocks_[block_id()]->run_command(get_storage_capacity_args);
  THROW_IF_NOT_OK(ret);
  block_size_ = std::stoul(ret[1]);
  std::vector<std::string> get_partition_size_args{"get_partition_size"};
  ret = blocks_[last_partition_]->run_command(get_partition_size_args);
  THROW_IF_NOT_OK(ret);
  last_offset_ = std::stoul(ret[1]);
  try {
    auto_scaling_ = (status.get_tag("file.auto_scale") == "true");
  } catch (directory::directory_ops_exception &e) {
    auto_scaling_ = true;
  }
}

int file_client::read(std::string &buf, size_t size) {
  std::size_t file_size = last_partition_ * block_size_ + last_offset_;
  if (file_size <= cur_partition_ * block_size_ + cur_offset_)
    return -1;
  std::size_t remain_size = file_size - cur_partition_ * block_size_ - cur_offset_;
  std::size_t remaining_data = std::min(remain_size, size);
  if (remaining_data == 0)
    return 0;
  // Parallel read here
  std::size_t start_partition = block_id();
  std::size_t count = 0;
  while (remaining_data > 0) {
    count++;
    std::size_t data_to_read = std::min(remaining_data, block_size_ - cur_offset_);
    std::vector<std::string>
        args{"read", std::to_string(cur_offset_), std::to_string(data_to_read)};
    blocks_[block_id()]->send_command(args);
    remaining_data -= data_to_read;
    cur_offset_ += data_to_read;
    if (cur_offset_ == block_size_ && cur_partition_ != last_partition_) {
      cur_offset_ = 0;
      cur_partition_++;
    }
  }
  auto previous_size = buf.size();
  for (std::size_t k = 0; k < count; k++) {
    buf += blocks_[start_partition + k]->recv_response().back();
  }
  auto after_size = buf.size();
  return static_cast<int>(after_size - previous_size);
}

int file_client::write(const std::string &data) {
  std::size_t file_size = (last_partition_ + 1) * block_size_;
  std::vector<std::string> _return;

  std::size_t remain_size;
  std::size_t num_chain_needed = 0;

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
          blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FILE_OPS));
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

void file_client::refresh() {
  bool redo;
  do {
    status_ = fs_->dstatus(path_);
    blocks_.clear();
    try {
      for (const auto &block: status_.data_blocks()) {
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, FILE_OPS, timeout_ms_));
      }
      redo = false;
    } catch (std::exception &e) {
      redo = true;
    }
  } while (redo);
}

bool file_client::seek(const std::size_t offset) {
  cur_partition_ = offset / block_size_;
  cur_offset_ = offset % block_size_;
  return true;
}

bool file_client::need_chain() const {
  return cur_partition_ >= blocks_.size() - 1;
}

std::size_t file_client::block_id() const {
  if (cur_partition_ >= blocks_.size()) {
    throw std::logic_error("Blocks are insufficient, need to add more");
  }
  return cur_partition_;
}

void file_client::handle_redirect(std::vector<std::string> &, const std::vector<std::string> &) {

}

}
}
