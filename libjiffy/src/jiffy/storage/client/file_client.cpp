#include "file_client.h"
#include "jiffy/utils/logger.h"
#include "jiffy/utils/string_utils.h"
#include <algorithm>
#include <thread>
#include <utility>

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

file_client::file_client(std::shared_ptr<directory::directory_interface> fs,
                         const std::string &path,
                         const directory::data_status &status,
                         std::size_t block_size,
                         int timeout_ms)
    : data_structure_client(std::move(fs), path, status, timeout_ms),
      cur_partition_(0),
      cur_offset_(0),
      last_partition_(0),
      block_size_(block_size) {
  for (const auto &block: status.data_blocks()) {
    blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, FILE_OPS, timeout_ms_));
  }
  last_partition_ = status.data_blocks().size() - 1;
}

std::string file_client::read_data(const std::size_t size) {
  if (cur_partition_ * block_size_ + cur_offset_ > last_partition_ * block_size_ + last_offset_) {
    return "";
  }
  std::size_t file_size = last_partition_ * block_size_ + last_offset_;
  std::size_t remain_size = file_size - cur_partition_ * block_size_ - cur_offset_;
  std::size_t has_more = MIN(remain_size, size);
  if (has_more <= 0)
    return "!msg_not_found";
  // Parallel read here
  std::size_t start_partition = block_id();
  std::size_t count = 0;
  while (has_more > 0) {
    count++;
    std::size_t data_to_read = MIN(has_more, block_size_ - cur_offset_);
    std::vector<std::string>
        args{"read", std::to_string(cur_offset_), std::to_string(data_to_read)};
    blocks_[block_id()]->send_command(args);
    has_more -= data_to_read;
    cur_offset_ += data_to_read;
    if (cur_offset_ == block_size_ && cur_partition_ != last_partition_) {
      cur_offset_ = 0;
      cur_partition_++;
    }
    if (has_more == 0) break;
  }
  std::string ret;
  for (std::size_t k = 0; k < count; k++) {
    auto tmp = blocks_[start_partition + k]->recv_response().back();
    ret.append(tmp);
  }
  return ret;

}

void file_client::write_data(const std::string &data) {
  std::size_t file_size = (last_partition_ + 1) * block_size_;;
  std::vector<std::string> _return;
  std::size_t remain_size;
  std::size_t init_size;
  std::size_t init_block_id;
  std::size_t init_offset;
  std::size_t num_chain_needed = 0;
  std::size_t off;
  std::size_t has_more;
  if (cur_partition_ * block_size_ + cur_offset_ > file_size) {
    init_size = 0;
    init_block_id = last_partition_;
    init_offset = last_offset_;
    num_chain_needed = cur_partition_ - last_partition_;
    file_size = (cur_partition_ + 1) * block_size_;
    remain_size = file_size - cur_partition_ * block_size_ - cur_offset_;
    num_chain_needed += (data.size() - remain_size) / block_size_ + ((data.size() - remain_size) % block_size_ != 0);
    off = 0;
  } else {
    file_size = (last_partition_ + 1) * block_size_;
    init_block_id = block_id();
    init_offset = cur_offset_;
    remain_size = file_size - cur_partition_ * block_size_ - cur_offset_;
    if (remain_size < data.size()) {
      num_chain_needed = (data.size() - remain_size) / block_size_ + ((data.size() - remain_size) % block_size_ != 0);
    }
    init_size = MIN(data.size(), block_size_ - cur_offset_);
    off = 1;
  }

  // First write to allocate new blocks if needed
  bool flag = false;
  std::vector<std::string> init_args
      {"write", data.substr(0, init_size), std::to_string(init_offset), std::to_string(num_chain_needed),
       std::to_string(last_partition_)};
  while (num_chain_needed != 0) {
    flag = true;
    _return = blocks_[init_block_id]->run_command(init_args);
    if (_return[0] == "!block_allocated") {
      last_partition_ += num_chain_needed;
      num_chain_needed = 0;
      for (auto x = _return.begin() + 1; x < _return.end(); x++) {
        auto chain = string_utils::split(*x, '!');
        blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, chain, FILE_OPS));
      }
      cur_partition_ += off;
      if (off)
        cur_offset_ = 0;
      update_last_partition();
    }
  }
  // Parallel write
  if (flag)
    has_more = data.size() - init_size;
  else
    has_more = data.size();
  std::size_t start_partition = block_id();
  std::size_t count = 0;
  while (has_more > 0) {
    count++;
    std::string data_to_write = data.substr(data.size() - has_more, MIN(has_more, block_size_ - cur_offset_));
    std::vector<std::string>
        args{"write", data_to_write, std::to_string(cur_offset_), std::to_string(0), std::to_string(0)};
    blocks_[block_id()]->send_command(args);
    has_more -= data_to_write.size();
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

}

void file_client::refresh() {
  status_ = fs_->dstatus(path_);
  blocks_.clear();
  for (const auto &block: status_.data_blocks()) {
    blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, FILE_OPS, timeout_ms_));
  }
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

void file_client::handle_redirect(std::vector<std::string> &_return, const std::vector<std::string> &args) {

}

}
}
