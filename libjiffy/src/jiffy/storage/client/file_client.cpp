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
                         int timeout_ms)
    : data_structure_client(std::move(fs), path, status, timeout_ms),
      cur_partition_(0),
      cur_offset_(0),
      last_partition_(0) {
  //TODO init block_size_
  block_size_ = 100;
  for (const auto &block: status.data_blocks()) {
    blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, FILE_OPS, timeout_ms_));
  }
}

void file_client::refresh() {
  status_ = fs_->dstatus(path_);
  blocks_.clear();
  for (const auto &block: status_.data_blocks()) {
    blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, FILE_OPS, timeout_ms_));
  }
}
// TODO currently only support the offset within the file, need to extend to support seek + write
bool file_client::seek(const std::size_t offset) {
  auto _return = blocks_[last_partition_]->run_command({"seek"});
  
  THROW_IF_NOT_OK(_return);
  
  auto size = static_cast<std::size_t>(std::stoull(_return[1]));
  auto cap = static_cast<std::size_t>(std::stoull(_return[2]));
  if (offset > last_partition_ * cap + size) {
    return false;
  } else {
    cur_partition_ = offset / cap;
    cur_offset_ = offset % cap;
    return true;
  }
}

bool file_client::need_chain() const {
  return cur_partition_ >= blocks_.size() - 1;
}

std::size_t file_client::num_chain(std::size_t data_size) const {

}


std::size_t file_client::block_id() const {
  if (cur_partition_ >= blocks_.size()) {
    //throw std::logic_error("Blocks are insufficient, need to add more");
  }
  return cur_partition_;
}

}
}
