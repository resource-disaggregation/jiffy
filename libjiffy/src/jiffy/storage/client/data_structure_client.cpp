#include "data_structure_client.h"


namespace jiffy {
namespace storage {

data_structure_client::data_structure_client(std::shared_ptr<directory::directory_interface> fs,
                                     const std::string &path,
                                     const directory::data_status &status,
                                     int timeout_ms)
    : fs_(std::move(fs)), path_(path), status_(status), timeout_ms_(timeout_ms) {
  blocks_.clear();
  for (const auto &block: status.data_blocks()) {
    blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, timeout_ms_));
  }
}


directory::data_status &data_structure_client::status() {
  return status_;
}



}
}