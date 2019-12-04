#include "data_structure_client.h"

namespace jiffy {
namespace storage {

data_structure_client::data_structure_client(std::shared_ptr<directory::directory_interface> fs,
                                             const std::string &path,
                                             const directory::data_status &status,
                                             connection_pool & pool,
                                             int timeout_ms)
    : fs_(std::move(fs)), path_(path), status_(status), pool_(pool), timeout_ms_(timeout_ms) {
  std::vector<std::string> block_names;
  for(const auto &chain : status.data_blocks()) {
    for(const auto &block : chain.block_ids) {
      block_names.push_back(block);
    }
  }
  //pool_.init(block_names);
}

directory::data_status &data_structure_client::status() {
  return status_;
}




}
}