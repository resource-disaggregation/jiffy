#include "connection_pool.h"
#include "jiffy/utils/logger.h"
namespace jiffy {
namespace storage {

using namespace utils;

void connection_pool::init(std::vector<std::string> block_ids, int timeout_ms){ //, worker_(response_) {
  timeout_ms_ = timeout_ms;
  connections_.resize(block_ids.size());
  free_.resize(block_ids.size());
  for (auto &block : block_ids) {
    auto block_info = block_id_parser::parse(block);
    connections_[block_info.id].connection = std::make_shared<block_client>();
    connections_[block_info.id].connection->connect(block_info.host,
                                                    block_info.service_port,
                                                    block_info.id,
                                                    timeout_ms_);
    free_[block_info.id] = true;
    //worker_.add_protocol(connections_[block_info.id].connection->protocol());
  }
}
connection_instance & connection_pool::request_connection(std::size_t block_id) {
  if (block_id > connections_.size()) { // TODO this is only for one connection
    throw std::logic_error("Invalid block id");
  }
  LOG(log_level::info) << "Requesting connection of block: " << block_id;
  //bool expected = false;
  /* free_.compare_exchange_strong(expected, true);
  if (expected) {
    //TODO create new connections
  } else {
    return connections_[block_id];
  } */
  if(free_[block_id]) {
    free_[block_id] = false;
    return connections_[block_id];
  }

}
void connection_pool::release_connection(std::size_t block_id) {
  /*bool expected = true;
  free_.compare_exchange_strong(expected, false);
  if (!expected) {
    throw std::logic_error("Connection already closed");
  } */
  LOG(log_level::info) << "Releasing connection of block: " << block_id;
  if(!free_[block_id]) {
    free_[block_id] = true;
  } else {
    throw std::logic_error("Connection already closed");
  }
}

}
}