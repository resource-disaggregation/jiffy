#include "connection_pool.h"
#include "jiffy/utils/logger.h"
namespace jiffy {
namespace storage {

using namespace utils;

void connection_pool::init(std::vector<std::string> block_ids, int timeout_ms){ //, worker_(response_) {
  timeout_ms_ = timeout_ms;
  for (auto &block : block_ids) {
    auto block_info = block_id_parser::parse(block);
    auto instance = new connection_instance();
    instance->connection->connect(block_info.host,
                                  block_info.service_port,
                                  block_info.id,
                                  timeout_ms_);
    instance->client_id = instance->connection->get_client_id();
    instance->response_reader = instance->connection->get_command_response_reader(instance->client_id);
    //worker_.add_protocol(instance->connection->protocol());
    connections_.insert(block_info.id, *instance);
    free_.emplace(block_info.id, true);
  }
}
connection_instance connection_pool::request_connection(block_id & block_info) { // TODO we should allow creating connections in batches
  //LOG(log_level::info) << "Requesting connection of block: " << block_info.id;
  bool found;
  auto id = static_cast<std::size_t>(block_info.id);
  //std::unique_lock<std::mutex> mlock(mutex_);
  auto it = free_.find(id);
  if(it != free_.end()) {
    found = true;
    if(!it->second) {
      //mlock.unlock();
      //LOG(log_level::info) << "This connection is in use!";
      throw std::logic_error("This connection is in use");
    }
    //LOG(log_level::info) << "look here 1";
    free_[id] = false;
  } else {
    found = false;
    free_.emplace(id, false);
  }
  //mlock.unlock();
  if (!found) {
    //LOG(log_level::info) << "look here 2";
    auto instance = new connection_instance();
    instance->connection->connect(block_info.host,
                                  block_info.service_port,
                                  block_info.id,
                                  timeout_ms_);
    instance->client_id = instance->connection->get_client_id();
    instance->response_reader = instance->connection->get_command_response_reader(instance->client_id);
    connections_.insert(block_info.id, *instance);
  }
  //LOG(log_level::info) << "look here 3";
  return connections_.find(block_info.id);
  //bool expected = false;
  /* free_.compare_exchange_strong(expected, true);
  if (expected) {
    //TODO create new connections
  } else {
    return connections_[block_id];
  }
  if(free_[block_id]) {
    free_[block_id] = false;
    return connections_[block_id];
  }
   return connections_.find(block_info.id); */

}
void connection_pool::release_connection(std::size_t block_id) {
  /*bool expected = true;
  free_.compare_exchange_strong(expected, false);
  if (!expected) {
    throw std::logic_error("Connection already closed");
  } */
  //LOG(log_level::info) << "Releasing connection of block: " << block_id;
  //std::unique_lock<std::mutex> mlock(mutex_);
  auto it = free_.find(block_id);
  if(it != free_.end()) {
    free_[block_id] = true;
  } else {
    //mlock.unlock();
    throw std::logic_error("Connection already closed");
  }
  //mlock.unlock();
}

}
}