#include "connection_pool.h"
#include "jiffy/utils/logger.h"
namespace jiffy {
namespace storage {

using namespace utils;

void connection_pool::init(std::vector<std::string> block_ids, int timeout_ms){ //, worker_(response_) {
  timeout_ms_ = timeout_ms;
  for (auto &block : block_ids) {
    auto block_info = block_id_parser::parse(block);
    if(free_.find(block_info.service_port) != free_.end()) {
      for(int i = 0; i < pool_size_; i++) {
        auto instance = new connection_instance();
        instance->connection->connect(block_info.host,
                                      block_info.service_port,
                                      timeout_ms_);
        instance->client_id = instance->connection->get_client_id();
        //instance->response_reader = instance->connection->get_command_response_reader(instance->client_id);
        //worker_.add_protocol(instance->connection->protocol());
        std::vector<connection_instance> connections;
        connections.push_back(*instance);
        connections_.insert(block_info.host + std::to_string(block_info.service_port), connections);
      }
      free_.emplace(block_info.id, true);
    }
  }
}
connection_pool::register_info connection_pool::request_connection(block_id & block_info) { // TODO we should allow creating connections in batches
  LOG(log_level::info) << "Requesting connection of block: " << block_info.id;
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
                                  timeout_ms_);
    instance->client_id = instance->connection->get_client_id();
    //instance->response_reader = instance->connection->get_command_response_reader(instance->client_id);
    std::vector<connection_instance> connections;
    connections.push_back(*instance);
    connections_.insert(block_info.host + std::to_string(block_info.service_port), connections);
  }
  //LOG(log_level::info) << "look here 3";
  // TODO currently all array has offset 0
  int64_t client_id;
  found = connections_.update_fn(block_info.host + std::to_string(block_info.service_port), [&](std::vector<connection_instance> &connections) {
    connections[0].block_id = block_info.id;
    client_id = connections[0].client_id;
  });
  if(!found) {
    throw std::logic_error("Failed to find connection");
  }
  return register_info(block_info.host + std::to_string(block_info.service_port), 0, client_id);
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
  LOG(log_level::info) << "Releasing connection of block: " << block_id;
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
void connection_pool::command_request(connection_pool::register_info connection_info,
                                      const sequence_id &seq,
                                      const std::vector<std::string> &args) {
  auto instance = connections_.find(connection_info.address)[connection_info.offset];
  instance.connection->command_request(seq, args, instance.block_id);
}
void connection_pool::send_run_command(connection_pool::register_info connection_info,
                                       const int32_t block_id,
                                       const std::vector<std::string> &arguments) {
  auto instance = connections_.find(connection_info.address)[connection_info.offset];
  instance.connection->send_run_command(block_id, arguments);

}
void connection_pool::recv_run_command(connection_pool::register_info connection_info,
                                       std::vector<std::string> &_return) {
  auto instance = connections_.find(connection_info.address)[connection_info.offset];
  instance.connection->recv_run_command(_return);

}
int64_t connection_pool::recv_response(connection_pool::register_info connection_info,
                                       std::vector<std::string> &out) {
  auto instance = connections_.find(connection_info.address)[connection_info.offset];
  return instance.response_reader.recv_response(out);
}
void connection_pool::get_command_response_reader(connection_pool::register_info connection_info, int64_t client_id) {
  bool found = connections_.update_fn(connection_info.address, [&](std::vector<connection_instance> &connections) {
    connections[connection_info.offset].response_reader = connections[connection_info.offset].connection->get_command_response_reader(client_id);
  });
  if(!found) {
    throw std::logic_error("Failed to find connection");
  }
}

}
}