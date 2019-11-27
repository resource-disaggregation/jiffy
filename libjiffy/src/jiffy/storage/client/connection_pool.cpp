#include <memory>

#include "connection_pool.h"
#include "jiffy/utils/logger.h"
namespace jiffy {
namespace storage {

using namespace utils;

void connection_pool::init(std::vector<std::string> block_ids, int timeout_ms) { //, worker_(response_) {
  LOG(log_level::info) << "Init this pool " << block_ids.size();
  timeout_ms_ = timeout_ms;
  for (auto &block : block_ids) {
    auto block_info = block_id_parser::parse(block);
    std::string block_address = block_info.host + std::to_string(block_info.service_port);
    try {
      connections_.find(block_address);
    } catch (std::out_of_range &e) {
      std::vector<connection_instance> connections;
      std::vector<std::shared_ptr<apache::thrift::protocol::TProtocol>> protocols;
      std::vector<std::shared_ptr<mailbox_t>> mailboxes;

      for (std::size_t i = 0; i < pool_size_; i++) {
        auto instance = new connection_instance();
        instance->connection->connect(block_info.host,
                                      block_info.service_port,
                                      timeout_ms_);
        instance->client_id = instance->connection->get_client_id();
        LOG(log_level::info) << "Trying to register client id: " << instance->client_id;
        //instance->connection->register_client_id(instance->client_id);
        //LOG(log_level::info) << "Assigning client id " << instance->client_id;
        instance->free_ = true;
        instance->block_id = -1;
        instance->response_mailbox = std::make_shared<mailbox_t>();
        protocols.push_back(instance->connection->protocol());
        mailboxes.emplace_back(instance->response_mailbox);
        //instance->response_reader = instance->connection->get_command_response_reader(instance->client_id);

        connections.push_back(*instance);
      }
      auto worker = std::make_shared<response_worker>(mailboxes);
      for(auto & protocol : protocols) {
        worker->add_protocol(protocol);
      }
      //LOG(log_level::info) << "Initialize connection of block: " << block_address;
      connections_.insert(block_address, connections);
      workers_.push_back(worker);
      workers_.back()->start();
    }
  }
}

connection_pool::register_info connection_pool::request_connection(block_id &block_info) { // TODO we should allow creating connections in batches
  auto block_address = block_info.host + std::to_string(block_info.service_port);
  LOG(log_level::info) << "Requesting connection of block: " << block_address << " " << block_info.id;
  //std::unique_lock<std::mutex> mlock(mutex_);
//  try {
//    auto it = connection_map_.find(block_address);
//    found = true;
//    if(!it) {
//      //mlock.unlock();
//      //LOG(log_level::info) << "This connection is in use!";
//      throw std::logic_error("This connection is in use");
//    }
//    //LOG(log_level::info) << "look here 1";
//    auto exist = connection_map_.update_fn(block_address, [&](bool &free) {
//      free = false;
//    });
//    if(!exist) {
//      throw std::logic_error("Failed to find connection");
//    }
//  } catch (std::out_of_range &e) {
//    found = false;
//    connection_map_.insert(block_address, false);
//  }
  //mlock.unlock();
  // TODO currently all array has offset 0
  int64_t client_id;
  std::size_t offset;
  bool found = connections_.update_fn(block_address, [&](std::vector<connection_instance> &connections) {
    std::size_t count = 0;
    for (auto &connection : connections) {
      if (connection.free_) {
        connection.free_ = false;
        connection.block_id = block_info.id;
        client_id = connection.client_id;
//        connection.connection->register_client_id(client_id);
        offset = count;
        break;
      }
      count++;
    }
  });
  if (!found) {
    std::vector<connection_instance> connections;
    std::vector<std::shared_ptr<apache::thrift::protocol::TProtocol>> protocols;
    std::vector<std::shared_ptr<mailbox_t>> mailboxes;
    for (std::size_t i = 0; i < pool_size_; i++) {
      auto instance = new connection_instance();
      instance->connection->connect(block_info.host,
                                    block_info.service_port,
                                    timeout_ms_);
      instance->client_id = instance->connection->get_client_id();
      instance->free_ = true;
      protocols.push_back(instance->connection->protocol());
      instance->response_mailbox = std::make_unique<mailbox_t>();
      mailboxes.emplace_back(instance->response_mailbox);
      //LOG(log_level::info) << "Assigning client id " << instance->client_id;
      //instance->response_reader = instance->connection->get_command_response_reader(instance->client_id);
      connections.push_back(*instance);
    }
    connections[0].free_ = false;
    connections[0].block_id = block_info.id;
    client_id = connections[0].client_id;
    offset = 0;
    connections_.insert(block_address, connections);
    auto worker = std::make_shared<response_worker>(mailboxes);
    for(auto & protocol : protocols) {
      worker->add_protocol(protocol);
    }
    workers_.push_back(worker);
    workers_.back()->start();
  }
  //LOG(log_level::info) << "Register result: " << block_address << " " << offset << " " << client_id;
  return register_info(block_address, offset, client_id);
  //bool expected = false;
  /* connection_map_.compare_exchange_strong(expected, true);
  if (expected) {
    //TODO create new connections
  } else {
    return connections_[block_id];
  }
  if(connection_map_[block_id]) {
    connection_map_[block_id] = false;
    return connections_[block_id];
  }
   return connections_.find(block_info.id); */

}
void connection_pool::release_connection(connection_pool::register_info &connection_info) {
  /*bool expected = true;
  connection_map_.compare_exchange_strong(expected, false);
  if (!expected) {
    throw std::logic_error("Connection already closed");
  } */
  auto block_address = connection_info.address;
  LOG(log_level::info) << "Releasing connection of block: " << block_address;
  bool found = connections_.update_fn(block_address, [&](std::vector<connection_instance> &connections) {
    if (connections[connection_info.offset].free_) {
      throw std::logic_error("Connection already closed");
    }
    connections[connection_info.offset].free_ = true;
    connections[connection_info.offset].block_id = -1;
  });
  if (!found) {
    throw std::logic_error("Connection does not exist");
  }

//  //std::unique_lock<std::mutex> mlock(mutex_);
//  auto it = connection_map_.find(block_id);
//  if(it != connection_map_.end()) {
//    connection_map_[block_id] = true;
//  } else {
//    //mlock.unlock();
//    throw std::logic_error("Connection already closed");
//  }
//  //mlock.unlock();
}
void connection_pool::command_request(connection_pool::register_info connection_info,
                                      const sequence_id &seq,
                                      const std::vector<std::string> &args) {
  LOG(log_level::info) << "requesting command " << args[0];
  bool found = connections_.update_fn(connection_info.address, [&](std::vector<connection_instance> &connections) {
    connections[connection_info.offset].in_flight_ = true;
    connections[connection_info.offset].connection->command_request(seq,
                                                                    args,
                                                                    connections[connection_info.offset].block_id);
  });
  if(!found) {
    throw std::logic_error("Connection does not request");
  }
}
void connection_pool::send_run_command(connection_pool::register_info connection_info,
                                       const int32_t block_id,
                                       const std::vector<std::string> &arguments) {
  auto &instance = connections_.find(connection_info.address)[connection_info.offset];
  instance.connection->send_run_command(block_id, arguments);

}
void connection_pool::recv_run_command(connection_pool::register_info connection_info,
                                       std::vector<std::string> &_return) {
  auto &instance = connections_.find(connection_info.address)[connection_info.offset];
  instance.connection->recv_run_command(_return);

}
int64_t connection_pool::recv_response(connection_pool::register_info connection_info,
                                       std::vector<std::string> &out) {
  auto &instance = connections_.find(connection_info.address)[connection_info.offset];
  //return instance.response_reader.recv_response(out);
  //return instance.response_mailbox->pop()
  auto ret = instance.response_mailbox->pop();
  out.insert(out.begin(), ret.begin(), ret.end());
  return 0;
}

void connection_pool::get_command_response_reader(connection_pool::register_info connection_info, int64_t client_id) {
  //bool found = connections_.update_fn(connection_info.address, [&](std::vector<connection_instance> &connections) {
    LOG(log_level::info) << "registering client id: " << client_id;
  auto &instance = connections_.find(connection_info.address)[connection_info.offset];
  instance.connection->register_client_id(client_id);
  //  connections[connection_info.offset].connection->register_client_id(client_id);
    //LOG(log_level::info) << "Vector size: " << connections.size();
    //if(!connections[connection_info.offset].response_reader.is_set()) {
    //  connections[connection_info.offset].response_reader =
    //      connections[connection_info.offset].connection->get_command_response_reader(client_id);
    //}
 // });
//  if (!found) {
//    throw std::logic_error("Failed to find connection");
//  }
}
connection_pool::~connection_pool() {

}

}
}
