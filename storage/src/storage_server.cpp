#include <iostream>
#include <storage/kv/kv_rpc_server.h>
#include <storage/manager/storage_management_rpc_server.h>

using namespace ::elasticmem::storage;

int main() {
  std::string host = "0.0.0.0";
  int service_port = 9092;
  int management_port = 9093;
  std::size_t num_blocks = 8;

  std::vector<std::shared_ptr<block_management_ops>> block_management;
  block_management.resize(num_blocks);
  for (auto &block : block_management) {
    block = std::make_shared<kv_block>();
  }

  std::vector<std::shared_ptr<kv_block>> kv_blocks;
  for (auto &block : block_management) {
    kv_blocks.push_back(std::dynamic_pointer_cast<kv_block>(block));
  }


  auto management_server = storage_management_rpc_server::create(block_management, host, management_port);
  std::thread management_serve_thread([&management_server] {
    try {
      management_server->serve();
    } catch (std::exception &e) {
      std::cerr << "KV management server error: " << e.what() << std::endl;
    }
  });

  std::cout << "Management server listening on " << host << ":" << management_port << std::endl;

  auto kv_server = kv_rpc_server::create(kv_blocks, host, service_port);
  std::thread kv_serve_thread([&kv_server] {
    try {
      kv_server->serve();
    } catch (std::exception &e) {
      std::cerr << "KV server error: " << e.what() << std::endl;
    }
  });

  std::cout << "KV server listening on " << host << ":" << service_port << std::endl;

  if (management_serve_thread.joinable()) {
    management_serve_thread.join();
  }

  if (kv_serve_thread.joinable()) {
    kv_serve_thread.join();
  }

  return 0;
}