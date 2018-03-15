#include <iostream>
#include <kv/rpc/kv_rpc_server.h>
#include <kv/rpc/kv_management_rpc_server.h>

using namespace ::elasticmem::kv;

int main() {
  std::string host = "0.0.0.0";
  int service_port = 9092;
  int management_port = 9093;
  std::size_t num_blocks = 8;

  std::vector<std::shared_ptr<kv_block>> blocks;
  blocks.resize(num_blocks);
  for (auto &block : blocks) {
    block = std::make_shared<kv_block>();
  }

  auto management_server = kv_management_rpc_server::create(blocks, host, management_port);
  std::thread management_serve_thread([&management_server] {
    try {
      management_server->serve();
    } catch (std::exception& e) {
      std::cerr << "KV management server error: " << e.what() << std::endl;
    }
  });

  std::cout << "Management server listening on " << host << ":" << management_port << std::endl;

  auto kv_server = kv_rpc_server::create(blocks, host, service_port);
  std::thread kv_serve_thread([&kv_server] {
    try {
      kv_server->serve();
    } catch (std::exception& e) {
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