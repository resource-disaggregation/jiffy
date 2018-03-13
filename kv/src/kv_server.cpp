#include <iostream>
#include <rpc/kv_rpc_server.h>
#include <rpc/kv_management_rpc_server.h>

using namespace ::elasticmem::kv;

int main() {
  std::string host;
  int service_port = 9090;
  int management_port = 9091;
  std::size_t num_blocks = 64;

  std::vector<std::shared_ptr<kv_block>> blocks;
  blocks.resize(num_blocks);
  for (auto &block : blocks) {
    block = std::make_shared<kv_block>();
  }

  auto management_server = kv_management_rpc_server::create(blocks, host, management_port);
  std::thread management_serve_thread([&management_server] { management_server->serve(); });

  auto kv_server = kv_rpc_server::create(blocks, host, service_port);
  std::thread kv_serve_thread([&kv_server] { kv_server->serve(); });

  if (management_serve_thread.joinable()) {
    management_serve_thread.join();
  }

  if (kv_serve_thread.joinable()) {
    kv_serve_thread.join();
  }

  return 0;
}