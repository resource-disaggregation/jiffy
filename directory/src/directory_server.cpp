#include <iostream>
#include <vector>
#include <thread>
#include <tree/directory_tree.h>
#include <tree/random_block_allocator.h>
#include <rpc/directory_rpc_server.h>
#include <manager/kv_manager.h>
#include <lease/lease_manager.h>
#include <rpc/directory_lease_server.h>

using namespace ::elasticmem::directory;
using namespace ::elasticmem::kv;

int main() {
  std::string host;
  int service_port = 9090;
  int lease_port = 9091;
  uint64_t lease_period_ms = 100;
  uint64_t grace_period_ms = 100;
  std::vector<std::string> blocks;

  auto alloc = std::make_shared<random_block_allocator>(blocks);
  auto t = std::make_shared<directory_tree>(alloc);
  auto directory_server = directory_rpc_server::create(t, host, service_port);
  std::thread directory_serve_thread([&directory_server] { directory_server->serve(); });

  auto kv = std::make_shared<kv_manager>();
  auto lease_server = directory_lease_server::create(t, kv, host, lease_port);
  std::thread lease_serve_thread([&lease_server] { lease_server->serve(); });

  lease_manager lmgr(t, lease_period_ms, grace_period_ms);
  lmgr.start();

  if (directory_serve_thread.joinable()) {
    directory_serve_thread.join();
  }

  if (lease_serve_thread.joinable()) {
    lease_serve_thread.join();
  }
  return 0;
}