#include "catch.hpp"

#include <thrift/transport/TTransportException.h>
#include "../src/kv/rpc/kv_management_rpc_server.h"
#include "../src/kv/rpc/kv_management_client.h"
#include "../src/kv/manager/kv_manager.h"
#include "../src/kv/manager/block_name_parser.h"

using namespace ::elasticmem::kv;
using namespace ::apache::thrift::transport;

#define NUM_BLOCKS 1
#define HOST "127.0.0.1"
#define PORT 9090

static std::vector<std::shared_ptr<kv_block>> init_blocks() {
  std::vector<std::shared_ptr<kv_block>> blks;
  blks.resize(NUM_BLOCKS);
  for (auto &block : blks) {
    block = std::make_shared<kv_block>();
  }
  return blks;
}

static void wait_till_server_ready(const std::string& host, int port) {
  bool check = true;
  while (check) {
    try {
      kv_management_client(host, port);
      check = false;
    } catch (TTransportException& e) {
      usleep(100000);
    }
  }
}

static std::vector<std::shared_ptr<kv_block>> blocks = init_blocks();

TEST_CASE("manager_storage_size_test", "[storage_size][storage_capacity][clear]") {
  auto server = kv_management_rpc_server::create(blocks, HOST, PORT);
  std::thread serve_thread([&server]{ server->serve(); });
  wait_till_server_ready(HOST, PORT);

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(blocks[0]->put(std::to_string(i), std::to_string(i)));
  }

  kv_manager manager;
  auto block_name = block_name_parser::make_block_name(std::make_tuple(HOST, PORT, 0));
  REQUIRE(manager.storage_size(block_name) == 1000);
  REQUIRE(manager.storage_size(block_name) <= manager.storage_capacity(block_name));
  REQUIRE_NOTHROW(manager.clear(block_name));
  REQUIRE(manager.storage_size(block_name) == 0);

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("manager_flush_load_test", "[put][flush][clear][load][get]") {
  auto server = kv_management_rpc_server::create(blocks, HOST, PORT);
  std::thread serve_thread([&server]{ server->serve(); });
  wait_till_server_ready(HOST, PORT);

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(blocks[0]->put(std::to_string(i), std::to_string(i)));
  }

  kv_manager manager;
  auto block_name = block_name_parser::make_block_name(std::make_tuple(HOST, PORT, 0));
  REQUIRE_NOTHROW(manager.flush(block_name, "/tmp", "/test"));
  REQUIRE_NOTHROW(manager.clear(block_name));
  REQUIRE(manager.storage_size(block_name) == 0);
  REQUIRE_NOTHROW(manager.load(block_name, "/tmp", "/test"));

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}