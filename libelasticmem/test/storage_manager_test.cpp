#include "catch.hpp"

#include <thrift/transport/TTransportException.h>
#include <thread>
#include "../src/storage/manager/storage_management_server.h"
#include "../src/storage/manager/storage_management_client.h"
#include "../src/storage/manager/storage_manager.h"
#include "../src/storage/kv/kv_block.h"
#include "test_utils.h"

using namespace ::elasticmem::storage;
using namespace ::apache::thrift::transport;

#define NUM_BLOCKS 1
#define HOST "127.0.0.1"
#define PORT 9090

static auto blocks = test_utils::init_kv_blocks(NUM_BLOCKS, 0, PORT, 0);

TEST_CASE("manager_storage_size_test", "[storage_size][storage_capacity][reset]") {
  auto server = storage_management_server::create(blocks, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(std::dynamic_pointer_cast<kv_block>(blocks[0])->put(std::to_string(i), std::to_string(i)));
  }

  storage_manager manager;
  auto block_name = block_name_parser::make(HOST, 0, PORT, 0, 0, 0);
  REQUIRE(manager.storage_size(block_name) == 1000);
  REQUIRE(manager.storage_size(block_name) <= manager.storage_capacity(block_name));
  REQUIRE_NOTHROW(manager.reset(block_name));
  REQUIRE(manager.storage_size(block_name) == 0);

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("manager_flush_load_test", "[put][flush][reset][load][get]") {
  auto server = storage_management_server::create(blocks, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(std::dynamic_pointer_cast<kv_block>(blocks[0])->put(std::to_string(i), std::to_string(i)));
  }

  storage_manager manager;
  auto block_name = block_name_parser::make(HOST, 0, PORT, 0, 0, 0);
  REQUIRE_NOTHROW(manager.flush(block_name, "/tmp", "/test"));
  REQUIRE_NOTHROW(manager.reset(block_name));
  REQUIRE(manager.storage_size(block_name) == 0);
  REQUIRE_NOTHROW(manager.load(block_name, "/tmp", "/test"));

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}