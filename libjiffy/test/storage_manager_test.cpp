#include "catch.hpp"

#include <thrift/transport/TTransportException.h>
#include <thread>
#include "jiffy/storage/manager/storage_management_server.h"
#include "jiffy/storage/manager/storage_management_client.h"
#include "jiffy/storage/manager/storage_manager.h"
#include "jiffy/storage/hashtable/hash_table_partition.h"
#include "jiffy/storage/hashtable/hash_slot.h"
#include "jiffy/storage/service/block_server.h"
#include "test_utils.h"

using namespace ::jiffy::storage;
using namespace ::apache::thrift::transport;

#define NUM_BLOCKS 1
#define HOST "127.0.0.1"
#define SERVICE_PORT 9090
#define MANAGEMENT_PORT 9091

TEST_CASE("manager_setup_block_test", "[setup_block][path][slot_range]") {
  static auto blocks = test_utils::init_kv_blocks(NUM_BLOCKS, SERVICE_PORT, MANAGEMENT_PORT, 0);
  auto server = storage_management_server::create(blocks, HOST, MANAGEMENT_PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, MANAGEMENT_PORT);

  storage_manager manager;
  auto block_name = block_name_parser::make(HOST, SERVICE_PORT, MANAGEMENT_PORT, 0, 0, 0);
  REQUIRE_NOTHROW(manager.setup_block(block_name,
                                      "/path/to/data",
                                      "storage",
                                      "0_65536",
                                      "regular",
                                      {block_name},
                                      chain_role::singleton,
                                      "nil"));
  auto block = blocks.front();
  REQUIRE(block->name() == block_name);
  REQUIRE(block->path() == "/path/to/data");
  REQUIRE(block->chain()[0] == block_name);
  REQUIRE(block->role() == chain_role::singleton);

  REQUIRE(manager.path(block_name) == "/path/to/data");

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("manager_storage_size_test", "[storage_size][storage_size][storage_capacity][reset]") {
  static auto blocks = test_utils::init_kv_blocks(NUM_BLOCKS, SERVICE_PORT, MANAGEMENT_PORT, 0);
  auto server = storage_management_server::create(blocks, HOST, MANAGEMENT_PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, MANAGEMENT_PORT);

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(std::dynamic_pointer_cast<hash_table_partition>(blocks[0])->put(std::to_string(i),
        std::to_string(i)));
  }

  storage_manager manager;
  auto block_name = block_name_parser::make(HOST, SERVICE_PORT, MANAGEMENT_PORT, 0, 0, 0);
  REQUIRE(manager.storage_size(block_name) == 5780);
  REQUIRE(manager.storage_size(block_name) <= manager.storage_capacity(block_name));
  REQUIRE_NOTHROW(manager.reset(block_name));
  REQUIRE(manager.storage_size(block_name) == 0);

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("manager_sync_load_test", "[put][sync][reset][load][get]") {
  static auto blocks = test_utils::init_kv_blocks(NUM_BLOCKS, SERVICE_PORT, MANAGEMENT_PORT, 0);
  auto server = storage_management_server::create(blocks, HOST, MANAGEMENT_PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, MANAGEMENT_PORT);

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(std::dynamic_pointer_cast<hash_table_partition>(blocks[0])->put(std::to_string(i), std::to_string(i)));
  }

  storage_manager manager;
  auto block_name = block_name_parser::make(HOST, SERVICE_PORT, MANAGEMENT_PORT, 0, 0, 0);
  REQUIRE_NOTHROW(manager.sync(block_name, "local://tmp"));
  REQUIRE_NOTHROW(manager.reset(block_name));
  REQUIRE(manager.storage_size(block_name) == 0);
  REQUIRE_NOTHROW(manager.load(block_name, "local://tmp"));

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}
