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

TEST_CASE("manager_setup_chain_test", "[setup_block][path][slot_range]") {
  static auto blocks = test_utils::init_hash_table_blocks(NUM_BLOCKS, SERVICE_PORT, MANAGEMENT_PORT);
  auto server = storage_management_server::create(blocks, HOST, MANAGEMENT_PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, MANAGEMENT_PORT);

  storage_manager manager;
  auto block_id = block_id_parser::make(HOST, SERVICE_PORT, MANAGEMENT_PORT, 0);
  REQUIRE_NOTHROW(manager.setup_chain(block_id, "/path/to/data", {block_id}, chain_role::singleton, "nil"));
  auto block = blocks.front();
  REQUIRE(block->id() == block_id);
  REQUIRE(block->impl()->path() == "/path/to/data");
  REQUIRE(block->impl()->chain()[0] == block_id);
  REQUIRE(block->impl()->role() == chain_role::singleton);

  REQUIRE(manager.path(block_id) == "/path/to/data");

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("manager_storage_size_test", "[storage_size][storage_size][storage_capacity][reset]") {
  static auto blocks = test_utils::init_hash_table_blocks(NUM_BLOCKS, SERVICE_PORT, MANAGEMENT_PORT);
  auto server = storage_management_server::create(blocks, HOST, MANAGEMENT_PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, MANAGEMENT_PORT);

  for (std::size_t i = 0; i < 1000; ++i) {
    std::vector<std::string> resp;
    REQUIRE_NOTHROW(std::dynamic_pointer_cast<hash_table_partition>(blocks[0]->impl())->put(resp,
                                                                                            {"put",
                                                                                             std::to_string(i),
                                                                                             std::to_string(i)}));
  }

  storage_manager manager;
  auto block_name = block_id_parser::make(HOST, SERVICE_PORT, MANAGEMENT_PORT, 0);
  REQUIRE(manager.storage_size(block_name) <= manager.storage_capacity(block_name));

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("manager_sync_load_test", "[put][sync][reset][load][get]") {
  static auto blocks = test_utils::init_hash_table_blocks(NUM_BLOCKS, SERVICE_PORT, MANAGEMENT_PORT);
  auto server = storage_management_server::create(blocks, HOST, MANAGEMENT_PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, MANAGEMENT_PORT);

  for (std::size_t i = 0; i < 1000; ++i) {
    std::vector<std::string> resp;
    REQUIRE_NOTHROW(std::dynamic_pointer_cast<hash_table_partition>(blocks[0]->impl())->put(resp,
                                                                                            {"put",
                                                                                             std::to_string(i),
                                                                                             std::to_string(i)}));
  }

  storage_manager manager;
  auto block_name = block_id_parser::make(HOST, SERVICE_PORT, MANAGEMENT_PORT, 0);
  REQUIRE_NOTHROW(manager.sync(block_name, "local://tmp"));
  REQUIRE_NOTHROW(manager.destroy_partition(block_name));
  REQUIRE_NOTHROW(manager.create_partition(block_name, "hashtable", "0_65536", "regular", {}));
  REQUIRE_NOTHROW(manager.load(block_name, "local://tmp"));

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}
