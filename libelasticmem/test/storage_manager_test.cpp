#include "catch.hpp"

#include <thrift/transport/TTransportException.h>
#include <thread>
#include "../src/storage/manager/storage_management_server.h"
#include "../src/storage/manager/storage_management_client.h"
#include "../src/storage/manager/storage_manager.h"
#include "../src/storage/kv/kv_block.h"
#include "test_utils.h"
#include "../src/storage/service/block_server.h"
#include "../src/storage/kv/hash_slot.h"

using namespace ::elasticmem::storage;
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
                                      0,
                                      128,
                                      {block_name},
                                      chain_role::singleton,
                                      "nil"));
  auto block = blocks.front();
  REQUIRE(block->name() == block_name);
  REQUIRE(block->path() == "/path/to/data");
  REQUIRE(block->slot_range() == std::make_pair(0, 128));
  REQUIRE(block->chain()[0] == block_name);
  REQUIRE(block->role() == chain_role::singleton);

  REQUIRE(manager.path(block_name) == "/path/to/data");
  REQUIRE(manager.slot_range(block_name) == std::make_pair(0, 128));

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
    REQUIRE_NOTHROW(std::dynamic_pointer_cast<kv_block>(blocks[0])->put(std::to_string(i), std::to_string(i)));
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

TEST_CASE("manager_flush_load_test", "[put][flush][reset][load][get]") {
  static auto blocks = test_utils::init_kv_blocks(NUM_BLOCKS, SERVICE_PORT, MANAGEMENT_PORT, 0);
  auto server = storage_management_server::create(blocks, HOST, MANAGEMENT_PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, MANAGEMENT_PORT);

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(std::dynamic_pointer_cast<kv_block>(blocks[0])->put(std::to_string(i), std::to_string(i)));
  }

  storage_manager manager;
  auto block_name = block_name_parser::make(HOST, SERVICE_PORT, MANAGEMENT_PORT, 0, 0, 0);
  REQUIRE_NOTHROW(manager.flush(block_name, "/tmp", "/test"));
  REQUIRE_NOTHROW(manager.reset(block_name));
  REQUIRE(manager.storage_size(block_name) == 0);
  REQUIRE_NOTHROW(manager.load(block_name, "/tmp", "/test"));

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("manager_set_state", "[setup_and_set_importing][set_exporting][export_slots][set_regular]") {
  static auto blocks = test_utils::init_kv_blocks(2, SERVICE_PORT, MANAGEMENT_PORT, 0);
  blocks[0]->slot_range(0, -1);
  blocks[1]->slot_range(0, -1);

  auto storage_server = block_server::create(blocks, HOST, SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, MANAGEMENT_PORT);

  auto block_name1 = block_name_parser::make(HOST, SERVICE_PORT, MANAGEMENT_PORT, 0, 0, 0);
  auto block_name2 = block_name_parser::make(HOST, SERVICE_PORT, MANAGEMENT_PORT, 0, 0, 1);

  storage_manager manager;
  REQUIRE_NOTHROW(manager.setup_block(block_name1,
                                      "/path/to/data",
                                      0,
                                      65536,
                                      {block_name1},
                                      chain_role::singleton,
                                      "nil"));

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(std::dynamic_pointer_cast<kv_block>(blocks[0])->put(std::to_string(i), std::to_string(i)));
  }

  REQUIRE_NOTHROW(manager.set_exporting(block_name1, {block_name2}, 32768, 65536));
  REQUIRE(blocks[0]->export_target()[0] == block_name2);
  REQUIRE(blocks[0]->export_slot_range() == std::make_pair(32768, 65536));
  REQUIRE(blocks[0]->state() == block_state::exporting);

  for (std::size_t i = 0; i < 1000; i++) {
    REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[0])->get(std::to_string(i)) == std::to_string(i));
  }

  REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[0])->get(std::to_string(1000)) == "!exporting!" + block_name2);
  REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[0])->get(std::to_string(1008)) == "!key_not_found");

  REQUIRE_NOTHROW(manager.setup_and_set_importing(block_name2,
                                                  "/path/to/data",
                                                  32768,
                                                  65536,
                                                  {block_name2},
                                                  chain_role::singleton,
                                                  "nil"));
  REQUIRE(blocks[1]->name() == block_name2);
  REQUIRE(blocks[1]->path() == "/path/to/data");
  REQUIRE(blocks[1]->slot_range() == std::make_pair(0, -1));
  REQUIRE(blocks[1]->import_slot_range() == std::make_pair(32768, 65536));
  REQUIRE(blocks[1]->chain()[0] == block_name2);
  REQUIRE(blocks[1]->role() == chain_role::singleton);
  REQUIRE(blocks[1]->state() == block_state::importing);

  REQUIRE_NOTHROW(manager.export_slots(block_name1));

  for (std::size_t i = 0; i < 1000; i++) {
    std::string key = std::to_string(i);
    auto h = hash_slot::get(key);
    if (h >= 0 && h < 32768) {
      REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[0])->get(key) == key);
      REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[1])->get(key) == "!block_moved");
    } else {
      REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[0])->get(key) == "!exporting!" + block_name2);
      REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[1])->get(key, true) == key);
    }
  }

  REQUIRE_NOTHROW(manager.set_regular(block_name1, 0, 32767));
  REQUIRE_NOTHROW(manager.set_regular(block_name2, 32768, 65536));

  for (std::size_t i = 0; i < 1000; i++) {
    std::string key = std::to_string(i);
    auto h = hash_slot::get(key);
    if (h >= 0 && h < 32768) {
      REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[0])->get(key) == key);
      REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[1])->get(key) == "!block_moved");
    } else {
      REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[0])->get(key) == "!block_moved");
      REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[1])->get(key) == key);
    }
  }

  storage_server->stop();
  if (storage_serve_thread.joinable()) {
    storage_serve_thread.join();
  }

  mgmt_server->stop();
  if (mgmt_serve_thread.joinable()) {
    mgmt_serve_thread.join();
  }
}