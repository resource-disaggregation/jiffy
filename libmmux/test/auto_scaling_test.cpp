#include "catch.hpp"

#include <thrift/transport/TTransportException.h>
#include <thread>
#include "../src/mmux/storage/manager/storage_management_server.h"
#include "../src/mmux/storage/manager/storage_management_client.h"
#include "../src/mmux/storage/manager/storage_manager.h"
#include "../src/mmux/storage/kv/kv_block.h"
#include "test_utils.h"
#include "../src/mmux/storage/service/block_server.h"
#include "../src/mmux/storage/kv/hash_slot.h"
#include "../src/mmux/directory/fs/directory_tree.h"
#include "../src/mmux/directory/fs/directory_server.h"

using namespace ::mmux::storage;
using namespace ::mmux::directory;
using namespace ::apache::thrift::transport;

#define NUM_BLOCKS 1
#define HOST "127.0.0.1"
#define DIRECTORY_SERVICE_PORT 9090
#define STORAGE_SERVICE_PORT 9091
#define STORAGE_MANAGEMENT_PORT 9092

TEST_CASE("scale_add_block_test", "[directory_tree][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(2, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT, 0, 0);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_kv_blocks(block_names);

  auto storage_server = block_server::create(blocks, HOST, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto tree = std::make_shared<directory_tree>(alloc, sm);

  REQUIRE_NOTHROW(tree->create("/sandbox/file.txt", "/tmp", 1, 1));

  // Write some data into it
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(std::dynamic_pointer_cast<kv_block>(blocks[0])->put(std::to_string(i), std::to_string(i)));
  }

  REQUIRE_NOTHROW(tree->add_block_to_file("/sandbox/file.txt"));
  // Busy wait until number of blocks increases
  while (tree->dstatus("/sandbox/file.txt").data_blocks().size() == 1);

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

TEST_CASE("scale_block_split_test", "[directory_tree][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(2, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT, 0, 0);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_kv_blocks(block_names);

  auto storage_server = block_server::create(blocks, HOST, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto tree = std::make_shared<directory_tree>(alloc, sm);

  REQUIRE_NOTHROW(tree->create("/sandbox/file.txt", "/tmp", 1, 1));

  // Write some data into it
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(std::dynamic_pointer_cast<kv_block>(blocks[0])->put(std::to_string(i), std::to_string(i)));
  }

  REQUIRE_NOTHROW(tree->split_slot_range("/sandbox/file.txt", 0, 65536));
  // Busy wait until number of blocks increases
  while (tree->dstatus("/sandbox/file.txt").data_blocks().size() == 1);

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

TEST_CASE("scale_block_merge_test", "[directory_tree][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(2, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT, 0, 0);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_kv_blocks(block_names);

  auto storage_server = block_server::create(blocks, HOST, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto tree = std::make_shared<directory_tree>(alloc, sm);

  REQUIRE_NOTHROW(tree->create("/sandbox/file.txt", "/tmp", 2, 1));

  // Write some data into it
  for (std::size_t i = 0; i < 1000; ++i) {
    std::string key = std::to_string(i);
    auto h = hash_slot::get(key);
    if (blocks[0]->in_slot_range(h)) {
      REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[0])->put(key, key) == "!ok");
    } else {
      REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[1])->put(key, key) == "!ok");
    }
  }

  REQUIRE_NOTHROW(tree->merge_slot_range("/sandbox/file.txt", 0, 32767));
  // Busy wait until number of blocks decreases
  while (tree->dstatus("/sandbox/file.txt").data_blocks().size() == 2);

  for (std::size_t i = 0; i < 1000; i++) {
    std::string key = std::to_string(i);
    REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[0])->get(key) == "!block_moved");
    REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[1])->get(key) == key);
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

TEST_CASE("auto_scale_up_test", "[directory_service][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(2, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT, 0, 0);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_kv_blocks(block_names, 7705);

  auto storage_server = block_server::create(blocks, HOST, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  REQUIRE_NOTHROW(t->create("/sandbox/file.txt", "/tmp", 1, 1));

  // Write data until auto scaling is triggered
  for (std::size_t i = 0; i < 1000; ++i) {
    std::vector<std::string> result;
    REQUIRE_NOTHROW(std::dynamic_pointer_cast<kv_block>(blocks[0])->run_command(result,
                                                                                kv_op_id::put,
                                                                                {std::to_string(i),
                                                                                 std::to_string(i)}));
    REQUIRE(result[0] == "!ok");
  }

  // Busy wait until number of blocks increases
  while (t->dstatus("/sandbox/file.txt").data_blocks().size() == 1);

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

  dir_server->stop();
  if (dir_serve_thread.joinable()) {
    dir_serve_thread.join();
  }
}

TEST_CASE("auto_scale_down_test", "[directory_service][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(2, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT, 0, 0);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_kv_blocks(block_names);

  auto storage_server = block_server::create(blocks, HOST, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  REQUIRE_NOTHROW(t->create("/sandbox/file.txt", "/tmp", 2, 1));

  // Write some initial data
  for (std::size_t i = 0; i < 1000; ++i) {
    std::string key = std::to_string(i);
    auto h = hash_slot::get(key);
    if (blocks[0]->in_slot_range(h)) {
      REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[0])->put(key, key) == "!ok");
    } else {
      REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[1])->put(key, key) == "!ok");
    }
  }

  // A single remove should trigger scale down
  std::vector<std::string> result;
  REQUIRE_NOTHROW(std::dynamic_pointer_cast<kv_block>(blocks[0])->run_command(result,
                                                                              kv_op_id::remove,
                                                                              {std::to_string(0)}));
  REQUIRE(result[0] == "0");

  // Busy wait until number of blocks decreases
  while (t->dstatus("/sandbox/file.txt").data_blocks().size() == 2);

  for (std::size_t i = 1; i < 1000; i++) {
    std::string key = std::to_string(i);
    REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[0])->get(key) == "!block_moved");
    REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[1])->get(key) == key);
  }

  storage_server->stop();
  if (storage_serve_thread.joinable()) {
    storage_serve_thread.join();
  }

  mgmt_server->stop();
  if (mgmt_serve_thread.joinable()) {
    mgmt_serve_thread.join();
  }

  dir_server->stop();
  if (dir_serve_thread.joinable()) {
    dir_serve_thread.join();
  }
}