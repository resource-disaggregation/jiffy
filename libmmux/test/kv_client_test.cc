#include <catch.hpp>
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
#include "../src/mmux/storage/client/kv_client.h"

using namespace ::mmux::storage;
using namespace ::mmux::directory;
using namespace ::apache::thrift::transport;

#define NUM_BLOCKS 3
#define HOST "127.0.0.1"
#define DIRECTORY_SERVICE_PORT 9090
#define STORAGE_SERVICE_PORT 9091
#define STORAGE_MANAGEMENT_PORT 9092

TEST_CASE("kv_client_put_get_test", "[put][get]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(NUM_BLOCKS, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT, 0, 0);
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

  data_status status = tree->create("/sandbox/file.txt", "/tmp", NUM_BLOCKS, 1);

  kv_client client(tree, "/sandbox/file.txt", status);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(client.put(std::to_string(i), std::to_string(i)));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.get(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE_THROWS_AS(client.get(std::to_string(i)), std::out_of_range);
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

TEST_CASE("kv_client_put_update_get_test", "[put][update][get]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(NUM_BLOCKS, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT, 0, 0);
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

  data_status status;
  REQUIRE_NOTHROW(status = tree->create("/sandbox/file.txt", "/tmp", NUM_BLOCKS, 1));

  kv_client client(tree, "/sandbox/file.txt", status);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(client.put(std::to_string(i), std::to_string(i)));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.get(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.update(std::to_string(i), std::to_string(i + 1000)) == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE_THROWS_AS(client.update(std::to_string(i), std::to_string(i + 1000)), std::out_of_range);
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.get(std::to_string(i)) == std::to_string(i + 1000));
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

TEST_CASE("kv_client_put_remove_get_test", "[put][remove][get]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(NUM_BLOCKS, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT, 0, 0);
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

  data_status status;
  REQUIRE_NOTHROW(status = tree->create("/sandbox/file.txt", "/tmp", NUM_BLOCKS, 1));

  kv_client client(tree, "/sandbox/file.txt", status);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(client.put(std::to_string(i), std::to_string(i)));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.get(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.remove(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_THROWS_AS(client.get(std::to_string(i)), std::out_of_range);
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