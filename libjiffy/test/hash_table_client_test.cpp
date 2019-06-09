#include <catch.hpp>
#include <thrift/transport/TTransportException.h>
#include <thread>
#include "test_utils.h"
#include "jiffy/utils/logger.h"
#include "jiffy/directory/fs/directory_tree.h"
#include "jiffy/directory/fs/directory_server.h"
#include "jiffy/storage/manager/storage_management_server.h"
#include "jiffy/storage/manager/storage_management_client.h"
#include "jiffy/storage/manager/storage_manager.h"
#include "jiffy/storage/hashtable/hash_table_partition.h"
#include "jiffy/storage/service/block_server.h"
#include "jiffy/storage/hashtable/hash_slot.h"
#include "jiffy/storage/client/hash_table_client.h"
#include "jiffy/auto_scaling/auto_scaling_client.h"
#include "jiffy/auto_scaling/auto_scaling_server.h"

using namespace ::jiffy::storage;
using namespace ::jiffy::directory;
using namespace ::jiffy::auto_scaling;
using namespace ::apache::thrift::transport;
using namespace ::jiffy::utils;

#define NUM_BLOCKS 3
#define HOST "127.0.0.1"
#define DIRECTORY_SERVICE_PORT 9090
#define STORAGE_SERVICE_PORT 9091
#define STORAGE_MANAGEMENT_PORT 9092
#define AUTO_SCALING_SERVICE_PORT 9095

TEST_CASE("hash_table_client_put_get_test", "[put][get]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(NUM_BLOCKS, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_hash_table_blocks(block_names, 134217728, 0, 1);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto tree = std::make_shared<directory_tree>(alloc, sm);
  data_status status = tree->create("/sandbox/file.txt", "hashtable", "/tmp", NUM_BLOCKS, 1, 0, 0,
      {"0_21845", "21845_43690", "43690_65536"}, {"regular", "regular", "regular"});

  hash_table_client client(tree, "/sandbox/file.txt", status);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.put(std::to_string(i), std::to_string(i)) == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.get(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE(client.get(std::to_string(i)) == "!key_not_found");
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

TEST_CASE("hash_table_client_put_update_get_test", "[put][update][get]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(NUM_BLOCKS, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_hash_table_blocks(block_names, 134217728, 0, 1);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto tree = std::make_shared<directory_tree>(alloc, sm);

  data_status status;
  REQUIRE_NOTHROW(status = tree->create("/sandbox/file.txt", "hashtable", "/tmp", NUM_BLOCKS, 1, 0, 0,
      {"0_21845", "21845_43690", "43690_65536"}, {"regular", "regular", "regular"}));

  hash_table_client client(tree, "/sandbox/file.txt", status);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.put(std::to_string(i), std::to_string(i)) == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.get(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.update(std::to_string(i), std::to_string(i + 1000)) == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE(client.update(std::to_string(i), std::to_string(i + 1000)) == "!key_not_found");
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


TEST_CASE("hash_table_client_put_remove_get_test", "[put][remove][get]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(NUM_BLOCKS, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_hash_table_blocks(block_names, 134217728, 0, 1);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto as_server = auto_scaling_server::create(HOST, DIRECTORY_SERVICE_PORT, HOST, AUTO_SCALING_SERVICE_PORT);
  std::thread auto_scaling_thread([&as_server]{as_server->serve(); });
  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto tree = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(tree, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  data_status status;
  REQUIRE_NOTHROW(status = tree->create("/sandbox/file.txt", "hashtable", "/tmp", NUM_BLOCKS, 1, 0, 0,
      {"0_21845", "21845_43690", "43690_65536"}, {"regular", "regular", "regular"}));

  hash_table_client client(tree, "/sandbox/file.txt", status);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.put(std::to_string(i), std::to_string(i)) == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.get(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.remove(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.get(std::to_string(i)) == "!key_not_found");
  }

  as_server->stop();
  if(auto_scaling_thread.joinable()) {
    auto_scaling_thread.join();
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


TEST_CASE("hash_table_client_pipelined_ops_test", "[put][update][remove][get]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(NUM_BLOCKS, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_hash_table_blocks(block_names, 134217728, 0, 1);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto as_server = auto_scaling_server::create(HOST, DIRECTORY_SERVICE_PORT, HOST, AUTO_SCALING_SERVICE_PORT);
  std::thread auto_scaling_thread([&as_server]{as_server->serve(); });
  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto tree = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(tree, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  data_status status;
  REQUIRE_NOTHROW(status = tree->create("/sandbox/file.txt", "hashtable", "/tmp", NUM_BLOCKS, 1, 0, 0,
      {"0_21845", "21845_43690", "43690_65536"}, {"regular", "regular", "regular"}));

  hash_table_client client(tree, "/sandbox/file.txt", status);
  std::vector<std::string> key_value_batch;
  for (size_t i = 0; i < 1000; i++) {
    key_value_batch.push_back(std::to_string(i));
    key_value_batch.push_back(std::to_string(i));
  }
  auto res = client.put(key_value_batch);
  for (size_t i = 0; i < 1000; i++) {
    REQUIRE(res[i] == "!ok");
  }

  std::vector<std::string> keys;
  for (size_t i = 0; i < 1000; i++) {
    keys.push_back(std::to_string(i));
  }
  auto res2 = client.get(keys);
  for (size_t i = 0; i < 1000; i++) {
    REQUIRE(res2[i] == std::to_string(i));
  }

  std::vector<std::string> keys2;
  for (size_t i = 1000; i < 2000; i++) {
    keys2.push_back(std::to_string(i));
  }
  auto res3 = client.get(keys2);
  for (size_t i = 0; i < 1000; i++) {
    REQUIRE(res3[i] == "!key_not_found");
  }

  std::vector<std::string> key_value_batch2;
  for (size_t i = 0; i < 1000; i++) {
    key_value_batch2.push_back(std::to_string(i));
    key_value_batch2.push_back(std::to_string(i + 1000));
  }
  auto res4 = client.update(key_value_batch2);
  for (size_t i = 0; i < 1000; i++) {
    REQUIRE(res4[i] == std::to_string(i));
  }

  std::vector<std::string> key_value_batch3;
  for (size_t i = 1000; i < 2000; i++) {
    key_value_batch3.push_back(std::to_string(i));
    key_value_batch3.push_back(std::to_string(i + 1000));
  }
  auto res5 = client.update(key_value_batch3);
  for (size_t i = 0; i < 1000; i++) {
    REQUIRE(res5[i] == "!key_not_found");
  }

  std::vector<std::string> keys3;
  for (size_t i = 0; i < 1000; i++) {
    keys3.push_back(std::to_string(i));
  }
  auto res6 = client.remove(keys3);
  for (size_t i = 0; i < 1000; i++) {
    REQUIRE(res6[i] == std::to_string(i + 1000));
  }

  std::vector<std::string> keys4;
  for (size_t i = 1000; i < 2000; i++) {
    keys4.push_back(std::to_string(i));
  }
  auto res7 = client.remove(keys4);
  for (size_t i = 0; i < 1000; i++) {
    REQUIRE(res7[i] == "!key_not_found");
  }

  std::vector<std::string> keys5;
  for (size_t i = 1000; i < 2000; i++) {
    keys5.push_back(std::to_string(i));
  }
  auto res8 = client.get(keys5);
  for (size_t i = 0; i < 1000; i++) {
    REQUIRE(res8[i] == "!key_not_found");
  }
  // Busy wait until number of blocks decreases
  while (tree->dstatus("/sandbox/file.txt").data_blocks().size() >= 2);

  as_server->stop();
  if(auto_scaling_thread.joinable()) {
    auto_scaling_thread.join();
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

