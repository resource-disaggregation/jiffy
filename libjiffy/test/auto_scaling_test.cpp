#include "catch.hpp"

#include <thrift/transport/TTransportException.h>
#include <thread>
#include "jiffy/storage/manager/storage_management_server.h"
#include "jiffy/storage/manager/storage_management_client.h"
#include "jiffy/storage/manager/storage_manager.h"
#include "jiffy/storage/hashtable/hash_table_partition.h"
#include "jiffy/storage/msgqueue/msg_queue_partition.h"
#include "jiffy/storage/hashtable/hash_slot.h"
#include "test_utils.h"
#include "jiffy/storage/service/block_server.h"
#include "jiffy/directory/fs/directory_tree.h"
#include "jiffy/directory/fs/directory_server.h"
#include "jiffy/storage/hashtable/hash_table_ops.h"
#include "jiffy/storage/msgqueue/msg_queue_ops.h"
#include "jiffy/storage/client/msg_queue_client.h"
#include "jiffy/client/jiffy_client.h"
#include "jiffy/directory/fs/sync_worker.h"
#include "jiffy/directory/lease/lease_expiry_worker.h"
#include "jiffy/directory/lease/lease_server.h"

using namespace jiffy::client;
using namespace ::jiffy::storage;
using namespace ::jiffy::directory;
using namespace ::apache::thrift::transport;

#define NUM_BLOCKS 1
#define HOST "127.0.0.1"
#define DIRECTORY_SERVICE_PORT 9090
#define STORAGE_SERVICE_PORT 9091
#define STORAGE_MANAGEMENT_PORT 9092

/*
TEST_CASE("hash_table_auto_scale_up_test", "[directory_service][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(2, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_hash_table_blocks(block_names, 119150);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
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

  REQUIRE_NOTHROW(t->create("/sandbox/scale_up.txt", "hashtable", "/tmp", 1, 1, 0, perms::all(), {"0_65536"}, {"regular"}, {}));

  // Write data until auto scaling is triggered
  for (std::size_t i = 0; i < 1000; ++i) {
    std::vector<std::string> result;
    REQUIRE_NOTHROW(std::dynamic_pointer_cast<hash_table_partition>(blocks[0]->impl())->run_command(result, hash_table_cmd_id::ht_put,{std::to_string(i), std::to_string(i)}));
    REQUIRE(result[0] == "!ok");
  }

  // Busy wait until number of blocks increases
  while (t->dstatus("/sandbox/scale_up.txt").data_blocks().size() == 1);

  for (std::size_t i = 0; i < 1000; i++) {
    std::string key = std::to_string(i);
    auto h = hash_slot::get(key);
    if (h >= 0 && h < 32768) {
      REQUIRE(std::dynamic_pointer_cast<hash_table_partition>(blocks[0]->impl())->get(key) == key);
      REQUIRE(std::dynamic_pointer_cast<hash_table_partition>(blocks[1]->impl())->get(key) == "!block_moved");
    } else {
      REQUIRE(std::dynamic_pointer_cast<hash_table_partition>(blocks[0]->impl())->get(key) == "!block_moved");
      REQUIRE(std::dynamic_pointer_cast<hash_table_partition>(blocks[1]->impl())->get(key) == key);
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

TEST_CASE("hash_table_auto_scale_down_test", "[directory_service][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(2, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_hash_table_blocks(block_names);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
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

  REQUIRE_NOTHROW(t->create("/sandbox/scale_down.txt", "hashtable", "/tmp", 2, 1, 0, perms::all(), {"0_32768", "32768_65536"}, {"regular", "regular"}, {}));

  // Write some initial data
  for (std::size_t i = 0; i < 1000; ++i) {
    std::string key = std::to_string(i);
    auto h = hash_slot::get(key);
    if (std::dynamic_pointer_cast<hash_table_partition>(blocks[0]->impl())->in_slot_range(h)) {
      REQUIRE(std::dynamic_pointer_cast<hash_table_partition>(blocks[0]->impl())->put(key, key) == "!ok");
    } else {
      REQUIRE(std::dynamic_pointer_cast<hash_table_partition>(blocks[0]->impl())->put(key, key) == "!ok");
    }
  }

  // A single remove should trigger scale down
  std::vector<std::string> result;
  REQUIRE_NOTHROW(std::dynamic_pointer_cast<hash_table_partition>(blocks[0]->impl())->run_command(result,
                                                                              hash_table_cmd_id::ht_remove,
                                                                              {std::to_string(0)}));
  REQUIRE(result[0] == "0");

  // Busy wait until number of blocks decreases
  while (t->dstatus("/sandbox/scale_down.txt").data_blocks().size() == 2);

  for (std::size_t i = 1; i < 1000; i++) {
    std::string key = std::to_string(i);
    REQUIRE(std::dynamic_pointer_cast<hash_table_partition>(blocks[0]->impl())->get(key) == "!block_moved");
    REQUIRE(std::dynamic_pointer_cast<hash_table_partition>(blocks[0]->impl())->get(key) == key);
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

*/

TEST_CASE("msg_queue_auto_scale_test", "[directory_service][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(100, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_msg_queue_blocks(block_names, 512, 0, 0.7);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread1([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  data_status status = t->create("/sandbox/scale_up.txt", "msgqueue", "/tmp", 1, 4, 0, perms::all(), {"0"}, {"regular"}, {});
  msg_queue_client client(t, "/sandbox/scale_up.txt", status);

  // Write data until auto scaling is triggered
  for (std::size_t i = 0; i < 158; ++i) {
    REQUIRE(client.send(std::to_string(i)) == "!ok");
  }
  for (std::size_t i = 0; i < 158; ++i) {
    REQUIRE(client.read() == std::to_string(i));
  }

  // Busy wait until number of blocks increases
  while (t->dstatus("/sandbox/scale_up.txt").data_blocks().size() == 1);

  storage_server->stop();
  if (storage_serve_thread1.joinable()) {
    storage_serve_thread1.join();
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