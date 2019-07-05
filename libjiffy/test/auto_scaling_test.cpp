#include "catch.hpp"

#include <thrift/transport/TTransportException.h>
#include <thread>
#include <chrono>
#include "jiffy/storage/manager/storage_management_server.h"
#include "jiffy/storage/manager/storage_management_client.h"
#include "jiffy/storage/manager/storage_manager.h"
#include "jiffy/storage/hashtable/hash_table_partition.h"
#include "jiffy/storage/file/file_partition.h"
#include "jiffy/storage/fifoqueue/fifo_queue_partition.h"
#include "jiffy/storage/hashtable/hash_slot.h"
#include "test_utils.h"
#include "jiffy/storage/service/block_server.h"
#include "jiffy/directory/fs/directory_tree.h"
#include "jiffy/directory/fs/directory_server.h"
#include "jiffy/storage/hashtable/hash_table_ops.h"
#include "jiffy/storage/file/file_ops.h"
#include "jiffy/storage/client/file_client.h"
#include "jiffy/storage/client/hash_table_client.h"
#include "jiffy/storage/client/fifo_queue_client.h"
#include "jiffy/client/jiffy_client.h"
#include "jiffy/directory/fs/sync_worker.h"
#include "jiffy/directory/lease/lease_expiry_worker.h"
#include "jiffy/directory/lease/lease_server.h"
#include "jiffy/auto_scaling/auto_scaling_client.h"
#include "jiffy/auto_scaling/auto_scaling_server.h"
#include "jiffy/utils/rand_utils.h"

using namespace jiffy::client;
using namespace ::jiffy::storage;
using namespace ::jiffy::directory;
using namespace ::jiffy::auto_scaling;
using namespace ::apache::thrift::transport;

#define NUM_BLOCKS 1
#define HOST "127.0.0.1"
#define DIRECTORY_SERVICE_PORT 9090
#define STORAGE_SERVICE_PORT 9091
#define STORAGE_MANAGEMENT_PORT 9092
#define AUTO_SCALING_SERVICE_PORT 9095

TEST_CASE("hash_table_auto_scale_up_test", "[directory_service][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(100, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_hash_table_blocks(block_names, 2200);
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
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  data_status status = t->create("/sandbox/scale_up.txt", "hashtable", "/tmp", 3, 3, 0, perms::all(), {"0_32768", "32768_49152", "49152_65536"}, {"regular", "regular", "regular"}, {});
  hash_table_client client(t, "/sandbox/scale_up.txt", status);
  // Write data until auto scaling is triggered
  for (std::size_t i = 0; i < 2000; ++i) {
    REQUIRE_NOTHROW(client.put(std::to_string(i), std::to_string(i)));
    REQUIRE_NOTHROW(client.update(std::to_string(i), std::to_string(2000 - i)));
  }

  // Busy wait until number of blocks increases
  while (t->dstatus("/sandbox/scale_up.txt").data_blocks().size() == 1);

  for (std::size_t i = 0; i < 2000; i++) {
    REQUIRE(client.get(std::to_string(i)) == std::to_string(2000 - i));
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



TEST_CASE("hash_table_auto_scale_down_test", "[directory_service][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(100, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_hash_table_blocks(block_names);

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
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  data_status status = t->create("/sandbox/scale_down.txt", "hashtable", "/tmp", 3, 5, 0, perms::all(), {"0_16384","16384_32768", "32768_65536"}, {"regular", "regular", "regular"}, {});
  hash_table_client client(t, "/sandbox/scale_down.txt", status);

  for(std::size_t i = 0; i <= 1000; ++i) {
    REQUIRE(client.put(std::to_string(i), std::to_string(i)) == "!ok");
  }
  for(std::size_t i = 0; i <= 1000; ++i) {
    REQUIRE_NOTHROW(client.update(std::to_string(i), std::to_string(1000 - i)));
  }
  // A single remove should trigger scale down
  std::vector<std::string> result;
  REQUIRE_NOTHROW(client.remove(std::to_string(0)));
  REQUIRE_NOTHROW(client.update(std::to_string(0), std::string(102400, 'x')));
  REQUIRE_NOTHROW(client.remove(std::to_string(600)));
  REQUIRE_NOTHROW(client.update(std::to_string(600), std::string(102400, 'x')));
  REQUIRE_NOTHROW(client.remove(std::to_string(1000)));
  REQUIRE_NOTHROW(client.update(std::to_string(1000), std::string(102400, 'x')));

  // Busy wait until number of blocks decreases
  while (t->dstatus("/sandbox/scale_down.txt").data_blocks().size() >= 2);

  for (std::size_t i = 1; i < 1000; i++) {
    std::string key = std::to_string(i);
    std::vector<std::string> ret;
    REQUIRE_NOTHROW(blocks[0]->impl()->run_command(ret, 0, {}));
    REQUIRE(ret.front() == "!block_moved");
    REQUIRE_NOTHROW(blocks[2]->impl()->run_command(ret, 0, {}));
    REQUIRE(ret.front() == "!block_moved");
  }
  for (std::size_t i = 1; i < 1000 && i != 600; i++) {
    REQUIRE(client.get(std::to_string(i)) == std::to_string(1000 - i));
  }

  for(std::size_t i = 1000; i < 4000; i++) {
    REQUIRE(client.put(std::to_string(i), std::string(102400, 'x')) == "!ok");
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


TEST_CASE("hash_table_auto_scale_mix_test", "[directory_service][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(500, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_hash_table_blocks(block_names);

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
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  data_status status = t->create("/sandbox/scale_down.txt", "hashtable", "/tmp", 3, 5, 0, perms::all(), {"0_16384","16384_32768", "32768_65536"}, {"regular", "regular", "regular"}, {});
  hash_table_client client(t, "/sandbox/scale_down.txt", status);
  std::vector<int> remain_keys;
  std::size_t iter = 15000;
  const std::size_t max_key = 100;
  const std::size_t string_size = 102400;
  int bitmap[max_key] = { 0 };
  for(std::size_t i = 0; i < iter; i++) {
    std::size_t j = rand_utils::rand_uint32(0, 3);
    std::string ret;
    std::size_t key;
    switch(j) {
      case 0:
	if(i % 2) {
          key = rand_utils::rand_uint32(0, max_key - 1);
          REQUIRE_NOTHROW(ret = client.put(std::to_string(key), std::string(string_size, 'a')));
          if(ret == "!ok")
            bitmap[key] = 1;
	}
        break;
      case 1:
        key = rand_utils::rand_uint32(0, max_key - 1);
        REQUIRE_NOTHROW(ret = client.update(std::to_string(key), std::string(string_size, 'b')));
        if(ret != "!key_not_found")
          bitmap[key] = 2;
        break;
      case 2:
        key = rand_utils::rand_uint32(0, max_key - 1);
        REQUIRE_NOTHROW(client.get(std::to_string(key)));
        break;
      case 3:
        key = rand_utils::rand_uint32(0, max_key - 1);
        REQUIRE_NOTHROW(ret = client.remove(std::to_string(key)));
        if(ret != "!key_not_found")
          bitmap[key] = 0;
        break;
    }
  }

  for(std::size_t k = 0; k < max_key; k++) {
    if(bitmap[k] == 1)
      REQUIRE(client.get(std::to_string(k)) == std::string(string_size,'a'));
    else if(bitmap[k] == 2)
      REQUIRE(client.get(std::to_string(k)) == std::string(string_size, 'b'));
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

TEST_CASE("file_auto_scale_test", "[directory_service][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(21, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_file_blocks(block_names);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread1([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto as_server = auto_scaling_server::create(HOST, DIRECTORY_SERVICE_PORT, HOST, AUTO_SCALING_SERVICE_PORT);
  std::thread auto_scaling_thread([&as_server]{as_server->serve(); });
  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  data_status status = t->create("/sandbox/scale_up.txt", "file", "/tmp", 1, 1, 0, perms::all(), {"0"}, {"regular"}, {});
  file_client client(t, "/sandbox/scale_up.txt", status);

  // Write data until auto scaling is triggered
  for (std::size_t i = 0; i < 5000; ++i) {
    REQUIRE(client.write(std::string(512, (std::to_string(i)).c_str()[0])) == "!ok");
  }
  for(std::size_t i = 0; i < 6000; ++i) {
    REQUIRE(client.write(std::string(102400, (std::to_string(i)).c_str()[0])) == "!ok");
  }

  for (std::size_t i = 0; i < 5000; ++i) {
    REQUIRE(client.read(512) == std::string(512, (std::to_string(i)).c_str()[0]));
  }
  for (std::size_t i = 0; i < 6000; ++i) {
    REQUIRE(client.read(102400) == std::string(102400, (std::to_string(i)).c_str()[0]));
  }

  // Busy wait until number of blocks increases
  while (t->dstatus("/sandbox/scale_up.txt").data_blocks().size() == 1);

  as_server->stop();
  if(auto_scaling_thread.joinable()) {
    auto_scaling_thread.join();
  }

  storage_server->stop();
  if (storage_serve_thread1.joinable()) {
    storage_serve_thread1.join();
  }
  mgmt_server->stop();
  if (mgmt_serve_thread.joinable()) {
    mgmt_serve_thread.join();
  }
  as_server->stop();
  if(auto_scaling_thread.joinable()) {
    auto_scaling_thread.join();
  }
  dir_server->stop();
  if (dir_serve_thread.joinable()) {
    dir_serve_thread.join();
  }
}

TEST_CASE("file_auto_scale_chain_replica_test", "[directory_service][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(64, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_file_blocks(block_names);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread1([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto as_server = auto_scaling_server::create(HOST, DIRECTORY_SERVICE_PORT, HOST, AUTO_SCALING_SERVICE_PORT);
  std::thread auto_scaling_thread([&as_server]{as_server->serve(); });
  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  data_status status = t->create("/sandbox/scale_up.txt", "file", "/tmp", 1, 3, 0, perms::all(), {"0"}, {"regular"}, {});
  file_client client(t, "/sandbox/scale_up.txt", status);

  // Write data until auto scaling is triggered
  for (std::size_t i = 0; i < 5000; ++i) {
    REQUIRE(client.write(std::string(512, (std::to_string(i)).c_str()[0])) == "!ok");
  }
  for(std::size_t i = 0; i < 6000; ++i) {
    REQUIRE(client.write(std::string(102400, (std::to_string(i)).c_str()[0])) == "!ok");
  }

  for (std::size_t i = 0; i < 5000; ++i) {
    REQUIRE(client.read(512) == std::string(512, (std::to_string(i)).c_str()[0]));
  }
  for (std::size_t i = 0; i < 6000; ++i) {
    REQUIRE(client.read(102400) == std::string(102400, (std::to_string(i)).c_str()[0]));
  }

  // Busy wait until number of blocks increases
  while (t->dstatus("/sandbox/scale_up.txt").data_blocks().size() == 1);

  as_server->stop();
  if(auto_scaling_thread.joinable()) {
    auto_scaling_thread.join();
  }

  storage_server->stop();
  if (storage_serve_thread1.joinable()) {
    storage_serve_thread1.join();
  }
  mgmt_server->stop();
  if (mgmt_serve_thread.joinable()) {
    mgmt_serve_thread.join();
  }
  as_server->stop();
  if(auto_scaling_thread.joinable()) {
    auto_scaling_thread.join();
  }
  dir_server->stop();
  if (dir_serve_thread.joinable()) {
    dir_serve_thread.join();
  }
}

TEST_CASE("file_auto_scale_multi_blocks_test", "[directory_service][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(64, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_file_blocks(block_names);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread1([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto as_server = auto_scaling_server::create(HOST, DIRECTORY_SERVICE_PORT, HOST, AUTO_SCALING_SERVICE_PORT);
  std::thread auto_scaling_thread([&as_server]{as_server->serve(); });
  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);
  std::map<std::string, std::string> conf;
  conf.emplace("file.auto_scale", "false");
  data_status status = t->create("/sandbox/scale_up.txt", "file", "/tmp", 8, 1, 0, perms::all(), {"0", "1", "2", "3", "4", "5", "6", "7"}, {"regular", "regular", "regular", "regular", "regular", "regular", "regular", "regular"}, conf);
  file_client client(t, "/sandbox/scale_up.txt", status);

  // Write data until auto scaling is triggered
  for (std::size_t i = 0; i < 5000; ++i) {
    REQUIRE(client.write(std::string(512, (std::to_string(i)).c_str()[0])) == "!ok");
  }
  for(std::size_t i = 0; i < 10000; ++i) {
    REQUIRE(client.write(std::string(102400, (std::to_string(i)).c_str()[0])) == "!ok");
  }

  for (std::size_t i = 0; i < 5000; ++i) {
    REQUIRE(client.read(512) == std::string(512, (std::to_string(i)).c_str()[0]));
  }
  for (std::size_t i = 0; i < 10000; ++i) {
    REQUIRE(client.read(102400) == std::string(102400, (std::to_string(i)).c_str()[0]));
  }

  // Busy wait until number of blocks increases
  while (t->dstatus("/sandbox/scale_up.txt").data_blocks().size() == 1);

  as_server->stop();
  if(auto_scaling_thread.joinable()) {
    auto_scaling_thread.join();
  }

  storage_server->stop();
  if (storage_serve_thread1.joinable()) {
    storage_serve_thread1.join();
  }
  mgmt_server->stop();
  if (mgmt_serve_thread.joinable()) {
    mgmt_serve_thread.join();
  }
  as_server->stop();
  if(auto_scaling_thread.joinable()) {
    auto_scaling_thread.join();
  }
  dir_server->stop();
  if (dir_serve_thread.joinable()) {
    dir_serve_thread.join();
  }
}


TEST_CASE("fifo_queue_auto_scale_test", "[directory_service][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(21, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_fifo_queue_blocks(block_names);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread1([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto as_server = auto_scaling_server::create(HOST, DIRECTORY_SERVICE_PORT, HOST, AUTO_SCALING_SERVICE_PORT);
  std::thread auto_scaling_thread([&as_server]{as_server->serve(); });
  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  data_status status = t->create("/sandbox/scale_up.txt", "fifoqueue", "/tmp", 1, 1, 0, perms::all(), {"0"}, {"regular"}, {});
  fifo_queue_client client(t, "/sandbox/scale_up.txt", status);

  // Write data until auto scaling is triggered
  for (std::size_t i = 0; i < 2100; ++i) {
    REQUIRE(client.enqueue(std::string(100000, (std::to_string(i)).c_str()[0])) == "!ok");
  }
  // Busy wait until number of blocks increases
  while (t->dstatus("/sandbox/scale_up.txt").data_blocks().size() == 1);

  for (std::size_t i = 0; i < 2100; ++i) {
    REQUIRE(client.dequeue() == std::string(100000, (std::to_string(i)).c_str()[0]));
  }
  // Busy wait until number of blocks decreases
  while (t->dstatus("/sandbox/scale_up.txt").data_blocks().size() > 1);


  as_server->stop();
  if(auto_scaling_thread.joinable()) {
    auto_scaling_thread.join();
  }

  storage_server->stop();
  if (storage_serve_thread1.joinable()) {
    storage_serve_thread1.join();
  }
  mgmt_server->stop();
  if (mgmt_serve_thread.joinable()) {
    mgmt_serve_thread.join();
  }
  as_server->stop();
  if(auto_scaling_thread.joinable()) {
    auto_scaling_thread.join();
  }
  dir_server->stop();
  if (dir_serve_thread.joinable()) {
    dir_serve_thread.join();
  }

}
