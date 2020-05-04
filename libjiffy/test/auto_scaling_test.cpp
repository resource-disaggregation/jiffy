#include "catch.hpp"
#include <thrift/transport/TTransportException.h>
#include <thread>
#include <chrono>
#include <queue>
#include "jiffy/storage/manager/storage_management_server.h"
#include "jiffy/storage/manager/storage_manager.h"
#include "jiffy/storage/file/file_partition.h"
#include "jiffy/storage/hashtable/hash_slot.h"
#include "test_utils.h"
#include "jiffy/storage/service/block_server.h"
#include "jiffy/directory/fs/directory_tree.h"
#include "jiffy/directory/fs/directory_server.h"
#include "jiffy/storage/client/hash_table_client.h"
#include "jiffy/storage/client/fifo_queue_client.h"
#include "jiffy/storage/client/file_client.h"
#include "jiffy/client/jiffy_client.h"
#include "jiffy/directory/fs/sync_worker.h"
#include "jiffy/directory/lease/lease_expiry_worker.h"
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
  std::thread auto_scaling_thread([&as_server] { as_server->serve(); });
  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  auto status = t->create("/sandbox/scale_up.txt", "hashtable", "/tmp", 3, 3, 0, perms::all(),
                          {"0_32768", "32768_49152", "49152_65536"}, {"regular", "regular", "regular"}, {});
  hash_table_client client(t, "/sandbox/scale_up.txt", status);
  // Write data until auto scaling is triggered
  for (std::size_t i = 0; i < 2000; ++i) {
    REQUIRE_NOTHROW(client.put(std::to_string(i), std::to_string(i)));
    REQUIRE_NOTHROW(client.update(std::to_string(i), std::to_string(2000 - i))); // FIXME(anuragkh): why?
  }

  // Busy wait until number of blocks increases
  while (t->dstatus("/sandbox/scale_up.txt").data_blocks().size() == 1);

  for (std::size_t i = 0; i < 2000; i++) {
    REQUIRE(client.get(std::to_string(i)) == std::to_string(2000 - i));
  }
  t->remove("/sandbox/scale_up.txt");
  as_server->stop();
  if (auto_scaling_thread.joinable()) {
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
  std::thread auto_scaling_thread([&as_server] { as_server->serve(); });
  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  auto status = t->create("/sandbox/scale_down.txt", "hashtable", "/tmp", 3, 5, 0, perms::all(),
                          {"0_16384", "16384_32768", "32768_65536"}, {"regular", "regular", "regular"}, {});
  hash_table_client client(t, "/sandbox/scale_down.txt", status);

  for (std::size_t i = 0; i <= 1000; ++i) {
    REQUIRE_NOTHROW(client.put(std::to_string(i), std::to_string(i)));
  }
  for (std::size_t i = 0; i <= 1000; ++i) {
    REQUIRE_NOTHROW(client.update(std::to_string(i), std::to_string(1000 - i)));
  }
  // A single remove should trigger scale down
  std::vector<std::string> result;
  REQUIRE_NOTHROW(client.remove(std::to_string(0)));
  // REQUIRE_NOTHROW(client.update(std::to_string(0), std::string(102400, 'x')));
  REQUIRE_NOTHROW(client.remove(std::to_string(600)));
  // REQUIRE_NOTHROW(client.update(std::to_string(600), std::string(102400, 'x')));
  REQUIRE_NOTHROW(client.remove(std::to_string(1000)));
  // REQUIRE_NOTHROW(client.update(std::to_string(1000), std::string(102400, 'x')));

  // Busy wait until number of blocks decreases
  while (t->dstatus("/sandbox/scale_down.txt").data_blocks().size() == 3);

  for (std::size_t i = 1; i < 1000 && i != 600; i++) {
    REQUIRE(client.get(std::to_string(i)) == std::to_string(1000 - i));
  }

  for (std::size_t i = 1000; i < 4000; i++) {
    REQUIRE_NOTHROW(client.put(std::to_string(i), std::string(102400, 'x')));
  }

  t->remove("/sandbox/scale_down.txt");

  as_server->stop();
  if (auto_scaling_thread.joinable()) {
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
  std::thread auto_scaling_thread([&as_server] { as_server->serve(); });
  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  auto status = t->create("/sandbox/scale_mix.txt", "hashtable", "/tmp", 3, 5, 0, perms::all(),
                          {"0_16384", "16384_32768", "32768_65536"}, {"regular", "regular", "regular"}, {});
  hash_table_client client(t, "/sandbox/scale_mix.txt", status);
  std::size_t iter = 10000;
  const std::size_t max_key = 200000;
  const std::size_t string_size = 102400;
  int bitmap[max_key] = {0};
  for (std::size_t i = 0; i < iter; i++) {
    std::size_t j = rand_utils::rand_uint32(0, 3);
    std::string ret;
    std::size_t key = rand_utils::rand_uint32(0, max_key - 1);
    switch (j) {
      case 0:
          try {
            client.put(std::to_string(key), std::string(string_size, 'a'));
            if(bitmap[key] != 2)
              bitmap[key] = 1;
          } catch (std::exception &e) {}
        break;
      case 1:
        try {
          ret = client.update(std::to_string(key), std::string(string_size, 'b'));
          if(bitmap[key] == 1)
            bitmap[key] = 2;
        } catch (std::exception &e) {}
        break;
      case 2:
        try {
          client.get(std::to_string(key));
        } catch (std::exception &e) {}
        break;
      case 3:
        try {
          ret = client.remove(std::to_string(key));
          bitmap[key] = 0;
        } catch (std::exception &e) {}
        break;
      default:throw std::logic_error("Invalid switch");
    }
  }

  for (std::size_t k = 0; k < max_key; k++) {
    if (bitmap[k] == 1)
      REQUIRE(client.get(std::to_string(k)) == std::string(string_size, 'a'));
    else if (bitmap[k] == 2)
      REQUIRE(client.get(std::to_string(k)) == std::string(string_size, 'b'));
  }

  t->remove("/sandbox/scale_mix.txt");

  as_server->stop();
  if (auto_scaling_thread.joinable()) {
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


TEST_CASE("hash_table_auto_scale_large_data_test", "[directory_service][storage_server][management_server]") {
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
  std::thread auto_scaling_thread([&as_server] { as_server->serve(); });
  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  auto status = t->create("/sandbox/scale_up.txt", "hashtable", "/tmp", 3, 1, 0, perms::all(),
                         {"0_32768", "32768_49152", "49152_65536"}, {"regular", "regular", "regular"}, {});
  hash_table_client client(t, "/sandbox/scale_up.txt", status);
  // Write data until auto scaling is triggered
  std::size_t data_size = 1024 * 512;
  for (std::size_t i = 0; i < 5000; ++i) {
    REQUIRE_NOTHROW(client.put(std::to_string(i), std::string(data_size, 'a')));
    REQUIRE_NOTHROW(client.update(std::to_string(i), std::string(data_size, 'b'))); // FIXME(anuragkh): why?
  }

  // Busy wait until number of blocks increases
  while (t->dstatus("/sandbox/scale_up.txt").data_blocks().size() == 1);

  for (std::size_t i = 0; i < 5000; i++) {
    REQUIRE(client.get(std::to_string(i)) == std::string(data_size, 'b'));
  }
  t->remove("/sandbox/scale_up.txt");
  as_server->stop();
  if (auto_scaling_thread.joinable()) {
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
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto as_server = auto_scaling_server::create(HOST, DIRECTORY_SERVICE_PORT, HOST, AUTO_SCALING_SERVICE_PORT);
  std::thread auto_scaling_thread([&as_server] { as_server->serve(); });
  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  auto status = t->create("/sandbox/scale_up.txt", "file", "/tmp", 1, 1, 0, perms::all(), {"0"}, {"regular"}, {});
  file_client client(t, "/sandbox/scale_up.txt", status);

  std::string buffer;
  // Write data until auto scaling is triggered
  for (std::size_t i = 0; i < 5000; ++i) {
    REQUIRE(client.write(std::string(512, (std::to_string(i)).c_str()[0])) == 512);
  }

  for (std::size_t i = 0; i < 6000; ++i) {
    REQUIRE(client.write(std::string(102400, (std::to_string(i)).c_str()[0])) == 102400);
  }

  REQUIRE_NOTHROW(client.seek(0));
  for (std::size_t i = 0; i < 5000; ++i) {
    buffer.clear();
    REQUIRE(client.read(buffer, 512) == 512);
    REQUIRE(buffer == std::string(512, (std::to_string(i)).c_str()[0]));
  }

  for (std::size_t i = 0; i < 6000; ++i) {
    buffer.clear();
    REQUIRE(client.read(buffer, 102400) == 102400);
    REQUIRE(buffer == std::string(102400, (std::to_string(i)).c_str()[0]));
  }

  REQUIRE_NOTHROW(client.seek(0));

  for (std::size_t i = 0; i < 5000; ++i) {
    buffer.clear();
    REQUIRE(client.read(buffer, 512) == 512);
    REQUIRE(buffer == std::string(512, (std::to_string(i)).c_str()[0]));
  }

  for (std::size_t i = 0; i < 6000; ++i) {
    buffer.clear();
    REQUIRE(client.read(buffer, 102400) == 102400);
    REQUIRE(buffer == std::string(102400, (std::to_string(i)).c_str()[0]));
  }

  REQUIRE_NOTHROW(client.seek(5000 * 512));

  for (std::size_t i = 0; i < 6000; ++i) {
    buffer.clear();
    REQUIRE(client.read(buffer, 102400) == 102400);
    REQUIRE(buffer == std::string(102400, (std::to_string(i)).c_str()[0]));
  }
  // Busy wait until number of blocks increases
  while (t->dstatus("/sandbox/scale_up.txt").data_blocks().size() == 1);

  as_server->stop();
  if (auto_scaling_thread.joinable()) {
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

TEST_CASE("file_auto_scale_chain_replica_test", "[directory_service][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(64, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_file_blocks(block_names);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto as_server = auto_scaling_server::create(HOST, DIRECTORY_SERVICE_PORT, HOST, AUTO_SCALING_SERVICE_PORT);
  std::thread auto_scaling_thread([&as_server] { as_server->serve(); });
  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  auto status = t->create("/sandbox/scale_up.txt", "file", "/tmp", 1, 3, 0, perms::all(), {"0"}, {"regular"}, {});
  file_client client(t, "/sandbox/scale_up.txt", status);

  std::string buffer;

  // Write data until auto scaling is triggered
  for (std::size_t i = 0; i < 5000; ++i) {
    REQUIRE(client.write(std::string(512, (std::to_string(i)).c_str()[0])) == 512);
  }

  for (std::size_t i = 0; i < 6000; ++i) {
    REQUIRE(client.write(std::string(102400, (std::to_string(i)).c_str()[0])) == 102400);
  }

  REQUIRE_NOTHROW(client.seek(0));

  for (std::size_t i = 0; i < 5000; ++i) {
    buffer.clear();
    REQUIRE(client.read(buffer, 512) == 512);
    REQUIRE(buffer == std::string(512, (std::to_string(i)).c_str()[0]));
  }

  REQUIRE_NOTHROW(client.seek(0));

  for (std::size_t i = 0; i < 5000; ++i) {
    buffer.clear();
    REQUIRE(client.read(buffer, 512) == 512);
    REQUIRE(buffer == std::string(512, (std::to_string(i)).c_str()[0]));
  }

  for (std::size_t i = 0; i < 6000; ++i) {
    buffer.clear();
    REQUIRE(client.read(buffer, 102400) == 102400);
    REQUIRE(buffer == std::string(102400, (std::to_string(i)).c_str()[0]));
  }

  REQUIRE_NOTHROW(client.seek(5000 * 512));

  for (std::size_t i = 0; i < 6000; ++i) {
    buffer.clear();
    REQUIRE(client.read(buffer, 102400) == 102400);
    REQUIRE(buffer == std::string(102400, (std::to_string(i)).c_str()[0]));
  }
  // Busy wait until number of blocks increases
  while (t->dstatus("/sandbox/scale_up.txt").data_blocks().size() == 1);

  as_server->stop();
  if (auto_scaling_thread.joinable()) {
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

TEST_CASE("file_auto_scale_multi_blocks_test", "[directory_service][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(64, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_file_blocks(block_names);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto as_server = auto_scaling_server::create(HOST, DIRECTORY_SERVICE_PORT, HOST, AUTO_SCALING_SERVICE_PORT);
  std::thread auto_scaling_thread([&as_server] { as_server->serve(); });
  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);
  std::map<std::string, std::string> conf;
  conf.emplace("file.auto_scale", "false");
  auto status = t->create("/sandbox/scale_up.txt", "file", "/tmp", 8, 1, 0, perms::all(),
                          {"0", "1", "2", "3", "4", "5", "6", "7"},
                          {"regular", "regular", "regular", "regular", "regular", "regular", "regular",
                           "regular"},
                          conf);
  file_client client(t, "/sandbox/scale_up.txt", status);
  std::string buffer;
  // Write data until auto scaling is triggered
  for (std::size_t i = 0; i < 5000; ++i) {
    REQUIRE(client.write(std::string(512, (std::to_string(i)).c_str()[0])) == 512);
  }

  for (std::size_t i = 0; i < 10000; ++i) {
    REQUIRE(client.write(std::string(102400, (std::to_string(i)).c_str()[0])) == 102400);
  }

  REQUIRE_NOTHROW(client.seek(0));

  for (std::size_t i = 0; i < 5000; ++i) {
    buffer.clear();
    REQUIRE(client.read(buffer, 512) == 512);
    REQUIRE(buffer == std::string(512, (std::to_string(i)).c_str()[0]));
  }

  REQUIRE_NOTHROW(client.seek(0));

  for (std::size_t i = 0; i < 5000; ++i) {
    buffer.clear();
    REQUIRE(client.read(buffer, 512) == 512);
    REQUIRE(buffer == std::string(512, (std::to_string(i)).c_str()[0]));
  }

  for (std::size_t i = 0; i < 10000; ++i) {
    buffer.clear();
    REQUIRE(client.read(buffer, 102400) == 102400);
    REQUIRE(buffer == std::string(102400, (std::to_string(i)).c_str()[0]));
  }

  REQUIRE_NOTHROW(client.seek(5000 * 512));

  for (std::size_t i = 0; i < 10000; ++i) {
    buffer.clear();
    REQUIRE(client.read(buffer, 102400) == 102400);
    REQUIRE(buffer == std::string(102400, (std::to_string(i)).c_str()[0]));
  }

  // Busy wait until number of blocks increases
  while (t->dstatus("/sandbox/scale_up.txt").data_blocks().size() == 1);

  as_server->stop();
  if (auto_scaling_thread.joinable()) {
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

TEST_CASE("file_auto_scale_mix_test", "[directory_service][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(1000, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_file_blocks(block_names, 50000);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto as_server = auto_scaling_server::create(HOST, DIRECTORY_SERVICE_PORT, HOST, AUTO_SCALING_SERVICE_PORT);
  std::thread auto_scaling_thread([&as_server] { as_server->serve(); });
  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  data_status
      status = t->create("/sandbox/scale_mix.txt", "file", "/tmp", 1, 5, 0, perms::all(), {"0"}, {"regular"}, {});
  file_client client(t, "/sandbox/scale_mix.txt", status);
  std::size_t file_size = 0;
  std::size_t iter = 10000;
  std::size_t current_offset = 0;
  for (std::size_t i = 0; i < iter; i++) {
    std::size_t j = rand_utils::rand_uint32(0, 2);
    std::string ret;
    std::size_t string_size = rand_utils::rand_uint32(0, 102400);
    switch (j) {
      case 0:
        REQUIRE_NOTHROW(client.write(std::string(string_size, 'a')));
        current_offset += string_size;
        if (current_offset > file_size) {
          file_size = current_offset;
        }
        break;
      case 1:
        {
          std::string buf;
          int ret;
          REQUIRE_NOTHROW(ret = client.read(buf, string_size));
          if(ret != -1) {
            current_offset += ret;
            REQUIRE(ret == buf.size());
          }
          break;
        }
      case 2:
        std::size_t seek_offset = rand_utils::rand_uint32(0, file_size);
        REQUIRE(client.seek(seek_offset));
        current_offset = seek_offset;
        break;
    }
  }
  REQUIRE_NOTHROW(client.seek(0));
  for (std::size_t i = 0; i < (std::size_t) (file_size / 102400); i++) {
    std::string buf;
    buf.clear();
    REQUIRE_NOTHROW(client.read(buf, 102400));
    REQUIRE(buf == std::string(102400, 'a'));
  }
  std::string buf;
  buf.clear();
  REQUIRE_NOTHROW(client.read(buf, file_size % 102400));
  REQUIRE(buf == std::string(file_size % 102400, 'a'));

  as_server->stop();
  if (auto_scaling_thread.joinable()) {
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

TEST_CASE("file_auto_scale_large_data_test", "[directory_service][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(21, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_file_blocks(block_names);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto as_server = auto_scaling_server::create(HOST, DIRECTORY_SERVICE_PORT, HOST, AUTO_SCALING_SERVICE_PORT);
  std::thread auto_scaling_thread([&as_server] { as_server->serve(); });
  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  auto status = t->create("/sandbox/scale_up.txt", "file", "/tmp", 1, 1, 0, perms::all(), {"0"}, {"regular"}, {});
  file_client client(t, "/sandbox/scale_up.txt", status);

  std::string buffer;
  std::size_t data_size = 1024 * 512;
  // Write data until auto scaling is triggered
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.write(std::string(data_size, (std::to_string(i)).c_str()[0])) == data_size);
  }

  REQUIRE_NOTHROW(client.seek(0));
  for (std::size_t i = 0; i < 1000; ++i) {
    buffer.clear();
    REQUIRE(client.read(buffer, data_size) == data_size);
    REQUIRE(buffer == std::string(data_size, (std::to_string(i)).c_str()[0]));
  }

  // Busy wait until number of blocks increases
  while (t->dstatus("/sandbox/scale_up.txt").data_blocks().size() == 1);

  as_server->stop();
  if (auto_scaling_thread.joinable()) {
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

TEST_CASE("fifo_queue_auto_scale_test", "[directory_service][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(50, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_fifo_queue_blocks(block_names, 5000);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread1([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto as_server = auto_scaling_server::create(HOST, DIRECTORY_SERVICE_PORT, HOST, AUTO_SCALING_SERVICE_PORT);
  std::thread auto_scaling_thread([&as_server] { as_server->serve(); });
  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  auto status = t->create("/sandbox/scale_up.txt", "fifoqueue", "/tmp", 1, 1, 0, perms::all(), {"0"}, {"regular"}, {});
  fifo_queue_client client(t, "/sandbox/scale_up.txt", status);

  // Write data until auto scaling is triggered
  for (std::size_t i = 0; i < 100; ++i) {
    REQUIRE_NOTHROW(client.enqueue(std::string(1024, (std::to_string(i)).c_str()[0])));
  }
  // Busy wait until number of blocks increases
  while (t->dstatus("/sandbox/scale_up.txt").data_blocks().size() == 1);

  for (std::size_t i = 0; i < 100; ++i) {
    REQUIRE(client.read_next() == std::string(1024, (std::to_string(i)).c_str()[0]));
  }

  for (std::size_t i = 0; i < 100; ++i) {
    REQUIRE(client.front() == std::string(1024, (std::to_string(i)).c_str()[0]));
    REQUIRE_NOTHROW(client.dequeue());
  }
  // Busy wait until number of blocks decreases
  while (t->dstatus("/sandbox/scale_up.txt").data_blocks().size() > 1);

  as_server->stop();
  if (auto_scaling_thread.joinable()) {
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
  dir_server->stop();
  if (dir_serve_thread.joinable()) {
    dir_serve_thread.join();
  }
}

TEST_CASE("fifo_queue_auto_scale_replica_chain_test", "[directory_service][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(100, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_fifo_queue_blocks(block_names);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread1([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto as_server = auto_scaling_server::create(HOST, DIRECTORY_SERVICE_PORT, HOST, AUTO_SCALING_SERVICE_PORT);
  std::thread auto_scaling_thread([&as_server] { as_server->serve(); });
  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  auto status = t->create("/sandbox/scale_up.txt", "fifoqueue", "/tmp", 1, 5, 0, perms::all(), {"0"}, {"regular"}, {});
  fifo_queue_client client(t, "/sandbox/scale_up.txt", status);

  // Write data until auto scaling is triggered
  for (std::size_t i = 0; i < 4000; ++i) {
    REQUIRE_NOTHROW(client.enqueue(std::string(100000, (std::to_string(i)).c_str()[0])));
  }

  for (std::size_t i = 0; i < 4000; ++i) {
    REQUIRE(client.read_next() == std::string(100000, (std::to_string(i)).c_str()[0]));
  }

  for (std::size_t i = 0; i < 4000; ++i) {
    REQUIRE(client.front() == std::string(100000, (std::to_string(i)).c_str()[0]));
    REQUIRE_NOTHROW(client.dequeue());
  }

  as_server->stop();
  if (auto_scaling_thread.joinable()) {
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

  dir_server->stop();
  if (dir_serve_thread.joinable()) {
    dir_serve_thread.join();
  }
}

TEST_CASE("fifo_queue_auto_scale_multi_block_test", "[directory_service][storage_server][management_server]") {
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
  std::thread auto_scaling_thread([&as_server] { as_server->serve(); });
  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);
  std::map<std::string, std::string> conf;
  conf.emplace("fifoqueue.auto_scale", "false");
  auto status = t->create("/sandbox/scale_up.txt", "fifoqueue", "/tmp", 4, 1, 0, perms::all(), {"0", "1", "2", "3"},
                          {"regular", "regular", "regular", "regular"}, conf);
  fifo_queue_client client(t, "/sandbox/scale_up.txt", status);

  // Write data until auto scaling is triggered
  for (std::size_t i = 0; i < 2100; ++i) {
    REQUIRE_NOTHROW(client.enqueue(std::string(100000, (std::to_string(i)).c_str()[0])));
  }

  for (std::size_t i = 0; i < 2100; ++i) {
    REQUIRE(client.read_next() == std::string(100000, (std::to_string(i)).c_str()[0]));
  }

  for (std::size_t i = 0; i < 2100; ++i) {
    REQUIRE(client.front() == std::string(100000, (std::to_string(i)).c_str()[0]));
    REQUIRE_NOTHROW(client.dequeue());
  }

  as_server->stop();
  if (auto_scaling_thread.joinable()) {
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

  dir_server->stop();
  if (dir_serve_thread.joinable()) {
    dir_serve_thread.join();
  }
}

// TODO multiple producer and consumer test removed, since we use front() and dequeue() which could be lead to inconsistent results
//TEST_CASE("fifo_queue_multiple_test", "[enqueue][dequeue]") {
//  auto alloc = std::make_shared<sequential_block_allocator>();
//  auto block_names = test_utils::init_block_names(64, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
//  alloc->add_blocks(block_names);
//  auto blocks = test_utils::init_fifo_queue_blocks(block_names);
//  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
//  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
//  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);
//
//  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
//  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
//  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);
//
//  auto as_server = auto_scaling_server::create(HOST, DIRECTORY_SERVICE_PORT, HOST, AUTO_SCALING_SERVICE_PORT);
//  std::thread auto_scaling_thread([&as_server]{as_server->serve(); });
//  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);
//
//  auto sm = std::make_shared<storage_manager>();
//  auto tree = std::make_shared<directory_tree>(alloc, sm);
//
//  auto dir_server = directory_server::create(tree, HOST, DIRECTORY_SERVICE_PORT);
//  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
//  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);
//
//  data_status status = tree->create("/sandbox/file.txt", "fifoqueue", "/tmp", NUM_BLOCKS, 1, 0, 0,
//                                   {"0"}, {"regular"});
//  uint32_t num_ops = 5000;
//  uint32_t data_size = 102400;
//  const uint32_t num_threads = 4;
//  std::vector<std::thread> workers;
//
//  // Start multiple consumers
//  std::vector<int> count(num_threads);
//  for (uint32_t k = 1; k <= num_threads; k++) {
//    workers.push_back(std::thread([k, &tree, &status, num_ops, &count] {
//      fifo_queue_client client(tree, "/sandbox/file.txt", status);
//      for (uint32_t j = 0; j < num_ops * num_threads * 2; j++) {
//        std::string ret;
//        try {
//          ret = client.dequeue();
//        } catch (std::logic_error &e) {
//          continue;
//        }
//        count[k - 1]++;
//      }
//    }));
//  }
//
//  // Start multiple producers
//  for (uint32_t i = 1; i <= num_threads; i++) {
//    workers.push_back(std::thread([i, &tree, &status, num_ops, data_size] {
//      std::string data_(data_size, std::to_string(i)[0]);
//      fifo_queue_client client(tree, "/sandbox/file.txt", status);
//      for (uint32_t j = 0; j < num_ops; j++) {
//        REQUIRE_NOTHROW(client.enqueue(data_));
//      }
//    }));
//  }
//  for (std::thread &worker : workers) {
//    worker.join();
//  }
//  // Check if data read is correct
//  int read_count = 0;
//  for(const auto &x : count)
//    read_count += x;
//
//  REQUIRE(read_count == num_threads * num_ops);
//
//  as_server->stop();
//  if(auto_scaling_thread.joinable()) {
//    auto_scaling_thread.join();
//  }
//  storage_server->stop();
//  if (storage_serve_thread.joinable()) {
//    storage_serve_thread.join();
//  }
//  mgmt_server->stop();
//  if (mgmt_serve_thread.joinable()) {
//    mgmt_serve_thread.join();
//  }
//  dir_server->stop();
//  if (dir_serve_thread.joinable()) {
//    dir_serve_thread.join();
//  }
//
//}


TEST_CASE("fifo_queue_auto_scale_mix_test", "[directory_service][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(50, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_fifo_queue_blocks(block_names);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread1([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto as_server = auto_scaling_server::create(HOST, DIRECTORY_SERVICE_PORT, HOST, AUTO_SCALING_SERVICE_PORT);
  std::thread auto_scaling_thread([&as_server] { as_server->serve(); });
  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);
  auto status = t->create("/sandbox/scale_up.txt", "fifoqueue", "/tmp", 1, 1, 0, perms::all(), {"0"}, {"regular"}, {});
  fifo_queue_client client(t, "/sandbox/scale_up.txt", status);
  std::queue<std::pair<std::size_t, char>> result;
  const std::size_t dict_size = 11;
  char dict[dict_size] = "abcdefghij";
  const std::size_t string_max_size = 102400;
  std::size_t iter = 50000;
  for (std::size_t i = 0; i < iter; i++) {
    std::size_t j = rand_utils::rand_uint32(0, 2);
    std::size_t char_offset = rand_utils::rand_uint32(0, dict_size - 2);
    std::string ret;
    std::size_t string_size = rand_utils::rand_uint32(0, string_max_size);
    switch (j) {
      case 0:
        REQUIRE_NOTHROW(client.enqueue(std::string(string_size, dict[char_offset])));
        result.push(std::make_pair(string_size, dict[char_offset]));
        break;
      case 1:
        try {
          ret = client.front();
          client.dequeue();
          if(!result.empty()) {
            auto str = result.front();
            result.pop();
            REQUIRE(ret == std::string(str.first, str.second));
          } else {
            REQUIRE(ret == "!msg_not_found");
          }
        } catch (std::exception& e) {}
        break;
      case 2:
        try {
          client.read_next(); // TODO: Better testing
        } catch (std::exception& e) {}
        break;
      default:
        throw std::logic_error("Invalid switch");
    }
  }

  for (std::size_t i = 0; i < result.size(); i++) {
    auto str = result.front();
    REQUIRE(client.front() == std::string(str.first, str.second));
    REQUIRE_NOTHROW(client.dequeue());
    result.pop();
  }

  as_server->stop();
  if (auto_scaling_thread.joinable()) {
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

  dir_server->stop();
  if (dir_serve_thread.joinable()) {
    dir_serve_thread.join();
  }
}

TEST_CASE("fifo_queue_large_data_test", "[enqueue][dequeue]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(64, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_fifo_queue_blocks(block_names, 134217728, 0, 1);

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

  data_status status = tree->create("/sandbox/file.txt", "fifoqueue", "/tmp", NUM_BLOCKS, 1, 0, 0,
                                  {"0"}, {"regular"});

  fifo_queue_client client(tree, "/sandbox/file.txt", status);

  const int data_size = 1024 * 512;
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(client.enqueue(std::string(data_size, std::to_string(i).c_str()[0])));
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.front() == std::string(data_size, std::to_string(i).c_str()[0]));
    REQUIRE_NOTHROW(client.dequeue());
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_THROWS_AS(client.dequeue(), std::logic_error);
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



TEST_CASE("fifo_queue_queue_size_test", "[enqueue][dequeue]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(64, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_fifo_queue_blocks(block_names, 134217728, 0, 1);

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

  data_status status = tree->create("/sandbox/file.txt", "fifoqueue", "/tmp", NUM_BLOCKS, 1, 0, 0,
                                  {"0"}, {"regular"});

  fifo_queue_client client(tree, "/sandbox/file.txt", status);

  const int data_size = 1024 * 512;
  std::size_t length = 0;
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(client.enqueue(std::string(data_size, std::to_string(i).c_str()[0])));
    length += data_size;
  }

  REQUIRE(client.length() == length);

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.front() == std::string(data_size, std::to_string(i).c_str()[0]));
    REQUIRE_NOTHROW(client.dequeue());
    length -= data_size;
  }

  REQUIRE(client.length() == length);

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_THROWS_AS(client.dequeue(), std::logic_error);
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


//TEST_CASE("fifo_queue_multiple_queue_size_test", "[enqueue][dequeue]") {
//  auto alloc = std::make_shared<sequential_block_allocator>();
//  auto block_names = test_utils::init_block_names(64, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
//  alloc->add_blocks(block_names);
//  auto blocks = test_utils::init_fifo_queue_blocks(block_names);
//  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
//  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
//  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);
//
//  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
//  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
//  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);
//
//  auto as_server = auto_scaling_server::create(HOST, DIRECTORY_SERVICE_PORT, HOST, AUTO_SCALING_SERVICE_PORT);
//  std::thread auto_scaling_thread([&as_server]{as_server->serve(); });
//  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);
//
//  auto sm = std::make_shared<storage_manager>();
//  auto tree = std::make_shared<directory_tree>(alloc, sm);
//
//  auto dir_server = directory_server::create(tree, HOST, DIRECTORY_SERVICE_PORT);
//  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
//  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);
//
//  data_status status = tree->create("/sandbox/file.txt", "fifoqueue", "/tmp", NUM_BLOCKS, 1, 0, 0,
//                                  {"0"}, {"regular"});
//  uint32_t num_ops = 5000;
//  uint32_t data_size = 102400;
//  const uint32_t num_threads = 4;
//  std::vector<std::thread> workers;
//
//  // Start multiple consumers
//  std::vector<int> count(num_threads);
//  for (uint32_t k = 1; k <= num_threads; k++) {
//    workers.push_back(std::thread([k, &tree, &status, num_ops, &count] {
//      fifo_queue_client client(tree, "/sandbox/file.txt", status);
//      for (uint32_t j = 0; j < num_ops * num_threads * 2; j++) {
//        std::string ret;
//        try {
//          ret = client.dequeue();
//          std::size_t queue_size;
//          REQUIRE_NOTHROW(queue_size = client.length());
//        } catch (std::logic_error &e) {
//          continue;
//        }
//        count[k - 1]++;
//      }
//    }));
//  }
//
//  // Start multiple producers
//  for (uint32_t i = 1; i <= num_threads; i++) {
//    workers.push_back(std::thread([i, &tree, &status, num_ops, data_size] {
//      std::string data_(data_size, std::to_string(i)[0]);
//      fifo_queue_client client(tree, "/sandbox/file.txt", status);
//      for (uint32_t j = 0; j < num_ops; j++) {
//        REQUIRE_NOTHROW(client.enqueue(data_));
//        std::size_t queue_size;
//        REQUIRE_NOTHROW(queue_size = client.length());
//      }
//    }));
//  }
//  for (std::thread &worker : workers) {
//    worker.join();
//  }
//  // Check if data read is correct
//  int read_count = 0;
//  for(const auto &x : count)
//    read_count += x;
//
//  REQUIRE(read_count == num_threads * num_ops);
//
//  as_server->stop();
//  if(auto_scaling_thread.joinable()) {
//    auto_scaling_thread.join();
//  }
//  storage_server->stop();
//  if (storage_serve_thread.joinable()) {
//    storage_serve_thread.join();
//  }
//  mgmt_server->stop();
//  if (mgmt_serve_thread.joinable()) {
//    mgmt_serve_thread.join();
//  }
//  dir_server->stop();
//  if (dir_serve_thread.joinable()) {
//    dir_serve_thread.join();
//  }
//
//}

TEST_CASE("fifo_queue_in_rate_out_rate_auto_scale_test", "[enqueue][dequeue]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(64, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_fifo_queue_blocks(block_names);

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

  data_status status = tree->create("/sandbox/file.txt", "fifoqueue", "/tmp", NUM_BLOCKS, 1, 0, 0,
                                    {"0"}, {"regular"});

  fifo_queue_client client(tree, "/sandbox/file.txt", status);
  double rate;
  std::size_t length = 0;
  std::size_t data_size = 1024 * 1024;
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(client.enqueue(std::string(data_size, 'a')));
    length += data_size;
  }

  REQUIRE_NOTHROW(rate = client.in_rate());
  LOG(log_level::info) << "In rate: " << rate;
  REQUIRE_NOTHROW(rate = client.out_rate());
  LOG(log_level::info) << "Out rate: " << rate;

  REQUIRE(client.length() == length);

  for (std::size_t i = 0; i < 500; ++i) {
    REQUIRE(client.front() == std::string(data_size, 'a'));
    REQUIRE_NOTHROW(client.dequeue());
    length -= data_size;
  }

  REQUIRE_NOTHROW(rate = client.in_rate());
  LOG(log_level::info) << "In rate: " << rate;
  REQUIRE_NOTHROW(rate = client.out_rate());
  LOG(log_level::info) << "Out rate: " << rate;

  REQUIRE(client.length() == length);

  for (std::size_t i = 500; i < 1000; ++i) {
    REQUIRE(client.front() == std::string(data_size, 'a'));
    REQUIRE_NOTHROW(client.dequeue());
    length -= data_size;
  }


  REQUIRE_NOTHROW(rate = client.in_rate());
  LOG(log_level::info) << "In rate: " << rate;
  REQUIRE_NOTHROW(rate = client.out_rate());
  LOG(log_level::info) << "Out rate: " << rate;

  REQUIRE(client.length() == length);

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


//TEST_CASE("fifo_queue_multiple_in_rate_out_rate_test", "[enqueue][dequeue]") {
//  auto alloc = std::make_shared<sequential_block_allocator>();
//  auto block_names = test_utils::init_block_names(64, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
//  alloc->add_blocks(block_names);
//  auto blocks = test_utils::init_fifo_queue_blocks(block_names);
//  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
//  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
//  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);
//
//  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
//  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
//  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);
//
//  auto as_server = auto_scaling_server::create(HOST, DIRECTORY_SERVICE_PORT, HOST, AUTO_SCALING_SERVICE_PORT);
//  std::thread auto_scaling_thread([&as_server]{as_server->serve(); });
//  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);
//
//  auto sm = std::make_shared<storage_manager>();
//  auto tree = std::make_shared<directory_tree>(alloc, sm);
//
//  auto dir_server = directory_server::create(tree, HOST, DIRECTORY_SERVICE_PORT);
//  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
//  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);
//
//  data_status status = tree->create("/sandbox/file.txt", "fifoqueue", "/tmp", NUM_BLOCKS, 1, 0, 0,
//                                   {"0"}, {"regular"});
//  uint32_t num_ops = 5000;
//  uint32_t data_size = 102400;
//  const uint32_t num_threads = 4;
//  std::vector<std::thread> workers;
//
//
//  // Start multiple consumers
//  std::vector<int> count(num_threads);
//  for (uint32_t k = 1; k <= num_threads; k++) {
//    workers.push_back(std::thread([k, &tree, &status, num_ops, &count] {
//      double rate;
//      fifo_queue_client client(tree, "/sandbox/file.txt", status);
//      for (uint32_t j = 0; j < num_ops * num_threads * 2; j++) {
//        std::string ret;
//        try {
//          ret = client.dequeue();
//        } catch (std::logic_error &e) {
//          continue;
//        }
//        REQUIRE_NOTHROW(rate = client.in_rate());
//        REQUIRE_NOTHROW(rate = client.out_rate());
//        count[k - 1]++;
//      }
//    }));
//  }
//
//  // Start multiple producers
//  for (uint32_t i = 1; i <= num_threads; i++) {
//    workers.push_back(std::thread([i, &tree, &status, num_ops, data_size] {
//      double rate;
//      std::string data_(data_size, std::to_string(i)[0]);
//      fifo_queue_client client(tree, "/sandbox/file.txt", status);
//      for (uint32_t j = 0; j < num_ops; j++) {
//        REQUIRE_NOTHROW(client.enqueue(data_));
//        REQUIRE_NOTHROW(rate = client.in_rate());
//        REQUIRE_NOTHROW(rate = client.out_rate());
//      }
//    }));
//  }
//  for (std::thread &worker : workers) {
//    worker.join();
//  }
//  // Check if data read is correct
//  int read_count = 0;
//  for(const auto &x : count)
//    read_count += x;
//
//  REQUIRE(read_count == num_threads * num_ops);
//
//  as_server->stop();
//  if(auto_scaling_thread.joinable()) {
//    auto_scaling_thread.join();
//  }
//  storage_server->stop();
//  if (storage_serve_thread.joinable()) {
//    storage_serve_thread.join();
//  }
//  mgmt_server->stop();
//  if (mgmt_serve_thread.joinable()) {
//    mgmt_serve_thread.join();
//  }
//  dir_server->stop();
//  if (dir_serve_thread.joinable()) {
//    dir_serve_thread.join();
//  }
//
//}
