#include <catch.hpp>
#include <thrift/transport/TTransportException.h>
#include <thread>
#include "test_utils.h"
#include "jiffy/directory/fs/directory_tree.h"
#include "jiffy/directory/fs/directory_server.h"
#include "jiffy/storage/manager/storage_management_server.h"
#include "jiffy/storage/manager/storage_management_client.h"
#include "jiffy/storage/manager/storage_manager.h"
#include "jiffy/storage/file/file_partition.h"
#include "jiffy/storage/service/block_server.h"
#include "jiffy/storage/client/file_client.h"
#include "jiffy/auto_scaling/auto_scaling_server.h"

using namespace ::jiffy::storage;
using namespace ::jiffy::directory;
using namespace ::jiffy::auto_scaling;
using namespace ::apache::thrift::transport;

#define NUM_BLOCKS 1
#define BLOCK_SIZE 500
#define HOST "127.0.0.1"
#define DIRECTORY_SERVICE_PORT 9090
#define STORAGE_SERVICE_PORT 9091
#define STORAGE_MANAGEMENT_PORT 9092
#define AUTO_SCALING_SERVICE_PORT 9095


TEST_CASE("file_client_write_read_seek_test", "[write][read][seek]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(NUM_BLOCKS, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_file_blocks(block_names, 134217728);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto tree = std::make_shared<directory_tree>(alloc, sm);

  data_status status = tree->create("/sandbox/file.txt", "file", "/tmp", NUM_BLOCKS, 1, 0, 0,
                                  {"0"}, {"regular"});

  file_client client(tree, "/sandbox/file.txt", status);

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.write(std::to_string(i)) == std::to_string(i).size());
  }

  REQUIRE_NOTHROW(client.seek(0));

  std::string buffer;

  for (std::size_t i = 0; i < 1000; ++i) {
    buffer.clear();
    REQUIRE(client.read(buffer, std::to_string(i).size()) == std::to_string(i).size());
    REQUIRE(buffer == std::to_string(i));
  }

  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE(client.read(buffer, std::to_string(i).size()) == -1);
  }

  REQUIRE_NOTHROW(client.seek(0));

  for (std::size_t i = 0; i < 1000; ++i) {
    buffer.clear();
    REQUIRE(client.read(buffer, std::to_string(i).size()) == std::to_string(i).size());
    REQUIRE(buffer == std::to_string(i));
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


TEST_CASE("file_client_concurrent_write_read_seek_test", "[write][read][seek]") {


  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(20, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_file_blocks(block_names, BLOCK_SIZE);

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

  auto status = t->create("/sandbox/scale_up.txt", "file", "/tmp", 5, 1, 0, perms::all(), {"0", "1", "2", "3", "4"}, {"regular", "regular", "regular", "regular", "regular"}, {});

  file_client client(t, "/sandbox/scale_up.txt", status);

  std::string buffer;

  for (std::size_t i = 0; i < 2; ++i) {
    REQUIRE(client.write(std::string(1024, 'x')) == 1024);
  }

  REQUIRE_NOTHROW(client.seek(0));

  REQUIRE(client.read(buffer, 2048) == 2048);
  REQUIRE(buffer == std::string(2048, 'x'));

  REQUIRE_NOTHROW(client.seek(4096));

  REQUIRE(client.read(buffer, 256) == -1);

  REQUIRE(client.write(std::string(4096, 'y')) == 4096);

  REQUIRE_NOTHROW(client.seek(0));

  buffer.clear();
  REQUIRE(client.read(buffer, 8192) == 8192);
  REQUIRE(buffer.substr(0, 2048) == std::string(2048, 'x'));
  REQUIRE(buffer.substr(4096, 4096) == std::string(4096, 'y'));


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
