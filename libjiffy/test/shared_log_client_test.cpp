#include <catch.hpp>
#include <thrift/transport/TTransportException.h>
#include <thread>
#include "test_utils.h"
#include "jiffy/directory/fs/directory_tree.h"
#include "jiffy/directory/fs/directory_server.h"
#include "jiffy/storage/manager/storage_management_server.h"
#include "jiffy/storage/manager/storage_management_client.h"
#include "jiffy/storage/manager/storage_manager.h"
#include "jiffy/storage/shared_log/shared_log_partition.h"
#include "jiffy/storage/service/block_server.h"
#include "jiffy/storage/client/shared_log_client.h"
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


TEST_CASE("shared_log_client_write_scan_test", "[write][scan]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(NUM_BLOCKS, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_shared_log_blocks(block_names, 134217728);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto tree = std::make_shared<directory_tree>(alloc, sm);

  data_status status = tree->create("/sandbox/file.txt", "shared_log", "/tmp", NUM_BLOCKS, 1, 0, 0,
                                  {"0"}, {"regular"});

  shared_log_client client(tree, "/sandbox/file.txt", status);

  for (std::size_t i = 0; i < 1000; ++i) {
    std::vector<std::string> stream = {std::to_string(i)+"_stream"};
    REQUIRE(client.write(std::to_string(i), std::to_string(i)+"_data", stream) == 2 * std::to_string(i).size() + 12);
  }


  std::vector<std::string> buffer;

  for (std::size_t i = 0; i < 1000; ++i) {
    buffer.clear();
    std::vector<std::string> stream = {std::to_string(i)+"_stream"};
    REQUIRE(client.scan(buffer, std::to_string(i), std::to_string(i), stream) == 1);
    REQUIRE(buffer[0] == std::to_string(i)+"_data");
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.trim(std::to_string(i), std::to_string(i)) == true);
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