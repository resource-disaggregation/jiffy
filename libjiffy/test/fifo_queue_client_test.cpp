#include <catch.hpp>
#include <thrift/transport/TTransportException.h>
#include <thread>
#include "test_utils.h"
#include "jiffy/directory/fs/directory_tree.h"
#include "jiffy/directory/fs/directory_server.h"
#include "jiffy/storage/manager/storage_management_server.h"
#include "jiffy/storage/manager/storage_management_client.h"
#include "jiffy/storage/manager/storage_manager.h"
#include "jiffy/storage/fifoqueue/fifo_queue_partition.h"
#include "jiffy/storage/service/block_server.h"
#include "jiffy/storage/client/fifo_queue_client.h"


using namespace ::jiffy::storage;
using namespace ::jiffy::directory;
using namespace ::apache::thrift::transport;

#define NUM_BLOCKS 1
#define HOST "127.0.0.1"
#define DIRECTORY_SERVICE_PORT 9090
#define STORAGE_SERVICE_PORT 9091
#define STORAGE_MANAGEMENT_PORT 9092

TEST_CASE("fifo_queue_client_enqueue_dequeue_test", "[enqueue][dequeue]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(NUM_BLOCKS, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_fifo_queue_blocks(block_names, 134217728, 0, 1);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto tree = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(tree, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  data_status status = tree->create("/sandbox/file.txt", "fifoqueue", "/tmp", NUM_BLOCKS, 1, 0, 0,
                                    {"0"}, {"regular"});

  fifo_queue_client client(tree, "/sandbox/file.txt", status);

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(client.enqueue(std::to_string(i)));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.read_next() == std::to_string(i));
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.front() == std::to_string(i));
    REQUIRE_NOTHROW(client.dequeue());
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE_THROWS_AS(client.dequeue(), std::logic_error);
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


TEST_CASE("fifo_queue_client_enqueue_length_dequeue_test", "[enqueue][dequeue]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(NUM_BLOCKS, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_fifo_queue_blocks(block_names, 134217728);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto tree = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(tree, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  data_status status = tree->create("/sandbox/file.txt", "fifoqueue", "/tmp", NUM_BLOCKS, 1, 0, 0,
                                     {"0"}, {"regular"});

  fifo_queue_client client(tree, "/sandbox/file.txt", status);

  std::size_t length = 0;
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(client.enqueue(std::to_string(i)));
    length += std::to_string(i).size();
  }
  REQUIRE(client.length() == length);

  for (std::size_t i = 0; i < 500; ++i) {
    REQUIRE(client.front() == std::to_string(i));
    REQUIRE_NOTHROW(client.dequeue());
    length -= std::to_string(i).size();
  }
  REQUIRE(client.length() == length);

  for (std::size_t i = 500; i < 1000; ++i) {
    REQUIRE(client.front() == std::to_string(i));
    REQUIRE_NOTHROW(client.dequeue());
    length -= std::to_string(i).size();
  }

  REQUIRE(client.length() == length);
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


TEST_CASE("fifo_queue_client_enqueue_in_rate_out_rate_dequeue_test", "[enqueue][dequeue]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(NUM_BLOCKS, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_fifo_queue_blocks(block_names, 134217728);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto tree = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(tree, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  data_status status = tree->create("/sandbox/file.txt", "fifoqueue", "/tmp", NUM_BLOCKS, 1, 0, 0,
                                     {"0"}, {"regular"});

  fifo_queue_client client(tree, "/sandbox/file.txt", status);
  double rate;
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(client.enqueue(std::to_string(i)));
  }

  REQUIRE_NOTHROW(rate = client.in_rate());
  LOG(log_level::info) << "In rate: " << rate;
  REQUIRE_NOTHROW(rate = client.out_rate());
  LOG(log_level::info) << "Out rate: " << rate;


  for (std::size_t i = 0; i < 500; ++i) {
    REQUIRE(client.front() == std::to_string(i));
    REQUIRE_NOTHROW(client.dequeue());
  }

  REQUIRE_NOTHROW(rate = client.in_rate());
  LOG(log_level::info) << "In rate: " << rate;
  REQUIRE_NOTHROW(rate = client.out_rate());
  LOG(log_level::info) << "Out rate: " << rate;


  for (std::size_t i = 500; i < 1000; ++i) {
    REQUIRE(client.front() == std::to_string(i));
    REQUIRE_NOTHROW(client.dequeue());
  }


  REQUIRE_NOTHROW(rate = client.in_rate());
  LOG(log_level::info) << "In rate: " << rate;
  REQUIRE_NOTHROW(rate = client.out_rate());
  LOG(log_level::info) << "Out rate: " << rate;

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
