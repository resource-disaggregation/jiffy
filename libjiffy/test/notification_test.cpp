#include <catch.hpp>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <iostream>

#include "jiffy/directory/fs/directory_tree.h"
#include "jiffy/directory/fs/directory_server.h"
#include "jiffy/storage/client/hash_table_client.h"
#include "jiffy/storage/client/data_structure_listener.h"
#include "jiffy/storage/hashtable/hash_table_partition.h"
#include "jiffy/storage/hashtable/hash_slot.h"
#include "jiffy/storage/manager/storage_management_server.h"
#include "jiffy/storage/manager/storage_management_client.h"
#include "jiffy/storage/manager/storage_manager.h"
#include "jiffy/storage/service/block_server.h"
#include "test_utils.h"

using namespace ::jiffy::storage;
using namespace ::jiffy::directory;
using namespace ::jiffy::utils;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::protocol;

#define NUM_BLOCKS 1
#define HOST "127.0.0.1"
#define DIRECTORY_SERVICE_PORT 9090
#define STORAGE_SERVICE_PORT 9091
#define STORAGE_MANAGEMENT_PORT 9092

TEST_CASE("notification_test", "[subscribe][get_message]") {

  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(NUM_BLOCKS,
                                                  STORAGE_SERVICE_PORT,
                                                  STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_hash_table_blocks(block_names);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto tree = std::make_shared<directory_tree>(alloc, sm);

  data_status status = tree->create("/sandbox/file.txt",  "hashtable", "/tmp", NUM_BLOCKS, 1, 0, 0, {"0_65536"},
      {"regular"});
  hash_table_client table(tree, "/sandbox/file.txt", status);

  std::string op1 = "put", op2 = "get";
  std::string key = "msg1";

  {
    data_structure_listener sub1("/sandbox/file.txt", status);
    data_structure_listener sub2("/sandbox/file.txt", status);
    data_structure_listener sub3("/sandbox/file.txt", status);

    REQUIRE_NOTHROW(sub1.subscribe({op1}));
    REQUIRE_NOTHROW(sub2.subscribe({op1, op2}));
    REQUIRE_NOTHROW(sub3.subscribe({op2}));

    REQUIRE_NOTHROW(table.put(key, "random data"));
    REQUIRE_NOTHROW(table.get(key));

    REQUIRE(sub1.get_notification() == std::make_pair(op1, key));
    REQUIRE(sub2.get_notification() == std::make_pair(op1, key));
    REQUIRE(sub2.get_notification() == std::make_pair(op2, key));
    REQUIRE(sub3.get_notification() == std::make_pair(op2, key));

    REQUIRE_THROWS_AS(sub1.get_notification(100), std::out_of_range);
    REQUIRE_THROWS_AS(sub2.get_notification(100), std::out_of_range);
    REQUIRE_THROWS_AS(sub3.get_notification(100), std::out_of_range);

    REQUIRE_NOTHROW(sub1.unsubscribe({op1}));
    REQUIRE_NOTHROW(sub2.unsubscribe({op2}));

    REQUIRE_NOTHROW(table.remove(key));
    REQUIRE_NOTHROW(table.put(key, "random data"));
    REQUIRE_NOTHROW(table.get(key));

    REQUIRE(sub2.get_notification() == std::make_pair(op1, key));
    REQUIRE(sub3.get_notification() == std::make_pair(op2, key));

    REQUIRE_THROWS_AS(sub1.get_notification(100), std::out_of_range);
    REQUIRE_THROWS_AS(sub2.get_notification(100), std::out_of_range);
    REQUIRE_THROWS_AS(sub3.get_notification(100), std::out_of_range);
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
