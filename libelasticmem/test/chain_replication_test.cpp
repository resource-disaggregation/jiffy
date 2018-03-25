#include <catch.hpp>
#include "test_utils.h"
#include "../src/storage/manager/storage_management_server.h"
#include "../src/storage/service/block_server.h"
#include "../src/storage/manager/storage_manager.h"

#define HOST "127.0.0.1"
#define SERVICE_PORT 9090
#define MANAGEMENT_PORT 9091
#define NUM_BLOCKS 3

using namespace elasticmem::storage;

TEST_CASE("kv_no_failure_test", "[put][get]") {
  auto blocks = test_utils::init_kv_blocks(NUM_BLOCKS, SERVICE_PORT, MANAGEMENT_PORT, 0);
  auto sub_maps = test_utils::init_submaps(NUM_BLOCKS);

  auto management_server = storage_management_server::create(blocks, HOST, MANAGEMENT_PORT);
  std::thread management_serve_thread([&management_server] {
    management_server->serve();
  });
  test_utils::wait_till_server_ready(HOST, MANAGEMENT_PORT);

  auto kv_server = block_server::create(blocks, sub_maps, HOST, SERVICE_PORT);
  std::thread kv_serve_thread([&kv_server] {
    kv_server->serve();
  });
  test_utils::wait_till_server_ready(HOST, SERVICE_PORT);

  storage_manager manager;
  REQUIRE_NOTHROW(manager.setup_block(blocks[0]->name(), "", chain_role::head, blocks[1]->name()));
  REQUIRE_NOTHROW(manager.setup_block(blocks[1]->name(), "", chain_role::mid, blocks[2]->name()));
  REQUIRE_NOTHROW(manager.setup_block(blocks[2]->name(), "", chain_role::tail, "nil"));

  block_client head(HOST, SERVICE_PORT, 0);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(head.put(std::to_string(i), std::to_string(i)));
  }
  block_client tail(HOST, SERVICE_PORT, NUM_BLOCKS - 1);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(tail.get(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE_THROWS_AS(tail.get(std::to_string(i)), block_exception);
  }

  // Ensure all three blocks have the datsa
  for (size_t i = 0; i < NUM_BLOCKS; i++) {
    for (std::size_t j = 0; j < 1000; j++) {
      REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[i])->get(std::to_string(j)) == std::to_string(j));
    }
  }

  management_server->stop();
  kv_server->stop();

  if (kv_serve_thread.joinable())
    kv_serve_thread.join();

  if (management_serve_thread.joinable())
    management_serve_thread.join();
}