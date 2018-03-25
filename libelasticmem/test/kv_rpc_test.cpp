#include <catch.hpp>
#include <thrift/transport/TTransportException.h>
#include "../src/storage/kv/kv_block.h"
#include "../src/storage/service/block_server.h"
#include "test_utils.h"

using namespace ::elasticmem::storage;
using namespace ::apache::thrift::transport;

#define NUM_BLOCKS 1
#define HOST "127.0.0.1"
#define PORT 9090

static auto blocks = test_utils::init_kv_blocks(NUM_BLOCKS, PORT, 0, 0);
static auto sub_maps = test_utils::init_submaps(NUM_BLOCKS);

TEST_CASE("rpc_put_get_test", "[put][get]") {
  auto server = block_server::create(blocks, sub_maps, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  block_client block_client(HOST, PORT, 0);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(block_client.put(std::to_string(i), std::to_string(i)));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block_client.get(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE_THROWS_AS(block_client.get(std::to_string(i)), block_exception);
  }

  server->stop();
  blocks[0]->reset();
  sub_maps[0]->clear();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_put_update_get_test", "[put][update][get]") {
  auto server = block_server::create(blocks, sub_maps, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  block_client block_client(HOST, PORT, 0);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(block_client.put(std::to_string(i), std::to_string(i)));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block_client.get(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(block_client.update(std::to_string(i), std::to_string(i + 1000)));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE_THROWS_AS(block_client.update(std::to_string(i), std::to_string(i + 1000)), block_exception);
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block_client.get(std::to_string(i)) == std::to_string(i + 1000));
  }

  server->stop();
  blocks[0]->reset();
  sub_maps[0]->clear();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_put_remove_get_test", "[put][update][get]") {
  auto server = block_server::create(blocks, sub_maps, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  block_client block_client(HOST, PORT, 0);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(block_client.put(std::to_string(i), std::to_string(i)));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block_client.get(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(block_client.remove(std::to_string(i)));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_THROWS_AS(block_client.get(std::to_string(i)), block_exception);
  }

  server->stop();
  blocks[0]->reset();
  sub_maps[0]->clear();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_storage_size_test", "[put][size][storage_size][reset]") {
  auto server = block_server::create(blocks, sub_maps, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  block_client block_client(HOST, PORT, 0);
  REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[0])->empty());
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(block_client.put(std::to_string(i), std::to_string(i)));
  }
  REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[0])->size() == 1000);

  server->stop();
  blocks[0]->reset();
  sub_maps[0]->clear();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}