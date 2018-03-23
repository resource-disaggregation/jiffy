#include <catch.hpp>
#include <thrift/transport/TTransportException.h>
#include "../src/storage/block/kv/kv_block.h"
#include "../src/storage/service/block_client.h"
#include "../src/storage/service/block_server.h"

using namespace ::elasticmem::storage;
using namespace ::apache::thrift::transport;

#define NUM_BLOCKS 1
#define HOST "127.0.0.1"
#define PORT 9090

static std::vector<std::shared_ptr<kv_block>> init_blocks() {
  std::vector<std::shared_ptr<kv_block>> blks;
  blks.resize(NUM_BLOCKS);
  for (auto &block : blks) {
    block = std::make_shared<kv_block>();
  }
  return blks;
}

static std::vector<std::shared_ptr<subscription_map>> init_submaps() {
  std::vector<std::shared_ptr<subscription_map>> sub_maps;
  sub_maps.resize(NUM_BLOCKS);
  for (auto &sub_map : sub_maps) {
    sub_map = std::make_shared<subscription_map>();
  }
  return sub_maps;
}

static void wait_till_server_ready(const std::string &host, int port) {
  bool check = true;
  while (check) {
    try {
      block_client(host, port, 0);
      check = false;
    } catch (TTransportException &e) {
      usleep(100000);
    }
  }
}

static std::vector<std::shared_ptr<kv_block>> blocks = init_blocks();
static std::vector<std::shared_ptr<subscription_map>> sub_maps = init_submaps();

TEST_CASE("rpc_put_get_test", "[put][get]") {
  auto server = block_server::create(blocks, sub_maps, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  wait_till_server_ready(HOST, PORT);

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
  blocks[0]->clear();
  sub_maps[0]->clear();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_put_update_get_test", "[put][update][get]") {
  auto server = block_server::create(blocks, sub_maps, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  wait_till_server_ready(HOST, PORT);

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
  blocks[0]->clear();
  sub_maps[0]->clear();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_put_remove_get_test", "[put][update][get]") {
  auto server = block_server::create(blocks, sub_maps, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  wait_till_server_ready(HOST, PORT);

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
  blocks[0]->clear();
  sub_maps[0]->clear();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_storage_size_test", "[put][size][storage_size][clear]") {
  auto server = block_server::create(blocks, sub_maps, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  wait_till_server_ready(HOST, PORT);

  block_client block_client(HOST, PORT, 0);
  REQUIRE(blocks[0]->empty());
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(block_client.put(std::to_string(i), std::to_string(i)));
  }
  REQUIRE(blocks[0]->size() == 1000);

  server->stop();
  blocks[0]->clear();
  sub_maps[0]->clear();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}