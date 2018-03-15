#include <catch.hpp>
#include <thrift/transport/TTransportException.h>
#include "../src/kv/block/kv_block.h"
#include "../src/kv/rpc/kv_client.h"
#include "../src/kv/rpc/kv_rpc_server.h"

using namespace ::elasticmem::kv;
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

static void wait_till_server_ready(const std::string &host, int port) {
  bool check = true;
  while (check) {
    try {
      kv_client(host, port, 0);
      check = false;
    } catch (TTransportException &e) {
      usleep(100000);
    }
  }
}

static std::vector<std::shared_ptr<kv_block>> blocks = init_blocks();

TEST_CASE("rpc_put_get_test", "[put][get]") {
  auto server = kv_rpc_server::create(blocks, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  wait_till_server_ready(HOST, PORT);

  kv_client block_client(HOST, PORT, 0);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(block_client.put(std::to_string(i), std::to_string(i)));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block_client.get(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE_THROWS_AS(block_client.get(std::to_string(i)), kv_rpc_exception);
  }

  server->stop();
  blocks[0]->clear();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_put_update_get_test", "[put][update][get]") {
  auto server = kv_rpc_server::create(blocks, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  wait_till_server_ready(HOST, PORT);

  kv_client block_client(HOST, PORT, 0);
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
    REQUIRE_THROWS_AS(block_client.update(std::to_string(i), std::to_string(i + 1000)), kv_rpc_exception);
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block_client.get(std::to_string(i)) == std::to_string(i + 1000));
  }

  server->stop();
  blocks[0]->clear();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_put_remove_get_test", "[put][update][get]") {
  auto server = kv_rpc_server::create(blocks, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  wait_till_server_ready(HOST, PORT);

  kv_client block_client(HOST, PORT, 0);
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
    REQUIRE_THROWS_AS(block_client.get(std::to_string(i)), kv_rpc_exception);
  }

  server->stop();
  blocks[0]->clear();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_storage_size_test", "[put][size][storage_size][clear]") {
  auto server = kv_rpc_server::create(blocks, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  wait_till_server_ready(HOST, PORT);

  kv_client block_client(HOST, PORT, 0);
  REQUIRE(block_client.empty());
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(block_client.put(std::to_string(i), std::to_string(i)));
  }
  REQUIRE(block_client.size() == 1000);

  server->stop();
  blocks[0]->clear();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}