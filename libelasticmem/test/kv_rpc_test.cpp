#include <catch.hpp>
#include <thrift/transport/TTransportException.h>
#include "../src/storage/kv/kv_block.h"
#include "../src/storage/service/block_server.h"
#include "test_utils.h"
#include "../src/storage/service/block_chain_client.h"

using namespace ::elasticmem::storage;
using namespace ::apache::thrift::transport;

#define NUM_BLOCKS 1
#define HOST "127.0.0.1"
#define PORT 9090

static auto blocks = test_utils::init_kv_blocks(NUM_BLOCKS, PORT, 0, 0);

TEST_CASE("rpc_put_get_test", "[put][get]") {
  auto server = block_server::create(blocks, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  block_chain_client client({block_name_parser::make(HOST, PORT, 0, 0, 0, 0)});
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.put(std::to_string(i), std::to_string(i)).get() == "ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.get(std::to_string(i)).get() == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE(client.get(std::to_string(i)).get() == "key_not_found");
  }

  server->stop();
  blocks[0]->reset();
  
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_put_update_get_test", "[put][update][get]") {
  auto server = block_server::create(blocks, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  block_chain_client client({block_name_parser::make(HOST, PORT, 0, 0, 0, 0)});
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.put(std::to_string(i), std::to_string(i)).get() == "ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.get(std::to_string(i)).get() == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.update(std::to_string(i), std::to_string(i + 1000)).get() == "ok");
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE(client.update(std::to_string(i), std::to_string(i + 1000)).get() == "key_not_found");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.get(std::to_string(i)).get() == std::to_string(i + 1000));
  }

  server->stop();
  blocks[0]->reset();
  
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_put_remove_get_test", "[put][update][get]") {
  auto server = block_server::create(blocks, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  block_chain_client client({block_name_parser::make(HOST, PORT, 0, 0, 0, 0)});
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.put(std::to_string(i), std::to_string(i)).get() == "ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.get(std::to_string(i)).get() == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.remove(std::to_string(i)).get() == "ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.get(std::to_string(i)).get() == "key_not_found");
  }

  server->stop();
  blocks[0]->reset();
  
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_storage_size_test", "[put][size][storage_size][reset]") {
  auto server = block_server::create(blocks, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  block_chain_client client({block_name_parser::make(HOST, PORT, 0, 0, 0, 0)});
  REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[0])->empty());
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.put(std::to_string(i), std::to_string(i)).get() == "ok");
  }
  REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[0])->size() == 1000);

  server->stop();
  blocks[0]->reset();
  
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}