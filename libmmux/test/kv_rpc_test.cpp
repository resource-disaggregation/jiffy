#include <catch.hpp>
#include <thrift/transport/TTransportException.h>
#include "../src/mmux/storage/kv/kv_block.h"
#include "../src/mmux/storage/service/block_server.h"
#include "test_utils.h"
#include "../src/mmux/storage/client/replica_chain_client.h"

using namespace ::mmux::storage;
using namespace ::apache::thrift::transport;

#define NUM_BLOCKS 1
#define HOST "127.0.0.1"
#define PORT 9090

TEST_CASE("rpc_put_get_test", "[put][get]") {
  auto blocks = test_utils::init_kv_blocks(NUM_BLOCKS, PORT, 0, 0);
  auto server = block_server::create(blocks, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  replica_chain_client client({block_name_parser::make(HOST, PORT, 0, 0, 0, 0)});
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command(kv_op_id::put, {std::to_string(i), std::to_string(i)}).front() == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command(kv_op_id::get, {std::to_string(i)}).front() == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE(client.run_command(kv_op_id::get, {std::to_string(i)}).front() == "!key_not_found");
  }

  server->stop();
  
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_put_update_get_test", "[put][update][get]") {
  auto blocks = test_utils::init_kv_blocks(NUM_BLOCKS, PORT, 0, 0);
  auto server = block_server::create(blocks, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  replica_chain_client client({block_name_parser::make(HOST, PORT, 0, 0, 0, 0)});
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command(kv_op_id::put, {std::to_string(i), std::to_string(i)}).front() == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command(kv_op_id::get, {std::to_string(i)}).front() == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command(kv_op_id::update, {std::to_string(i), std::to_string(i + 1000)}).front() == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE(client.run_command(kv_op_id::update, {std::to_string(i), std::to_string(i + 1000)}).front() == "!key_not_found");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command(kv_op_id::get, {std::to_string(i)}).front() == std::to_string(i + 1000));
  }

  server->stop();
  
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_put_remove_get_test", "[put][remove][get]") {
  auto blocks = test_utils::init_kv_blocks(NUM_BLOCKS, PORT, 0, 0);
  auto server = block_server::create(blocks, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  replica_chain_client client({block_name_parser::make(HOST, PORT, 0, 0, 0, 0)});
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command(kv_op_id::put, {std::to_string(i), std::to_string(i)}).front() == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command(kv_op_id::get, {std::to_string(i)}).front() == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command(kv_op_id::remove, {std::to_string(i)}).front() == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command(kv_op_id::get, {std::to_string(i)}).front() == "!key_not_found");
  }

  server->stop();
  
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_locked_put_get_test", "[put][get]") {
  auto blocks = test_utils::init_kv_blocks(NUM_BLOCKS, PORT, 0, 0);
  auto server = block_server::create(blocks, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  replica_chain_client rcc({block_name_parser::make(HOST, PORT, 0, 0, 0, 0)});
  auto client = rcc.lock();
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client->run_command(kv_op_id::locked_put, {std::to_string(i), std::to_string(i)}).front() == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client->run_command(kv_op_id::locked_get, {std::to_string(i)}).front() == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE(client->run_command(kv_op_id::locked_get, {std::to_string(i)}).front() == "!key_not_found");
  }

  server->stop();

  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_locked_put_update_get_test", "[put][update][get]") {
  auto blocks = test_utils::init_kv_blocks(NUM_BLOCKS, PORT, 0, 0);
  auto server = block_server::create(blocks, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  replica_chain_client rcc({block_name_parser::make(HOST, PORT, 0, 0, 0, 0)});
  auto client = rcc.lock();
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client->run_command(kv_op_id::locked_put, {std::to_string(i), std::to_string(i)}).front() == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client->run_command(kv_op_id::locked_get, {std::to_string(i)}).front() == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client->run_command(kv_op_id::locked_update, {std::to_string(i), std::to_string(i + 1000)}).front() == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE(client->run_command(kv_op_id::locked_update, {std::to_string(i), std::to_string(i + 1000)}).front() == "!key_not_found");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client->run_command(kv_op_id::locked_get, {std::to_string(i)}).front() == std::to_string(i + 1000));
  }

  server->stop();

  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_locked_put_remove_get_test", "[put][remove][get]") {
  auto blocks = test_utils::init_kv_blocks(NUM_BLOCKS, PORT, 0, 0);
  auto server = block_server::create(blocks, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  replica_chain_client rcc({block_name_parser::make(HOST, PORT, 0, 0, 0, 0)});
  auto client = rcc.lock();
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client->run_command(kv_op_id::locked_put, {std::to_string(i), std::to_string(i)}).front() == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client->run_command(kv_op_id::locked_get, {std::to_string(i)}).front() == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client->run_command(kv_op_id::locked_remove, {std::to_string(i)}).front() == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client->run_command(kv_op_id::locked_get, {std::to_string(i)}).front() == "!key_not_found");
  }

  server->stop();
  
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}