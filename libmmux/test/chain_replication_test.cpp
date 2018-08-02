#include <catch.hpp>
#include "test_utils.h"
#include "../src/mmux/storage/manager/storage_management_server.h"
#include "../src/mmux/storage/service/block_server.h"
#include "../src/mmux/storage/manager/storage_manager.h"
#include "../src/mmux/directory/fs/directory_server.h"
#include "../src/mmux/storage/service/chain_server.h"
#include "../src/mmux/storage/client/replica_chain_client.h"

#define HOST "127.0.0.1"
#define DIRECTORY_SERVICE_PORT 9090
#define STORAGE_SERVICE_PORT_N(i) (9091 + (i) * 3)
#define STORAGE_MANAGEMENT_PORT_N(i) (9092 + (i) * 3)
#define STORAGE_CHAIN_PORT_N(i) (9093 + (i) * 3)
#define NUM_BLOCKS 3

using namespace apache::thrift::server;
using namespace mmux::storage;
using namespace mmux::directory;

TEST_CASE("kv_no_failure_test", "[put][get]") {
  std::vector<std::vector<std::string>> block_names(NUM_BLOCKS);
  std::vector<std::vector<std::shared_ptr<mmux::storage::chain_module>>> blocks(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> management_servers(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> chain_servers(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> kv_servers(NUM_BLOCKS);
  std::vector<std::thread> server_threads;

  auto alloc = std::make_shared<sequential_block_allocator>();
  for (int32_t i = 0; i < NUM_BLOCKS; i++) {
    block_names[i] = test_utils::init_block_names(1,
                                                  STORAGE_SERVICE_PORT_N(i),
                                                  STORAGE_MANAGEMENT_PORT_N(i),
                                                  0,
                                                  STORAGE_CHAIN_PORT_N(i));
    alloc->add_blocks(block_names[i]);
    blocks[i] = test_utils::init_kv_blocks(block_names[i]);

    management_servers[i] = storage_management_server::create(blocks[i], HOST, STORAGE_MANAGEMENT_PORT_N(i));
    server_threads.emplace_back([i, &management_servers] { management_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT_N(i));

    chain_servers[i] = chain_server::create(blocks[i], HOST, STORAGE_CHAIN_PORT_N(i));
    server_threads.emplace_back([i, &chain_servers] { chain_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_CHAIN_PORT_N(i));

    kv_servers[i] = block_server::create(blocks[i], HOST, STORAGE_SERVICE_PORT_N(i));
    server_threads.emplace_back([i, &kv_servers] { kv_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT_N(i));
  }

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dserver = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  server_threads.emplace_back([&] { dserver->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  t->create("/file", "/tmp", 1, 3, 0);
  auto chain = t->dstatus("/file").data_blocks()[0];

  replica_chain_client client(t, "/file", chain, 100);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command(kv_op_id::put, {std::to_string(i), std::to_string(i)}).front() == "!ok");
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command(kv_op_id::get, {std::to_string(i)}).front() == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE(client.run_command(kv_op_id::get, {std::to_string(i)}).front() == "!key_not_found");
  }

  // Ensure all three blocks have the data
  for (size_t i = 0; i < NUM_BLOCKS; i++) {
    for (std::size_t j = 0; j < 1000; j++) {
      REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[i][0])->get(std::to_string(j)) == std::to_string(j));
    }
  }

  for (const auto &kv: kv_servers) {
    kv->stop();
  }

  for (const auto &c: chain_servers) {
    c->stop();
  }

  for (const auto &m: management_servers) {
    m->stop();
  }

  dserver->stop();

  for (auto &st: server_threads) {
    if (st.joinable())
      st.join();
  }
}

TEST_CASE("kv_head_failure_test", "[put][get]") {
  std::vector<std::vector<std::string>> block_names(NUM_BLOCKS);
  std::vector<std::vector<std::shared_ptr<mmux::storage::chain_module>>> blocks(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> management_servers(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> chain_servers(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> kv_servers(NUM_BLOCKS);
  std::vector<std::thread> server_threads;

  auto alloc = std::make_shared<sequential_block_allocator>();
  for (int32_t i = 0; i < NUM_BLOCKS; i++) {
    block_names[i] = test_utils::init_block_names(1,
                                                  STORAGE_SERVICE_PORT_N(i),
                                                  STORAGE_MANAGEMENT_PORT_N(i),
                                                  0,
                                                  STORAGE_CHAIN_PORT_N(i));
    alloc->add_blocks(block_names[i]);
    blocks[i] = test_utils::init_kv_blocks(block_names[i]);

    management_servers[i] = storage_management_server::create(blocks[i], HOST, STORAGE_MANAGEMENT_PORT_N(i));
    server_threads.emplace_back([i, &management_servers] { management_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT_N(i));

    chain_servers[i] = chain_server::create(blocks[i], HOST, STORAGE_CHAIN_PORT_N(i));
    server_threads.emplace_back([i, &chain_servers] { chain_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_CHAIN_PORT_N(i));

    kv_servers[i] = block_server::create(blocks[i], HOST, STORAGE_SERVICE_PORT_N(i));
    server_threads.emplace_back([i, &kv_servers] { kv_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT_N(i));
  }

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dserver = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  server_threads.emplace_back([&] { dserver->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  t->create("/file", "/tmp", 1, 3, 0);
  auto chain = t->dstatus("/file").data_blocks()[0];
  replica_chain_client client(t, "/file", chain, 100);

  kv_servers[0]->stop();
  chain_servers[0]->stop();
  management_servers[0]->stop();

  for (int32_t i = 0; i < 3; i++) {
    if (server_threads[i].joinable()) {
      server_threads[i].join();
    }
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command(kv_op_id::put, {std::to_string(i), std::to_string(i)}).front() == "!ok");
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command(kv_op_id::get, {std::to_string(i)}).front() == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE(client.run_command(kv_op_id::get, {std::to_string(i)}).front() == "!key_not_found");
  }

  // Ensure all three blocks have the data
  for (size_t i = 1; i < NUM_BLOCKS; i++) {
    for (std::size_t j = 0; j < 1000; j++) {
      REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[i][0])->get(std::to_string(j)) == std::to_string(j));
    }
  }

  for (const auto &kv: kv_servers) {
    kv->stop();
  }

  for (const auto &c: chain_servers) {
    c->stop();
  }

  for (const auto &m: management_servers) {
    m->stop();
  }

  dserver->stop();

  for (auto &st: server_threads) {
    if (st.joinable())
      st.join();
  }
}

TEST_CASE("kv_mid_failure_test", "[put][get]") {
  std::vector<std::vector<std::string>> block_names(NUM_BLOCKS);
  std::vector<std::vector<std::shared_ptr<mmux::storage::chain_module>>> blocks(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> management_servers(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> chain_servers(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> kv_servers(NUM_BLOCKS);
  std::vector<std::thread> server_threads;

  auto alloc = std::make_shared<sequential_block_allocator>();
  for (int32_t i = 0; i < NUM_BLOCKS; i++) {
    block_names[i] = test_utils::init_block_names(1,
                                                  STORAGE_SERVICE_PORT_N(i),
                                                  STORAGE_MANAGEMENT_PORT_N(i),
                                                  0,
                                                  STORAGE_CHAIN_PORT_N(i));
    alloc->add_blocks(block_names[i]);
    blocks[i] = test_utils::init_kv_blocks(block_names[i]);

    management_servers[i] = storage_management_server::create(blocks[i], HOST, STORAGE_MANAGEMENT_PORT_N(i));
    server_threads.emplace_back([i, &management_servers] { management_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT_N(i));

    chain_servers[i] = chain_server::create(blocks[i], HOST, STORAGE_CHAIN_PORT_N(i));
    server_threads.emplace_back([i, &chain_servers] { chain_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_CHAIN_PORT_N(i));

    kv_servers[i] = block_server::create(blocks[i], HOST, STORAGE_SERVICE_PORT_N(i));
    server_threads.emplace_back([i, &kv_servers] { kv_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT_N(i));
  }

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto dserver = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  server_threads.emplace_back([&] { dserver->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  t->create("/file", "/tmp", 1, 3, 0);
  auto chain = t->dstatus("/file").data_blocks()[0];

  replica_chain_client client(t, "/file", chain, 100);

  kv_servers[1]->stop();
  management_servers[1]->stop();
  chain_servers[1]->stop();
  for (int32_t i = 3; i < 6; i++) {
    if (server_threads[i].joinable()) {
      server_threads[i].join();
    }
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command(kv_op_id::put, {std::to_string(i), std::to_string(i)}).front() == "!ok");
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command(kv_op_id::get, {std::to_string(i)}).front() == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE(client.run_command(kv_op_id::get, {std::to_string(i)}).front() == "!key_not_found");
  }

  // Ensure all three blocks have the data
  for (size_t i = 0; i < NUM_BLOCKS; i++) {
    if (i == 1) continue;
    for (std::size_t j = 0; j < 1000; j++) {
      REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[i][0])->get(std::to_string(j)) == std::to_string(j));
    }
  }

  for (const auto &kv: kv_servers) {
    kv->stop();
  }

  for (const auto &c: chain_servers) {
    c->stop();
  }

  for (const auto &m: management_servers) {
    m->stop();
  }

  dserver->stop();

  for (auto &st: server_threads) {
    if (st.joinable())
      st.join();
  }
}

TEST_CASE("kv_tail_failure_test", "[put][get]") {
  std::vector<std::vector<std::string>> block_names(NUM_BLOCKS);
  std::vector<std::vector<std::shared_ptr<mmux::storage::chain_module>>> blocks(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> management_servers(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> chain_servers(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> kv_servers(NUM_BLOCKS);
  std::vector<std::thread> server_threads;

  auto alloc = std::make_shared<sequential_block_allocator>();
  for (int32_t i = 0; i < NUM_BLOCKS; i++) {
    block_names[i] = test_utils::init_block_names(1,
                                                  STORAGE_SERVICE_PORT_N(i),
                                                  STORAGE_MANAGEMENT_PORT_N(i),
                                                  0,
                                                  STORAGE_CHAIN_PORT_N(i));
    alloc->add_blocks(block_names[i]);
    blocks[i] = test_utils::init_kv_blocks(block_names[i]);

    management_servers[i] = storage_management_server::create(blocks[i], HOST, STORAGE_MANAGEMENT_PORT_N(i));
    server_threads.emplace_back([i, &management_servers] { management_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT_N(i));

    chain_servers[i] = chain_server::create(blocks[i], HOST, STORAGE_CHAIN_PORT_N(i));
    server_threads.emplace_back([i, &chain_servers] { chain_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_CHAIN_PORT_N(i));

    kv_servers[i] = block_server::create(blocks[i], HOST, STORAGE_SERVICE_PORT_N(i));
    server_threads.emplace_back([i, &kv_servers] { kv_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT_N(i));
  }

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dserver = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  server_threads.emplace_back([&] { dserver->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  t->create("/file", "/tmp", 1, 3, 0);
  auto chain = t->dstatus("/file").data_blocks()[0];

  replica_chain_client client(t, "/file", chain, 100);

  kv_servers[2]->stop();
  management_servers[2]->stop();
  chain_servers[2]->stop();
  for (int32_t i = 6; i < 9; i++) {
    if (server_threads[i].joinable()) {
      server_threads[i].join();
    }
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command(kv_op_id::put, {std::to_string(i), std::to_string(i)}).front() == "!ok");
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command(kv_op_id::get, {std::to_string(i)}).front() == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE(client.run_command(kv_op_id::get, {std::to_string(i)}).front() == "!key_not_found");
  }

  // Ensure all three blocks have the data
  for (size_t i = 0; i < NUM_BLOCKS - 1; i++) {
    for (std::size_t j = 0; j < 1000; j++) {
      REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[i][0])->get(std::to_string(j)) == std::to_string(j));
    }
  }

  for (const auto &kv: kv_servers) {
    kv->stop();
  }

  for (const auto &c: chain_servers) {
    c->stop();
  }

  for (const auto &m: management_servers) {
    m->stop();
  }

  dserver->stop();

  for (auto &st: server_threads) {
    if (st.joinable())
      st.join();
  }
}

TEST_CASE("kv_add_block_test", "[put][get]") {
  std::vector<std::vector<std::string>> block_names(NUM_BLOCKS);
  std::vector<std::vector<std::shared_ptr<mmux::storage::chain_module>>> blocks(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> management_servers(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> chain_servers(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> kv_servers(NUM_BLOCKS);
  std::vector<std::thread> server_threads;

  auto alloc = std::make_shared<sequential_block_allocator>();
  for (int32_t i = 0; i < NUM_BLOCKS; i++) {
    block_names[i] = test_utils::init_block_names(1,
                                                  STORAGE_SERVICE_PORT_N(i),
                                                  STORAGE_MANAGEMENT_PORT_N(i),
                                                  0,
                                                  STORAGE_CHAIN_PORT_N(i));
    alloc->add_blocks(block_names[i]);
    blocks[i] = test_utils::init_kv_blocks(block_names[i]);

    management_servers[i] = storage_management_server::create(blocks[i], HOST, STORAGE_MANAGEMENT_PORT_N(i));
    server_threads.emplace_back([i, &management_servers] { management_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT_N(i));

    chain_servers[i] = chain_server::create(blocks[i], HOST, STORAGE_CHAIN_PORT_N(i));
    server_threads.emplace_back([i, &chain_servers] { chain_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_CHAIN_PORT_N(i));

    kv_servers[i] = block_server::create(blocks[i], HOST, STORAGE_SERVICE_PORT_N(i));
    server_threads.emplace_back([i, &kv_servers] { kv_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT_N(i));
  }

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto dserver = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  server_threads.emplace_back([&] { dserver->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  t->create("/file", "/tmp", 1, 2, 0);

  auto chain = t->dstatus("/file").data_blocks()[0].block_names;
  {
    replica_chain_client client(t, "/file", chain, 100);
    for (std::size_t i = 0; i < 1000; ++i) {
      REQUIRE(client.run_command(kv_op_id::put, {std::to_string(i), std::to_string(i)}).front() == "!ok");
    }
  }

  auto fixed_chain = t->add_replica_to_chain("/file", t->dstatus("/file").data_blocks()[0]);

  {
    replica_chain_client client2(t, "/file", fixed_chain, 100);
    for (std::size_t i = 0; i < 1000; ++i) {
      REQUIRE(client2.run_command(kv_op_id::get, {std::to_string(i)}).front() == std::to_string(i));
    }
    for (std::size_t i = 1000; i < 2000; ++i) {
      REQUIRE(client2.run_command(kv_op_id::get, {std::to_string(i)}).front() == "!key_not_found");
    }
  }

  // Ensure all three blocks have the data
  for (size_t i = 0; i < NUM_BLOCKS; i++) {
    for (std::size_t j = 0; j < 1000; j++) {
      REQUIRE(std::dynamic_pointer_cast<kv_block>(blocks[i][0])->get(std::to_string(j)) == std::to_string(j));
    }
  }

  for (const auto &kv: kv_servers) {
    kv->stop();
  }

  for (const auto &c: chain_servers) {
    c->stop();
  }

  for (const auto &m: management_servers) {
    m->stop();
  }

  dserver->stop();

  for (auto &st: server_threads) {
    if (st.joinable())
      st.join();
  }
}