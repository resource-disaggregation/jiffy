#include <catch.hpp>
#include "test_utils.h"
#include "jiffy/storage/manager/storage_management_server.h"
#include "jiffy/storage/service/block_server.h"
#include "jiffy/storage/manager/storage_manager.h"
#include "jiffy/directory/fs/directory_server.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/storage/hashtable/hash_table_ops.h"

#define HOST "127.0.0.1"
#define DIRECTORY_SERVICE_PORT 9090
#define STORAGE_SERVICE_PORT_N(i) (9091 + (i) * 3)
#define STORAGE_MANAGEMENT_PORT_N(i) (9092 + (i) * 3)
#define STORAGE_CHAIN_PORT_N(i) (9093 + (i) * 3)
#define NUM_BLOCKS 3

using namespace apache::thrift::server;
using namespace jiffy::storage;
using namespace jiffy::directory;

TEST_CASE("chain_replication_no_failure_test", "[put][get]") {
  std::vector<std::vector<std::string>> block_names(NUM_BLOCKS);
  std::vector<std::vector<std::shared_ptr<block>>> blocks(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> management_servers(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> chain_servers(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> storage_servers(NUM_BLOCKS);
  std::vector<std::thread> server_threads;

  auto alloc = std::make_shared<sequential_block_allocator>();
  for (int32_t i = 0; i < NUM_BLOCKS; i++) {
    block_names[i] = test_utils::init_block_names(1,
                                                  STORAGE_SERVICE_PORT_N(i),
                                                  STORAGE_MANAGEMENT_PORT_N(i));
    alloc->add_blocks(block_names[i]);
    blocks[i] = test_utils::init_hash_table_blocks(block_names[i]);

    management_servers[i] = storage_management_server::create(blocks[i], HOST, STORAGE_MANAGEMENT_PORT_N(i));
    server_threads.emplace_back([i, &management_servers] { management_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT_N(i));

    chain_servers[i] = block_server::create(blocks[i], STORAGE_CHAIN_PORT_N(i));
    server_threads.emplace_back([i, &chain_servers] { chain_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_CHAIN_PORT_N(i));

    storage_servers[i] = block_server::create(blocks[i], STORAGE_SERVICE_PORT_N(i));
    server_threads.emplace_back([i, &storage_servers] { storage_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT_N(i));
  }

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dserver = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  server_threads.emplace_back([&] { dserver->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  t->create("/file", "hashtable", "/tmp", 1, 3, 0, 0, {"0_65536"}, {"regular"});
  auto chain = t->dstatus("/file").data_blocks()[0];

  replica_chain_client client(t, "/file", chain, HT_OPS, 100);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command({"put", std::to_string(i), std::to_string(i)}).front() == "!ok");
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    auto ret = client.run_command({"get", std::to_string(i)});
    REQUIRE(ret[0] == "!ok");
    REQUIRE(ret[1] == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE(client.run_command({"get", std::to_string(i)}).front() == "!key_not_found");
  }

  // Ensure all three blocks have the data
  for (size_t i = 0; i < NUM_BLOCKS; i++) {
    auto ht = std::dynamic_pointer_cast<hash_table_partition>(blocks[i][0]->impl());
    for (std::size_t j = 0; j < 1000; j++) {
      response resp;
      REQUIRE_NOTHROW(ht->get(resp, {"get", std::to_string(j)}));
      REQUIRE(resp[0] == "!ok");
      REQUIRE(resp[1] == std::to_string(j));
    }
  }

  for (const auto &s: storage_servers) {
    s->stop();
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

TEST_CASE("chain_replication_head_failure_test", "[put][get]") {
  std::vector<std::vector<std::string>> block_names(NUM_BLOCKS);
  std::vector<std::vector<std::shared_ptr<block>>> blocks(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> management_servers(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> chain_servers(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> storage_servers(NUM_BLOCKS);
  std::vector<std::thread> server_threads;

  auto alloc = std::make_shared<sequential_block_allocator>();
  for (int32_t i = 0; i < NUM_BLOCKS; i++) {
    block_names[i] = test_utils::init_block_names(1,
                                                  STORAGE_SERVICE_PORT_N(i),
                                                  STORAGE_MANAGEMENT_PORT_N(i));
    alloc->add_blocks(block_names[i]);
    blocks[i] = test_utils::init_hash_table_blocks(block_names[i]);

    management_servers[i] = storage_management_server::create(blocks[i], HOST, STORAGE_MANAGEMENT_PORT_N(i));
    server_threads.emplace_back([i, &management_servers] { management_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT_N(i));

    chain_servers[i] = block_server::create(blocks[i], STORAGE_CHAIN_PORT_N(i));
    server_threads.emplace_back([i, &chain_servers] { chain_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_CHAIN_PORT_N(i));

    storage_servers[i] = block_server::create(blocks[i], STORAGE_SERVICE_PORT_N(i));
    server_threads.emplace_back([i, &storage_servers] { storage_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT_N(i));
  }

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dserver = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  server_threads.emplace_back([&] { dserver->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  t->create("/file", "hashtable", "/tmp", 1, 3, 0, 0, {"0_65536"}, {"regular"});
  auto chain = t->dstatus("/file").data_blocks()[0];
  replica_chain_client client(t, "/file", chain, HT_OPS, 100);

  storage_servers[0]->stop();
  chain_servers[0]->stop();
  management_servers[0]->stop();

  for (int32_t i = 0; i < 3; i++) {
    if (server_threads[i].joinable()) {
      server_threads[i].join();
    }
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command({"put", std::to_string(i), std::to_string(i)}).front() == "!ok");
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    auto ret = client.run_command({"get", std::to_string(i)});
    REQUIRE(ret[0] == "!ok");
    REQUIRE(ret[1] == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE(client.run_command({"get", std::to_string(i)}).front() == "!key_not_found");
  }

  // Ensure all three blocks have the data
  for (size_t i = 1; i < NUM_BLOCKS; i++) {
    auto ht = std::dynamic_pointer_cast<hash_table_partition>(blocks[i][0]->impl());
    for (std::size_t j = 0; j < 1000; j++) {
      response resp;
      REQUIRE_NOTHROW(ht->get(resp, {"get", std::to_string(j)}));
      REQUIRE(resp[0] == "!ok");
      REQUIRE(resp[1] == std::to_string(j));
    }
  }

  for (const auto &s: storage_servers) {
    s->stop();
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

TEST_CASE("chain_replication_mid_failure_test", "[put][get]") {
  std::vector<std::vector<std::string>> block_names(NUM_BLOCKS);
  std::vector<std::vector<std::shared_ptr<block>>> blocks(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> management_servers(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> chain_servers(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> storage_servers(NUM_BLOCKS);
  std::vector<std::thread> server_threads;

  auto alloc = std::make_shared<sequential_block_allocator>();
  for (int32_t i = 0; i < NUM_BLOCKS; i++) {
    block_names[i] = test_utils::init_block_names(1,
                                                  STORAGE_SERVICE_PORT_N(i),
                                                  STORAGE_MANAGEMENT_PORT_N(i));
    alloc->add_blocks(block_names[i]);
    blocks[i] = test_utils::init_hash_table_blocks(block_names[i]);

    management_servers[i] = storage_management_server::create(blocks[i], HOST, STORAGE_MANAGEMENT_PORT_N(i));
    server_threads.emplace_back([i, &management_servers] { management_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT_N(i));

    chain_servers[i] = block_server::create(blocks[i], STORAGE_CHAIN_PORT_N(i));
    server_threads.emplace_back([i, &chain_servers] { chain_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_CHAIN_PORT_N(i));

    storage_servers[i] = block_server::create(blocks[i], STORAGE_SERVICE_PORT_N(i));
    server_threads.emplace_back([i, &storage_servers] { storage_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT_N(i));
  }

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto dserver = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  server_threads.emplace_back([&] { dserver->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  t->create("/file", "hashtable", "/tmp", 1, 3, 0, 0, {"0_65536"}, {"regular"});
  auto chain = t->dstatus("/file").data_blocks()[0];

  replica_chain_client client(t, "/file", chain, HT_OPS, 100);

  storage_servers[1]->stop();
  management_servers[1]->stop();
  chain_servers[1]->stop();
  for (int32_t i = 3; i < 6; i++) {
    if (server_threads[i].joinable()) {
      server_threads[i].join();
    }
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command({"put", std::to_string(i), std::to_string(i)}).front() == "!ok");
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    auto ret = client.run_command({"get", std::to_string(i)});
    REQUIRE(ret[0] == "!ok");
    REQUIRE(ret[1] == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE(client.run_command({"get", std::to_string(i)}).front() == "!key_not_found");
  }

  // Ensure all three blocks have the data
  for (size_t i = 0; i < NUM_BLOCKS; i++) {
    if (i == 1) continue;
    auto ht = std::dynamic_pointer_cast<hash_table_partition>(blocks[i][0]->impl());
    for (std::size_t j = 0; j < 1000; j++) {
      response resp;
      REQUIRE_NOTHROW(ht->get(resp, {"get", std::to_string(j)}));
      REQUIRE(resp[0] == "!ok");
      REQUIRE(resp[1] == std::to_string(j));
    }
  }

  for (const auto &s: storage_servers) {
    s->stop();
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

TEST_CASE("chain_replication_tail_failure_test", "[put][get]") {
  std::vector<std::vector<std::string>> block_names(NUM_BLOCKS);
  std::vector<std::vector<std::shared_ptr<block>>> blocks(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> management_servers(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> chain_servers(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> storage_servers(NUM_BLOCKS);
  std::vector<std::thread> server_threads;

  auto alloc = std::make_shared<sequential_block_allocator>();
  for (int32_t i = 0; i < NUM_BLOCKS; i++) {
    block_names[i] = test_utils::init_block_names(1,
                                                  STORAGE_SERVICE_PORT_N(i),
                                                  STORAGE_MANAGEMENT_PORT_N(i));
    alloc->add_blocks(block_names[i]);
    blocks[i] = test_utils::init_hash_table_blocks(block_names[i]);

    management_servers[i] = storage_management_server::create(blocks[i], HOST, STORAGE_MANAGEMENT_PORT_N(i));
    server_threads.emplace_back([i, &management_servers] { management_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT_N(i));

    chain_servers[i] = block_server::create(blocks[i], STORAGE_CHAIN_PORT_N(i));
    server_threads.emplace_back([i, &chain_servers] { chain_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_CHAIN_PORT_N(i));

    storage_servers[i] = block_server::create(blocks[i], STORAGE_SERVICE_PORT_N(i));
    server_threads.emplace_back([i, &storage_servers] { storage_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT_N(i));
  }

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dserver = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  server_threads.emplace_back([&] { dserver->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  t->create("/file", "hashtable", "/tmp", 1, 3, 0, 0, {"0_65536"}, {"regular"});
  auto chain = t->dstatus("/file").data_blocks()[0];

  replica_chain_client client(t, "/file", chain, HT_OPS, 100);

  storage_servers[2]->stop();
  management_servers[2]->stop();
  chain_servers[2]->stop();
  for (int32_t i = 6; i < 9; i++) {
    if (server_threads[i].joinable()) {
      server_threads[i].join();
    }
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.run_command({"put", std::to_string(i), std::to_string(i)}).front() == "!ok");
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    auto ret = client.run_command({"get", std::to_string(i)});
    REQUIRE(ret[0] == "!ok");
    REQUIRE(ret[1] == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE(client.run_command({"get", std::to_string(i)}).front() == "!key_not_found");
  }

  // Ensure all three blocks have the data
  for (size_t i = 0; i < NUM_BLOCKS - 1; i++) {
    auto ht = std::dynamic_pointer_cast<hash_table_partition>(blocks[i][0]->impl());
    for (std::size_t j = 0; j < 1000; j++) {
      response resp;
      REQUIRE_NOTHROW(ht->get(resp, {"get", std::to_string(j)}));
      REQUIRE(resp[0] == "!ok");
      REQUIRE(resp[1] == std::to_string(j));
    }
  }

  for (const auto &s: storage_servers) {
    s->stop();
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

TEST_CASE("chain_replication_add_block_test", "[put][get]") {
  std::vector<std::vector<std::string>> block_names(NUM_BLOCKS);
  std::vector<std::vector<std::shared_ptr<block>>> blocks(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> management_servers(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> chain_servers(NUM_BLOCKS);
  std::vector<std::shared_ptr<TServer>> storage_servers(NUM_BLOCKS);
  std::vector<std::thread> server_threads;

  auto alloc = std::make_shared<sequential_block_allocator>();
  for (int32_t i = 0; i < NUM_BLOCKS; i++) {
    block_names[i] = test_utils::init_block_names(1,
                                                  STORAGE_SERVICE_PORT_N(i),
                                                  STORAGE_MANAGEMENT_PORT_N(i));
    alloc->add_blocks(block_names[i]);
    blocks[i] = test_utils::init_hash_table_blocks(block_names[i]);

    management_servers[i] = storage_management_server::create(blocks[i], HOST, STORAGE_MANAGEMENT_PORT_N(i));
    server_threads.emplace_back([i, &management_servers] { management_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT_N(i));

    chain_servers[i] = block_server::create(blocks[i], STORAGE_CHAIN_PORT_N(i));
    server_threads.emplace_back([i, &chain_servers] { chain_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_CHAIN_PORT_N(i));

    storage_servers[i] = block_server::create(blocks[i], STORAGE_SERVICE_PORT_N(i));
    server_threads.emplace_back([i, &storage_servers] { storage_servers[i]->serve(); });
    test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT_N(i));
  }

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto dserver = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  server_threads.emplace_back([&] { dserver->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  t->create("/file", "hashtable", "/tmp", 1, 2, 0, 0, {"0_65536"}, {"regular"});

  auto chain = t->dstatus("/file").data_blocks()[0].block_ids;
  {
    replica_chain_client client(t, "/file", chain, HT_OPS, 100);
    for (std::size_t i = 0; i < 1000; ++i) {
      REQUIRE(client.run_command({"put", std::to_string(i), std::to_string(i)}).front() == "!ok");
    }
  }

  auto fixed_chain = t->add_replica_to_chain("/file", t->dstatus("/file").data_blocks()[0]);

  {
    replica_chain_client client2(t, "/file", fixed_chain, HT_OPS, 100);
    for (std::size_t i = 0; i < 1000; ++i) {
      auto ret = client2.run_command({"get", std::to_string(i)});
      REQUIRE(ret[0] == "!ok");
      REQUIRE(ret[1] == std::to_string(i));
    }
    for (std::size_t i = 1000; i < 2000; ++i) {
      REQUIRE(client2.run_command({"get", std::to_string(i)}).front() == "!key_not_found");
    }
  }

  // Ensure all three blocks have the data
  for (size_t i = 0; i < NUM_BLOCKS; i++) {
    auto ht = std::dynamic_pointer_cast<hash_table_partition>(blocks[i][0]->impl());
    for (std::size_t j = 0; j < 1000; j++) {
      response resp;
      REQUIRE_NOTHROW(ht->get(resp, {"get", std::to_string(j)}));
      REQUIRE(resp[0] == "!ok");
      REQUIRE(resp[1] == std::to_string(j));
    }
  }

  for (const auto &s: storage_servers) {
    s->stop();
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
