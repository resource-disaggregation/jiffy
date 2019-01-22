#include <catch.hpp>

#include <thrift/transport/TTransportException.h>
#include <thread>
#include "jiffy/directory/client/lease_client.h"
#include "jiffy/directory/lease/lease_server.h"
#include "jiffy/directory/block/random_block_allocator.h"
#include "test_utils.h"

using namespace ::jiffy::storage;
using namespace ::jiffy::directory;
using namespace ::apache::thrift::transport;

#define NUM_BLOCKS 1
#define HOST "127.0.0.1"
#define PORT 9090
#define LEASE_PERIOD_MS 100

TEST_CASE("update_lease_test", "[update_lease]") {
  using namespace std::chrono_literals;

  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto server = lease_server::create(t, LEASE_PERIOD_MS, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  lease_client client(HOST, PORT);

  t->create("/sandbox/a/b/c/file.txt", "testtype", "local://tmp");

  std::vector<std::string> to_renew = {"/sandbox/a/b/c/file.txt"};
  rpc_lease_ack ack;
  REQUIRE_NOTHROW(ack = client.renew_leases(to_renew));
  REQUIRE(ack.renewed == 1);
  REQUIRE(t->exists("/sandbox/a/b/c/file.txt"));
  REQUIRE(t->dstatus("/sandbox/a/b/c/file.txt").mode() == std::vector<storage_mode>{storage_mode::in_memory});
  REQUIRE(sm->COMMANDS.size() == 2);
  REQUIRE(sm->COMMANDS[0] == "create_partition:0:testtype:0:");
  REQUIRE(sm->COMMANDS[1] == "setup_chain:0:/sandbox/a/b/c/file.txt:0:nil");

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}
