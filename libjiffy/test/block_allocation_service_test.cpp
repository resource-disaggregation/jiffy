#include <catch.hpp>
#include <thread>
#include "jiffy/directory/block/block_allocator.h"
#include "jiffy/directory/block/block_registration_client.h"
#include "jiffy/directory/block/block_registration_server.h"
#include "test_utils.h"

using namespace ::jiffy::directory;
using namespace ::apache::thrift::transport;

#define HOST "127.0.0.1"
#define PORT 9090

TEST_CASE("block_registration_service_test", "[add_replica_to_chain][remove_blocks]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto server = block_registration_server::create(alloc, HOST, PORT);
  std::thread serve_thread([&server] {
    server->serve();
  });
  test_utils::wait_till_server_ready(HOST, PORT);

  block_registration_client allocator(HOST, PORT);
  REQUIRE_NOTHROW(allocator.register_blocks({"4", "5", "6"}));
  REQUIRE(alloc->num_free_blocks() == 7);
  REQUIRE(alloc->num_allocated_blocks() == 0);
  REQUIRE(alloc->num_total_blocks() == 7);

  REQUIRE_NOTHROW(allocator.deregister_blocks({"0", "1", "2"}));
  REQUIRE(alloc->num_free_blocks() == 4);
  REQUIRE(alloc->num_allocated_blocks() == 0);
  REQUIRE(alloc->num_total_blocks() == 4);
  REQUIRE_THROWS_AS(allocator.deregister_blocks({"3", "4", "5", "6", "7"}), block_registration_service_exception);

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}


