#include "catch.hpp"

#include "test_utils.h"
#include "../src/directory/lease/lease_expiry_worker.h"
#include "../src/directory/block/random_block_allocator.h"

#define LEASE_PERIOD_MS 100
#define GRACE_PERIOD_MS 100

using namespace ::elasticmem::directory;

TEST_CASE("lease_manager_test") {
  using namespace std::chrono_literals;

  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto tree = std::make_shared<directory_tree>(alloc, sm);
  lease_expiry_worker mgr(tree, LEASE_PERIOD_MS, GRACE_PERIOD_MS);
  REQUIRE_NOTHROW(tree->create_file("/sandbox/a/b/c/file.txt", "/tmp"));
  REQUIRE_NOTHROW(tree->create_file("/sandbox/a/b/file.txt", "/tmp"));
  REQUIRE_NOTHROW(tree->create_file("/sandbox/a/file.txt", "/tmp"));
  REQUIRE(tree->exists("/sandbox/a/b/c/file.txt"));
  REQUIRE(tree->exists("/sandbox/a/b/file.txt"));
  REQUIRE(tree->exists("/sandbox/a/file.txt"));

  REQUIRE_NOTHROW(mgr.start());
  std::this_thread::sleep_for(100ms);
  REQUIRE_NOTHROW(tree->touch("/sandbox/a/b/c"));
  std::this_thread::sleep_for(150ms);
  REQUIRE(tree->exists("/sandbox/a/b/c/file.txt"));
  REQUIRE(!tree->exists("/sandbox/a/b/file.txt"));
  REQUIRE(!tree->exists("/sandbox/a/file.txt"));
  REQUIRE(sm->COMMANDS.size() == 2);
  REQUIRE(sm->COMMANDS[0] == "clear:1");
  REQUIRE(sm->COMMANDS[1] == "clear:2");

  REQUIRE_NOTHROW(mgr.stop());
}