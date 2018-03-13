#include "catch.hpp"

#include "../src/lease/lease_manager.h"
#include "../src/tree/random_block_allocator.h"

#define LEASE_PERIOD_MS 100
#define GRACE_PERIOD_MS 100

using namespace ::elasticmem::directory;

TEST_CASE("lease_manager_test") {
  using namespace std::chrono_literals;

  std::vector<std::string> blocks = {"a", "b", "c", "d", "e", "f", "g", "h"};
  auto alloc = std::make_shared<random_block_allocator>(blocks);
  auto tree = std::make_shared<directory_tree>(alloc);
  lease_manager mgr(tree, LEASE_PERIOD_MS, GRACE_PERIOD_MS);
  REQUIRE_NOTHROW(tree->create_file("/sandbox/a/b/c/file.txt", "/tmp"));
  REQUIRE_NOTHROW(tree->create_file("/sandbox/a/b/file.txt", "/tmp"));
  REQUIRE_NOTHROW(tree->create_file("/sandbox/a/file.txt", "/tmp"));
  REQUIRE(tree->exists("/sandbox/a/b/c/file.txt"));
  REQUIRE(tree->exists("/sandbox/a/b/file.txt"));
  REQUIRE(tree->exists("/sandbox/a/file.txt"));

  REQUIRE_NOTHROW(mgr.start());
  std::this_thread::sleep_for(100ms);
  REQUIRE_NOTHROW(tree->touch("/sandbox/a/b/c"));
  std::this_thread::sleep_for(200ms);
  REQUIRE(tree->exists("/sandbox/a/b/c/file.txt"));
  REQUIRE(!tree->exists("/sandbox/a/b/file.txt"));
  REQUIRE(!tree->exists("/sandbox/a/file.txt"));

  REQUIRE_NOTHROW(mgr.stop());
}