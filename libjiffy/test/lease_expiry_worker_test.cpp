#include "catch.hpp"

#include "test_utils.h"
#include "jiffy/directory/lease/lease_expiry_worker.h"
#include "jiffy/directory/block/random_block_allocator.h"

#define LEASE_PERIOD_MS 100
#define GRACE_PERIOD_MS 100

using namespace ::jiffy::directory;

TEST_CASE("lease_manager_test") {
  using namespace std::chrono_literals;

  auto alloc = std::make_shared<dummy_block_allocator>(5);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto tree = std::make_shared<directory_tree>(alloc, sm);
  lease_expiry_worker mgr(tree, LEASE_PERIOD_MS, GRACE_PERIOD_MS);
  REQUIRE_NOTHROW(tree->create("/sandbox/a/b/c/file.txt", "testtype", "local://tmp", 1, 1, 0));
  REQUIRE_NOTHROW(tree->create("/sandbox/a/b/file.txt", "testtype", "local://tmp", 1, 1, 0));
  REQUIRE_NOTHROW(tree->create("/sandbox/a/file.txt", "testtype", "local://tmp", 1, 1, 0));
  REQUIRE_NOTHROW(tree->create("/sandbox/a/c/file.txt", "testtype", "local://tmp", 1, 1, data_status::PINNED));
  REQUIRE_NOTHROW(tree->create("/sandbox/a/d/file.txt", "testtype", "local://tmp", 1, 1, data_status::MAPPED));
  REQUIRE(tree->exists("/sandbox/a/b/c/file.txt"));
  REQUIRE(tree->exists("/sandbox/a/b/file.txt"));
  REQUIRE(tree->exists("/sandbox/a/file.txt"));
  REQUIRE(tree->exists("/sandbox/a/c/file.txt"));

  REQUIRE_NOTHROW(mgr.start());
  std::this_thread::sleep_for(100ms);
  REQUIRE_NOTHROW(tree->touch("/sandbox/a/b/c"));
  std::this_thread::sleep_for(150ms);
  REQUIRE_NOTHROW(mgr.stop());
  REQUIRE(tree->exists("/sandbox/a/b/c/file.txt"));
  REQUIRE(!tree->exists("/sandbox/a/b/file.txt"));
  REQUIRE(!tree->exists("/sandbox/a/file.txt"));
  REQUIRE(tree->exists("/sandbox/a/c/file.txt"));
  REQUIRE(tree->exists("/sandbox/a/d/file.txt"));
  auto s1 = tree->dstatus("/sandbox/a/c/file.txt");
  for (const auto& blk: s1.data_blocks()) {
    REQUIRE(blk.mode == storage_mode::in_memory);
  }
  auto s2 = tree->dstatus("/sandbox/a/d/file.txt");
  for (const auto& blk: s2.data_blocks()) {
    REQUIRE(blk.mode == storage_mode::on_disk);
  }
  REQUIRE(sm->COMMANDS.size() == 13);
  REQUIRE(sm->COMMANDS[0] == "create_partition:0:testtype:0:");
  REQUIRE(sm->COMMANDS[1] == "setup_chain:0:/sandbox/a/b/c/file.txt:0:nil");
  REQUIRE(sm->COMMANDS[2] == "create_partition:1:testtype:0:");
  REQUIRE(sm->COMMANDS[3] == "setup_chain:1:/sandbox/a/b/file.txt:0:nil");
  REQUIRE(sm->COMMANDS[4] == "create_partition:2:testtype:0:");
  REQUIRE(sm->COMMANDS[5] == "setup_chain:2:/sandbox/a/file.txt:0:nil");
  REQUIRE(sm->COMMANDS[6] == "create_partition:3:testtype:0:");
  REQUIRE(sm->COMMANDS[7] == "setup_chain:3:/sandbox/a/c/file.txt:0:nil");
  REQUIRE(sm->COMMANDS[8] == "create_partition:4:testtype:0:");
  REQUIRE(sm->COMMANDS[9] == "setup_chain:4:/sandbox/a/d/file.txt:0:nil");
  REQUIRE(sm->COMMANDS[10] == "destroy_partition:1");
  REQUIRE(sm->COMMANDS[11] == "dump:4:local://tmp/0");
  REQUIRE(sm->COMMANDS[12] == "destroy_partition:2");
}
