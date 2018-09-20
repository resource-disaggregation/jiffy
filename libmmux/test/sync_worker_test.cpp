#include "catch.hpp"

#include "test_utils.h"
#include "../src/mmux/directory/fs/sync_worker.h"

#define SYNC_PERIOD_MS 100

using namespace ::mmux::directory;

TEST_CASE("sync_worker_test") {
  using namespace std::chrono_literals;

  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto tree = std::make_shared<directory_tree>(alloc, sm);

  sync_worker worker(tree, SYNC_PERIOD_MS);
  REQUIRE_NOTHROW(tree->create("/sandbox/a/b/c/file.txt", "local://tmp", 1, 1, data_status::MAPPED));
  REQUIRE(tree->exists("/sandbox/a/b/c/file.txt"));

  REQUIRE_NOTHROW(worker.start());
  while (worker.num_epochs() != 3);
  REQUIRE_NOTHROW(worker.stop());

  REQUIRE(sm->COMMANDS.size() == 4);
  REQUIRE(sm->COMMANDS[0] == "setup_block:0:/sandbox/a/b/c/file.txt:0:65536:0:1:0:nil");
  REQUIRE(sm->COMMANDS[1] == "sync:0:local://tmp/0_65536");
  REQUIRE(sm->COMMANDS[2] == "sync:0:local://tmp/0_65536");
  REQUIRE(sm->COMMANDS[3] == "sync:0:local://tmp/0_65536");
}