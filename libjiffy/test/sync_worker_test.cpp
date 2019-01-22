#include "catch.hpp"

#include "test_utils.h"
#include "jiffy/directory/fs/sync_worker.h"

#define SYNC_PERIOD_MS 100

using namespace ::jiffy::directory;

TEST_CASE("sync_worker_test") {
  using namespace std::chrono_literals;

  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto tree = std::make_shared<directory_tree>(alloc, sm);

  sync_worker worker(tree, SYNC_PERIOD_MS);
  REQUIRE_NOTHROW(tree->create("/sandbox/a/b/c/file.txt", "testtype", "local://tmp", 1, 1, data_status::MAPPED));
  REQUIRE(tree->exists("/sandbox/a/b/c/file.txt"));

  REQUIRE_NOTHROW(worker.start());
  while (worker.num_epochs() != 3);
  REQUIRE_NOTHROW(worker.stop());

  REQUIRE(sm->COMMANDS.size() == 5);
  REQUIRE(sm->COMMANDS[0] == "create_partition:0:testtype:0:");
  REQUIRE(sm->COMMANDS[1] == "setup_chain:0:/sandbox/a/b/c/file.txt:0:nil");
  REQUIRE(sm->COMMANDS[2] == "sync:0:local://tmp/0");
  REQUIRE(sm->COMMANDS[3] == "sync:0:local://tmp/0");
  REQUIRE(sm->COMMANDS[4] == "sync:0:local://tmp/0");
}
