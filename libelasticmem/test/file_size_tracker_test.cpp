#include <sstream>
#include "catch.hpp"

#include "test_utils.h"
#include "../src/directory/lease/lease_expiry_worker.h"
#include "../src/directory/block/random_block_allocator.h"
#include "../src/directory/block/file_size_tracker.h"

#define PERIODICITY_MS 100

using namespace ::elasticmem::directory;
using namespace ::elasticmem::storage;

TEST_CASE("file_size_tracker_test") {
  using namespace std::chrono_literals;

  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto tree = std::make_shared<directory_tree>(alloc, sm);
  file_size_tracker tracker(tree, PERIODICITY_MS, "/tmp/file.trace");
  REQUIRE_NOTHROW(tree->create("/sandbox/a/b/c/file.txt", "/tmp", 1, 1));
  REQUIRE_NOTHROW(tree->create("/sandbox/a/b/file.txt", "/tmp", 1, 1));
  REQUIRE_NOTHROW(tree->create("/sandbox/a/file.txt", "/tmp", 1, 1));

  REQUIRE_NOTHROW(tracker.start());
  std::this_thread::sleep_for(150ms);
  REQUIRE_NOTHROW(tracker.stop());

  REQUIRE(sm->COMMANDS.size() == 9);
  REQUIRE(sm->COMMANDS[0] == "setup_block:0:/sandbox/a/b/c/file.txt:0:65536:0:0:nil");
  REQUIRE(sm->COMMANDS[1] == "setup_block:1:/sandbox/a/b/file.txt:0:65536:1:0:nil");
  REQUIRE(sm->COMMANDS[2] == "setup_block:2:/sandbox/a/file.txt:0:65536:2:0:nil");
  REQUIRE(sm->COMMANDS[3] == "storage_size:0");
  REQUIRE(sm->COMMANDS[4] == "storage_size:1");
  REQUIRE(sm->COMMANDS[5] == "storage_size:2");
  REQUIRE(sm->COMMANDS[6] == "storage_size:0");
  REQUIRE(sm->COMMANDS[7] == "storage_size:1");
  REQUIRE(sm->COMMANDS[8] == "storage_size:2");

  std::ifstream in("/tmp/file.trace");
  std::string line;
  std::vector<std::string> paths = { "/sandbox/a/b/c/file.txt", "/sandbox/a/b/file.txt", "/sandbox/a/file.txt"};
  std::size_t i = 0;
  while (std::getline(in, line)) {
    std::istringstream ss(line);
    uint64_t ts = 0, file_size = UINT64_MAX, num_blocks = 0;
    std::string path;
    ss >> ts >> path >> file_size >> num_blocks;
    REQUIRE(file_size == 0);
    REQUIRE(path == paths[i % 3]);
    REQUIRE(num_blocks == 1);
    ++i;
  }
  in.close();
}