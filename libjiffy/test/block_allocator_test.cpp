#include "catch.hpp"
#include "jiffy/directory/block/block_allocator.h"
#include "jiffy/directory/block/random_block_allocator.h"

using namespace ::jiffy::directory;

TEST_CASE("random_block_allocator_test", "[allocate][free][add_replica_to_chain][remove_blocks]") {
  std::vector<std::string> blocks = {"a:0", "a:1", "b:0", "b:1"};
  random_block_allocator allocator;
  allocator.add_blocks(blocks);

  REQUIRE(allocator.num_free_blocks() == 4);
  REQUIRE(allocator.num_allocated_blocks() == 0);
  REQUIRE(allocator.num_total_blocks() == 4);

  std::vector<std::string> blk;
  REQUIRE_NOTHROW(blk = allocator.allocate(1, {}, ""));
  REQUIRE(std::find(blocks.begin(), blocks.end(), blk[0]) != blocks.end());
  REQUIRE(allocator.num_free_blocks() == 3);
  REQUIRE(allocator.num_allocated_blocks() == 1);
  REQUIRE(allocator.num_total_blocks() == 4);

  REQUIRE_NOTHROW(blk = allocator.allocate(1, {}, ""));
  REQUIRE(std::find(blocks.begin(), blocks.end(), blk[0]) != blocks.end());
  REQUIRE(allocator.num_free_blocks() == 2);
  REQUIRE(allocator.num_allocated_blocks() == 2);
  REQUIRE(allocator.num_total_blocks() == 4);

  REQUIRE_NOTHROW(blk = allocator.allocate(1, {}, ""));
  REQUIRE(std::find(blocks.begin(), blocks.end(), blk[0]) != blocks.end());
  REQUIRE(allocator.num_free_blocks() == 1);
  REQUIRE(allocator.num_allocated_blocks() == 3);
  REQUIRE(allocator.num_total_blocks() == 4);

  REQUIRE_NOTHROW(blk = allocator.allocate(1, {}, ""));
  REQUIRE(std::find(blocks.begin(), blocks.end(), blk[0]) != blocks.end());
  REQUIRE(allocator.num_free_blocks() == 0);
  REQUIRE(allocator.num_allocated_blocks() == 4);
  REQUIRE(allocator.num_total_blocks() == 4);
  REQUIRE_THROWS_AS(allocator.allocate(1, {}, ""), std::out_of_range);

  REQUIRE_NOTHROW(allocator.free({"a:0"}, ""));
  REQUIRE(allocator.num_free_blocks() == 1);
  REQUIRE(allocator.num_allocated_blocks() == 3);
  REQUIRE(allocator.num_total_blocks() == 4);

  REQUIRE_NOTHROW(allocator.free({"a:1"}, ""));
  REQUIRE(allocator.num_free_blocks() == 2);
  REQUIRE(allocator.num_allocated_blocks() == 2);
  REQUIRE(allocator.num_total_blocks() == 4);

  REQUIRE_NOTHROW(allocator.free({"b:0"}, ""));
  REQUIRE(allocator.num_free_blocks() == 3);
  REQUIRE(allocator.num_allocated_blocks() == 1);
  REQUIRE(allocator.num_total_blocks() == 4);

  REQUIRE_NOTHROW(allocator.free({"b:1"}, ""));
  REQUIRE(allocator.num_free_blocks() == 4);
  REQUIRE(allocator.num_allocated_blocks() == 0);
  REQUIRE(allocator.num_total_blocks() == 4);

  REQUIRE_THROWS_AS(allocator.allocate(3, {}, ""), std::out_of_range);
  REQUIRE_THROWS_AS(allocator.free({"a:2"}, ""), std::out_of_range);

  REQUIRE_NOTHROW(allocator.add_blocks({"a:2", "c:0", "c:1"}));
  REQUIRE(allocator.num_free_blocks() == 7);
  REQUIRE(allocator.num_allocated_blocks() == 0);
  REQUIRE(allocator.num_total_blocks() == 7);

  REQUIRE_NOTHROW(blk = allocator.allocate(3, {}, ""));
  REQUIRE(allocator.num_free_blocks() == 4);
  REQUIRE(allocator.num_allocated_blocks() == 3);
  REQUIRE(allocator.num_total_blocks() == 7);

  REQUIRE_NOTHROW(allocator.free(blk, ""));
  REQUIRE_NOTHROW(allocator.remove_blocks({"a:0", "c:0", "c:1"}));
  REQUIRE(allocator.num_free_blocks() == 4);
  REQUIRE(allocator.num_allocated_blocks() == 0);
  REQUIRE(allocator.num_total_blocks() == 4);
  REQUIRE_THROWS_AS(allocator.remove_blocks({"c:1"}), std::out_of_range);
}
