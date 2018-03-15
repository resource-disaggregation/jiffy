#include "catch.hpp"
#include "../src/directory/tree/block_allocator.h"
#include "../src/directory/tree/random_block_allocator.h"

using namespace ::elasticmem::directory;

TEST_CASE("random_block_allocator_test", "[allocate][free][add_blocks][remove_blocks]") {
  std::vector<std::string> blocks = {"a", "b", "c", "d"};
  random_block_allocator allocator(blocks);

  REQUIRE(allocator.num_free_blocks() == 4);
  REQUIRE(allocator.num_allocated_blocks() == 0);
  REQUIRE(allocator.num_total_blocks() == 4);

  std::string blk;
  REQUIRE_NOTHROW(blk = allocator.allocate());
  REQUIRE(std::find(blocks.begin(), blocks.end(), blk) != blocks.end());
  REQUIRE(allocator.num_free_blocks() == 3);
  REQUIRE(allocator.num_allocated_blocks() == 1);
  REQUIRE(allocator.num_total_blocks() == 4);

  REQUIRE_NOTHROW(blk = allocator.allocate());
  REQUIRE(std::find(blocks.begin(), blocks.end(), blk) != blocks.end());
  REQUIRE(allocator.num_free_blocks() == 2);
  REQUIRE(allocator.num_allocated_blocks() == 2);
  REQUIRE(allocator.num_total_blocks() == 4);

  REQUIRE_NOTHROW(blk = allocator.allocate());
  REQUIRE(std::find(blocks.begin(), blocks.end(), blk) != blocks.end());
  REQUIRE(allocator.num_free_blocks() == 1);
  REQUIRE(allocator.num_allocated_blocks() == 3);
  REQUIRE(allocator.num_total_blocks() == 4);


  REQUIRE_NOTHROW(blk = allocator.allocate());
  REQUIRE(std::find(blocks.begin(), blocks.end(), blk) != blocks.end());
  REQUIRE(allocator.num_free_blocks() == 0);
  REQUIRE(allocator.num_allocated_blocks() == 4);
  REQUIRE(allocator.num_total_blocks() == 4);
  REQUIRE_THROWS_AS(allocator.allocate(), std::out_of_range);

  REQUIRE_NOTHROW(allocator.free("a"));
  REQUIRE(allocator.num_free_blocks() == 1);
  REQUIRE(allocator.num_allocated_blocks() == 3);
  REQUIRE(allocator.num_total_blocks() == 4);

  REQUIRE_NOTHROW(allocator.free("b"));
  REQUIRE(allocator.num_free_blocks() == 2);
  REQUIRE(allocator.num_allocated_blocks() == 2);
  REQUIRE(allocator.num_total_blocks() == 4);

  REQUIRE_NOTHROW(allocator.free("c"));
  REQUIRE(allocator.num_free_blocks() == 3);
  REQUIRE(allocator.num_allocated_blocks() == 1);
  REQUIRE(allocator.num_total_blocks() == 4);

  REQUIRE_NOTHROW(allocator.free("d"));
  REQUIRE(allocator.num_free_blocks() == 4);
  REQUIRE(allocator.num_allocated_blocks() == 0);
  REQUIRE(allocator.num_total_blocks() == 4);

  REQUIRE_THROWS_AS(allocator.free("e"), std::out_of_range);

  REQUIRE_NOTHROW(allocator.add_blocks({"e", "f", "g"}));
  REQUIRE(allocator.num_free_blocks() == 7);
  REQUIRE(allocator.num_allocated_blocks() == 0);
  REQUIRE(allocator.num_total_blocks() == 7);

  REQUIRE_NOTHROW(allocator.remove_blocks({"a", "d", "g"}));
  REQUIRE(allocator.num_free_blocks() == 4);
  REQUIRE(allocator.num_allocated_blocks() == 0);
  REQUIRE(allocator.num_total_blocks() == 4);
  REQUIRE_THROWS_AS(allocator.remove_blocks({ "h" }), std::out_of_range);
}