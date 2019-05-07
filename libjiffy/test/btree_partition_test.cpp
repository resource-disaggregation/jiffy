#include "catch.hpp"
#include "jiffy/storage/btree/btree_ops.h"
#include "jiffy/storage/btree/btree_defs.h"
#include "jiffy/storage/btree/btree_partition.h"
#include <vector>
#include <iostream>
#include <string>

using namespace ::jiffy::storage;
using namespace ::jiffy::persistent;

TEST_CASE("btree_put_get_test", "[put][get]") {
  block_memory_manager manager;
  btree_partition block(&manager);

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.put(std::to_string(i), std::to_string(i)) == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.get(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE(block.get(std::to_string(i)) == "!key_not_found");
  }

}

TEST_CASE("btree_put_update_get_test", "[put][update][get]") {
  block_memory_manager manager;
  btree_partition block(&manager);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.put(std::to_string(i), std::to_string(i)) == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.get(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.update(std::to_string(i), std::to_string(i + 1000)) == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE(block.update(std::to_string(i), std::to_string(i + 1000)) == "!key_not_found");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.get(std::to_string(i)) == std::to_string(i + 1000));
  }
}


TEST_CASE("btree_put_remove_get_test", "[put][update][get]") {
  block_memory_manager manager;
  btree_partition block(&manager);
  block.slot_range(jiffy::storage::MIN_KEY, jiffy::storage::MAX_KEY);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.put(std::to_string(i), std::to_string(i)) == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.get(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.remove(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.get(std::to_string(i)) == "!key_not_found");
  }
}

TEST_CASE("btree_put_range_lookup_range_count_test", "[put][range_lookup][range_count]") {
  block_memory_manager manager;
  btree_partition block(&manager);

  for (std::size_t i = 0; i < 10; ++i) {
    REQUIRE(block.put(std::to_string(i), std::to_string(i)) == "!ok");
  }
  std::vector<std::string> ret;
  block.range_lookup(ret, std::to_string(0), std::to_string(9));
  for(std::size_t i = 0; i < 20; i += 2) {
    REQUIRE(ret.at(i) == std::to_string(i/2));
    REQUIRE(ret.at(i + 1) == std::to_string(i/2));
  }
  auto count = std::stoi(block.range_count(std::to_string(0), std::to_string(9)));
  REQUIRE(count == ret.size()/2);
}

TEST_CASE("btree_storage_size_test", "[put][size][storage_size][reset]") {
  block_memory_manager manager;
  btree_partition block(&manager);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.put(std::to_string(i), std::to_string(i)) == "!ok");
  }
  REQUIRE(block.size() == 1000);
  //REQUIRE(block.storage_size() == 5780); Remove this test since we don't use bytes_ as storage size anymore
  REQUIRE(block.storage_size() <= block.storage_capacity());
}

TEST_CASE("btree_flush_load_test", "[put][sync][reset][load][get]") {
  block_memory_manager manager;
  btree_partition block(&manager);
  for (std::size_t i = 0; i < 1000; ++i) {
    std::vector<std::string> res;
    block.run_command(res, btree_cmd_id::bt_put, {std::to_string(i), std::to_string(i)});
    REQUIRE(res.front() == "!ok");
  }
  REQUIRE(block.is_dirty());
  REQUIRE(block.sync("local://tmp/test"));
  REQUIRE(!block.is_dirty());
  REQUIRE_FALSE(block.sync("local://tmp/test"));
  REQUIRE_NOTHROW(block.load("local://tmp/test"));
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.get(std::to_string(i)) == std::to_string(i));
  }
}


