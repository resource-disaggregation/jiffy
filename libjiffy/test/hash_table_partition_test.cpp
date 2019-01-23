#include "catch.hpp"
#include "jiffy/storage/hashtable/hash_table_partition.h"

using namespace ::jiffy::storage;
using namespace ::jiffy::persistent;

TEST_CASE("put_get_test", "[put][get]") {
  block_memory_manager manager;
  hash_table_partition block(&manager);
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

TEST_CASE("put_update_get_test", "[put][update][get]") {
  block_memory_manager manager;
  hash_table_partition block(&manager);
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

TEST_CASE("put_upsert_get_test", "[put][upsert][get]") {
  block_memory_manager manager;
  hash_table_partition block(&manager);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.put(std::to_string(i), std::to_string(i)) == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.get(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 0; i < 2000; ++i) {
    REQUIRE(block.upsert(std::to_string(i), std::to_string(i + 1000)) == "!ok");
  }
  for (std::size_t i = 0; i < 2000; ++i) {
    REQUIRE(block.get(std::to_string(i)) == std::to_string(i + 1000));
  }
}

TEST_CASE("put_remove_get_test", "[put][update][get]") {
  block_memory_manager manager;
  hash_table_partition block(&manager);
  block.slot_range(0, hash_table_partition::SLOT_MAX);
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

TEST_CASE("storage_size_test", "[put][size][storage_size][reset]") {
  block_memory_manager manager;
  hash_table_partition block(&manager);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.put(std::to_string(i), std::to_string(i)) == "!ok");
  }
  REQUIRE(block.size() == 1000);
  REQUIRE(block.storage_size() == 5780);
  REQUIRE(block.storage_size() <= block.storage_capacity());
}

TEST_CASE("flush_load_test", "[put][sync][reset][load][get]") {
  block_memory_manager manager;
  hash_table_partition block(&manager);
  for (std::size_t i = 0; i < 1000; ++i) {
    std::vector<std::string> res;
    block.run_command(res, hash_table_cmd_id::put, {std::to_string(i), std::to_string(i)});
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
