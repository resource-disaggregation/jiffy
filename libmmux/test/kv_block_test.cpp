#include "catch.hpp"
#include "../src/mmux/storage/kv/kv_block.h"

using namespace ::mmux::storage;
using namespace ::mmux::persistent;

TEST_CASE("put_get_test", "[put][get]") {
  kv_block block("nil");
  block.slot_range(0, block::SLOT_MAX);
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
  kv_block block("nil");
  block.slot_range(0, block::SLOT_MAX);
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

TEST_CASE("put_remove_get_test", "[put][update][get]") {
  kv_block block("nil");
  block.slot_range(0, block::SLOT_MAX);
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
  kv_block block("nil");
  block.slot_range(0, block::SLOT_MAX);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.put(std::to_string(i), std::to_string(i)) == "!ok");
  }
  REQUIRE(block.size() == 1000);
  REQUIRE(block.storage_size() == 5780);
  REQUIRE(block.storage_size() <= block.storage_capacity());
  REQUIRE_NOTHROW(block.reset());
  REQUIRE(block.empty());
}

TEST_CASE("flush_load_test", "[put][flush][reset][load][get]") {
  kv_block block("nil");
  block.slot_range(0, block::SLOT_MAX);
  for (std::size_t i = 0; i < 1000; ++i) {
    std::vector<std::string> res;
    block.run_command(res, kv_op_id::put, {std::to_string(i), std::to_string(i)});
    REQUIRE(res.front() == "!ok");
  }
  REQUIRE(block.is_dirty());
  REQUIRE(block.flush("local://tmp/test"));
  REQUIRE(!block.is_dirty());
  REQUIRE_FALSE(block.flush("local://tmp/test"));
  REQUIRE_NOTHROW(block.reset());
  REQUIRE_NOTHROW(block.slot_range(0, block::SLOT_MAX));
  REQUIRE(block.empty());
  REQUIRE_NOTHROW(block.load("local://tmp/test"));
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.get(std::to_string(i)) == std::to_string(i));
  }
}