#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "../src/block/kv_block.h"

using namespace ::elasticmem::kv;
using namespace ::elasticmem::persistent;

TEST_CASE("put_get_test", "[put][get]") {
  kv_block block;
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(block.put(std::to_string(i), std::to_string(i)));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.get(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE_THROWS_AS(block.get(std::to_string(i)), std::out_of_range);
  }
}

TEST_CASE("put_update_get_test", "[put][update][get]") {
  kv_block block;
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(block.put(std::to_string(i), std::to_string(i)));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.get(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(block.update(std::to_string(i), std::to_string(i + 1000)));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE_THROWS_AS(block.update(std::to_string(i), std::to_string(i + 1000)), std::out_of_range);
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.get(std::to_string(i)) == std::to_string(i + 1000));
  }
}

TEST_CASE("put_remove_get_test", "[put][update][get]") {
  kv_block block;
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(block.put(std::to_string(i), std::to_string(i)));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.get(std::to_string(i)) == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(block.remove(std::to_string(i)));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_THROWS_AS(block.get(std::to_string(i)), std::out_of_range);
  }
}

TEST_CASE("size_test", "[put][size][storage_size][clear]") {
  kv_block block;
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(block.put(std::to_string(i), std::to_string(i)));
  }
  REQUIRE(block.size() == 1000);
  REQUIRE(block.storage_size() == 1000);
  REQUIRE(block.storage_size() <= block.storage_capacity());
  REQUIRE_NOTHROW(block.clear());
  REQUIRE(block.empty());
}

TEST_CASE("flush_load_test", "[put][flush][clear][load][get]") {
  kv_block block;
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE_NOTHROW(block.put(std::to_string(i), std::to_string(i)));
  }
  REQUIRE_NOTHROW(block.flush("/tmp", "/test"));
  REQUIRE_NOTHROW(block.clear());
  REQUIRE(block.empty());
  REQUIRE_NOTHROW(block.load("/tmp", "/test"));
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.get(std::to_string(i)) == std::to_string(i));
  }
}