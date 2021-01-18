#include <fstream>
#include <catch.hpp>
#include <iostream>
#include "jiffy/persistent/persistent_service.h"
#include "test_utils.h"
#include "jiffy/utils/directory_utils.h"
#include "jiffy/storage/serde/serde_all.h"

using namespace ::jiffy::persistent;
using namespace ::jiffy::storage;

binary make_binary(const std::string& str, const block_memory_allocator<uint8_t>& allocator) {
  return binary(str, allocator);
}

TEST_CASE("local_write_test", "[write]") {
  std::string pmem_path = getenv("PMEM_PATH"); 
  std::string memory_mode = getenv("JIFFY_TEST_MODE");
  struct memkind* pmem_kind = nullptr;
  if (memory_mode == "PMEM") {
    pmem_kind = test_utils::create_kind(pmem_path);
  }
  size_t capacity = 134217728;
  block_memory_manager manager(capacity, memory_mode, pmem_kind);
  block_memory_allocator<uint8_t> binary_allocator(&manager);
  hash_table_type table;
  auto bkey = binary("key", binary_allocator);
  auto bval = binary("value", binary_allocator);
  table.emplace(std::make_pair(bkey, bval));
  auto ser = std::make_shared<csv_serde>(binary_allocator);
  local_store store(ser);
  REQUIRE_NOTHROW(store.write(table, "/tmp/a.txt"));
  std::ifstream in("/tmp/a.txt", std::ifstream::in);
  std::string data;
  in >> data;
  REQUIRE(data == "key,value");
  std::remove("/tmp/a.txt");
  test_utils::destroy_kind(pmem_kind);
}

TEST_CASE("local_read_test", "[read]") {
  std::ofstream out("/tmp/a.txt", std::ofstream::out);
  out << "key,value\n";
  out.close();
  std::string pmem_path = getenv("PMEM_PATH"); 
  std::string memory_mode = getenv("JIFFY_TEST_MODE");
  struct memkind* pmem_kind = nullptr;
  if (memory_mode == "PMEM") {
    pmem_kind = test_utils::create_kind(pmem_path);
  }
  size_t capacity = 134217728;
  block_memory_manager manager(capacity, memory_mode, pmem_kind);
  block_memory_allocator<kv_pair_type> allocator(&manager);
  block_memory_allocator<uint8_t> binary_allocator(&manager);
  auto ser = std::make_shared<csv_serde>(binary_allocator);
  local_store store(ser);
  hash_table_type table;
  auto bkey = make_binary("key", binary_allocator);
  auto bval = make_binary("value", binary_allocator);
  REQUIRE_NOTHROW(store.read("/tmp/a.txt", table));
  REQUIRE(table.at(bkey) == bval);
  std::remove("/tmp/a.txt");
  test_utils::destroy_kind(pmem_kind);
}
