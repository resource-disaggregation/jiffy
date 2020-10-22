#include <memkind.h>
#include <fstream>
#include <catch.hpp>
#include <iostream>
#include "jiffy/persistent/persistent_service.h"
#include "jiffy/utils/directory_utils.h"
#include "jiffy/storage/serde/serde_all.h"

#include <thrift/transport/TTransportException.h>
#include <thread>
#include <chrono>
#include <queue>
#include "jiffy/storage/manager/storage_management_server.h"
#include "jiffy/storage/manager/storage_manager.h"
#include "jiffy/storage/file/file_partition.h"
#include "jiffy/storage/hashtable/hash_slot.h"
#include "test_utils.h"
#include "jiffy/storage/service/block_server.h"
#include "jiffy/directory/fs/directory_tree.h"
#include "jiffy/directory/fs/directory_server.h"
#include "jiffy/storage/client/hash_table_client.h"
#include "jiffy/storage/client/fifo_queue_client.h"
#include "jiffy/storage/client/file_client.h"
#include "jiffy/client/jiffy_client.h"

using namespace ::jiffy::persistent;
using namespace ::jiffy::storage;

binary make_binary(const std::string& str, const block_memory_allocator<uint8_t>& allocator) {
  return binary(str, allocator);
}

TEST_CASE("local_write_test", "[write]") {
  struct memkind* pmem_kind = nullptr;
  std::string pmem_path = "media/pmem0/shijie"; 
  std::string memory_mode = "PMEM";
  size_t err = memkind_create_pmem(pmem_path.c_str(),0,&pmem_kind);
  if(err) {
    char error_message[MEMKIND_ERROR_MESSAGE_SIZE];
    memkind_error_message(err, error_message, MEMKIND_ERROR_MESSAGE_SIZE);
    fprintf(stderr, "%s\n", error_message);
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
}

TEST_CASE("local_read_test", "[read]") {
  std::ofstream out("/tmp/a.txt", std::ofstream::out);
  out << "key,value\n";
  out.close();
  block_memory_manager manager;
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
}
