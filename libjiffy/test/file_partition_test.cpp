#include "catch.hpp"
#include "jiffy/storage/file/file_defs.h"
#include "jiffy/storage/file/file_partition.h"
#include <vector>
#include <string>
#include <memkind.h>

using namespace ::jiffy::storage;
using namespace ::jiffy::persistent;

TEST_CASE("file_write_read_test", "[write][read]") {
  struct memkind* pmem_kind = nullptr;
  std::string pmem_path = "/media/pmem0/shijie"; 
  std::string memory_mode = "PMEM";
  size_t err = memkind_create_pmem(pmem_path.c_str(),0,&pmem_kind);
  if(err) {
    char error_message[MEMKIND_ERROR_MESSAGE_SIZE];
    memkind_error_message(err, error_message, MEMKIND_ERROR_MESSAGE_SIZE);
    fprintf(stderr, "%s\n", error_message);
  }
  size_t capacity = 134217728;
  block_memory_manager manager(capacity, memory_mode, pmem_kind);
  file_partition block(&manager);
  std::size_t offset = 0;
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.write(resp, {"write", std::to_string(i), std::to_string(offset)}));
    REQUIRE(resp[0] == "!ok");
    offset += std::to_string(i).size();
  }
  int read_pos = 0;
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.read(resp, {"read", std::to_string(read_pos), std::to_string(std::to_string(i).size())}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i));
    read_pos += std::to_string(i).size();
  }
  response resp;
  REQUIRE_NOTHROW(block.read(resp, {"read", std::to_string(read_pos + 1), std::to_string(std::to_string(1).size())}));
  REQUIRE(resp[1] == std::string(std::to_string(1).size(), 0));
  // memkind_destroy_kind(pmem_kind);
}

TEST_CASE("file_write_clear_read_test", "[write][read]") {
  struct memkind* pmem_kind = nullptr;
  std::string pmem_path = "/media/pmem0/shijie"; 
  std::string memory_mode = "PMEM";
  size_t err = memkind_create_pmem(pmem_path.c_str(),0,&pmem_kind);
  if(err) {
    char error_message[MEMKIND_ERROR_MESSAGE_SIZE];
    memkind_error_message(err, error_message, MEMKIND_ERROR_MESSAGE_SIZE);
    fprintf(stderr, "%s\n", error_message);
  }
  size_t capacity = 134217728;
  block_memory_manager manager(capacity, memory_mode, pmem_kind);
  file_partition block(&manager);
  std::size_t offset = 0;
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.write(resp, {"write", std::to_string(i), std::to_string(offset)}));
    REQUIRE(resp[0] == "!ok");
    offset += std::to_string(i).size();
  }

  {
    response resp;
    REQUIRE_NOTHROW(block.clear(resp, {"clear"}));
    REQUIRE(resp[0] == "!ok");
  }

  int read_pos = 0;
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.read(resp, {"read", std::to_string(read_pos), std::to_string(std::to_string(i).size())}));
    REQUIRE(resp[1] == std::string(std::to_string(i).size(), 0));
    read_pos += std::to_string(i).size();
  }
  memkind_destroy_kind(pmem_kind);
}

TEST_CASE("file_storage_size_test", "[put][size][storage_size][reset]") {
  struct memkind* pmem_kind = nullptr;
  std::string pmem_path = "/media/pmem0/shijie"; 
  std::string memory_mode = "PMEM";
  size_t err = memkind_create_pmem(pmem_path.c_str(),0,&pmem_kind);
  if(err) {
    char error_message[MEMKIND_ERROR_MESSAGE_SIZE];
    memkind_error_message(err, error_message, MEMKIND_ERROR_MESSAGE_SIZE);
    fprintf(stderr, "%s\n", error_message);
  }
  size_t capacity = 134217728;
  block_memory_manager manager(capacity, memory_mode, pmem_kind);
  file_partition block(&manager);
  std::size_t offset = 0;
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.write(resp, {"write", std::to_string(i), std::to_string(offset)}));
    REQUIRE(resp[0] == "!ok");
    offset += std::to_string(i).size();
  }
  REQUIRE(block.storage_size() <= block.storage_capacity());
  memkind_destroy_kind(pmem_kind);
}

TEST_CASE("file_flush_load_test", "[write][sync][reset][load][read]") {
  struct memkind* pmem_kind = nullptr;
  std::string pmem_path = "/media/pmem0/shijie"; 
  std::string memory_mode = "DRAM";
  // size_t err = memkind_create_pmem(pmem_path.c_str(),0,&pmem_kind);
  // if(err) {
  //   char error_message[MEMKIND_ERROR_MESSAGE_SIZE];
  //   memkind_error_message(err, error_message, MEMKIND_ERROR_MESSAGE_SIZE);
  //   fprintf(stderr, "%s\n", error_message);
  // }
  size_t capacity = 134217728;
  block_memory_manager manager(capacity, memory_mode, pmem_kind);
  file_partition block(&manager);
  std::size_t offset = 0;
  for (std::size_t i = 0; i < 1000; ++i) {
    std::vector<std::string> res;
    block.run_command(res, {"write", std::to_string(i), std::to_string(offset)});
    offset += std::to_string(i).size();
    REQUIRE(res.front() == "!ok");
  }
  REQUIRE(block.is_dirty());
  REQUIRE(block.sync("local://tmp/test"));
  REQUIRE(!block.is_dirty());
  REQUIRE_FALSE(block.sync("local://tmp/test"));
  REQUIRE_NOTHROW(block.load("local://tmp/test"));
  int read_pos = 0;
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.read(resp, {"read", std::to_string(read_pos), std::to_string(std::to_string(i).size())}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i));
    read_pos += std::to_string(i).size();
  }
  // memkind_destroy_kind(pmem_kind);
}



