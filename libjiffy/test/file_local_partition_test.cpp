#include "catch.hpp"
#include "jiffy/storage/file/file_defs.h"
#include "jiffy/storage/file/file_partition.h"
#include <vector>
#include <string>
#include <cstdio>
#include <memkind.h>

using namespace ::jiffy::storage;
using namespace ::jiffy::persistent;

TEST_CASE("file_ls_write_read_csv_test", "[write][read]") {
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
  property_map conf;
  conf.set("file.serializer", "csv");
  file_partition block(&manager, "local://tmp", "0", "regular", conf);
  std::size_t offset = 0;
  for (std::size_t i = 0; i < 500; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.run_command(resp, {"write", std::to_string(i), std::to_string(offset)}));
    REQUIRE(resp[0] == "!ok");
    offset += std::to_string(i).size();
  }
  REQUIRE(block.is_dirty());
  REQUIRE(block.dump("local://tmp/0"));

  for (std::size_t i = 500; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.write_ls(resp, {"write_ls", std::to_string(i), std::to_string(offset)}));
    REQUIRE(resp[0] == "!ok");
    offset += std::to_string(i).size();
  }

  int read_pos = 0;
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.read_ls(resp, {"read_ls", std::to_string(read_pos), std::to_string(std::to_string(i).size())}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i));
    read_pos += std::to_string(i).size();
  }
  read_pos = 0;
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.read_ls(resp, {"read_ls", std::to_string(read_pos), std::to_string(std::to_string(i).size())}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i));
    read_pos += std::to_string(i).size();
  }
  
  response resp;
  REQUIRE_NOTHROW(block.read_ls(resp, {"read_ls", std::to_string(read_pos + 1), std::to_string(std::to_string(1).size())}));
  REQUIRE(resp[1].size() == 0);
  remove("/tmp/0");
  
}

TEST_CASE("file_ls_write_read_binary_test", "[write][read]") {
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
  property_map conf;
  conf.set("file.serializer", "binary");
  file_partition block(&manager, "local://tmp", "0", "regular", conf);
  std::size_t offset = 0;
  for (std::size_t i = 0; i < 500; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.run_command(resp, {"write", std::to_string(i), std::to_string(offset)}));
    REQUIRE(resp[0] == "!ok");
    offset += std::to_string(i).size();
  }
  REQUIRE(block.is_dirty());
  REQUIRE(block.dump("local://tmp/0"));

  for (std::size_t i = 500; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.write_ls(resp, {"write_ls", std::to_string(i), std::to_string(offset)}));
    REQUIRE(resp[0] == "!ok");
    offset += std::to_string(i).size();
  }

  int read_pos = 0;
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.read_ls(resp, {"read_ls", std::to_string(read_pos), std::to_string(std::to_string(i).size())}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i));
    read_pos += std::to_string(i).size();
  }
  read_pos = 0;
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.read_ls(resp, {"read_ls", std::to_string(read_pos), std::to_string(std::to_string(i).size())}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i));
    read_pos += std::to_string(i).size();
  }
  
  response resp;
  REQUIRE_NOTHROW(block.read_ls(resp, {"read_ls", std::to_string(read_pos + 1), std::to_string(std::to_string(1).size())}));
  REQUIRE(resp[1].size() == 0);
  remove("/tmp/0");
  
}




