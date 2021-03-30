#include "catch.hpp"
#include "jiffy/storage/file/file_defs.h"
#include "jiffy/storage/file/file_partition.h"
#include <vector>
#include <string>
#include <cstdio>
#include "test_utils.h"

using namespace ::jiffy::storage;
using namespace ::jiffy::persistent;

TEST_CASE("file_ls_write_read_csv_test", "[write][read]") {
  std::string memory_mode = getenv("JIFFY_TEST_MODE");
  void* mem_kind = test_utils::init_kind();
  size_t capacity = 134217728;
  block_memory_manager manager(capacity, memory_mode, mem_kind);
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
  std::string memory_mode = getenv("JIFFY_TEST_MODE");
  void* mem_kind = test_utils::init_kind();
  size_t capacity = 134217728;
  block_memory_manager manager(capacity, memory_mode, mem_kind);
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

