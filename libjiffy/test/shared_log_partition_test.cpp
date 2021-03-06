#include "catch.hpp"
#include "jiffy/storage/shared_log/shared_log_defs.h"
#include "jiffy/storage/shared_log/shared_log_partition.h"
#include <vector>
#include <string>

using namespace ::jiffy::storage;
using namespace ::jiffy::persistent;

TEST_CASE("shared_log_write_scan_test", "[write][scan]") {
  block_memory_manager manager;
  shared_log_partition block(&manager);
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.write(resp, {"write", std::to_string(i), std::to_string(i)+"_data", std::to_string(i)+"_stream"}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t start_pos = 0; start_pos < 998; ++start_pos) {
    response resp;
    REQUIRE_NOTHROW(block.scan(resp, {"scan", std::to_string(start_pos), std::to_string(start_pos + 2), std::to_string(start_pos)+"_stream"}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(start_pos)+"_data");
  }
}

TEST_CASE("shared_log_write_trim_scan_test", "[write][trim][scan]") {
  block_memory_manager manager;
  shared_log_partition block(&manager);
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.write(resp, {"write", std::to_string(i), std::to_string(i)+"_data", std::to_string(i)+"_stream"}));
    REQUIRE(resp[0] == "!ok");
  }

  {
    response resp;
    REQUIRE_NOTHROW(block.trim(resp, {"trim", std::to_string(0), std::to_string(1)}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == "28");
  }

  for (std::size_t start_pos = 0; start_pos < 2; ++start_pos) {
    response resp;
    REQUIRE_NOTHROW(block.scan(resp, {"scan", std::to_string(start_pos), std::to_string(start_pos + 2), std::to_string(start_pos)+"_stream"}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp.size() == 1);
  }
  
  for (std::size_t start_pos = 2; start_pos < 998; ++start_pos) {
    response resp;
    REQUIRE_NOTHROW(block.scan(resp, {"scan", std::to_string(start_pos), std::to_string(start_pos + 2), std::to_string(start_pos)+"_stream"}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(start_pos)+"_data");
  }
}

TEST_CASE("shared_log_flush_load_test", "[write][sync][reset][load][scan]") {
  block_memory_manager manager;
  shared_log_partition block(&manager);
  std::size_t offset = 0;
  for (std::size_t i = 0; i < 10; ++i) {
    std::vector<std::string> res;
    block.run_command(res,{"write", std::to_string(i), std::to_string(i)+"_data", std::to_string(i)+"_stream"});
    REQUIRE(res.front() == "!ok");
  }
  REQUIRE(block.is_dirty());
  REQUIRE(block.sync("local://tmp/test"));
  REQUIRE(!block.is_dirty());
  REQUIRE_FALSE(block.sync("local://tmp/test"));
  REQUIRE_NOTHROW(block.load("local://tmp/test"));
  for (std::size_t start_pos = 0; start_pos < 8; ++start_pos) {
    response resp;
    REQUIRE_NOTHROW(block.scan(resp, {"scan", std::to_string(start_pos), std::to_string(start_pos + 2), std::to_string(start_pos)+"_stream"}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(start_pos)+"_data");
  }
}