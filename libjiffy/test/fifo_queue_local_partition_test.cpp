#include "catch.hpp"
#include "jiffy/storage/fifoqueue/fifo_queue_defs.h"
#include "jiffy/storage/fifoqueue/fifo_queue_partition.h"
#include <jiffy/utils/property_map.h>
#include <vector>
#include <string>
#include <iostream>
#include <stdio.h>
#include "test_utils.h"

using namespace ::jiffy::storage;
using namespace ::jiffy::persistent;


TEST_CASE("fifo_queue_local_enqueue_read_dequeue_csv_test", "[enqueue][dequeue]") {
  std::string memory_mode = getenv("JIFFY_TEST_MODE");
  void* mem_kind = test_utils::init_kind();
  size_t capacity = 134217728;
  block_memory_manager manager(capacity, memory_mode, mem_kind);
  property_map conf;
  conf.set("fifoqueue.serializer", "csv");
  fifo_queue_partition block(&manager, "local://tmp", "0", "regular", conf);
  for (std::size_t i = 0; i < 500; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.run_command(resp, {"enqueue", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }

  REQUIRE(block.is_dirty());
  REQUIRE(block.sync("local://tmp/0"));
  for (std::size_t i = 500; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.enqueue_ls(resp, {"enqueue_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp1;
    REQUIRE_NOTHROW(block.read_next_ls(resp1, {"read_next_ls"}));
    REQUIRE(resp1[1] == std::to_string(i));
    REQUIRE(resp1[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp1;
    REQUIRE_NOTHROW(block.dequeue_ls(resp1, {"dequeue_ls"}));
    REQUIRE(resp1[1] == std::to_string(i));
    REQUIRE(resp1[0] == "!ok");
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.dequeue_ls(resp, {"dequeue_ls"}));
    REQUIRE(resp[0] == "!queue_is_empty");
  }
  remove("/tmp/0");
}

TEST_CASE("fifo_queue_local_enqueue_read_dequeue_binary_test", "[enqueue][dequeue]") {
  std::string memory_mode = getenv("JIFFY_TEST_MODE");
  void* mem_kind = test_utils::init_kind();
  size_t capacity = 134217728;
  block_memory_manager manager(capacity, memory_mode, mem_kind);
  property_map conf;
  conf.set("fifoqueue.serializer", "binary");
  fifo_queue_partition block(&manager, "local://tmp", "0", "regular", conf);

  for (std::size_t i = 0; i < 500; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.run_command(resp, {"enqueue", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }

  REQUIRE(block.is_dirty());
  REQUIRE(block.sync("local://tmp/0"));
  
  for (std::size_t i = 500; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.enqueue_ls(resp, {"enqueue_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp1;
    REQUIRE_NOTHROW(block.read_next_ls(resp1, {"read_next_ls"}));
    REQUIRE(resp1[1] == std::to_string(i));
    REQUIRE(resp1[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp1;
    REQUIRE_NOTHROW(block.dequeue_ls(resp1, {"dequeue_ls"}));
    REQUIRE(resp1[1] == std::to_string(i));
    REQUIRE(resp1[0] == "!ok");
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.dequeue_ls(resp, {"dequeue_ls"}));
    REQUIRE(resp[0] == "!queue_is_empty");
  }
  remove("/tmp/0");
}
