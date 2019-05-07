#include "catch.hpp"
#include "jiffy/storage/fifoqueue/fifo_queue_ops.h"
#include "jiffy/storage/fifoqueue/fifo_queue_defs.h"
#include "jiffy/storage/fifoqueue/fifo_queue_partition.h"
#include <vector>
#include <iostream>
#include <string>

using namespace ::jiffy::storage;
using namespace ::jiffy::persistent;

TEST_CASE("fifo_queue_enqueue_dequeue_test", "[enqueue][dequeue]") {
  block_memory_manager manager;
  fifo_queue_partition block(&manager);

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.enqueue(std::to_string(i)) == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.dequeue() == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    REQUIRE(block.dequeue() == "!msg_not_found");
  }
}


TEST_CASE("fifo_queue_send_clear_read_test", "[send][read]") {
  block_memory_manager manager;
  fifo_queue_partition block(&manager);

  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.enqueue(std::to_string(i)) == "!ok");
  }
  REQUIRE(block.clear() == "!ok");
  REQUIRE(block.size() == 0);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.dequeue() == "!msg_not_found");
  }
}

TEST_CASE("fifo_queue_storage_size_test", "[put][size][storage_size][reset]") {
  block_memory_manager manager;
  fifo_queue_partition block(&manager);
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.enqueue(std::to_string(i)) == "!ok");
  }
  REQUIRE(block.storage_size() <= block.storage_capacity());
}

TEST_CASE("fifo_queue_flush_load_test", "[send][sync][reset][load][read]") {
  block_memory_manager manager;
  fifo_queue_partition block(&manager);
  for (std::size_t i = 0; i < 1000; ++i) {
    std::vector<std::string> res;
    block.run_command(res, fifo_queue_cmd_id::fq_enqueue, {std::to_string(i)});
    REQUIRE(res.front() == "!ok");
  }
  REQUIRE(block.is_dirty());
  REQUIRE(block.sync("local://tmp/test"));
  REQUIRE(!block.is_dirty());
  REQUIRE_FALSE(block.sync("local://tmp/test"));
  REQUIRE_NOTHROW(block.load("local://tmp/test"));
  for (std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(block.dequeue() == std::to_string(i));
  }
}



