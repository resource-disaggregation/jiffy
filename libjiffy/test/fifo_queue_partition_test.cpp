#include "catch.hpp"
#include "jiffy/storage/fifoqueue/fifo_queue_defs.h"
#include "jiffy/storage/fifoqueue/fifo_queue_partition.h"
#include <vector>
#include <string>

using namespace ::jiffy::storage;
using namespace ::jiffy::persistent;

TEST_CASE("fifo_queue_enqueue_dequeue_test", "[enqueue][dequeue]") {
  block_memory_manager manager;
  fifo_queue_partition block(&manager);

  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.enqueue(resp, {"enqueue", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp1, resp2;
    REQUIRE_NOTHROW(block.front(resp1, {"front"}));
    REQUIRE_NOTHROW(block.dequeue(resp2, {"dequeue"}));
    REQUIRE(resp1[0] == "!ok");
    REQUIRE(resp1[1] == std::to_string(i));
    REQUIRE(resp2[0] == "!ok");
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.dequeue(resp, {"dequeue"}));
    REQUIRE(resp[0] == "!msg_not_found");
  }
}

TEST_CASE("fifo_queue_enqueue_clear_dequeue_test", "[enqueue][dequeue]") {
  block_memory_manager manager;
  fifo_queue_partition block(&manager);

  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.enqueue(resp, {"enqueue", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  {
    response resp;
    REQUIRE_NOTHROW(block.clear(resp, {"clear"}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(block.size() == 0);
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.dequeue(resp, {"dequeue"}));
    REQUIRE(resp[0] == "!msg_not_found");
  }
}

TEST_CASE("fifo_queue_enqueue_readnext_dequeue", "[enqueue][read_next][dequeue]") {
  block_memory_manager manager;
  fifo_queue_partition block(&manager);
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.enqueue(resp, {"enqueue", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.read_next(resp, {"read_next"}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp1, resp2;
    REQUIRE_NOTHROW(block.front(resp1, {"front"}));
    REQUIRE_NOTHROW(block.dequeue(resp2, {"dequeue"}));
    REQUIRE(resp1[0] == "!ok");
    REQUIRE(resp1[1] == std::to_string(i));
    REQUIRE(resp2[0] == "!ok");
  }
}

TEST_CASE("fifo_queue_storage_size_test", "[put][size][storage_size][reset]") {
  block_memory_manager manager;
  fifo_queue_partition block(&manager);
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.enqueue(resp, {"enqueue", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  REQUIRE(block.storage_size() <= block.storage_capacity());
}

TEST_CASE("fifo_queue_flush_load_test", "[enqueue][sync][reset][load][dequeue]") {
  block_memory_manager manager;
  fifo_queue_partition block(&manager);
  for (std::size_t i = 0; i < 1000; ++i) {
    std::vector<std::string> res;
    block.run_command(res, {"enqueue", std::to_string(i)});
    REQUIRE(res.front() == "!ok");
  }
  REQUIRE(block.is_dirty());
  REQUIRE(block.sync("local://tmp/test"));
  REQUIRE(!block.is_dirty());
  REQUIRE_FALSE(block.sync("local://tmp/test"));
  REQUIRE_NOTHROW(block.load("local://tmp/test"));
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp1, resp2;
    REQUIRE_NOTHROW(block.front(resp1, {"front"}));
    REQUIRE_NOTHROW(block.dequeue(resp2, {"dequeue"}));
    REQUIRE(resp1[0] == "!ok");
    REQUIRE(resp1[1] == std::to_string(i));
    REQUIRE(resp2[0] == "!ok");
  }
}



