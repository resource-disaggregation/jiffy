#include "catch.hpp"
#include "jiffy/storage/fifoqueue/fifo_queue_defs.h"
#include "jiffy/storage/fifoqueue/fifo_queue_partition.h"
#include <vector>
#include <string>

using namespace ::jiffy::storage;
using namespace ::jiffy::persistent;

TEST_CASE("fifo_queue_local_enqueue_dequeue_test", "[enqueue][dequeue]") {
  block_memory_manager manager;
  fifo_queue_partition block(&manager);

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
    std::string file_path, front;
    file_path = "/tmp/0";
    std::ifstream in(file_path);
    REQUIRE_NOTHROW(getline(in,front));
    REQUIRE_NOTHROW(block.dequeue_ls(resp1, {"dequeue_ls"}));
    REQUIRE(front == std::to_string(i));
    REQUIRE(resp1[0] == "!ok");
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.dequeue_ls(resp, {"dequeue_ls"}));
    REQUIRE(resp[0] == "!queue_is_empty");
  }
}

