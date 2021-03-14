#include "catch.hpp"
#include "jiffy/storage/hashtable/hash_slot.h"
#include "jiffy/storage/hashtable/hash_table_ops.h"
#include "jiffy/storage/hashtable/hash_table_partition.h"
#include "test_utils.h"

using namespace ::jiffy::storage;
using namespace ::jiffy::persistent;

TEST_CASE("hash_table_ls_put_get_csv_test", "[put][get]") {
  std::string memory_mode = getenv("JIFFY_TEST_MODE");
  void* pmem_kind = test_utils::init_kind();
  size_t capacity = 134217728;
  block_memory_manager manager(capacity, memory_mode, pmem_kind);
  property_map conf;
  conf.set("hashtable.serializer", "csv");
  hash_table_partition block(&manager, "local://tmp", "0_65536", "regular", conf);
  for (std::size_t i = 0; i < 500; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.run_command(resp, {"put", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }

  REQUIRE(block.is_dirty());
  REQUIRE(block.dump("local://tmp/0_65536"));

  for (std::size_t i = 500; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.put_ls(resp, {"put_ls", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get_ls(resp, {"get_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i));
  }
  
  for (std::size_t i = 1000; i < 2000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get_ls(resp, {"get_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!key_not_found");
  }
  remove("/tmp/0_65536");
}


TEST_CASE("hash_table_ls_put_update_get_csv_test", "[put][update][get]") {
  std::string memory_mode = getenv("JIFFY_TEST_MODE");
  void* pmem_kind = test_utils::init_kind();
  size_t capacity = 134217728;
  block_memory_manager manager(capacity, memory_mode, pmem_kind);
  property_map conf;
  conf.set("hashtable.serializer", "csv");
  hash_table_partition block(&manager, "local://tmp", "0_65536", "regular", conf);
  for (std::size_t i = 0; i < 500; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.run_command(resp, {"put", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  REQUIRE(block.is_dirty());
  REQUIRE(block.dump("local://tmp/0_65536"));

  for (std::size_t i = 500; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.put_ls(resp, {"put_ls", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.update_ls(resp, {"update_ls", std::to_string(i), std::to_string(i + 1000)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.update_ls(resp, {"update_ls", std::to_string(i), std::to_string(i + 1000)}));
    REQUIRE(resp[0] == "!key_not_found");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get_ls(resp, {"get_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i + 1000));
  }
  remove("/tmp/0_65536");
}


TEST_CASE("hash_table_ls_put_upsert_get_csv_test", "[put][upsert][get]") {
  std::string memory_mode = getenv("JIFFY_TEST_MODE");
  void* pmem_kind = test_utils::init_kind();
  size_t capacity = 134217728;
  block_memory_manager manager(capacity, memory_mode, pmem_kind);
  property_map conf;
  conf.set("hashtable.serializer", "csv");
  hash_table_partition block(&manager, "local://tmp", "0_65536", "regular", conf);
  for (std::size_t i = 0; i < 500; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.run_command(resp, {"put", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  
  REQUIRE(block.is_dirty());
  REQUIRE(block.dump("local://tmp/0_65536"));

  for (std::size_t i = 500; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.put_ls(resp, {"put_ls", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get_ls(resp, {"get_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i));
  }
  for (std::size_t i = 0; i < 2000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.upsert_ls(resp, {"upsert_ls", std::to_string(i), std::to_string(i + 1000)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 2000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get_ls(resp, {"get_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i + 1000));
  }
  remove("/tmp/0_65536");
}

TEST_CASE("hash_table_ls_put_exists_remove_exists_csv_test", "[put][exists][remove][exists]") {
  std::string memory_mode = getenv("JIFFY_TEST_MODE");
  void* pmem_kind = test_utils::init_kind();
  size_t capacity = 134217728;
  block_memory_manager manager(capacity, memory_mode, pmem_kind);
  property_map conf;
  conf.set("hashtable.serializer", "csv");
  hash_table_partition block(&manager, "local://tmp", "0_65536", "regular", conf);
  block.slot_range(0, hash_slot::MAX);
  for (std::size_t i = 0; i < 500; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.run_command(resp, {"put", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  
  REQUIRE(block.is_dirty());
  REQUIRE(block.dump("local://tmp/0_65536"));

  for (std::size_t i = 500; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.put_ls(resp, {"put_ls", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.exists_ls(resp, {"exists_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.remove_ls(resp, {"remove_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.exists_ls(resp, {"exists_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!key_not_found");
  }
  remove("/tmp/0_65536");
}

TEST_CASE("hash_table_ls_put_remove_get_csv_test", "[put][update][get]") {
  std::string memory_mode = getenv("JIFFY_TEST_MODE");
  void* pmem_kind = test_utils::init_kind();
  size_t capacity = 134217728;
  block_memory_manager manager(capacity, memory_mode, pmem_kind);
  property_map conf;
  conf.set("hashtable.serializer", "csv");
  hash_table_partition block(&manager, "local://tmp", "0_65536", "regular", conf);
  block.slot_range(0, hash_slot::MAX);
  for (std::size_t i = 0; i < 500; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.run_command(resp, {"put", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  
  REQUIRE(block.is_dirty());
  REQUIRE(block.dump("local://tmp/0_65536"));

  for (std::size_t i = 500; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.put_ls(resp, {"put_ls", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get_ls(resp, {"get_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.remove_ls(resp, {"remove_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get_ls(resp, {"get_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!key_not_found");
  }
  remove("/tmp/0_65536");
}

TEST_CASE("hash_table_ls_put_get_binary_test", "[put][get]") {
  std::string memory_mode = getenv("JIFFY_TEST_MODE");
  void* pmem_kind = test_utils::init_kind();
  size_t capacity = 134217728;
  block_memory_manager manager(capacity, memory_mode, pmem_kind);
  property_map conf;
  conf.set("hashtable.serializer", "binary");
  hash_table_partition block(&manager, "local://tmp", "0_65536", "regular", conf);
  for (std::size_t i = 0; i < 500; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.run_command(resp, {"put", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }

  REQUIRE(block.is_dirty());
  REQUIRE(block.dump("local://tmp/0_65536"));

  for (std::size_t i = 500; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.put_ls(resp, {"put_ls", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get_ls(resp, {"get_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get_ls(resp, {"get_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!key_not_found");
  }
  remove("/tmp/0_65536");
}


TEST_CASE("hash_table_ls_put_update_get_binary_test", "[put][update][get]") {
  std::string memory_mode = getenv("JIFFY_TEST_MODE");
  void* pmem_kind = test_utils::init_kind();
  size_t capacity = 134217728;
  block_memory_manager manager(capacity, memory_mode, pmem_kind);
  property_map conf;
  conf.set("hashtable.serializer", "binary");
  hash_table_partition block(&manager, "local://tmp", "0_65536", "regular", conf);
  for (std::size_t i = 0; i < 500; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.run_command(resp, {"put", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  REQUIRE(block.is_dirty());
  REQUIRE(block.dump("local://tmp/0_65536"));

  for (std::size_t i = 500; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.put_ls(resp, {"put_ls", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.update_ls(resp, {"update_ls", std::to_string(i), std::to_string(i + 1000)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.update_ls(resp, {"update_ls", std::to_string(i), std::to_string(i + 1000)}));
    REQUIRE(resp[0] == "!key_not_found");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get_ls(resp, {"get_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i + 1000));
  }
  remove("/tmp/0_65536");
}


TEST_CASE("hash_table_ls_put_upsert_get_binary_test", "[put][upsert][get]") {
  std::string memory_mode = getenv("JIFFY_TEST_MODE");
  void* pmem_kind = test_utils::init_kind();
  size_t capacity = 134217728;
  block_memory_manager manager(capacity, memory_mode, pmem_kind);
  property_map conf;
  conf.set("hashtable.serializer", "binary");
  hash_table_partition block(&manager, "local://tmp", "0_65536", "regular", conf);
  for (std::size_t i = 0; i < 500; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.run_command(resp, {"put", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  
  REQUIRE(block.is_dirty());
  REQUIRE(block.dump("local://tmp/0_65536"));

  for (std::size_t i = 500; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.put_ls(resp, {"put_ls", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get_ls(resp, {"get_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i));
  }
  for (std::size_t i = 0; i < 2000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.upsert_ls(resp, {"upsert_ls", std::to_string(i), std::to_string(i + 1000)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 2000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get_ls(resp, {"get_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i + 1000));
  }
  remove("/tmp/0_65536");
}

TEST_CASE("hash_table_ls_put_exists_remove_exists_binary_test", "[put][exists][remove][exists]") {
  std::string memory_mode = getenv("JIFFY_TEST_MODE");
  void* pmem_kind = test_utils::init_kind();
  size_t capacity = 134217728;
  block_memory_manager manager(capacity, memory_mode, pmem_kind);
  property_map conf;
  conf.set("hashtable.serializer", "binary");
  hash_table_partition block(&manager, "local://tmp", "0_65536", "regular", conf);
  block.slot_range(0, hash_slot::MAX);
  for (std::size_t i = 0; i < 500; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.run_command(resp, {"put", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  
  REQUIRE(block.is_dirty());
  REQUIRE(block.dump("local://tmp/0_65536"));

  for (std::size_t i = 500; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.put_ls(resp, {"put_ls", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.exists_ls(resp, {"exists_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.remove_ls(resp, {"remove_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.exists_ls(resp, {"exists_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!key_not_found");
  }
  remove("/tmp/0_65536");
}

TEST_CASE("hash_table_ls_put_remove_get_binary_test", "[put][update][get]") {
  std::string memory_mode = getenv("JIFFY_TEST_MODE");
  void* pmem_kind = test_utils::init_kind();
  size_t capacity = 134217728;
  block_memory_manager manager(capacity, memory_mode, pmem_kind);
  property_map conf;
  conf.set("hashtable.serializer", "binary");
  hash_table_partition block(&manager, "local://tmp", "0_65536", "regular", conf);
  block.slot_range(0, hash_slot::MAX);
  for (std::size_t i = 0; i < 500; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.run_command(resp, {"put", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  
  REQUIRE(block.is_dirty());
  REQUIRE(block.dump("local://tmp/0_65536"));

  for (std::size_t i = 500; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.put_ls(resp, {"put_ls", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }

  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get_ls(resp, {"get_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.remove_ls(resp, {"remove_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get_ls(resp, {"get_ls", std::to_string(i)}));
    REQUIRE(resp[0] == "!key_not_found");
  }
  remove("/tmp/0_65536");
}