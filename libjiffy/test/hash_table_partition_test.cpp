#include "catch.hpp"
#include "jiffy/storage/hashtable/hash_slot.h"
#include "jiffy/storage/hashtable/hash_table_ops.h"
#include "jiffy/storage/hashtable/hash_table_partition.h"
#include <memkind.h>

using namespace ::jiffy::storage;
using namespace ::jiffy::persistent;

TEST_CASE("hash_table_put_get_test", "[put][get]") {
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
  hash_table_partition block(&manager);
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.put(resp, {"put", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get(resp, {"get", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i));
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get(resp, {"get", std::to_string(i)}));
    REQUIRE(resp[0] == "!key_not_found");
  }
}


TEST_CASE("hash_table_put_update_get_test", "[put][update][get]") {
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
  hash_table_partition block(&manager);
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.put(resp, {"put", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get(resp, {"get", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.update(resp, {"update", std::to_string(i), std::to_string(i + 1000)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 1000; i < 2000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.update(resp, {"update", std::to_string(i), std::to_string(i + 1000)}));
    REQUIRE(resp[0] == "!key_not_found");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get(resp, {"get", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i + 1000));
  }
}


TEST_CASE("hash_table_put_upsert_get_test", "[put][upsert][get]") {
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
  hash_table_partition block(&manager);
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.put(resp, {"put", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get(resp, {"get", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i));
  }
  for (std::size_t i = 0; i < 2000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.upsert(resp, {"upsert", std::to_string(i), std::to_string(i + 1000)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 2000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get(resp, {"get", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i + 1000));
  }
}

TEST_CASE("hash_table_put_exists_remove_exists_test", "[put][exists][remove][exists]") {
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
  hash_table_partition block(&manager);
  block.slot_range(0, hash_slot::MAX);
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.put(resp, {"put", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get(resp, {"exists", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.remove(resp, {"remove", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get(resp, {"exists", std::to_string(i)}));
    REQUIRE(resp[0] == "!key_not_found");
  }
}

TEST_CASE("hash_table_put_remove_get_test", "[put][update][get]") {
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
  hash_table_partition block(&manager);
  block.slot_range(0, hash_slot::MAX);
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.put(resp, {"put", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get(resp, {"get", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i));
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.remove(resp, {"remove", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get(resp, {"get", std::to_string(i)}));
    REQUIRE(resp[0] == "!key_not_found");
  }
}

TEST_CASE("hash_table_storage_size_test", "[put][size][storage_size][reset]") {
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
  hash_table_partition block(&manager);
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.put(resp, {"put", std::to_string(i), std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
  }
  REQUIRE(block.size() == 1000);
  //REQUIRE(block.storage_size() == 311712);
  REQUIRE(block.storage_size() <= block.storage_capacity());
}

TEST_CASE("hash_table_flush_load_test", "[put][sync][reset][load][get]") {
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
  hash_table_partition block(&manager);
  for (std::size_t i = 0; i < 1000; ++i) {
    std::vector<std::string> res;
    block.run_command(res, {"put", std::to_string(i), std::to_string(i)});
    REQUIRE(res.front() == "!ok");
  }
  REQUIRE(block.is_dirty());
  REQUIRE(block.sync("local://tmp/test"));
  REQUIRE(!block.is_dirty());
  REQUIRE_FALSE(block.sync("local://tmp/test"));
  REQUIRE_NOTHROW(block.load("local://tmp/test"));
  for (std::size_t i = 0; i < 1000; ++i) {
    response resp;
    REQUIRE_NOTHROW(block.get(resp, {"get", std::to_string(i)}));
    REQUIRE(resp[0] == "!ok");
    REQUIRE(resp[1] == std::to_string(i));
  }
}
