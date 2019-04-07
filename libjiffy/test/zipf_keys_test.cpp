#include "catch.hpp"
#include <cassert>
#include <cstdio>
#include <cmath>
#include <ctime>
#include <thrift/transport/TTransportException.h>
#include <thread>
#include <chrono>
#include "jiffy/storage/manager/storage_management_server.h"
#include "jiffy/storage/manager/storage_management_client.h"
#include "jiffy/storage/manager/storage_manager.h"
#include "jiffy/storage/hashtable/hash_table_partition.h"
#include "jiffy/storage/msgqueue/msg_queue_partition.h"
#include "jiffy/storage/hashtable/hash_slot.h"
#include "test_utils.h"
#include "jiffy/storage/service/block_server.h"
#include "jiffy/directory/fs/directory_tree.h"
#include "jiffy/directory/fs/directory_server.h"
#include "jiffy/storage/hashtable/hash_table_ops.h"
#include "jiffy/storage/msgqueue/msg_queue_ops.h"
#include "jiffy/storage/client/msg_queue_client.h"
#include "jiffy/storage/client/hash_table_client.h"
#include "jiffy/client/jiffy_client.h"
#include "jiffy/directory/fs/sync_worker.h"
#include "jiffy/directory/lease/lease_expiry_worker.h"
#include "jiffy/directory/lease/lease_server.h"
#include "jiffy/auto_scaling/auto_scaling_client.h"
#include "jiffy/auto_scaling/auto_scaling_server.h"
#include "../../benchmark/src/zipf_generator.h"

using namespace jiffy::client;
using namespace ::jiffy::storage;
using namespace ::jiffy::directory;
using namespace ::jiffy::auto_scaling;
using namespace ::apache::thrift::transport;

#define NUM_BLOCKS 1
#define HOST "127.0.0.1"
#define DIRECTORY_SERVICE_PORT 9090
#define STORAGE_SERVICE_PORT 9091
#define STORAGE_MANAGEMENT_PORT 9092
#define AUTO_SCALING_SERVICE_PORT 9095

std::vector<std::string> keygenerator(std::size_t num_keys, double theta, int num_buckets = 512) {
  int bucket_size = 65536 / num_buckets;
  jiffy::storage::hash_slot hashslot;
  zipfgenerator zipf(theta, num_keys);
  std::vector<std::uint64_t> bucket_dist;
  for(std::size_t i = 0; i < num_keys; i++) {
    bucket_dist.push_back(zipf.next());
  }
  std::uint64_t count = 1;
  std::vector<std::string> keys;
  while(keys.size() != num_keys) {
    char key[8];
    memcpy((char*)key, (char*)&count, 8);
    std::string key_string = std::string(8, '1');
    for(int p = 0;p < 8; p++) {
      key_string[p] = key[p];
     // if((int)((std::uint8_t)key[p]) == 0)
     //   key_string[p] = 1;
    }
    auto bucket = std::uint64_t(hashslot.crc16(key, 8) / bucket_size);
    auto it = std::find (bucket_dist.begin(), bucket_dist.end(), bucket);
    if(it != bucket_dist.end()) {
      *it = *it - 1;
      keys.push_back(key_string);
      LOG(log_level::info) << "Found key" << keys.size();
      if (*it == 0) {
        it = bucket_dist.erase(it);
      }
    }
    count++;
  }
  return keys;
}


TEST_CASE("hash_table_auto_scale_down_test", "[directory_service][storage_server][management_server]") {
  auto alloc = std::make_shared<sequential_block_allocator>();
  auto block_names = test_utils::init_block_names(3, STORAGE_SERVICE_PORT, STORAGE_MANAGEMENT_PORT);
  alloc->add_blocks(block_names);
  auto blocks = test_utils::init_hash_table_blocks(block_names);

  auto storage_server = block_server::create(blocks, STORAGE_SERVICE_PORT);
  std::thread storage_serve_thread([&storage_server] { storage_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_SERVICE_PORT);

  auto mgmt_server = storage_management_server::create(blocks, HOST, STORAGE_MANAGEMENT_PORT);
  std::thread mgmt_serve_thread([&mgmt_server] { mgmt_server->serve(); });
  test_utils::wait_till_server_ready(HOST, STORAGE_MANAGEMENT_PORT);

  auto as_server = auto_scaling_server::create(HOST, DIRECTORY_SERVICE_PORT, HOST, AUTO_SCALING_SERVICE_PORT);
  std::thread auto_scaling_thread([&as_server]{as_server->serve(); });
  test_utils::wait_till_server_ready(HOST, AUTO_SCALING_SERVICE_PORT);

  auto sm = std::make_shared<storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);

  auto dir_server = directory_server::create(t, HOST, DIRECTORY_SERVICE_PORT);
  std::thread dir_serve_thread([&dir_server] { dir_server->serve(); });
  test_utils::wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);

  data_status status = t->create("/sandbox/scale_down.txt", "hashtable", "/tmp", 3, 1, 0, perms::all(), {"0_16384","16384_32768", "32768_65536"}, {"regular", "regular", "regular"}, {});
  hash_table_client client(t, "/sandbox/scale_down.txt", status);

  size_t num_ops = 1000;
  std::vector<std::string> keys = keygenerator(num_ops, 0.5);
  for(std::size_t i = 0; i < 1000; ++i) {
    REQUIRE(client.put(keys[i], std::to_string(i)) == "!ok");
  }

  // A single remove should trigger scale down
  std::vector<std::string> result;
  REQUIRE_NOTHROW(std::dynamic_pointer_cast<hash_table_partition>(blocks[0]->impl())->run_command(result,
      hash_table_cmd_id::ht_remove, {keys[0]}));

  // Busy wait until number of blocks decreases
  while (t->dstatus("/sandbox/scale_down.txt").data_blocks().size() == 3);

  for (std::size_t i = 1; i < 1000; i++) {
    std::string key = keys[i];
    REQUIRE(client.get(key) == std::to_string(i));
  }

  as_server->stop();
  if(auto_scaling_thread.joinable()) {
    auto_scaling_thread.join();
  }

  storage_server->stop();
  if (storage_serve_thread.joinable()) {
    storage_serve_thread.join();
  }

  mgmt_server->stop();
  if (mgmt_serve_thread.joinable()) {
    mgmt_serve_thread.join();
  }

  dir_server->stop();
  if (dir_serve_thread.joinable()) {
    dir_serve_thread.join();
  }
}