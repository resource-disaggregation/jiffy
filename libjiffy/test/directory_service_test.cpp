#include "catch.hpp"

#include <thrift/transport/TTransportException.h>
#include <thread>
#include "jiffy/directory/client/directory_client.h"
#include "jiffy/directory/fs/directory_server.h"
#include "jiffy/directory/block/random_block_allocator.h"
#include "test_utils.h"

using namespace ::jiffy::directory;
using namespace ::apache::thrift::transport;
using namespace ::jiffy::utils;

#define NUM_BLOCKS 1
#define HOST "127.0.0.1"
#define PORT 9090

TEST_CASE("rpc_create_directory_test", "[dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto server = directory_server::create(t, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  directory_client tree(HOST, PORT);
  REQUIRE_NOTHROW(tree.create_directories("/sandbox/1/2/a"));
  REQUIRE(tree.is_directory("/sandbox/1/2/a"));
  REQUIRE(tree.is_directory("/sandbox/1/2"));
  REQUIRE(tree.is_directory("/sandbox/1"));
  REQUIRE(tree.is_directory("/sandbox"));

  REQUIRE_NOTHROW(tree.create_directory("/sandbox/1/2/b"));
  REQUIRE(tree.is_directory("/sandbox/1/2/b"));

  REQUIRE_THROWS_AS(tree.create_directory("/sandbox/1/1/b"), directory_service_exception);

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_create_file_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto server = directory_server::create(t, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  directory_client tree(HOST, PORT);
  REQUIRE_NOTHROW(tree.create("/sandbox/a.txt", "testtype", "/tmp", 1, 1, 0));
  REQUIRE(tree.is_regular_file("/sandbox/a.txt"));

  REQUIRE_NOTHROW(tree.create("/sandbox/foo/bar/baz/a", "testtype", "/tmp", 1, 1, 0));
  REQUIRE(tree.is_regular_file("/sandbox/foo/bar/baz/a"));

  REQUIRE_THROWS_AS(tree.create("/sandbox/foo/bar/baz/a/b", "testtype", "/tmp", 1, 1, 0), directory_service_exception);
  REQUIRE_THROWS_AS(tree.create_directories("/sandbox/foo/bar/baz/a/b"), directory_service_exception);

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_open_file_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto server = directory_server::create(t, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  directory_client tree(HOST, PORT);
  REQUIRE_NOTHROW(tree.create("/sandbox/a.txt", "testtype", "/tmp", 1, 1, 0));
  REQUIRE_NOTHROW(tree.create("/sandbox/foo/bar/baz/a", "testtype", "/tmp", 1, 1, 0));

  data_status s;
  REQUIRE_NOTHROW(s = tree.open("/sandbox/a.txt"));
  REQUIRE(s.chain_length() == 1);
  REQUIRE(s.mode() == std::vector<storage_mode>{storage_mode::in_memory});
  REQUIRE(s.backing_path() == "/tmp");
  REQUIRE(s.data_blocks().size() == 1);

  REQUIRE_NOTHROW(s = tree.open("/sandbox/foo/bar/baz/a"));
  REQUIRE(s.chain_length() == 1);
  REQUIRE(s.mode() == std::vector<storage_mode>{storage_mode::in_memory});
  REQUIRE(s.backing_path() == "/tmp");
  REQUIRE(s.data_blocks().size() == 1);

  REQUIRE_THROWS_AS(tree.open("/sandbox/b.txt"), directory_service_exception);

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_open_or_create_file_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto server = directory_server::create(t, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  directory_client tree(HOST, PORT);
  REQUIRE_NOTHROW(tree.create("/sandbox/a.txt", "testtype", "/tmp", 1, 1, 0));
  REQUIRE_NOTHROW(tree.create("/sandbox/foo/bar/baz/a", "testtype", "/tmp", 1, 1, 0));

  data_status s;
  REQUIRE_NOTHROW(s = tree.open_or_create("/sandbox/a.txt", "testtype", "/tmp", 1, 1, 0));
  REQUIRE(s.chain_length() == 1);
  REQUIRE(s.mode() == std::vector<storage_mode>{storage_mode::in_memory});
  REQUIRE(s.backing_path() == "/tmp");
  REQUIRE(s.data_blocks().size() == 1);

  REQUIRE_NOTHROW(s = tree.open_or_create("/sandbox/foo/bar/baz/a", "testtype", "/tmp", 1, 1, 0));
  REQUIRE(s.chain_length() == 1);
  REQUIRE(s.mode() == std::vector<storage_mode>{storage_mode::in_memory});
  REQUIRE(s.backing_path() == "/tmp");
  REQUIRE(s.data_blocks().size() == 1);

  REQUIRE_NOTHROW(s = tree.open_or_create("/sandbox/b.txt", "testtype", "/tmp", 1, 1, 0));
  REQUIRE(s.chain_length() == 1);
  REQUIRE(s.mode() == std::vector<storage_mode>{storage_mode::in_memory});
  REQUIRE(s.backing_path() == "/tmp");
  REQUIRE(s.data_blocks().size() == 1);

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_exists_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto server = directory_server::create(t, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  directory_client tree(HOST, PORT);

  REQUIRE_NOTHROW(tree.create("/sandbox/file", "testtype", "/tmp", 1, 1, 0));
  REQUIRE(tree.exists("/sandbox"));
  REQUIRE(tree.exists("/sandbox/file"));
  REQUIRE(!tree.exists("/sandbox/foo"));

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_last_write_time_test", "[file][dir][touch]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto server = directory_server::create(t, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  directory_client tree(HOST, PORT);

  std::uint64_t before = time_utils::now_ms();
  REQUIRE_NOTHROW(tree.create("/sandbox/file", "testtype", "/tmp", 1, 1, 0));
  std::uint64_t after = time_utils::now_ms();
  REQUIRE(before <= tree.last_write_time("/sandbox/file"));
  REQUIRE(tree.last_write_time("/sandbox/file") <= after);

  before = time_utils::now_ms();
  REQUIRE_NOTHROW(t->touch("/sandbox/file"));
  after = time_utils::now_ms();
  REQUIRE(before <= tree.last_write_time("/sandbox/file"));
  REQUIRE(tree.last_write_time("/sandbox/file") <= after);

  before = time_utils::now_ms();
  REQUIRE_NOTHROW(t->touch("/sandbox"));
  after = time_utils::now_ms();
  REQUIRE(before <= tree.last_write_time("/sandbox"));
  REQUIRE(tree.last_write_time("/sandbox") <= after);
  REQUIRE(before <= tree.last_write_time("/sandbox/file"));
  REQUIRE(tree.last_write_time("/sandbox/file") <= after);
  REQUIRE(tree.last_write_time("/sandbox") == tree.last_write_time("/sandbox/file"));

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_permissions_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto server = directory_server::create(t, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  directory_client tree(HOST, PORT);

  REQUIRE_NOTHROW(tree.create("/sandbox/file", "testtype", "/tmp", 1, 1, 0));
  REQUIRE(tree.permissions("/sandbox") == perms::all);
  REQUIRE(tree.permissions("/sandbox/file") == perms::all);

  REQUIRE_NOTHROW(tree.permissions("/sandbox/file", perms::owner_all | perms::group_all, perm_options::replace));
  REQUIRE(tree.permissions("/sandbox/file") == (perms::owner_all | perms::group_all));

  REQUIRE_NOTHROW(tree.permissions("/sandbox/file", perms::others_all, perm_options::add));
  REQUIRE(tree.permissions("/sandbox/file") == (perms::owner_all | perms::group_all | perms::others_all));

  REQUIRE_NOTHROW(tree.permissions("/sandbox/file", perms::group_all | perms::others_all, perm_options::remove));
  REQUIRE(tree.permissions("/sandbox/file") == perms::owner_all);

  REQUIRE_NOTHROW(tree.permissions("/sandbox", perms::owner_all | perms::group_all, perm_options::replace));
  REQUIRE(tree.permissions("/sandbox") == (perms::owner_all | perms::group_all));

  REQUIRE_NOTHROW(tree.permissions("/sandbox", perms::others_all, perm_options::add));
  REQUIRE(tree.permissions("/sandbox") == (perms::owner_all | perms::group_all | perms::others_all));

  REQUIRE_NOTHROW(tree.permissions("/sandbox", perms::group_all | perms::others_all, perm_options::remove));
  REQUIRE(tree.permissions("/sandbox") == perms::owner_all);

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_remove_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto server = directory_server::create(t, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  directory_client tree(HOST, PORT);

  REQUIRE_NOTHROW(tree.create("/sandbox/abcdef/example/a/b", "testtype", "/tmp", 1, 1, 0));
  REQUIRE(alloc->num_free_blocks() == 3);

  REQUIRE_NOTHROW(tree.remove("/sandbox/abcdef/example/a/b"));
  REQUIRE(!tree.exists("/sandbox/abcdef/example/a/b"));

  REQUIRE_NOTHROW(tree.remove("/sandbox/abcdef/example/a"));
  REQUIRE(!tree.exists("/sandbox/abcdef/example/a"));

  REQUIRE_THROWS_AS(tree.remove("/sandbox/abcdef"), directory_service_exception);
  REQUIRE(tree.exists("/sandbox/abcdef"));

  REQUIRE_NOTHROW(tree.remove_all("/sandbox/abcdef"));
  REQUIRE(!tree.exists("/sandbox/abcdef"));
  REQUIRE(alloc->num_free_blocks() == 4);

  REQUIRE(sm->COMMANDS.size() == 3);
  REQUIRE(sm->COMMANDS[0] == "create_partition:0:testtype:0:");
  REQUIRE(sm->COMMANDS[1] == "setup_chain:0:/sandbox/abcdef/example/a/b:0:nil");
  REQUIRE(sm->COMMANDS[2] == "destroy_partition:0");

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_path_flush_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto server = directory_server::create(t, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  directory_client tree(HOST, PORT);
  REQUIRE_NOTHROW(tree.create("/sandbox/abcdef/example/a/b", "testtype", "/tmp", 1, 1, 0, 0, {"0_65536"}));
  REQUIRE_NOTHROW(tree.create("/sandbox/abcdef/example/c", "testtype", "/tmp", 1, 1, 0, 0, {"0_65536"}));
  REQUIRE(alloc->num_free_blocks() == 2);

  REQUIRE_NOTHROW(tree.sync("/sandbox/abcdef/example/c", "local://tmp"));
  REQUIRE(tree.dstatus("/sandbox/abcdef/example/c").mode() == std::vector<storage_mode>{storage_mode::in_memory});

  REQUIRE_NOTHROW(tree.sync("/sandbox/abcdef/example/a", "local://tmp"));
  REQUIRE(tree.dstatus("/sandbox/abcdef/example/a/b").mode() == std::vector<storage_mode>{storage_mode::in_memory});

  REQUIRE(alloc->num_free_blocks() == 2);
  REQUIRE(sm->COMMANDS.size() == 6);
  REQUIRE(sm->COMMANDS[0] == "create_partition:0:testtype:0_65536:");
  REQUIRE(sm->COMMANDS[1] == "setup_chain:0:/sandbox/abcdef/example/a/b:0:nil");
  REQUIRE(sm->COMMANDS[2] == "create_partition:1:testtype:0_65536:");
  REQUIRE(sm->COMMANDS[3] == "setup_chain:1:/sandbox/abcdef/example/c:0:nil");
  REQUIRE(sm->COMMANDS[4] == "sync:1:local://tmp/0_65536");
  REQUIRE(sm->COMMANDS[5] == "sync:0:local://tmp/0_65536");

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_rename_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto server = directory_server::create(t, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  directory_client tree(HOST, PORT);
  REQUIRE_NOTHROW(tree.create("/sandbox/from/file1.txt", "testtype", "/tmp", 1, 1, 0));
  REQUIRE_NOTHROW(tree.create_directory("/sandbox/to"));
  REQUIRE_NOTHROW(tree.create_directory("/sandbox/to2"));

  REQUIRE_NOTHROW(tree.rename("/sandbox/from/file1.txt", "/sandbox/to/file2.txt"));
  REQUIRE(tree.exists("/sandbox/to/file2.txt"));
  REQUIRE(!tree.exists("/sandbox/from/file1.txt"));

  REQUIRE_NOTHROW(tree.rename("/sandbox/from", "/sandbox/to/subdir"));
  REQUIRE(tree.exists("/sandbox/to/subdir"));
  REQUIRE(!tree.exists("/sandbox/from"));

  REQUIRE_NOTHROW(tree.rename("/sandbox/to/subdir", "/sandbox/to2"));
  REQUIRE(tree.exists("/sandbox/to2/subdir"));
  REQUIRE(!tree.exists("/sandbox/to/subdir"));

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_status_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto server = directory_server::create(t, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  directory_client tree(HOST, PORT);
  std::uint64_t before = time_utils::now_ms();
  REQUIRE_NOTHROW(tree.create("/sandbox/file", "testtype", "/tmp", 1, 1, 0));
  std::uint64_t after = time_utils::now_ms();
  REQUIRE(tree.status("/sandbox/file").permissions() == perms::all);
  REQUIRE(tree.status("/sandbox/file").type() == file_type::regular);
  REQUIRE(before <= tree.status("/sandbox/file").last_write_time());
  REQUIRE(tree.status("/sandbox/file").last_write_time() <= after);

  before = time_utils::now_ms();
  REQUIRE_NOTHROW(tree.create_directory("/sandbox/dir"));
  after = time_utils::now_ms();
  REQUIRE(tree.status("/sandbox/dir").permissions() == perms::all);
  REQUIRE(tree.status("/sandbox/dir").type() == file_type::directory);
  REQUIRE(before <= tree.status("/sandbox/dir").last_write_time());
  REQUIRE(tree.status("/sandbox/dir").last_write_time() <= after);

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_directory_entries_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto server = directory_server::create(t, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  directory_client tree(HOST, PORT);

  std::uint64_t t0 = time_utils::now_ms();
  REQUIRE_NOTHROW(tree.create_directories("/sandbox/a/b"));
  std::uint64_t t1 = time_utils::now_ms();
  REQUIRE_NOTHROW(tree.create("/sandbox/file1.txt", "testtype", "/tmp", 1, 1, 0));
  std::uint64_t t2 = time_utils::now_ms();
  REQUIRE_NOTHROW(tree.create("/sandbox/file2.txt", "testtype", "/tmp", 1, 1, 0));
  std::uint64_t t3 = time_utils::now_ms();

  std::vector<directory_entry> entries;
  REQUIRE_NOTHROW(entries = tree.directory_entries("/sandbox"));
  REQUIRE(entries.size() == 3);
  REQUIRE(entries.at(0).name() == "a");
  REQUIRE(entries.at(0).type() == file_type::directory);
  REQUIRE(entries.at(0).permissions() == perms::all);
  REQUIRE(t0 <= entries.at(0).last_write_time());
  REQUIRE(entries.at(0).last_write_time() <= t1);
  REQUIRE(entries.at(1).name() == "file1.txt");
  REQUIRE(entries.at(1).type() == file_type::regular);
  REQUIRE(entries.at(1).permissions() == perms::all);
  REQUIRE(entries.at(1).last_write_time() <= t2);
  REQUIRE(t1 <= entries.at(1).last_write_time());
  REQUIRE(entries.at(2).name() == "file2.txt");
  REQUIRE(entries.at(2).type() == file_type::regular);
  REQUIRE(entries.at(2).permissions() == perms::all);
  REQUIRE(t2 <= entries.at(2).last_write_time());
  REQUIRE(entries.at(2).last_write_time() <= t3);

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_recursive_directory_entries_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto server = directory_server::create(t, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  directory_client tree(HOST, PORT);

  std::uint64_t t0 = time_utils::now_ms();
  REQUIRE_NOTHROW(tree.create_directories("/sandbox/a/b"));
  std::uint64_t t1 = time_utils::now_ms();
  REQUIRE_NOTHROW(tree.create("/sandbox/file1.txt", "testtype", "/tmp", 1, 1, 0));
  std::uint64_t t2 = time_utils::now_ms();
  REQUIRE_NOTHROW(tree.create("/sandbox/file2.txt", "testtype", "/tmp", 1, 1, 0));
  std::uint64_t t3 = time_utils::now_ms();

  std::vector<directory_entry> entries;
  REQUIRE_NOTHROW(entries = tree.recursive_directory_entries("/sandbox"));
  REQUIRE(entries.size() == 4);
  REQUIRE(entries.at(0).name() == "a");
  REQUIRE(entries.at(0).type() == file_type::directory);
  REQUIRE(entries.at(0).permissions() == perms::all);
  REQUIRE(t0 <= entries.at(0).last_write_time());
  REQUIRE(entries.at(0).last_write_time() <= t1);
  REQUIRE(entries.at(1).name() == "b");
  REQUIRE(entries.at(1).type() == file_type::directory);
  REQUIRE(entries.at(1).permissions() == perms::all);
  REQUIRE(t0 <= entries.at(1).last_write_time());
  REQUIRE(entries.at(1).last_write_time() <= t1);
  REQUIRE(entries.at(2).name() == "file1.txt");
  REQUIRE(entries.at(2).type() == file_type::regular);
  REQUIRE(entries.at(2).permissions() == perms::all);
  REQUIRE(t1 <= entries.at(2).last_write_time());
  REQUIRE(entries.at(2).last_write_time() <= t2);
  REQUIRE(entries.at(3).name() == "file2.txt");
  REQUIRE(entries.at(3).type() == file_type::regular);
  REQUIRE(entries.at(3).permissions() == perms::all);
  REQUIRE(t2 <= entries.at(3).last_write_time());
  REQUIRE(entries.at(3).last_write_time() <= t3);

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_dstatus_test", "[file]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto server = directory_server::create(t, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  directory_client tree(HOST, PORT);
  REQUIRE_NOTHROW(tree.create("/sandbox/file.txt", "testtype", "/tmp", 1, 1, 0));
  REQUIRE_THROWS_AS(tree.dstatus("/sandbox"), directory_service_exception);
  REQUIRE(tree.dstatus("/sandbox/file.txt").mode() == std::vector<storage_mode>{storage_mode::in_memory});
  REQUIRE(tree.dstatus("/sandbox/file.txt").backing_path() == "/tmp");
  REQUIRE(tree.dstatus("/sandbox/file.txt").data_blocks().size() == 1);

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}

TEST_CASE("rpc_file_type_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  auto t = std::make_shared<directory_tree>(alloc, sm);
  auto server = directory_server::create(t, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  directory_client tree(HOST, PORT);

  REQUIRE_NOTHROW(tree.create("/sandbox/file.txt", "testtype", "/tmp", 1, 1, 0));
  REQUIRE(tree.is_regular_file("/sandbox/file.txt"));
  REQUIRE_FALSE(tree.is_directory("/sandbox/file.txt"));

  REQUIRE(tree.is_directory("/sandbox"));
  REQUIRE_FALSE(tree.is_regular_file("/sandbox"));

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}
