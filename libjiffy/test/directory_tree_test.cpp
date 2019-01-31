#include "catch.hpp"
#include "jiffy/directory/fs/directory_tree.h"
#include "jiffy/directory/block/random_block_allocator.h"
#include "test_utils.h"

using namespace ::jiffy::directory;
using namespace ::jiffy::utils;

TEST_CASE("create_directory_test", "[dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  directory_tree tree(alloc, sm);

  REQUIRE_NOTHROW(tree.create_directories("/sandbox/1/2/a"));
  REQUIRE(tree.is_directory("/sandbox/1/2/a"));
  REQUIRE(tree.is_directory("/sandbox/1/2"));
  REQUIRE(tree.is_directory("/sandbox/1"));
  REQUIRE(tree.is_directory("/sandbox"));

  REQUIRE_NOTHROW(tree.create_directory("/sandbox/1/2/b"));
  REQUIRE(tree.is_directory("/sandbox/1/2/b"));

  REQUIRE_THROWS_AS(tree.create_directory("/sandbox/1/1/b"), directory_ops_exception);
}

TEST_CASE("create_file_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  directory_tree tree(alloc, sm);

  REQUIRE_NOTHROW(tree.create("/sandbox/a.txt", "testtype", "local://tmp", 1, 1, 0));
  REQUIRE(tree.is_regular_file("/sandbox/a.txt"));

  REQUIRE_NOTHROW(tree.create("/sandbox/foo/bar/baz/a", "testtype", "local://tmp", 1, 1, 0));
  REQUIRE(tree.is_regular_file("/sandbox/foo/bar/baz/a"));

  REQUIRE_THROWS_AS(tree.create("/sandbox/foo/bar/baz/a/b", "testtype", "local://tmp", 1, 1, 0), directory_ops_exception);
  REQUIRE_THROWS_AS(tree.create_directories("/sandbox/foo/bar/baz/a/b"), directory_ops_exception);
}

TEST_CASE("open_file_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  directory_tree tree(alloc, sm);

  REQUIRE_NOTHROW(tree.create("/sandbox/a.txt", "testtype", "local://tmp", 1, 1, 0));
  REQUIRE_NOTHROW(tree.create("/sandbox/foo/bar/baz/a", "testtype", "local://tmp", 1, 1, 0));

  data_status s;
  REQUIRE_NOTHROW(s = tree.open("/sandbox/a.txt"));
  REQUIRE(s.chain_length() == 1);
  REQUIRE(s.mode() == std::vector<storage_mode>{storage_mode::in_memory});
  REQUIRE(s.backing_path() == "local://tmp");
  REQUIRE(s.data_blocks().size() == 1);

  REQUIRE_NOTHROW(s = tree.open("/sandbox/foo/bar/baz/a"));
  REQUIRE(s.chain_length() == 1);
  REQUIRE(s.mode() == std::vector<storage_mode>{storage_mode::in_memory});
  REQUIRE(s.backing_path() == "local://tmp");
  REQUIRE(s.data_blocks().size() == 1);

  REQUIRE_THROWS_AS(tree.open("/sandbox/b.txt"), directory_ops_exception);
}

TEST_CASE("open_or_create_file_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  directory_tree tree(alloc, sm);

  REQUIRE_NOTHROW(tree.create("/sandbox/a.txt", "testtype", "local://tmp", 1, 1, 0));
  REQUIRE_NOTHROW(tree.create("/sandbox/foo/bar/baz/a", "testtype", "local://tmp", 1, 1, 0));

  data_status s;
  REQUIRE_NOTHROW(s = tree.open_or_create("/sandbox/a.txt", "testtype", "local://tmp", 1, 1, 0));
  REQUIRE(s.chain_length() == 1);
  REQUIRE(s.mode() == std::vector<storage_mode>{storage_mode::in_memory});
  REQUIRE(s.backing_path() == "local://tmp");
  REQUIRE(s.data_blocks().size() == 1);

  REQUIRE_NOTHROW(s = tree.open_or_create("/sandbox/foo/bar/baz/a", "testtype", "local://tmp", 1, 1, 0));
  REQUIRE(s.chain_length() == 1);
  REQUIRE(s.mode() == std::vector<storage_mode>{storage_mode::in_memory});
  REQUIRE(s.backing_path() == "local://tmp");
  REQUIRE(s.data_blocks().size() == 1);

  REQUIRE_NOTHROW(s = tree.open_or_create("/sandbox/b.txt", "testtype", "local://tmp", 1, 1, 0));
  REQUIRE(s.chain_length() == 1);
  REQUIRE(s.mode() == std::vector<storage_mode>{storage_mode::in_memory});
  REQUIRE(s.backing_path() == "local://tmp");
  REQUIRE(s.data_blocks().size() == 1);
}

TEST_CASE("exists_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  directory_tree tree(alloc, sm);

  REQUIRE_NOTHROW(tree.create("/sandbox/file", "testtype", "local://tmp", 1, 1, 0));
  REQUIRE(tree.exists("/sandbox"));
  REQUIRE(tree.exists("/sandbox/file"));
  REQUIRE(!tree.exists("/sandbox/foo"));
}

TEST_CASE("last_write_time_test", "[file][dir][touch]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  directory_tree tree(alloc, sm);

  std::uint64_t before = time_utils::now_ms();
  REQUIRE_NOTHROW(tree.create("/sandbox/file", "testtype", "local://tmp", 1, 1, 0));
  std::uint64_t after = time_utils::now_ms();
  REQUIRE(before <= tree.last_write_time("/sandbox/file"));
  REQUIRE(tree.last_write_time("/sandbox/file") <= after);

  before = time_utils::now_ms();
  REQUIRE_NOTHROW(tree.touch("/sandbox/file"));
  after = time_utils::now_ms();
  REQUIRE(before <= tree.last_write_time("/sandbox/file"));
  REQUIRE(tree.last_write_time("/sandbox/file") <= after);

  before = time_utils::now_ms();
  REQUIRE_NOTHROW(tree.touch("/sandbox"));
  after = time_utils::now_ms();
  REQUIRE(before <= tree.last_write_time("/sandbox"));
  REQUIRE(tree.last_write_time("/sandbox") <= after);
  REQUIRE(before <= tree.last_write_time("/sandbox/file"));
  REQUIRE(tree.last_write_time("/sandbox/file") <= after);
  REQUIRE(tree.last_write_time("/sandbox") == tree.last_write_time("/sandbox/file"));
}

TEST_CASE("permissions_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  directory_tree tree(alloc, sm);

  REQUIRE_NOTHROW(tree.create("/sandbox/file", "testtype", "local://tmp", 1, 1, 0));
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
}

TEST_CASE("path_remove_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  directory_tree tree(alloc, sm);

  REQUIRE_NOTHROW(tree.create("/sandbox/abcdef/example/a/b", "testtype", "local://tmp", 1, 1, 0));
  REQUIRE(alloc->num_free_blocks() == 3);

  REQUIRE_NOTHROW(tree.remove("/sandbox/abcdef/example/a/b"));
  REQUIRE(!tree.exists("/sandbox/abcdef/example/a/b"));

  REQUIRE_NOTHROW(tree.remove("/sandbox/abcdef/example/a"));
  REQUIRE(!tree.exists("/sandbox/abcdef/example/a"));

  REQUIRE_THROWS_AS(tree.remove("/sandbox/abcdef"), directory_ops_exception);
  REQUIRE(tree.exists("/sandbox/abcdef"));

  REQUIRE_NOTHROW(tree.remove_all("/sandbox/abcdef"));
  REQUIRE(!tree.exists("/sandbox/abcdef"));
  REQUIRE(alloc->num_free_blocks() == 4);

  REQUIRE(sm->COMMANDS.size() == 3);
  REQUIRE(sm->COMMANDS[0] == "create_partition:0:testtype:0:");
  REQUIRE(sm->COMMANDS[1] == "setup_chain:0:/sandbox/abcdef/example/a/b:0:nil");
  REQUIRE(sm->COMMANDS[2] == "destroy_partition:0");
}

TEST_CASE("path_sync_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  directory_tree tree(alloc, sm);
  REQUIRE_NOTHROW(tree.create("/sandbox/abcdef/example/a/b", "testtype", "local://tmp", 1, 1, 0));
  REQUIRE_NOTHROW(tree.create("/sandbox/abcdef/example/c", "testtype", "local://tmp", 1, 1, 0));
  REQUIRE(alloc->num_free_blocks() == 2);

  REQUIRE_NOTHROW(tree.sync("/sandbox/abcdef/example/c", "local://tmp"));
  REQUIRE(tree.dstatus("/sandbox/abcdef/example/c").mode() == std::vector<storage_mode>{storage_mode::in_memory});

  REQUIRE_NOTHROW(tree.sync("/sandbox/abcdef/example/a", "local://tmp"));
  REQUIRE(tree.dstatus("/sandbox/abcdef/example/a/b").mode() == std::vector<storage_mode>{storage_mode::in_memory});

  REQUIRE(alloc->num_free_blocks() == 2);
  REQUIRE(sm->COMMANDS.size() == 6);
  REQUIRE(sm->COMMANDS[0] == "create_partition:0:testtype:0:");
  REQUIRE(sm->COMMANDS[1] == "setup_chain:0:/sandbox/abcdef/example/a/b:0:nil");
  REQUIRE(sm->COMMANDS[2] == "create_partition:1:testtype:0:");
  REQUIRE(sm->COMMANDS[3] == "setup_chain:1:/sandbox/abcdef/example/c:0:nil");
  REQUIRE(sm->COMMANDS[4] == "sync:1:local://tmp/0");
  REQUIRE(sm->COMMANDS[5] == "sync:0:local://tmp/0");
}

TEST_CASE("rename_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  directory_tree tree(alloc, sm);

  REQUIRE_NOTHROW(tree.create("/sandbox/from/file1.txt", "testtype", "local://tmp", 1, 1, 0));
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
}

TEST_CASE("status_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  directory_tree tree(alloc, sm);

  std::uint64_t before = time_utils::now_ms();
  REQUIRE_NOTHROW(tree.create("/sandbox/file", "testtype", "local://tmp", 1, 1, 0, perms::owner_all()));
  std::uint64_t after = time_utils::now_ms();
  REQUIRE(tree.status("/sandbox/file").permissions() == perms::owner_all);
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
}

TEST_CASE("directory_entries_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  directory_tree tree(alloc, sm);

  std::uint64_t t0 = time_utils::now_ms();
  REQUIRE_NOTHROW(tree.create_directories("/sandbox/a/b"));
  std::uint64_t t1 = time_utils::now_ms();
  REQUIRE_NOTHROW(tree.create("/sandbox/file1.txt", "testtype", "local://tmp", 1, 1, 0));
  std::uint64_t t2 = time_utils::now_ms();
  REQUIRE_NOTHROW(tree.create("/sandbox/file2.txt", "testtype", "local://tmp", 1, 1, 0));
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
}

TEST_CASE("recursive_directory_entries_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  directory_tree tree(alloc, sm);

  std::uint64_t t0 = time_utils::now_ms();
  REQUIRE_NOTHROW(tree.create_directories("/sandbox/a/b"));
  std::uint64_t t1 = time_utils::now_ms();
  REQUIRE_NOTHROW(tree.create("/sandbox/file1.txt", "testtype", "local://tmp", 1, 1, 0));
  std::uint64_t t2 = time_utils::now_ms();
  REQUIRE_NOTHROW(tree.create("/sandbox/file2.txt", "testtype", "local://tmp", 1, 1, 0));
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
}

TEST_CASE("dstatus_test", "[file]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  directory_tree tree(alloc, sm);

  REQUIRE_NOTHROW(tree.create("/sandbox/file.txt", "testtype", "local://tmp", 1, 1, 0, perms::all(), {"partition_name"},
      {"partition_metadata"}, {{"key", "value"}}));
  REQUIRE_THROWS_AS(tree.dstatus("/sandbox"), directory_ops_exception);
  REQUIRE(tree.dstatus("/sandbox/file.txt").mode() == std::vector<storage_mode>{storage_mode::in_memory});
  REQUIRE(tree.dstatus("/sandbox/file.txt").backing_path() == "local://tmp");
  REQUIRE(tree.dstatus("/sandbox/file.txt").data_blocks().size() == 1);
  REQUIRE(tree.dstatus("/sandbox/file.txt").data_blocks().at(0).block_ids[0] == "0");
  REQUIRE(tree.dstatus("/sandbox/file.txt").data_blocks().at(0).name == "partition_name");
  REQUIRE(tree.dstatus("/sandbox/file.txt").data_blocks().at(0).metadata == "partition_metadata");
  REQUIRE(tree.dstatus("/sandbox/file.txt").get_tag("key") == "value");
}

template <typename Map>
bool map_equals(Map const &lhs, Map const &rhs) {
  return lhs.size() == rhs.size() && std::equal(lhs.begin(), lhs.end(), rhs.begin());
}

TEST_CASE("tags_test", "[file]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  directory_tree tree(alloc, sm);

  REQUIRE_NOTHROW(tree.create("/sandbox/file.txt", "testtype", "local://tmp", 1, 1, 0, perms::all(), {"0"}, {""},
      {{"key", "value"}}));
  REQUIRE(tree.dstatus("/sandbox/file.txt").get_tag("key") == "value");
  REQUIRE_NOTHROW(tree.add_tags("/sandbox/file.txt", {{"key2", "value2"}}));
  REQUIRE(map_equals(tree.dstatus("/sandbox/file.txt").get_tags(), {{"key", "value"}, {"key2", "value2"}}));
  REQUIRE_NOTHROW(tree.add_tags("/sandbox/file.txt", {{"key", "value2"}}));
  REQUIRE(tree.dstatus("/sandbox/file.txt").get_tag("key") == "value2");
}

TEST_CASE("file_type_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  directory_tree tree(alloc, sm);

  REQUIRE_NOTHROW(tree.create("/sandbox/file.txt", "testtype", "local://tmp", 1, 1, 0));
  REQUIRE(tree.is_regular_file("/sandbox/file.txt"));
  REQUIRE_FALSE(tree.is_directory("/sandbox/file.txt"));

  REQUIRE(tree.is_directory("/sandbox"));
  REQUIRE_FALSE(tree.is_regular_file("/sandbox"));
}

TEST_CASE("add_remove_block_test", "[file][dir]") {
  auto alloc = std::make_shared<dummy_block_allocator>(4);
  auto sm = std::make_shared<dummy_storage_manager>();
  directory_tree tree(alloc, sm);

  REQUIRE(alloc->num_allocated_blocks() == 0);
  REQUIRE_NOTHROW(tree.create("/sandbox/file.txt", "testtype", "local://tmp", 1, 1, 0));
  REQUIRE(alloc->num_allocated_blocks() == 1);

  auto block = tree.add_block("/sandbox/file.txt", "1", "test");
  REQUIRE(alloc->num_allocated_blocks() == 2);
  REQUIRE(block.name == "1");
  REQUIRE(block.metadata == "test");
  REQUIRE(block.block_ids.size() == 1);
  REQUIRE(block.mode == storage_mode::in_memory);

  REQUIRE_NOTHROW(tree.remove_block("/sandbox/file.txt", "1"));
  REQUIRE(alloc->num_allocated_blocks() == 1);

  REQUIRE(sm->COMMANDS.size() == 5);
  REQUIRE(sm->COMMANDS[0] == "create_partition:0:testtype:0:");
  REQUIRE(sm->COMMANDS[1] == "setup_chain:0:/sandbox/file.txt:0:nil");
  REQUIRE(sm->COMMANDS[2] == "create_partition:1:testtype:1:test");
  REQUIRE(sm->COMMANDS[3] == "setup_chain:1:/sandbox/file.txt:0:nil");
  REQUIRE(sm->COMMANDS[4] == "destroy_partition:1");
}