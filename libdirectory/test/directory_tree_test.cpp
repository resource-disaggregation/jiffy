#include "catch.hpp"
#include "../src/tree/directory_tree.h"
#include "../src/tree/random_block_allocator.h"

using namespace ::elasticmem::directory;

TEST_CASE("create_directory_test", "[dir]") {
  std::vector<std::string> blocks = {"a", "b", "c", "d", "e", "f", "g", "h"};
  auto alloc = std::make_shared<random_block_allocator>(blocks);
  directory_tree tree(alloc);

  REQUIRE_NOTHROW(tree.create_directories("/sandbox/1/2/a"));
  REQUIRE(tree.is_directory("/sandbox/1/2/a"));
  REQUIRE(tree.is_directory("/sandbox/1/2"));
  REQUIRE(tree.is_directory("/sandbox/1"));
  REQUIRE(tree.is_directory("/sandbox"));

  REQUIRE_NOTHROW(tree.create_directory("/sandbox/1/2/b"));
  REQUIRE(tree.is_directory("/sandbox/1/2/b"));

  REQUIRE_THROWS_AS(tree.create_directory("/sandbox/1/1/b"), directory_service_exception);
}

TEST_CASE("create_file_test", "[file][dir]") {
  std::vector<std::string> blocks = {"a", "b", "c", "d", "e", "f", "g", "h"};
  auto alloc = std::make_shared<random_block_allocator>(blocks);
  directory_tree tree(alloc);

  REQUIRE_NOTHROW(tree.create_file("/sandbox/a.txt", "/tmp"));
  REQUIRE(tree.is_regular_file("/sandbox/a.txt"));

  REQUIRE_NOTHROW(tree.create_file("/sandbox/foo/bar/baz/a", "/tmp"));
  REQUIRE(tree.is_regular_file("/sandbox/foo/bar/baz/a"));

  REQUIRE_THROWS_AS(tree.create_file("/sandbox/foo/bar/baz/a/b", "/tmp"), directory_service_exception);
  REQUIRE_THROWS_AS(tree.create_directories("/sandbox/foo/bar/baz/a/b"),
                    directory_service_exception);
}

TEST_CASE("exists_test", "[file][dir]") {
  std::vector<std::string> blocks = {"a", "b", "c", "d", "e", "f", "g", "h"};
  auto alloc = std::make_shared<random_block_allocator>(blocks);
  directory_tree tree(alloc);

  REQUIRE_NOTHROW(tree.create_file("/sandbox/file", "/tmp"));
  REQUIRE(tree.exists("/sandbox"));
  REQUIRE(tree.exists("/sandbox/file"));
  REQUIRE(!tree.exists("/sandbox/foo"));
}

TEST_CASE("file_size", "[file][dir][grow][shrink]") {
  std::vector<std::string> blocks = {"a", "b", "c", "d", "e", "f", "g", "h"};
  auto alloc = std::make_shared<random_block_allocator>(blocks);
  directory_tree tree(alloc);

  REQUIRE_NOTHROW(tree.create_file("/sandbox/file", "/tmp"));
  REQUIRE(tree.file_size("/sandbox/file") == 0);
  REQUIRE_NOTHROW(tree.grow("/sandbox/file", 20));
  REQUIRE(tree.file_size("/sandbox/file") == 20);
  REQUIRE_NOTHROW(tree.shrink("/sandbox/file", 5));
  REQUIRE(tree.file_size("/sandbox/file") == 15);

  REQUIRE_NOTHROW(tree.create_file("/sandbox/file2", "/tmp"));
  REQUIRE_NOTHROW(tree.grow("/sandbox/file2", 20));
  REQUIRE(tree.file_size("/sandbox") == 35);
}

TEST_CASE("last_write_time_test", "[file][dir][touch]") {
  std::vector<std::string> blocks = {"a", "b", "c", "d", "e", "f", "g", "h"};
  auto alloc = std::make_shared<random_block_allocator>(blocks);
  directory_tree tree(alloc);

  std::uint64_t before = detail::now_ms();
  REQUIRE_NOTHROW(tree.create_file("/sandbox/file", "/tmp"));
  std::uint64_t after = detail::now_ms();
  REQUIRE(before <= tree.last_write_time("/sandbox/file"));
  REQUIRE(tree.last_write_time("/sandbox/file") <= after);

  before = detail::now_ms();
  REQUIRE_NOTHROW(tree.touch("/sandbox/file"));
  after = detail::now_ms();
  REQUIRE(before <= tree.last_write_time("/sandbox/file"));
  REQUIRE(tree.last_write_time("/sandbox/file") <= after);

  before = detail::now_ms();
  REQUIRE_NOTHROW(tree.touch("/sandbox"));
  after = detail::now_ms();
  REQUIRE(before <= tree.last_write_time("/sandbox"));
  REQUIRE(tree.last_write_time("/sandbox") <= after);
  REQUIRE(before <= tree.last_write_time("/sandbox/file"));
  REQUIRE(tree.last_write_time("/sandbox/file") <= after);
  REQUIRE(tree.last_write_time("/sandbox") == tree.last_write_time("/sandbox/file"));
}

TEST_CASE("permissions_test", "[file][dir]") {
  std::vector<std::string> blocks = {"a", "b", "c", "d", "e", "f", "g", "h"};
  auto alloc = std::make_shared<random_block_allocator>(blocks);
  directory_tree tree(alloc);

  REQUIRE_NOTHROW(tree.create_file("/sandbox/file", "/tmp"));
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

TEST_CASE("remove_test", "[file][dir]") {
  std::vector<std::string> blocks = {"a", "b", "c", "d", "e", "f", "g", "h"};
  auto alloc = std::make_shared<random_block_allocator>(blocks);
  directory_tree tree(alloc);

  REQUIRE_NOTHROW(tree.create_file("/sandbox/abcdef/example/a/b", "/tmp"));

  REQUIRE_NOTHROW(tree.remove("/sandbox/abcdef/example/a/b"));
  REQUIRE(!tree.exists("/sandbox/abcdef/example/a/b"));

  REQUIRE_NOTHROW(tree.remove("/sandbox/abcdef/example/a"));
  REQUIRE(!tree.exists("/sandbox/abcdef/example/a"));

  REQUIRE_THROWS_AS(tree.remove("/sandbox/abcdef"), directory_service_exception);
  REQUIRE(tree.exists("/sandbox/abcdef"));

  REQUIRE_NOTHROW(tree.remove_all("/sandbox/abcdef"));
  REQUIRE(!tree.exists("/sandbox/abcdef"));
}

TEST_CASE("rename_test", "[file][dir]") {
  std::vector<std::string> blocks = {"a", "b", "c", "d", "e", "f", "g", "h"};
  auto alloc = std::make_shared<random_block_allocator>(blocks);
  directory_tree tree(alloc);

  REQUIRE_NOTHROW(tree.create_file("/sandbox/from/file1.txt", "/tmp"));
  REQUIRE_NOTHROW(tree.create_directory("/sandbox/to"));

  REQUIRE_THROWS_AS(tree.rename("/sandbox/from/file1.txt", "/sandbox/to/"), directory_service_exception);
  REQUIRE_NOTHROW(tree.rename("/sandbox/from/file1.txt", "/sandbox/to/file2.txt"));
  REQUIRE(tree.exists("/sandbox/to/file2.txt"));
  REQUIRE(!tree.exists("/sandbox/from/file1.txt"));

  REQUIRE_THROWS_AS(tree.rename("/sandbox/from", "/sandbox/to"), directory_service_exception);
  REQUIRE_NOTHROW(tree.rename("/sandbox/from", "/sandbox/to/subdir"));
  REQUIRE(tree.exists("/sandbox/to/subdir"));
  REQUIRE(!tree.exists("/sandbox/from"));
}

TEST_CASE("status_test", "[file][dir]") {
  std::vector<std::string> blocks = {"a", "b", "c", "d", "e", "f", "g", "h"};
  auto alloc = std::make_shared<random_block_allocator>(blocks);
  directory_tree tree(alloc);

  std::uint64_t before = detail::now_ms();
  REQUIRE_NOTHROW(tree.create_file("/sandbox/file", "/tmp"));
  std::uint64_t after = detail::now_ms();
  REQUIRE(tree.status("/sandbox/file").permissions() == perms::all);
  REQUIRE(tree.status("/sandbox/file").type() == file_type::regular);
  REQUIRE(before <= tree.status("/sandbox/file").last_write_time());
  REQUIRE(tree.status("/sandbox/file").last_write_time() <= after);

  before = detail::now_ms();
  REQUIRE_NOTHROW(tree.create_directory("/sandbox/dir"));
  after = detail::now_ms();
  REQUIRE(tree.status("/sandbox/dir").permissions() == perms::all);
  REQUIRE(tree.status("/sandbox/dir").type() == file_type::directory);
  REQUIRE(before <= tree.status("/sandbox/dir").last_write_time());
  REQUIRE(tree.status("/sandbox/dir").last_write_time() <= after);
}

TEST_CASE("directory_entries_test", "[file][dir]") {
  std::vector<std::string> blocks = {"a", "b", "c", "d", "e", "f", "g", "h"};
  auto alloc = std::make_shared<random_block_allocator>(blocks);
  directory_tree tree(alloc);

  std::uint64_t t0 = detail::now_ms();
  REQUIRE_NOTHROW(tree.create_directories("/sandbox/a/b"));
  std::uint64_t t1 = detail::now_ms();
  REQUIRE_NOTHROW(tree.create_file("/sandbox/file1.txt", "/tmp"));
  std::uint64_t t2 = detail::now_ms();
  REQUIRE_NOTHROW(tree.create_file("/sandbox/file2.txt", "/tmp"));
  std::uint64_t t3 = detail::now_ms();

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
  std::vector<std::string> blocks = {"a", "b", "c", "d", "e", "f", "g", "h"};
  auto alloc = std::make_shared<random_block_allocator>(blocks);
  directory_tree tree(alloc);

  std::uint64_t t0 = detail::now_ms();
  REQUIRE_NOTHROW(tree.create_directories("/sandbox/a/b"));
  std::uint64_t t1 = detail::now_ms();
  REQUIRE_NOTHROW(tree.create_file("/sandbox/file1.txt", "/tmp"));
  std::uint64_t t2 = detail::now_ms();
  REQUIRE_NOTHROW(tree.create_file("/sandbox/file2.txt", "/tmp"));
  std::uint64_t t3 = detail::now_ms();

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
  std::vector<std::string> blocks = {"a", "b", "c", "d", "e", "f", "g", "h"};
  auto alloc = std::make_shared<random_block_allocator>(blocks);
  directory_tree tree(alloc);

  REQUIRE_NOTHROW(tree.create_file("/sandbox/file.txt", "/tmp"));
  REQUIRE_THROWS_AS(tree.dstatus("/sandbox"), directory_service_exception);
  REQUIRE(tree.dstatus("/sandbox/file.txt").mode() == storage_mode::in_memory);
  REQUIRE(tree.dstatus("/sandbox/file.txt").persistent_store_prefix() == "/tmp");
  REQUIRE(tree.dstatus("/sandbox/file.txt").data_blocks().size() == 1);

  data_status status(storage_mode::in_memory_grace, "/tmp2", {"a", "b", "c", "d"});
  REQUIRE_NOTHROW(tree.dstatus("/sandbox/file.txt", status));
  REQUIRE(tree.dstatus("/sandbox/file.txt").mode() == storage_mode::in_memory_grace);
  REQUIRE(tree.dstatus("/sandbox/file.txt").persistent_store_prefix() == "/tmp2");
  REQUIRE(tree.dstatus("/sandbox/file.txt").data_blocks().size() == 4);
  REQUIRE(tree.dstatus("/sandbox/file.txt").data_blocks().at(0) == "a");
  REQUIRE(tree.dstatus("/sandbox/file.txt").data_blocks().at(1) == "b");
  REQUIRE(tree.dstatus("/sandbox/file.txt").data_blocks().at(2) == "c");
  REQUIRE(tree.dstatus("/sandbox/file.txt").data_blocks().at(3) == "d");
}

TEST_CASE("storage_mode_test", "[file]") {
  std::vector<std::string> blocks = {"a", "b", "c", "d", "e", "f", "g", "h"};
  auto alloc = std::make_shared<random_block_allocator>(blocks);
  directory_tree tree(alloc);

  REQUIRE_NOTHROW(tree.create_file("/sandbox/file.txt", "/tmp"));
  REQUIRE_THROWS_AS(tree.mode("/sandbox"), directory_service_exception);
  REQUIRE(tree.mode("/sandbox/file.txt") == storage_mode::in_memory);

  REQUIRE_NOTHROW(tree.mode("/sandbox/file.txt", storage_mode::in_memory_grace));
  REQUIRE(tree.mode("/sandbox/file.txt") == storage_mode::in_memory_grace);

  REQUIRE_NOTHROW(tree.mode("/sandbox/file.txt", storage_mode::flushing));
  REQUIRE(tree.mode("/sandbox/file.txt") == storage_mode::flushing);

  REQUIRE_NOTHROW(tree.mode("/sandbox/file.txt", storage_mode::on_disk));
  REQUIRE(tree.mode("/sandbox/file.txt") == storage_mode::on_disk);
}

TEST_CASE("blocks_test", "[file]") {
  std::vector<std::string> blocks = {"a", "b", "c", "d"};
  auto alloc = std::make_shared<random_block_allocator>(blocks);
  directory_tree tree(alloc);

  REQUIRE_NOTHROW(tree.create_file("/sandbox/file.txt", "/tmp"));
  REQUIRE_THROWS_AS(tree.data_blocks("/sandbox"), directory_service_exception);
  REQUIRE(tree.data_blocks("/sandbox/file.txt").size() == 1);

  REQUIRE_NOTHROW(tree.add_data_block("/sandbox/file.txt"));
  REQUIRE_NOTHROW(tree.add_data_block("/sandbox/file.txt"));
  REQUIRE_NOTHROW(tree.add_data_block("/sandbox/file.txt"));
  REQUIRE_THROWS_AS(tree.add_data_block("/sandbox/file.txt"), std::out_of_range);
  std::vector<std::string> file_blocks;
  REQUIRE_NOTHROW(file_blocks = tree.data_blocks("/sandbox/file.txt"));
  std::sort(file_blocks.begin(), file_blocks.end());
  REQUIRE(file_blocks.size() == 4);
  REQUIRE(file_blocks.at(0) == "a");
  REQUIRE(file_blocks.at(1) == "b");
  REQUIRE(file_blocks.at(2) == "c");
  REQUIRE(file_blocks.at(3) == "d");
  REQUIRE(alloc->num_free_blocks() == 0);
  REQUIRE(alloc->num_allocated_blocks() == 4);

  REQUIRE_NOTHROW(tree.remove_data_block("/sandbox/file.txt", "c"));
  REQUIRE_NOTHROW(file_blocks = tree.data_blocks("/sandbox/file.txt"));
  std::sort(file_blocks.begin(), file_blocks.end());
  REQUIRE(file_blocks.size() == 3);
  REQUIRE(file_blocks.at(0) == "a");
  REQUIRE(file_blocks.at(1) == "b");
  REQUIRE(file_blocks.at(2) == "d");
  REQUIRE(alloc->num_free_blocks() == 1);
  REQUIRE(alloc->num_allocated_blocks() == 3);

  REQUIRE_NOTHROW(tree.remove_all_data_blocks("/sandbox/file.txt"));
  REQUIRE(tree.data_blocks("/sandbox/file.txt").empty());
  REQUIRE(alloc->num_free_blocks() == 4);
  REQUIRE(alloc->num_allocated_blocks() == 0);
}

TEST_CASE("file_type_test", "[file][dir]") {
  std::vector<std::string> blocks = {"a", "b", "c", "d", "e", "f", "g", "h"};
  auto alloc = std::make_shared<random_block_allocator>(blocks);
  directory_tree tree(alloc);

  REQUIRE_NOTHROW(tree.create_file("/sandbox/file.txt", "/tmp"));
  REQUIRE(tree.is_regular_file("/sandbox/file.txt"));
  REQUIRE_FALSE(tree.is_directory("/sandbox/file.txt"));

  REQUIRE(tree.is_directory("/sandbox"));
  REQUIRE_FALSE(tree.is_regular_file("/sandbox"));
}