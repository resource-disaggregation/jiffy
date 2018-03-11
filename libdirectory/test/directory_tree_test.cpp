#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "../src/tree/directory_tree.h"

using namespace ::elasticmem::directory;

TEST_CASE("create_directory_test", "[dir]") {
  directory_tree tree;

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
  directory_tree tree;

  REQUIRE_NOTHROW(tree.create_file("/sandbox/a.txt"));
  REQUIRE(tree.is_regular_file("/sandbox/a.txt"));

  REQUIRE_NOTHROW(tree.create_file("/sandbox/foo/bar/baz/a"));
  REQUIRE(tree.is_regular_file("/sandbox/foo/bar/baz/a"));

  REQUIRE_THROWS_AS(tree.create_file("/sandbox/foo/bar/baz/a/b"), directory_service_exception);
  REQUIRE_THROWS_AS(tree.create_directories("/sandbox/foo/bar/baz/a/b"),
                    directory_service_exception);
}

TEST_CASE("exists_test", "[file][dir]") {
  directory_tree tree;

  REQUIRE_NOTHROW(tree.create_file("/sandbox/file"));
  REQUIRE(tree.exists("/sandbox"));
  REQUIRE(tree.exists("/sandbox/file"));
  REQUIRE(!tree.exists("/sandbox/foo"));
}

TEST_CASE("file_size", "[file][dir][grow][shrink]") {
  directory_tree tree;

  REQUIRE_NOTHROW(tree.create_file("/sandbox/file"));
  REQUIRE(tree.file_size("/sandbox/file") == 0);
  REQUIRE_NOTHROW(tree.grow("/sandbox/file", 20));
  REQUIRE(tree.file_size("/sandbox/file") == 20);
  REQUIRE_NOTHROW(tree.shrink("/sandbox/file", 5));
  REQUIRE(tree.file_size("/sandbox/file") == 15);

  REQUIRE_NOTHROW(tree.create_file("/sandbox/file2"));
  REQUIRE_NOTHROW(tree.grow("/sandbox/file2", 20));
  REQUIRE(tree.file_size("/sandbox") == 35);
}

TEST_CASE("last_write_time_test", "[file][dir][touch]") {
  directory_tree tree;

  std::time_t before = std::time(nullptr);
  REQUIRE_NOTHROW(tree.create_file("/sandbox/file"));
  std::time_t after = std::time(nullptr);
  REQUIRE(before <= tree.last_write_time("/sandbox/file"));
  REQUIRE(tree.last_write_time("/sandbox/file") <= after);

  before = std::time(nullptr);
  REQUIRE_NOTHROW(tree.touch("/sandbox/file"));
  after = std::time(nullptr);
  REQUIRE(before <= tree.last_write_time("/sandbox/file"));
  REQUIRE(tree.last_write_time("/sandbox/file") <= after);

  before = std::time(nullptr);
  REQUIRE_NOTHROW(tree.touch("/sandbox"));
  after = std::time(nullptr);
  REQUIRE(before <= tree.last_write_time("/sandbox"));
  REQUIRE(tree.last_write_time("/sandbox") <= after);
  REQUIRE(before <= tree.last_write_time("/sandbox/file"));
  REQUIRE(tree.last_write_time("/sandbox/file") <= after);
  REQUIRE(tree.last_write_time("/sandbox") == tree.last_write_time("/sandbox/file"));
}

TEST_CASE("permissions_test", "[file][dir]") {
  directory_tree tree;

  REQUIRE_NOTHROW(tree.create_file("/sandbox/file"));
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
  directory_tree tree;

  REQUIRE_NOTHROW(tree.create_file("/sandbox/abcdef/example/a/b"));

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
  directory_tree tree;
  REQUIRE_NOTHROW(tree.create_file("/sandbox/from/file1.txt"));
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
  directory_tree tree;

  std::time_t before = std::time(nullptr);
  REQUIRE_NOTHROW(tree.create_file("/sandbox/file"));
  std::time_t after = std::time(nullptr);
  REQUIRE(tree.status("/sandbox/file").permissions() == perms::all);
  REQUIRE(tree.status("/sandbox/file").type() == file_type::regular);
  REQUIRE(before <= tree.status("/sandbox/file").last_write_time());
  REQUIRE(tree.status("/sandbox/file").last_write_time() <= after);

  before = std::time(nullptr);
  REQUIRE_NOTHROW(tree.create_directory("/sandbox/dir"));
  after = std::time(nullptr);
  REQUIRE(tree.status("/sandbox/dir").permissions() == perms::all);
  REQUIRE(tree.status("/sandbox/dir").type() == file_type::directory);
  REQUIRE(before <= tree.status("/sandbox/dir").last_write_time());
  REQUIRE(tree.status("/sandbox/dir").last_write_time() <= after);
}

TEST_CASE("directory_entries_test", "[file][dir]") {
  directory_tree tree;

  std::time_t t0 = std::time(nullptr);
  REQUIRE_NOTHROW(tree.create_directories("/sandbox/a/b"));
  std::time_t t1 = std::time(nullptr);
  REQUIRE_NOTHROW(tree.create_file("/sandbox/file1.txt"));
  std::time_t t2 = std::time(nullptr);
  REQUIRE_NOTHROW(tree.create_file("/sandbox/file2.txt"));
  std::time_t t3 = std::time(nullptr);

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
  directory_tree tree;

  std::time_t t0 = std::time(nullptr);
  REQUIRE_NOTHROW(tree.create_directories("/sandbox/a/b"));
  std::time_t t1 = std::time(nullptr);
  REQUIRE_NOTHROW(tree.create_file("/sandbox/file1.txt"));
  std::time_t t2 = std::time(nullptr);
  REQUIRE_NOTHROW(tree.create_file("/sandbox/file2.txt"));
  std::time_t t3 = std::time(nullptr);

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
  directory_tree tree;
  REQUIRE_NOTHROW(tree.create_file("/sandbox/file.txt"));
  REQUIRE_THROWS_AS(tree.dstatus("/sandbox"), directory_service_exception);
  REQUIRE(tree.dstatus("/sandbox/file.txt").mode() == storage_mode::in_memory);
  REQUIRE(tree.dstatus("/sandbox/file.txt").persistent_store_prefix().empty());
  REQUIRE(tree.dstatus("/sandbox/file.txt").data_blocks().empty());

  data_status status(storage_mode::in_memory_grace, "/tmp", {"a", "b", "c", "d"});
  REQUIRE_NOTHROW(tree.dstatus("/sandbox/file.txt", status));
  REQUIRE(tree.dstatus("/sandbox/file.txt").mode() == storage_mode::in_memory_grace);
  REQUIRE(tree.dstatus("/sandbox/file.txt").persistent_store_prefix() == "/tmp");
  REQUIRE(tree.dstatus("/sandbox/file.txt").data_blocks().size() == 4);
  REQUIRE(tree.dstatus("/sandbox/file.txt").data_blocks().at(0) == "a");
  REQUIRE(tree.dstatus("/sandbox/file.txt").data_blocks().at(1) == "b");
  REQUIRE(tree.dstatus("/sandbox/file.txt").data_blocks().at(2) == "c");
  REQUIRE(tree.dstatus("/sandbox/file.txt").data_blocks().at(3) == "d");
}

TEST_CASE("storage_mode_test", "[file]") {
  directory_tree tree;

  REQUIRE_NOTHROW(tree.create_file("/sandbox/file.txt"));
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
  directory_tree tree;

  REQUIRE_NOTHROW(tree.create_file("/sandbox/file.txt"));
  REQUIRE_THROWS_AS(tree.data_blocks("/sandbox"), directory_service_exception);
  REQUIRE(tree.data_blocks("/sandbox/file.txt").empty());

  REQUIRE_NOTHROW(tree.add_data_block("/sandbox/file.txt", "a"));
  REQUIRE_NOTHROW(tree.add_data_block("/sandbox/file.txt", "b"));
  REQUIRE_NOTHROW(tree.add_data_block("/sandbox/file.txt", "c"));
  REQUIRE_NOTHROW(tree.add_data_block("/sandbox/file.txt", "d"));
  REQUIRE(tree.data_blocks("/sandbox/file.txt").size() == 4);
  REQUIRE(tree.data_blocks("/sandbox/file.txt").at(0) == "a");
  REQUIRE(tree.data_blocks("/sandbox/file.txt").at(1) == "b");
  REQUIRE(tree.data_blocks("/sandbox/file.txt").at(2) == "c");
  REQUIRE(tree.data_blocks("/sandbox/file.txt").at(3) == "d");

  REQUIRE_NOTHROW(tree.remove_data_block("/sandbox/file.txt", 1));
  REQUIRE(tree.data_blocks("/sandbox/file.txt").size() == 3);
  REQUIRE(tree.data_blocks("/sandbox/file.txt").at(0) == "a");
  REQUIRE(tree.data_blocks("/sandbox/file.txt").at(1) == "c");
  REQUIRE(tree.data_blocks("/sandbox/file.txt").at(2) == "d");

  REQUIRE_NOTHROW(tree.remove_data_block("/sandbox/file.txt", "c"));
  REQUIRE(tree.data_blocks("/sandbox/file.txt").size() == 2);
  REQUIRE(tree.data_blocks("/sandbox/file.txt").at(0) == "a");
  REQUIRE(tree.data_blocks("/sandbox/file.txt").at(1) == "d");

  REQUIRE_NOTHROW(tree.remove_all_data_blocks("/sandbox/file.txt"));
  REQUIRE(tree.data_blocks("/sandbox/file.txt").empty());
}

TEST_CASE("file_type_test", "[file][dir]") {
  directory_tree tree;

  REQUIRE_NOTHROW(tree.create_file("/sandbox/file.txt"));
  REQUIRE(tree.is_regular_file("/sandbox/file.txt"));
  REQUIRE_FALSE(tree.is_directory("/sandbox/file.txt"));

  REQUIRE(tree.is_directory("/sandbox"));
  REQUIRE_FALSE(tree.is_regular_file("/sandbox"));
}