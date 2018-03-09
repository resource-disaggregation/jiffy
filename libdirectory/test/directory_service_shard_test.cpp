#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "../src/shard/directory_service_shard.h"

using namespace ::elasticmem::directory;

TEST_CASE("create_directory_test", "[dir]") {
  directory_service_shard shard;

  REQUIRE_NOTHROW(shard.create_directories("/sandbox/1/2/a"));
  REQUIRE(shard.is_directory("/sandbox/1/2/a"));
  REQUIRE(shard.is_directory("/sandbox/1/2"));
  REQUIRE(shard.is_directory("/sandbox/1"));
  REQUIRE(shard.is_directory("/sandbox"));

  REQUIRE_NOTHROW(shard.create_directory("/sandbox/1/2/b"));
  REQUIRE(shard.is_directory("/sandbox/1/2/b"));

  REQUIRE_THROWS_AS(shard.create_directory("/sandbox/1/1/b"), directory_service_exception);
}

TEST_CASE("create_file_test", "[file][dir]") {
  directory_service_shard shard;

  REQUIRE_NOTHROW(shard.create_file("/sandbox/a.txt"));
  REQUIRE(shard.is_regular_file("/sandbox/a.txt"));

  REQUIRE_NOTHROW(shard.create_file("/sandbox/foo/bar/baz/a"));
  REQUIRE(shard.is_regular_file("/sandbox/foo/bar/baz/a"));

  REQUIRE_THROWS_AS(shard.create_file("/sandbox/foo/bar/baz/a/b"), directory_service_exception);
  REQUIRE_THROWS_AS(shard.create_directories("/sandbox/foo/bar/baz/a/b"),
                    directory_service_exception);
}

TEST_CASE("exists_test", "[file][dir]") {
  directory_service_shard shard;

  REQUIRE_NOTHROW(shard.create_file("/sandbox/file"));
  REQUIRE(shard.exists("/sandbox"));
  REQUIRE(shard.exists("/sandbox/file"));
  REQUIRE(!shard.exists("/sandbox/foo"));
}

TEST_CASE("file_size", "[file][dir][grow][shrink]") {
  directory_service_shard shard;

  REQUIRE_NOTHROW(shard.create_file("/sandbox/file"));
  REQUIRE(shard.file_size("/sandbox/file") == 0);
  REQUIRE_NOTHROW(shard.grow("/sandbox/file", 20));
  REQUIRE(shard.file_size("/sandbox/file") == 20);
  REQUIRE_NOTHROW(shard.shrink("/sandbox/file", 5));
  REQUIRE(shard.file_size("/sandbox/file") == 15);

  REQUIRE_NOTHROW(shard.create_file("/sandbox/file2"));
  REQUIRE_NOTHROW(shard.grow("/sandbox/file2", 20));
  REQUIRE(shard.file_size("/sandbox") == 35);
}

TEST_CASE("last_write_time_test", "[file][dir][touch]") {
  directory_service_shard shard;

  std::time_t before = std::time(nullptr);
  REQUIRE_NOTHROW(shard.create_file("/sandbox/file"));
  std::time_t after = std::time(nullptr);
  REQUIRE(before <= shard.last_write_time("/sandbox/file"));
  REQUIRE(shard.last_write_time("/sandbox/file") <= after);

  before = std::time(nullptr);
  REQUIRE_NOTHROW(shard.touch("/sandbox/file"));
  after = std::time(nullptr);
  REQUIRE(before <= shard.last_write_time("/sandbox/file"));
  REQUIRE(shard.last_write_time("/sandbox/file") <= after);

  before = std::time(nullptr);
  REQUIRE_NOTHROW(shard.touch("/sandbox"));
  after = std::time(nullptr);
  REQUIRE(before <= shard.last_write_time("/sandbox"));
  REQUIRE(shard.last_write_time("/sandbox") <= after);
  REQUIRE(before <= shard.last_write_time("/sandbox/file"));
  REQUIRE(shard.last_write_time("/sandbox/file") <= after);
  REQUIRE(shard.last_write_time("/sandbox") == shard.last_write_time("/sandbox/file"));
}

TEST_CASE("permissions_test", "[file][dir]") {
  directory_service_shard shard;

  REQUIRE_NOTHROW(shard.create_file("/sandbox/file"));
  REQUIRE(shard.permissions("/sandbox") == perms::all);
  REQUIRE(shard.permissions("/sandbox/file") == perms::all);

  REQUIRE_NOTHROW(shard.permissions("/sandbox/file", perms::owner_all | perms::group_all, perm_options::replace));
  REQUIRE(shard.permissions("/sandbox/file") == (perms::owner_all | perms::group_all));

  REQUIRE_NOTHROW(shard.permissions("/sandbox/file", perms::others_all, perm_options::add));
  REQUIRE(shard.permissions("/sandbox/file") == (perms::owner_all | perms::group_all | perms::others_all));

  REQUIRE_NOTHROW(shard.permissions("/sandbox/file", perms::group_all | perms::others_all, perm_options::remove));
  REQUIRE(shard.permissions("/sandbox/file") == perms::owner_all);

  REQUIRE_NOTHROW(shard.permissions("/sandbox", perms::owner_all | perms::group_all, perm_options::replace));
  REQUIRE(shard.permissions("/sandbox") == (perms::owner_all | perms::group_all));

  REQUIRE_NOTHROW(shard.permissions("/sandbox", perms::others_all, perm_options::add));
  REQUIRE(shard.permissions("/sandbox") == (perms::owner_all | perms::group_all | perms::others_all));

  REQUIRE_NOTHROW(shard.permissions("/sandbox", perms::group_all | perms::others_all, perm_options::remove));
  REQUIRE(shard.permissions("/sandbox") == perms::owner_all);
}

TEST_CASE("remove_test", "[file][dir]") {
  directory_service_shard shard;

  REQUIRE_NOTHROW(shard.create_file("/sandbox/abcdef/example/a/b"));

  REQUIRE_NOTHROW(shard.remove("/sandbox/abcdef/example/a/b"));
  REQUIRE(!shard.exists("/sandbox/abcdef/example/a/b"));

  REQUIRE_NOTHROW(shard.remove("/sandbox/abcdef/example/a"));
  REQUIRE(!shard.exists("/sandbox/abcdef/example/a"));

  REQUIRE_THROWS_AS(shard.remove("/sandbox/abcdef"), directory_service_exception);
  REQUIRE(shard.exists("/sandbox/abcdef"));

  REQUIRE_NOTHROW(shard.remove_all("/sandbox/abcdef"));
  REQUIRE(!shard.exists("/sandbox/abcdef"));
}

TEST_CASE("rename_test", "[file][dir]") {
  directory_service_shard shard;
  REQUIRE_NOTHROW(shard.create_file("/sandbox/from/file1.txt"));
  REQUIRE_NOTHROW(shard.create_directory("/sandbox/to"));

  REQUIRE_THROWS_AS(shard.rename("/sandbox/from/file1.txt", "/sandbox/to/"), directory_service_exception);
  REQUIRE_NOTHROW(shard.rename("/sandbox/from/file1.txt", "/sandbox/to/file2.txt"));
  REQUIRE(shard.exists("/sandbox/to/file2.txt"));
  REQUIRE(!shard.exists("/sandbox/from/file1.txt"));

  REQUIRE_THROWS_AS(shard.rename("/sandbox/from", "/sandbox/to"), directory_service_exception);
  REQUIRE_NOTHROW(shard.rename("/sandbox/from", "/sandbox/to/subdir"));
  REQUIRE(shard.exists("/sandbox/to/subdir"));
  REQUIRE(!shard.exists("/sandbox/from"));
}

TEST_CASE("status_test", "[file][dir]") {
  directory_service_shard shard;

  std::time_t before = std::time(nullptr);
  REQUIRE_NOTHROW(shard.create_file("/sandbox/file"));
  std::time_t after = std::time(nullptr);
  REQUIRE(shard.status("/sandbox/file").permissions() == perms::all);
  REQUIRE(shard.status("/sandbox/file").type() == file_type::regular);
  REQUIRE(before <= shard.status("/sandbox/file").last_write_time());
  REQUIRE(shard.status("/sandbox/file").last_write_time() <= after);

  before = std::time(nullptr);
  REQUIRE_NOTHROW(shard.create_directory("/sandbox/dir"));
  after = std::time(nullptr);
  REQUIRE(shard.status("/sandbox/dir").permissions() == perms::all);
  REQUIRE(shard.status("/sandbox/dir").type() == file_type::directory);
  REQUIRE(before <= shard.status("/sandbox/dir").last_write_time());
  REQUIRE(shard.status("/sandbox/dir").last_write_time() <= after);
}

TEST_CASE("directory_entries_test", "[file][dir]") {
  directory_service_shard shard;

  std::time_t t0 = std::time(nullptr);
  REQUIRE_NOTHROW(shard.create_directories("/sandbox/a/b"));
  std::time_t t1 = std::time(nullptr);
  REQUIRE_NOTHROW(shard.create_file("/sandbox/file1.txt"));
  std::time_t t2 = std::time(nullptr);
  REQUIRE_NOTHROW(shard.create_file("/sandbox/file2.txt"));
  std::time_t t3 = std::time(nullptr);

  std::vector<directory_entry> entries;
  REQUIRE_NOTHROW(entries = shard.directory_entries("/sandbox"));
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
  directory_service_shard shard;

  std::time_t t0 = std::time(nullptr);
  REQUIRE_NOTHROW(shard.create_directories("/sandbox/a/b"));
  std::time_t t1 = std::time(nullptr);
  REQUIRE_NOTHROW(shard.create_file("/sandbox/file1.txt"));
  std::time_t t2 = std::time(nullptr);
  REQUIRE_NOTHROW(shard.create_file("/sandbox/file2.txt"));
  std::time_t t3 = std::time(nullptr);

  std::vector<directory_entry> entries;
  REQUIRE_NOTHROW(entries = shard.recursive_directory_entries("/sandbox"));
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
  directory_service_shard shard;
  REQUIRE_NOTHROW(shard.create_file("/sandbox/file.txt"));
  REQUIRE_THROWS_AS(shard.dstatus("/sandbox"), directory_service_exception);
  REQUIRE(shard.dstatus("/sandbox/file.txt").mode() == storage_mode::in_memory);
  REQUIRE(shard.dstatus("/sandbox/file.txt").data_blocks().empty());

  data_status status(storage_mode::in_memory_grace, {"a", "b", "c", "d"});
  REQUIRE_NOTHROW(shard.dstatus("/sandbox/file.txt", status));
  REQUIRE(shard.dstatus("/sandbox/file.txt").mode() == storage_mode::in_memory_grace);
  REQUIRE(shard.dstatus("/sandbox/file.txt").data_blocks().size() == 4);
  REQUIRE(shard.dstatus("/sandbox/file.txt").data_blocks().at(0) == "a");
  REQUIRE(shard.dstatus("/sandbox/file.txt").data_blocks().at(1) == "b");
  REQUIRE(shard.dstatus("/sandbox/file.txt").data_blocks().at(2) == "c");
  REQUIRE(shard.dstatus("/sandbox/file.txt").data_blocks().at(3) == "d");
}

TEST_CASE("storage_mode_test", "[file]") {
  directory_service_shard shard;

  REQUIRE_NOTHROW(shard.create_file("/sandbox/file.txt"));
  REQUIRE_THROWS_AS(shard.mode("/sandbox"), directory_service_exception);
  REQUIRE(shard.mode("/sandbox/file.txt") == storage_mode::in_memory);

  REQUIRE_NOTHROW(shard.mode("/sandbox/file.txt", storage_mode::in_memory_grace));
  REQUIRE(shard.mode("/sandbox/file.txt") == storage_mode::in_memory_grace);

  REQUIRE_NOTHROW(shard.mode("/sandbox/file.txt", storage_mode::flushing));
  REQUIRE(shard.mode("/sandbox/file.txt") == storage_mode::flushing);

  REQUIRE_NOTHROW(shard.mode("/sandbox/file.txt", storage_mode::on_disk));
  REQUIRE(shard.mode("/sandbox/file.txt") == storage_mode::on_disk);
}

TEST_CASE("blocks_test", "[file]") {
  directory_service_shard shard;

  REQUIRE_NOTHROW(shard.create_file("/sandbox/file.txt"));
  REQUIRE_THROWS_AS(shard.data_blocks("/sandbox"), directory_service_exception);
  auto blocks = shard.data_blocks("/sandbox/file.txt");
  REQUIRE(blocks.empty());

//  REQUIRE_NOTHROW(shard.add_data_block("/sandbox/file.txt", "a"));
//  REQUIRE_NOTHROW(shard.add_data_block("/sandbox/file.txt", "b"));
//  REQUIRE_NOTHROW(shard.add_data_block("/sandbox/file.txt", "c"));
//  REQUIRE_NOTHROW(shard.add_data_block("/sandbox/file.txt", "d"));
//  REQUIRE(shard.data_blocks("/sandbox/file.txt").size() == 4);
//  REQUIRE(shard.data_blocks("/sandbox/file.txt").at(0) == "a");
//  REQUIRE(shard.data_blocks("/sandbox/file.txt").at(1) == "b");
//  REQUIRE(shard.data_blocks("/sandbox/file.txt").at(2) == "c");
//  REQUIRE(shard.data_blocks("/sandbox/file.txt").at(3) == "d");
//
//  REQUIRE_NOTHROW(shard.remove_data_block("/sandbox/file.txt", 1));
//  REQUIRE(shard.data_blocks("/sandbox/file.txt").size() == 3);
//  REQUIRE(shard.data_blocks("/sandbox/file.txt").at(0) == "a");
//  REQUIRE(shard.data_blocks("/sandbox/file.txt").at(1) == "c");
//  REQUIRE(shard.data_blocks("/sandbox/file.txt").at(2) == "d");
//
//  REQUIRE_NOTHROW(shard.remove_data_block("/sandbox/file.txt", "c"));
//  REQUIRE(shard.data_blocks("/sandbox/file.txt").size() == 2);
//  REQUIRE(shard.data_blocks("/sandbox/file.txt").at(0) == "a");
//  REQUIRE(shard.data_blocks("/sandbox/file.txt").at(1) == "d");
//
//  REQUIRE_NOTHROW(shard.remove_all_data_blocks("/sandbox/file.txt"));
//  REQUIRE(shard.data_blocks("/sandbox/file.txt").empty());
}

TEST_CASE("file_type_test", "[file][dir]") {
  directory_service_shard shard;

  REQUIRE_NOTHROW(shard.create_file("/sandbox/file.txt"));
  REQUIRE(shard.is_regular_file("/sandbox/file.txt"));
  REQUIRE_FALSE(shard.is_directory("/sandbox/file.txt"));

  REQUIRE(shard.is_directory("/sandbox"));
  REQUIRE_FALSE(shard.is_regular_file("/sandbox"));
}