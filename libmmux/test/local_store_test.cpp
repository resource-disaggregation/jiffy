#include <fstream>
#include <catch.hpp>
#include <iostream>
#include "../src/persistent/local/local_store.h"
#include "../src/utils/directory_utils.h"

using namespace ::mmux::persistent;
TEST_CASE("local_write_test", "[write]") {
  local_store store;
  std::ofstream out("/tmp/a.txt", std::ofstream::out);
  out << "a";
  out.close();
  REQUIRE_NOTHROW(store.write("/tmp/a.txt", "/tmp/b.txt"));
  std::ifstream in("/tmp/b.txt", std::ifstream::in);
  std::string data;
  in >> data;
  REQUIRE(data == "a");
  std::remove("/tmp/a.txt");
  std::remove("/tmp/b.txt");
}

TEST_CASE("local_read_test", "[read]") {
  local_store store;
  std::ofstream out("/tmp/b.txt", std::ofstream::out);
  out << "a";
  out.close();
  REQUIRE_NOTHROW(store.read("/tmp/b.txt", "/tmp/a.txt"));
  std::ifstream in("/tmp/a.txt", std::ifstream::in);
  std::string data;
  in >> data;
  REQUIRE(data == "a");
  std::remove("/tmp/a.txt");
  std::remove("/tmp/b.txt");
}

TEST_CASE("local_remove_test", "[remove]") {
  local_store store;
  std::ofstream out("/tmp/a.txt", std::ofstream::out);
  out << "a";
  out.close();
  REQUIRE_NOTHROW(store.remove("/tmp/a.txt"));
  REQUIRE_FALSE(std::ifstream("/tmp/a.txt"));
}