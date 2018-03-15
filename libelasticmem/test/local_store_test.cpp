#include <experimental/filesystem>
#include <fstream>
#include "catch.hpp"
#include "../src/persistent/local/local_store.h"

using namespace ::elasticmem::persistent;
TEST_CASE("write_test", "[write]") {
  local_store store;
  std::ofstream out("/tmp/a.txt", std::ofstream::out);
  out << "a";
  out.close();
  REQUIRE_NOTHROW(store.write("/tmp/a.txt", "/tmp/b.txt"));
  std::ifstream in("/tmp/b.txt", std::ifstream::in);
  std::string data;
  in >> data;
  REQUIRE(data == "a");
  namespace fs = std::experimental::filesystem;
  fs::path a("/tmp/a.txt");
  fs::path b("/tmp/b.txt");
  fs::remove_all(a);
  fs::remove_all(b);
}

TEST_CASE("read_test", "[read]") {
  local_store store;
  std::ofstream out("/tmp/b.txt", std::ofstream::out);
  out << "a";
  out.close();
  REQUIRE_NOTHROW(store.read("/tmp/b.txt", "/tmp/a.txt"));
  std::ifstream in("/tmp/a.txt", std::ifstream::in);
  std::string data;
  in >> data;
  REQUIRE(data == "a");
  namespace fs = std::experimental::filesystem;
  fs::path a("/tmp/a.txt");
  fs::path b("/tmp/b.txt");
  fs::remove_all(a);
  fs::remove_all(b);
}

TEST_CASE("remove_test", "[remove]") {
  local_store store;
  std::ofstream out("/tmp/a.txt", std::ofstream::out);
  out << "a";
  out.close();
  REQUIRE_NOTHROW(store.remove("/tmp/a.txt"));
  namespace fs = std::experimental::filesystem;
  fs::path a("/tmp/a.txt");
  REQUIRE_FALSE(fs::exists(a));
}