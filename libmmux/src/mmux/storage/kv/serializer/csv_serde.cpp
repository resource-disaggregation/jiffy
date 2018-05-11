#include <fstream>
#include "csv_serde.h"

namespace mmux {
namespace storage {

std::size_t csv_serializer::serialize(const block_type &table, const std::string &path) {
  std::ofstream out(path, std::ofstream::trunc | std::ofstream::out);
  for (auto e: table) {
    out << e.first << "," << e.second << "\n";
  }
  auto sz = out.tellp();
  out.close();
  return static_cast<std::size_t>(sz);
}

std::size_t csv_deserializer::deserialize(const std::string &path, block_type &table) {
  std::ifstream in(path, std::ofstream::in);
  while (!in.eof()) {
    std::string line;
    std::getline(in, line, '\n');
    auto ret = split(line, ',', 2);
    table.insert(ret[0], ret[1]);
  }
  auto sz = in.tellg();
  in.close();
  return static_cast<std::size_t>(sz);
}
}
}
