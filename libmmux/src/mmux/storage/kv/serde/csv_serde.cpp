#include <fstream>
#include <iostream>
#include "csv_serde.h"

namespace mmux {
namespace storage {

std::size_t csv_serde::serialize(const block_type &table, std::shared_ptr<std::ostream> out) {
  for (auto e: table) {
    *out << e.first << "," << e.second << "\n";
  }
  out->flush();
  auto sz = out->tellp();
  return static_cast<std::size_t>(sz);
}

std::size_t csv_serde::deserialize(std::shared_ptr<std::istream> in, block_type &table) {
  while (!in->eof()) {
    std::string line;
    std::getline(*in, line, '\n');
    if (line == "")
      break;
    auto ret = split(line, ',', 2);
    table.insert(ret[0], ret[1]);
  }
  auto sz = in->tellg();
  return static_cast<std::size_t>(sz);
}
}
}
