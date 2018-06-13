#include "binary_serde.h"

#include <fstream>

namespace mmux {
namespace storage {

std::size_t binary_serde::serialize(const block_type &table, std::shared_ptr<std::ostream> out) {
  for (auto e: table) {
    std::size_t key_size = e.first.size();
    std::size_t value_size = e.second.size();
    out->write(reinterpret_cast<const char *>(&key_size), sizeof(size_t))
        .write(e.first.data(), key_size)
        .write(reinterpret_cast<const char *>(&value_size), sizeof(size_t))
        .write(e.second.data(), value_size);
  }
  out->flush();
  auto sz = out->tellp();
  return static_cast<std::size_t>(sz);
}

std::size_t binary_serde::deserialize(std::shared_ptr<std::istream> in, block_type &table) {
  while (!in->eof()) {
    std::size_t key_size;
    std::size_t value_size;
    in->read(reinterpret_cast<char *>(&key_size), sizeof(key_size));
    std::string key;
    key.resize(key_size);
    in->read(&key[0], key_size);
    in->read(reinterpret_cast<char *>(&value_size), sizeof(value_size));
    std::string value;
    value.resize(value_size);
    in->read(&value[0], value_size);
    table.insert(key, value);
  }
  auto sz = in->tellg();
  return static_cast<std::size_t>(sz);
}

}
}