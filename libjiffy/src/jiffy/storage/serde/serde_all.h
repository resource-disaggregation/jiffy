#ifndef JIFFY_SERDE_H
#define JIFFY_SERDE_H

#include "jiffy/storage/hashtable/hash_table_defs.h"
#include "jiffy/storage/file/file_defs.h"
#include "jiffy/storage/fifoqueue/fifo_queue_defs.h"
#include "jiffy/storage/types/binary.h"
#include "jiffy/utils/logger.h"
#include <sstream>
#include <iostream>
#include <fstream>

using namespace jiffy::utils;

namespace jiffy {
namespace storage {
/* Virtual class for Custom serializer/deserializer */
class serde {
 public:
  /**
   * @brief Constructor
   * @param allocator Block memory allocator
   */

  explicit serde(const block_memory_allocator<uint8_t> &allocator) : allocator_(allocator) {}

  /**
   * @brief Destructor
   */

  virtual ~serde() = default;

  /**
   * @brief Serialize
   * @param data Date structure
   * @param out Output stream
   * @return Output stream position
   */

  template<typename DataType>
  std::size_t serialize(const DataType &data, std::shared_ptr<std::ostream> out) {
    return virtual_serialize(data, out);
  }

  /**
   * @brief Deserialize
   * @param in Input stream
   * @param data Date structure
   * @return Input stream position
   */

  template<typename DataType>
  std::size_t deserialize(std::shared_ptr<std::istream> in, DataType &data) {
    return virtual_deserialize(in, data);
  }

  /**
   * @brief Make a string binary
   * @param str String
   * @return Binary string
   */

  binary make_binary(const std::string &str) {
    return binary(str, allocator_);
  }

 private:

  /**
   * @brief Virtual serialize function for new hash table type
   * @param table Hash table
   * @param out Output stream
   * @return Output stream position
   */

  virtual std::size_t virtual_serialize(const hash_table_type &table, std::shared_ptr<std::ostream> out) = 0;

  /**
   * @brief Virtual serialize function for fifo queue
   * @param table Fifo queue
   * @param out Output stream
   * @return Output stream position
   */

  virtual std::size_t virtual_serialize(const fifo_queue_type &table, std::shared_ptr<std::ostream> out) = 0;

  /**
   * @brief Virtual serialize function for file
   * @param table File
   * @param out Output stream
   * @return Output stream position
   */

  virtual std::size_t virtual_serialize(const file_type &table, std::shared_ptr<std::ostream> out) = 0;

  /**
   * @brief Virtual deserialize function for new hash table type
   * @param in Input stream
   * @param table Hash table
   * @return Input stream position
   */

  virtual std::size_t virtual_deserialize(std::shared_ptr<std::istream> in, hash_table_type &table) = 0;

  /**
   * @brief Virtual deserialize function for fifo queue
   * @param in Input stream
   * @param table Fifo queue
   * @return Input stream position
   */

  virtual std::size_t virtual_deserialize(std::shared_ptr<std::istream> in, fifo_queue_type &table) = 0;

  /**
   * @brief Virtual deserialize function for file
   * @param in Input stream
   * @param table File
   * @return Input stream position
   */

  virtual std::size_t virtual_deserialize(std::shared_ptr<std::istream> in, file_type &table) = 0;

  /* Block memory allocator */
  block_memory_allocator<uint8_t> allocator_;

};

/**
 * Derived serializer and deserializer class
 */
template<class impl>
class derived : public impl {

 public:
  /**
   * @brief Constructor
   */
  template<class... TArgs>
  explicit derived(TArgs &&... args): impl(std::forward<TArgs>(args)...) {
  }

 private:

  /**
   * @brief Virtual serialize function for new hash table type
   * @param table Hash table
   * @param out Output stream
   * @return Output stream position
   */
  std::size_t virtual_serialize(const hash_table_type &table, std::shared_ptr<std::ostream> out) final {
    return impl::serialize_impl(table, out);
  }

  /**
   * @brief Virtual serialize function for fifo queue
   * @param table Fifo queue
   * @param out Output stream
   * @return Output stream position
   */

  std::size_t virtual_serialize(const fifo_queue_type &table, std::shared_ptr<std::ostream> out) final {
    return impl::serialize_impl(table, out);
  }

  /**
   * @brief Virtual serialize function for file
   * @param table File
   * @param out Output stream
   * @return Output stream position
   */

  std::size_t virtual_serialize(const file_type &table, std::shared_ptr<std::ostream> out) final {
    return impl::serialize_impl(table, out);
  }

  /**
   * @brief Virtual deserialize function for new hash table type
   * @param in Input stream
   * @param table Hash table
   * @return Input stream position
   */
  std::size_t virtual_deserialize(std::shared_ptr<std::istream> in, hash_table_type &table) final {
    return impl::deserialize_impl(in, table);
  }

  /**
   * @brief Virtual deserialize function for fifo queue
   * @param in Input stream
   * @param table Fifo queue
   * @return Input stream position
   */

  std::size_t virtual_deserialize(std::shared_ptr<std::istream> in, fifo_queue_type &table) final {
    return impl::deserialize_impl(in, table);
  }

  /**
   * @brief Virtual deserialize function for file
   * @param in Input stream
   * @param table File
   * @return Input stream position
   */

  std::size_t virtual_deserialize(std::shared_ptr<std::istream> in, file_type &table) final {
    return impl::deserialize_impl(in, table);
  }
};

/* CSV serializer/deserializer class
 * Inherited from serde class
 */

class csv_serde_impl : public serde {
 public:
  /**
   * @brief Constructor
   */
  explicit csv_serde_impl(block_memory_allocator<uint8_t> &allocator) : serde(allocator) {}

  ~csv_serde_impl() override = default;
 protected:

  /**
   * @brief Serialize hash table in CSV format
   * @param table Hash table
   * @param path Output stream
   * @return Output stream position after flushing
   */

  template<typename DataType>
  std::size_t serialize_impl(const DataType &table, const std::shared_ptr<std::ostream> &out) {
    for (auto e: table) {
      *out << to_string(e.first) << "," << to_string(e.second)
           << "\n";
    }
    out->flush();
    auto sz = out->tellp();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Serialize fifo queue in CSV format
   * @param table Fifo queue
   * @param path Output stream
   * @return Output stream position after flushing
   */

  std::size_t serialize_impl(const fifo_queue_type &table, const std::shared_ptr<std::ostream> &out) {
    for (auto e = table.begin(); e != table.end(); e++) {
      *out << *e << "\n";
    }
    out->flush();
    auto sz = out->tellp();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Serialize file in CSV format
   * @param table File
   * @param out Output stream
   * @return Output stream position after flushing
   */

  std::size_t serialize_impl(const file_type &table, const std::shared_ptr<std::ostream> &out) {
    *out << std::string(table.data(), table.size()) << "\n";
    out->flush();
    auto sz = out->tellp();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Deserialize Input stream to hash table in CSV format
   * @param in Input stream
   * @param data Locked hash table
   * @return Input stream position after reading
   */

  template<typename DataType>
  std::size_t deserialize_impl(const std::shared_ptr<std::istream> &in, DataType &data) {
    while (!in->eof()) {
      std::string line;
      std::getline(*in, line, '\n');
      if (line.empty())
        break;
      auto ret = split(line, ',', 2);
      data.emplace(std::make_pair(make_binary(ret[0]), make_binary(ret[1])));
    }
    auto sz = in->tellg();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Deserialize input stream to fifo queue in CSV format
   * @param in Input stream
   * @param data Fifo queue
   * @return Input stream position after reading
   */

  std::size_t deserialize_impl(const std::shared_ptr<std::istream> &in, fifo_queue_type &data) {
    while (!in->eof()) {
      std::string line;
      std::getline(*in, line, '\n');
      if (line.empty())
        break;
      data.push_back(line);
    }
    auto sz = in->tellg();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Deserialize input stream to file in CSV format
   * @param in Input stream
   * @param data File
   * @return Input stream position after reading
   */

  std::size_t deserialize_impl(const std::shared_ptr<std::istream> &in, file_type &data) {
    std::size_t offset = 0;
    while (!in->eof()) {
      std::string line;
      std::getline(*in, line, '\n');
      if (line.empty())
        break;
      data.write(line, offset);
      offset += line.size();
    }
    auto sz = in->tellg();
    return static_cast<std::size_t>(sz);
  }

 private:

  /**
   * @brief Split the string separated by separation symbol in count parts
   * @param s String
   * @param delim Separation symbol
   * @param count Count
   * @return Split result
   */

  inline std::vector<std::string> split(const std::string &s, char delim, size_t count) {
    std::stringstream ss(s);
    std::string item;
    std::vector<std::string> elems;
    size_t i = 0;
    while (std::getline(ss, item, delim) && i < count) {
      elems.push_back(std::move(item));
      i++;
    }
    while (std::getline(ss, item, delim))
      elems.back() += item;
    return elems;
  }

  /**
   * @brief Split with default count
   * @param s String
   * @param delim Separation symbol
   * @return Split result
   */

  inline std::vector<std::string> split(const std::string &s, char delim) {
    return split(s, delim, UINT64_MAX);
  }
};

using csv_serde = derived<csv_serde_impl>;

/* Binary serializer/deserializer class
 * Inherited from serde class
 */
class binary_serde_impl : public serde {
 public:
  /**
   * @brief Constructor
   */
  explicit binary_serde_impl(const block_memory_allocator<uint8_t> &allocator) : serde(allocator) {}

  ~binary_serde_impl() override = default;

 protected:
  /**
   * @brief Binary serialization
   * @param table Hash table
   * @param out Output stream
   * @return Output stream position
   */

  template<typename Datatype>
  size_t serialize_impl(const Datatype &table, const std::shared_ptr<std::ostream> &out) {
    for (const auto &e: table) {
      std::size_t key_size = e.first.size();
      std::size_t value_size = e.second.size();
      out->write(reinterpret_cast<const char *>(&key_size), sizeof(size_t))
          .write(reinterpret_cast<const char *>(e.first.data()), key_size)
          .write(reinterpret_cast<const char *>(&value_size), sizeof(size_t))
          .write(reinterpret_cast<const char *>(e.second.data()), value_size);
    }
    out->flush();
    auto sz = out->tellp();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Binary serialization
   * @param table Fifo queue
   * @param out Output stream
   * @return Output stream position
   */

  size_t serialize_impl(const fifo_queue_type &table, const std::shared_ptr<std::ostream> &out) {
    for (auto e = table.begin(); e != table.end(); e++) {
      std::size_t msg_size = (*e).size();
      out->write(reinterpret_cast<const char *>(&msg_size), sizeof(size_t))
          .write(reinterpret_cast<const char *>((*e).data()), msg_size);
    }
    out->flush();
    auto sz = out->tellp();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Binary serialization
   * @param table File
   * @param out Output stream
   * @return Output stream position
   */

  size_t serialize_impl(const file_type &table, const std::shared_ptr<std::ostream> &out) {
    std::size_t msg_size = table.size();
    out->write(reinterpret_cast<const char *>(table.data()), msg_size);
    out->flush();
    auto sz = out->tellp();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Binary deserialization
   * @param in Input stream
   * @param table Hash table
   * @return Input stream position
   */

  template<typename DataType>
  size_t deserialize_impl(const std::shared_ptr<std::istream> &in, DataType &table) {
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
      table.emplace(std::make_pair(make_binary(key), make_binary(value)));
    }
    auto sz = in->tellg();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Binary deserialization
   * @param in Input stream
   * @param table Fifo queue
   * @return Input stream position
   */

  size_t deserialize_impl(const std::shared_ptr<std::istream> &in, fifo_queue_type &table) {
    while (!in->eof()) {
      std::size_t msg_size;
      in->read(reinterpret_cast<char *>(&msg_size), sizeof(msg_size));
      std::string msg;
      msg.resize(msg_size);
      in->read(&msg[0], msg_size);
      table.push_back(msg);
    }
    auto sz = in->tellg();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Binary deserialization
   * @param in Input stream
   * @param table File
   * @return Input stream position
   */

  size_t deserialize_impl(const std::shared_ptr<std::istream> &in, file_type &table) {
    std::size_t msg_size = table.size();
    std::string msg;
    msg.resize(msg_size);
    in->read(&msg[0], msg_size);
    table.write(msg, 0);
    auto sz = in->tellg();
    return static_cast<std::size_t>(sz);
  }
};

using binary_serde = derived<binary_serde_impl>;

}
}

#endif //JIFFY_SERDE_H
