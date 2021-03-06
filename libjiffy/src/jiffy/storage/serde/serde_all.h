#ifndef JIFFY_SERDE_H
#define JIFFY_SERDE_H

#include "jiffy/storage/hashtable/hash_table_defs.h"
#include "jiffy/storage/file/file_defs.h"
#include "jiffy/storage/fifoqueue/fifo_queue_defs.h"
#include "jiffy/storage/shared_log/shared_log_defs.h"
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
  std::size_t serialize(const DataType &data, const std::string &out_path) {
    return virtual_serialize(data, out_path);
  }

  /**
   * @brief Deserialize
   * @param in Input stream
   * @param data Date structure
   * @return Input stream position
   */

  template<typename DataType>
  std::size_t deserialize(DataType &data, const std::string &in_path) {
    return virtual_deserialize(data, in_path);
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

  virtual std::size_t virtual_serialize(const hash_table_type &table, const std::string &out_path) = 0;

  /**
   * @brief Virtual serialize function for fifo queue
   * @param table Fifo queue
   * @param out Output stream
   * @return Output stream position
   */

  virtual std::size_t virtual_serialize(const fifo_queue_type &table, const std::string &out_path) = 0;

  /**
   * @brief Virtual serialize function for file
   * @param table File
   * @param out Output stream
   * @return Output stream position
   */

  virtual std::size_t virtual_serialize(const file_type &table, const std::string &out_path) = 0;

/**
   * @brief Virtual serialize function for shared_log
   * @param table Shared_log
   * @param out Output stream
   * @return Output stream position
   */

  virtual std::size_t virtual_serialize(const shared_log_serde_type &table, const std::string &out_path) = 0;

  /**
   * @brief Virtual deserialize function for new hash table type
   * @param in Input stream
   * @param table Hash table
   * @return Input stream position
   */

  virtual std::size_t virtual_deserialize(hash_table_type &table, const std::string &in_path) = 0;

  /**
   * @brief Virtual deserialize function for fifo queue
   * @param in Input stream
   * @param table Fifo queue
   * @return Input stream position
   */

  virtual std::size_t virtual_deserialize(fifo_queue_type &table, const std::string &in_path) = 0;

  /**
   * @brief Virtual deserialize function for file
   * @param in Input stream
   * @param table File
   * @return Input stream position
   */

  virtual std::size_t virtual_deserialize(file_type &table, const std::string &in_path) = 0;

  /**
   * @brief Virtual deserialize function for shared_log
   * @param in Input stream
   * @param table Shared_log
   * @return Input stream position
   */

  virtual std::size_t virtual_deserialize(shared_log_serde_type &table, const std::string &in_path) = 0;

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

  std::size_t virtual_serialize(const hash_table_type &table, const std::string &out_path) final {
    return impl::serialize_impl(table, out_path);
  }

  /**
   * @brief Virtual serialize function for fifo queue
   * @param table Fifo queue
   * @param out Output stream
   * @return Output stream position
   */

  std::size_t virtual_serialize(const fifo_queue_type &table, const std::string &out_path) final {
    return impl::serialize_impl(table, out_path);
  }

  /**
   * @brief Virtual serialize function for file
   * @param table File
   * @param out Output stream
   * @return Output stream position
   */

  std::size_t virtual_serialize(const file_type &table, const std::string &out_path) final {
    return impl::serialize_impl(table, out_path);
  }

  /**
   * @brief Virtual serialize function for shared_log
   * @param table Shared_log
   * @param out Output stream
   * @return Output stream position
   */

  std::size_t virtual_serialize(const shared_log_serde_type &table, const std::string &out_path) final {
    return impl::serialize_impl(table, out_path);
  }

  /**
   * @brief Virtual deserialize function for new hash table type
   * @param in Input stream
   * @param table Hash table
   * @return Input stream position
   */
  std::size_t virtual_deserialize(hash_table_type &table, const std::string &in_path) final {
    return impl::deserialize_impl(table, in_path);
  }

  /**
   * @brief Virtual deserialize function for fifo queue
   * @param in Input stream
   * @param table Fifo queue
   * @return Input stream position
   */

  std::size_t virtual_deserialize(fifo_queue_type &table, const std::string &in_path) final {
    return impl::deserialize_impl(table, in_path);
  }

  /**
   * @brief Virtual deserialize function for file
   * @param in Input stream
   * @param table File
   * @return Input stream position
   */

  std::size_t virtual_deserialize(file_type &table, const std::string &in_path) final {
    return impl::deserialize_impl(table, in_path);
  }

  /**
   * @brief Virtual deserialize function for shared_log
   * @param in Input stream
   * @param table Shared_log
   * @return Input stream position
   */

  std::size_t virtual_deserialize(shared_log_serde_type &table, const std::string &in_path) final {
    return impl::deserialize_impl(table, in_path);
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
  std::size_t serialize_impl(const DataType &table, const std::string &out_path) {
    std::ofstream out(out_path, std::ios::out);
    for (auto e: table) {
      out << to_string(e.first) << "," << to_string(e.second)
          << "\n";
    }
    out.flush();
    auto sz = out.tellp();
    out.close();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Serialize fifo queue in CSV format
   * @param table Fifo queue
   * @param path Output stream
   * @return Output stream position after flushing
   */

  std::size_t serialize_impl(const fifo_queue_type &table, const std::string &out_path) {
    std::ofstream out(out_path, std::ios::out);
    for (auto e = table.begin(); e != table.end(); e++) {
      out << *e << "\n";
    }
    out.flush();
    auto sz = out.tellp();
    out.close();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Serialize file in CSV format
   * @param table File
   * @param out Output stream
   * @return Output stream position after flushing
   */

  std::size_t serialize_impl(const file_type &table, const std::string &out_path) {
    std::ofstream out(out_path, std::ios::out);
    out << std::string(table.data(), table.size()) << "\n";
    out.flush();
    auto sz = out.tellp();
    out.close();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Serialize shared_log in CSV format
   * @param table Shared_log
   * @param out Output stream
   * @return Output stream position after flushing
   */

  std::size_t serialize_impl(const shared_log_serde_type &, const std::string &) {
    throw std::runtime_error("Shared_Log does not support CSV format!");
  }

  /**
   * @brief Deserialize Input stream to hash table in CSV format
   * @param in Input stream
   * @param data Locked hash table
   * @return Input stream position after reading
   */

  template<typename DataType>
  std::size_t deserialize_impl(DataType &data, const std::string &in_path) {
    std::ifstream in(in_path);
    while (!in.eof()) {
      std::string line;
      std::getline(in, line, '\n');
      if (line.empty())
        break;
      auto ret = split(line, ',', 2);
      data.emplace(std::make_pair(make_binary(ret[0]), make_binary(ret[1])));
    }
    auto sz = in.tellg();
    in.close();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Deserialize input stream to fifo queue in CSV format
   * @param in Input stream
   * @param data Fifo queue
   * @return Input stream position after reading
   */

  std::size_t deserialize_impl(fifo_queue_type &data, const std::string &in_path) {
    std::ifstream in(in_path);
    while (!in.eof()) {
      std::string line;
      std::getline(in, line, '\n');
      if (line.empty())
        break;
      data.push_back(line);
    }
    auto sz = in.tellg();
    in.close();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Deserialize input stream to file in CSV format
   * @param in Input stream
   * @param data File
   * @return Input stream position after reading
   */

  std::size_t deserialize_impl(file_type &data, const std::string &in_path) {
    std::ifstream in(in_path);
    std::size_t offset = 0;
    while (!in.eof()) {
      std::string line;
      std::getline(in, line, '\n');
      if (line.empty())
        break;
      data.write(line, offset);
      offset += line.size();
    }
    auto sz = in.tellg();
    in.close();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Deserialize input stream to shared_log in CSV format
   * @param in Input stream
   * @param data Shared_log
   * @return Input stream position after reading
   */

  std::size_t deserialize_impl(shared_log_serde_type &, const std::string &) {
    throw std::runtime_error("Shared_Log does not support CSV format!");
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
  size_t serialize_impl(const Datatype &table, const std::string &out_path) {
    std::ofstream out(out_path, std::ios::binary);
    std::string offset_out_path = out_path;
    offset_out_path.append("_offset");
    std::ofstream offset_out(offset_out_path, std::ios::binary);
    for (const auto &e: table) {
      std::size_t key_size = e.first.size();
      std::size_t value_size = e.second.size();
      offset_out.write(reinterpret_cast<const char *>(&key_size), sizeof(size_t));
      offset_out.write(reinterpret_cast<const char *>(e.first.data()), key_size);
      offset_out.write(reinterpret_cast<const char *>(&value_size), sizeof(size_t));
      out.write(reinterpret_cast<const char *>(e.second.data()), value_size);
    }
    out.flush();
    offset_out.flush();
    auto sz = out.tellp();
    out.close();
    offset_out.close();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Binary serialization
   * @param table Fifo queue
   * @param out Output stream
   * @return Output stream position
   */

  size_t serialize_impl(const fifo_queue_type &table, const std::string &out_path) {
    std::ofstream out(out_path, std::ios::binary);
    std::string offset_out_path = out_path;
    offset_out_path.append("_offset");
    std::ofstream offset_out(offset_out_path, std::ios::binary);
    for (auto e = table.begin(); e != table.end(); e++) {
      std::size_t msg_size = (*e).size();
      offset_out.write(reinterpret_cast<const char *>(&msg_size), sizeof(size_t));
      out.write(reinterpret_cast<const char *>((*e).data()), msg_size);
    }
    out.flush();
    offset_out.flush();
    offset_out.close();
    auto sz = out.tellp();
    out.close();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Binary serialization
   * @param table File
   * @param out Output stream
   * @return Output stream position
   */

  size_t serialize_impl(const file_type &table, const std::string &out_path) {
    std::ofstream out(out_path, std::ios::binary);
    std::size_t msg_size = table.size();
    out.write(reinterpret_cast<const char *>(table.data()), msg_size);
    out.flush();
    auto sz = out.tellp();
    out.close();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Binary serialization
   * @param table Shared_log
   * @param out Output stream
   * @return Output stream position
   */

  size_t serialize_impl(const shared_log_serde_type &table, const std::string &out_path) {
    std::vector<std::vector<int>> log_info = table.log_info;
    std::size_t seq_no = table.seq_no;

    std::ofstream out(out_path, std::ios::binary);
    std::string offset_out_path = out_path;
    offset_out_path.append("_offset");
    std::ofstream offset_out(offset_out_path, std::ios::binary);

    out.write(reinterpret_cast<const char *>(&seq_no), sizeof(size_t));
    std::size_t log_info_size = log_info.size();
    out.write(reinterpret_cast<const char *>(&log_info_size), sizeof(size_t));
    for (size_t i = 0; i < log_info.size(); ++i) {

      auto info_set = log_info[i];
      std::size_t num_args = info_set.size() - 1;
      int temp_offset = info_set[0];

      if (temp_offset == -1) continue;

      std::size_t data_size = info_set[1];

      offset_out.write(reinterpret_cast<const char *>(&i), sizeof(size_t));
      offset_out.write(reinterpret_cast<const char *>(&num_args), sizeof(size_t));
      offset_out.write(reinterpret_cast<const char *>(&data_size), sizeof(size_t));

      for (size_t j = 2; j < info_set.size(); j++) {
        size_t stream_size = info_set[j];
        std::string stream =
            (*table.block).read(static_cast<std::size_t>(temp_offset), static_cast<std::size_t>(info_set[j])).second;

        offset_out.write(reinterpret_cast<const char *>(&stream_size), sizeof(size_t));
        out.write(reinterpret_cast<const char *>(stream.data()), stream_size);
        temp_offset += info_set[j];
      }
      std::string
          data = (*table.block).read(static_cast<std::size_t>(temp_offset), static_cast<std::size_t>(data_size)).second;
      out.write(reinterpret_cast<const char *>(data.data()), data_size);

    }
    out.flush();
    offset_out.flush();
    auto sz = out.tellp();
    out.close();
    offset_out.close();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Binary deserialization
   * @param in Input stream
   * @param table Hash table
   * @return Input stream position
   */

  template<typename DataType>
  size_t deserialize_impl(DataType &table, const std::string &in_path) {
    std::ifstream in(in_path, std::ios::binary);
    std::string offset_in_path = in_path;
    offset_in_path.append("_offset");
    std::ifstream offset_in(offset_in_path, std::ios::binary);
    while (in.peek() != EOF && offset_in.peek() != EOF) {
      std::size_t key_size;
      offset_in.read(reinterpret_cast<char *>(&key_size), sizeof(key_size));
      std::string key;
      key.resize(key_size);
      offset_in.read(&key[0], key_size);
      std::size_t value_size;
      offset_in.read(reinterpret_cast<char *>(&value_size), sizeof(value_size));
      std::string value;
      value.resize(value_size);
      in.read(&value[0], value_size);
      table.emplace(std::make_pair(make_binary(key), make_binary(value)));
    }
    auto sz = in.tellg();
    in.close();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Binary deserialization
   * @param in Input stream
   * @param table Fifo queue
   * @return Input stream position
   */

  size_t deserialize_impl(fifo_queue_type &table, const std::string &in_path) {
    std::ifstream in(in_path, std::ios::binary);
    std::string offset_in_path = in_path;
    offset_in_path.append("_offset");
    std::ifstream offset_in(offset_in_path, std::ios::binary);
    while (in.peek() != EOF && offset_in.peek() != EOF) {
      std::size_t msg_size;
      offset_in.read(reinterpret_cast<char *>(&msg_size), sizeof(msg_size));
      std::string msg;
      msg.resize(msg_size);
      in.read(&msg[0], msg_size);
      table.push_back(msg);
    }
    auto sz = in.tellg();
    offset_in.close();
    in.close();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Binary deserialization
   * @param in Input stream
   * @param table File
   * @return Input stream position
   */

  size_t deserialize_impl(file_type &table, const std::string &in_path) {
    std::ifstream in(in_path, std::ios::binary);
    std::size_t msg_size = table.size();
    std::string msg;
    msg.resize(msg_size);
    in.read(&msg[0], msg_size);
    table.write(msg, 0);
    auto sz = in.tellg();
    in.close();
    return static_cast<std::size_t>(sz);
  }

  /**
   * @brief Binary deserialization
   * @param in Input stream
   * @param table shared_log
   * @return Input stream position
   */

  size_t deserialize_impl(shared_log_serde_type &table, const std::string &in_path) {
    std::ifstream in(in_path, std::ios::binary);
    std::string offset_in_path = in_path;
    offset_in_path.append("_offset");
    std::ifstream offset_in(offset_in_path, std::ios::binary);
    std::vector<std::vector<int>> log_info;

    std::size_t seq_no;
    in.read(reinterpret_cast<char *>(&seq_no), sizeof(seq_no));
    table.seq_no = seq_no;

    std::size_t log_size;
    in.read(reinterpret_cast<char *>(&log_size), sizeof(log_size));

    std::size_t temp_offset = 0;

    while (in.peek() != EOF && offset_in.peek() != EOF) {
      std::size_t log_position;
      offset_in.read(reinterpret_cast<char *>(&log_position), sizeof(log_position));
      while (log_position > log_info.size()) {
        std::vector<int> info_set = {-1, 0};
        log_info.push_back(info_set);
      }
      std::size_t num_args;
      offset_in.read(reinterpret_cast<char *>(&num_args), sizeof(num_args));
      std::size_t data_size;
      offset_in.read(reinterpret_cast<char *>(&data_size), sizeof(data_size));
      for (size_t i = 0; i < num_args - 1; ++i) {
        std::size_t stream_size;
        offset_in.read(reinterpret_cast<char *>(&stream_size), sizeof(stream_size));
        std::string stream;
        stream.resize(stream_size);
        in.read(&stream[0], stream_size);
        (*table.block).write(stream, temp_offset);
        temp_offset += stream.size();
      }
      std::string data;
      data.resize(data_size);
      in.read(&data[0], data_size);
      (*table.block).write(data, temp_offset);
      temp_offset += data.size();
    }

    while (log_info.size() < log_size) {
      std::vector<int> info_set = {-1, 0};
      log_info.push_back(info_set);
    }
    table.log_info = log_info;

    auto sz = in.tellg();
    in.close();
    offset_in.close();
    return static_cast<std::size_t>(sz);
  }
};

using binary_serde = derived<binary_serde_impl>;

}
}

#endif //JIFFY_SERDE_H
