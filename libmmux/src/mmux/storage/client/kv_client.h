#ifndef MMUX_KV_CLIENT_H
#define MMUX_KV_CLIENT_H

#include "../../directory/fs/directory_client.h"
#include "../../utils/client_cache.h"
#include "replica_chain_client.h"

namespace mmux {
namespace storage {

class redo_error : public std::exception {
 public:
  redo_error() {}
};

class redirect_error : public std::exception {
 public:
  redirect_error(const std::vector<std::string> &blocks) : blocks_(blocks) {}

  std::vector<std::string> blocks() const {
    return blocks_;
  }
 private:
  std::vector<std::string> blocks_;
};

class kv_client {
 public:
  kv_client(directory::directory_client client, const std::string &path, const directory::data_status &status);

  void refresh();

  void put(const std::string &key, const std::string &value);
  std::string get(const std::string &key);
  bool exists(const std::string &key);
  std::string update(const std::string &key, const std::string &value);
  std::string remove(const std::string &key);
 private:
  size_t block_id(const std::string& key);
  std::string parse_response(const std::string &raw_response);

  directory::directory_client fs_;
  std::string path_;
  directory::data_status status_;
  std::vector<replica_chain_client> blocks_;
  std::vector<int32_t> slots_;
  block_client::client_cache cache_;
};

}
}

#endif //MMUX_KV_CLIENT_H
