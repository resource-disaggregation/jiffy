#ifndef MMUX_KV_CLIENT_H
#define MMUX_KV_CLIENT_H

#include "../../directory/client/directory_client.h"
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
  kv_client(std::shared_ptr<directory::directory_ops> fs, const std::string &path, const directory::data_status &status);

  void refresh();

  directory::data_status &status();

  void put(const std::string &key, const std::string &value);
  std::string get(const std::string &key);
  std::string update(const std::string &key, const std::string &value);
  std::string remove(const std::string &key);
 private:
  size_t block_id(const std::string& key);
  std::string parse_response(const std::string &raw_response);

  std::shared_ptr<directory::directory_ops> fs_;
  std::string path_;
  directory::data_status status_;
  std::vector<std::shared_ptr<replica_chain_client>> blocks_;
  std::vector<int32_t> slots_;
};

}
}

#endif //MMUX_KV_CLIENT_H
