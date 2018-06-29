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

  class locked_client {
   public:
    typedef replica_chain_client::locked_client block_t;
    typedef std::shared_ptr<block_t> block_ptr_t;

    locked_client(kv_client &parent);

    void unlock();

    std::string put(const std::string &key, const std::string &value);
    std::string get(const std::string &key);
    std::string update(const std::string &key, const std::string &value);
    std::string remove(const std::string &key);

    std::vector<std::string> put(const std::vector<std::string> &kvs);
    std::vector<std::string> get(const std::vector<std::string> &keys);
    std::vector<std::string> update(const std::vector<std::string> &kvs);
    std::vector<std::string> remove(const std::vector<std::string> &keys);
   private:
    void handle_redirect(int32_t cmd_id, const std::vector<std::string> &args, std::string &response);
    void handle_redirects(int32_t cmd_id, const std::vector<std::string> &args, std::vector<std::string> &responses);

    kv_client &parent_;
    std::vector<block_ptr_t> blocks_;
  };

  kv_client(std::shared_ptr<directory::directory_ops> fs,
            const std::string &path,
            const directory::data_status &status,
            int timeout_ms = 0);

  void refresh();

  directory::data_status &status();

  std::string put(const std::string &key, const std::string &value);
  std::string get(const std::string &key);
  std::string update(const std::string &key, const std::string &value);
  std::string remove(const std::string &key);

  std::vector<std::string> put(const std::vector<std::string> &kvs);
  std::vector<std::string> get(const std::vector<std::string> &keys);
  std::vector<std::string> update(const std::vector<std::string> &kvs);
  std::vector<std::string> remove(const std::vector<std::string> &keys);
 private:
  size_t block_id(const std::string &key);
  std::vector<std::string> batch_command(const kv_op_id &id, const std::vector<std::string> &args, size_t args_per_op);
  void handle_redirect(int32_t cmd_id, const std::vector<std::string> &args, std::string &response);
  void handle_redirects(int32_t cmd_id, const std::vector<std::string> &args, std::vector<std::string> &responses);

  std::shared_ptr<directory::directory_ops> fs_;
  std::string path_;
  directory::data_status status_;
  std::vector<std::shared_ptr<replica_chain_client>> blocks_;
  std::vector<int32_t> slots_;
};

}
}

#endif //MMUX_KV_CLIENT_H
