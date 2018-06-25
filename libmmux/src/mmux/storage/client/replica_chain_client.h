#ifndef MMUX_REPLICA_CHAIN_CLIENT_H
#define MMUX_REPLICA_CHAIN_CLIENT_H

#include <map>
#include "block_client.h"

namespace mmux {
namespace storage {

class replica_chain_client {
 public:
  typedef block_client *client_ref;

  explicit replica_chain_client(const std::vector<std::string> &chain, int timeout_ms = 0);

  ~replica_chain_client();
  void disconnect();

  const std::vector<std::string> &chain() const;

  std::string get(const std::string &key);
  std::string put(const std::string &key, const std::string &value);
  std::string remove(const std::string &key);
  std::string update(const std::string &key, const std::string &value);

  void send_lock();
  void lock();
  void send_unlock();
  void unlock();
  std::string lget(const std::string &key);
  std::string lput(const std::string &key, const std::string &value);
  std::string lremove(const std::string &key);
  std::string lupdate(const std::string &key, const std::string &value);

  std::vector<std::string> lget(const std::vector<std::string> &keys);
  std::vector<std::string> lput(const std::vector<std::string> &kvs);
  std::vector<std::string> lremove(const std::vector<std::string> &keys);
  std::vector<std::string> lupdate(const std::vector<std::string> &kvs);

  std::string redirected_put(const std::string &key, const std::string &value);
  std::string redirected_get(const std::string &key);
  std::string redirected_update(const std::string &key, const std::string &value);
  std::string redirected_remove(const std::string &key);

  void send_command(block_client *client,
                    const std::vector<int32_t> &cmd_ids,
                    const std::vector<std::vector<std::string>> &args);
  std::vector<std::vector<std::string>> recv_response();
  std::vector<std::vector<std::string>> run_command(int32_t cmd_id,
                                                    const std::vector<std::string> &args);
  std::vector<std::vector<std::string>> run_command(block_client *client,
                                                    const std::vector<int32_t> &cmd_id,
                                                    const std::vector<std::vector<std::string>> &args);
 private:
  void connect(const std::vector<std::string> &chain, int timeout_ms = 0);

  sequence_id seq_;
  std::vector<std::string> chain_;
  block_client head_;
  block_client tail_;
  block_client::command_response_reader response_reader_;
  std::vector<client_ref> cmd_client_;
  bool in_flight_;
};

}
}

#endif //MMUX_REPLICA_CHAIN_CLIENT_H
