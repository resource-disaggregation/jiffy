#ifndef MMUX_REPLICA_CHAIN_CLIENT_H
#define MMUX_REPLICA_CHAIN_CLIENT_H

#include <map>
#include "block_client.h"

namespace mmux {
namespace storage {

class replica_chain_client {
 public:
  typedef block_client *client_ref;

  explicit replica_chain_client(const std::vector<std::string> &chain);

  ~replica_chain_client();
  void disconnect();

  const std::vector<std::string> &chain() const;

  std::string get(const std::string &key);
  std::string put(const std::string &key, const std::string &value);
  std::string remove(const std::string &key);
  std::string update(const std::string &key, const std::string &value);
  std::string num_keys();

  void send_command(int32_t cmd_id, const std::vector<std::string> &args);
  std::vector<std::string> recv_response();
  std::vector<std::string> run_command(int32_t cmd_id, const std::vector<std::string> &args);
 private:
  void connect(const std::vector<std::string> &chain);

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
