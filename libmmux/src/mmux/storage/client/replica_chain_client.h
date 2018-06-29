#ifndef MMUX_REPLICA_CHAIN_CLIENT_H
#define MMUX_REPLICA_CHAIN_CLIENT_H

#include <map>
#include "block_client.h"
#include "../kv/kv_block.h"

namespace mmux {
namespace storage {

class replica_chain_client {
 public:
  typedef block_client *client_ref;

  class locked_client {
   public:
    locked_client(replica_chain_client &parent);

    ~locked_client();

    void unlock();
    const std::vector<std::string> &chain();

    bool redirecting() const;
    const std::vector<std::string> &redirect_chain();

    void send_command(int32_t cmd_id, const std::vector<std::string> &args);
    std::vector<std::string> recv_response();
    std::vector<std::string> run_command(int32_t cmd_id, const std::vector<std::string> &args);
    std::vector<std::string> run_command_redirected(int32_t cmd_id, const std::vector<std::string> &args);

   private:
    replica_chain_client &parent_;

    bool redirecting_;
    std::vector<std::string> redirect_chain_;
  };

  explicit replica_chain_client(const std::vector<std::string> &chain, int timeout_ms = 0);

  ~replica_chain_client();

  const std::vector<std::string> &chain() const;

  std::shared_ptr<locked_client> lock();

  bool is_connected() const;

  void send_command(int32_t cmd_id, const std::vector<std::string> &args);
  std::vector<std::string> recv_response();
  std::vector<std::string> run_command(int32_t cmd_id, const std::vector<std::string> &args);
  std::vector<std::string> run_command_redirected(int32_t cmd_id, const std::vector<std::string> &args);
 private:
  void connect(const std::vector<std::string> &chain, int timeout_ms = 0);
  void disconnect();

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
