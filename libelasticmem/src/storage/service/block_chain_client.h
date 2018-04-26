#ifndef ELASTICMEM_BLOCK_CHAIN_CLIENT_H
#define ELASTICMEM_BLOCK_CHAIN_CLIENT_H

#include <map>
#include "block_client.h"

namespace elasticmem {
namespace storage {

class block_chain_client {
 public:
  typedef block_client *client_ref;

  explicit block_chain_client(const std::vector<std::string> &chain);
  ~block_chain_client();
  void disconnect();

  const std::vector<std::string> &chain() const;

  std::string get(const std::string &key);
  std::string put(const std::string &key, const std::string &value);
  std::string remove(const std::string &key);
  std::string update(const std::string &key, const std::string &value);
  std::string num_keys();

  std::vector<std::string> run_command(int32_t cmd_id, const std::vector<std::string> &args);
 private:
  void connect(const std::vector<std::string> &chain);

  sequence_id seq_;
  std::vector<std::string> chain_;
  block_client head_;
  block_client tail_;
  block_client::command_response_reader response_reader_;
  std::vector<client_ref> cmd_client_;
  std::map<int64_t, std::vector<std::string>> response_cache_;
};

}
}

#endif //ELASTICMEM_BLOCK_CHAIN_CLIENT_H
