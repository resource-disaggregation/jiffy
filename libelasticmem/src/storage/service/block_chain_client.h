#ifndef ELASTICMEM_BLOCK_CHAIN_CLIENT_H
#define ELASTICMEM_BLOCK_CHAIN_CLIENT_H

#include "block_client.h"

namespace elasticmem {
namespace storage {

class block_chain_client {
 public:
  explicit block_chain_client(const std::vector<std::string>& chain);
  ~block_chain_client();
  void disconnect();

  const std::vector<std::string>& chain() const;

  std::string get(const std::string& key);
  std::string put(const std::string& key, const std::string& value);
  std::string remove(const std::string& key);
  std::string update(const std::string& key, const std::string& value);
 private:
  std::string run_command_sync(block_client &client,
                                 int32_t cmd_id,
                                 const std::vector<std::string> &args);

  void connect(const std::vector<std::string> &chain);

  sequence_id seq_;
  std::vector<std::string> chain_;
  block_client head_;
  block_client tail_;
  block_client::event_map event_map_;
  std::thread response_processor_;
};

}
}

#endif //ELASTICMEM_BLOCK_CHAIN_CLIENT_H
