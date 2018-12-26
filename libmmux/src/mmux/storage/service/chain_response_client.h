#ifndef MMUX_CHAIN_RESPONSE_CLIENT_H
#define MMUX_CHAIN_RESPONSE_CLIENT_H

#include "chain_response_service.h"

namespace mmux {
namespace storage {
/* Chain response client */
class chain_response_client {
 public:
  typedef chain_response_serviceClient thrift_client;
  chain_response_client() = default;

  /**
   * @brief Constructor
   * Reset client protocol
   * @param prot Protocol
   */

  explicit chain_response_client(std::shared_ptr<::apache::thrift::protocol::TProtocol> prot);

  /**
   * @brief Check if chain response serviceClient is set
   * @return Bool value, true if set
   */

  bool is_set() const;

  /**
   * @brief Reset client protocol
   * Construct a new chain response serviceClient
   * @param prot Protocol
   */

  void reset_prot(std::shared_ptr<::apache::thrift::protocol::TProtocol> prot);

  /**
   * @brief Acknowledge previous block
   * @param seq Sequence id
   */

  void ack(const sequence_id &seq);
 private:
  /* Chain response serviceClient */
  std::unique_ptr<thrift_client> client_{};
};

}
}

#endif //MMUX_CHAIN_RESPONSE_CLIENT_H