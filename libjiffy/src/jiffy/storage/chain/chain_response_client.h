#ifndef JIFFY_CHAIN_RESPONSE_CLIENT_H
#define JIFFY_CHAIN_RESPONSE_CLIENT_H

#include "jiffy/storage/service/block_response_service.h"

namespace jiffy {
namespace storage {
/* Chain response client */
class chain_response_client {
 public:
  typedef block_response_serviceClient thrift_client;
  chain_response_client() = default;

  /**
   * @brief Constructor
   * Reset client protocol
   * @param prot Protocol
   */

  explicit chain_response_client(std::shared_ptr<::apache::thrift::protocol::TProtocol> prot);

  /**
   * @brief Check if chain response service client is set
   * @return Bool value, true if set
   */

  bool is_set() const;

  /**
   * @brief Reset client protocol
   * Construct a new chain response service client
   * @param prot Protocol
   */

  void reset_prot(std::shared_ptr<::apache::thrift::protocol::TProtocol> prot);

  /**
   * @brief Acknowledge previous block
   * @param seq Sequence identifier
   */

  void ack(const sequence_id &seq);
 private:
  /* Chain response service client */
  std::unique_ptr<thrift_client> client_{};
};

}
}

#endif //JIFFY_CHAIN_RESPONSE_CLIENT_H
