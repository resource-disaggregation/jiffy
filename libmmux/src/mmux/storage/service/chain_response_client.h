#ifndef MMUX_CHAIN_RESPONSE_CLIENT_H
#define MMUX_CHAIN_RESPONSE_CLIENT_H

#include "chain_response_service.h"

namespace mmux {
namespace storage {
/* */
class chain_response_client {
 public:
  typedef chain_response_serviceClient thrift_client;
  chain_response_client() = default;

  /**
   * @brief
   * @param prot
   */

  explicit chain_response_client(std::shared_ptr<::apache::thrift::protocol::TProtocol> prot);

  /**
   * @brief
   * @return
   */

  bool is_set() const;

  /**
   * @brief
   * @param prot
   */

  void reset_prot(std::shared_ptr<::apache::thrift::protocol::TProtocol> prot);

  /**
   * @brief
   * @param seq
   */

  void ack(const sequence_id &seq);
 private:
  /* */
  std::unique_ptr<thrift_client> client_{};
};

}
}

#endif //MMUX_CHAIN_RESPONSE_CLIENT_H
