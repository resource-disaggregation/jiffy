#ifndef MMUX_CHAIN_REQUEST_CLIENT_H
#define MMUX_CHAIN_REQUEST_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "chain_request_service.h"
#include "chain_response_service.h"

namespace mmux {
namespace storage {
/* */
class chain_request_client {
 public:
  typedef chain_request_serviceClient thrift_client;

  chain_request_client() = default;

  /**
   * @brief
   * @param host
   * @param port
   * @param block_id
   */

  explicit chain_request_client(const std::string &host, int32_t port, int32_t block_id);

  /**
   * @brief
   */

  ~chain_request_client();

  /**
   * @brief
   * @param host
   * @param port
   * @param block_id
   */

  void connect(const std::string &host, int port, int32_t block_id);

  /**
   * @brief
   */

  void disconnect();

  /**
   * @brief
   * @return
   */

  bool is_connected() const;

  /**
   * @brief
   * @return
   */

  std::string endpoint();

  /**
   * @brief
   * @return
   */

  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol() const;

  /**
   * @brief
   * @param seq
   * @param cmd_id
   * @param arguments
   */

  void request(const sequence_id &seq, int32_t cmd_id, const std::vector<std::string> &arguments);

  /**
   * @brief
   * @param _return
   * @param cmd_id
   * @param arguments
   */

  void run_command(std::vector<std::string> &_return, int32_t cmd_id, const std::vector<std::string> &arguments);

 private:
  /* */
  std::string host_;
  /* */
  int32_t port_{};
  /* */
  int32_t block_id_{};
  /* */
  std::shared_ptr<apache::thrift::transport::TSocket> socket_{};
  /* */
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  /* */
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  /* */
  std::shared_ptr<thrift_client> client_{};
};

}
}

#endif //MMUX_CHAIN_REQUEST_CLIENT_H
