#ifndef MMUX_CHAIN_REQUEST_CLIENT_H
#define MMUX_CHAIN_REQUEST_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "chain_request_service.h"
#include "chain_response_service.h"

namespace mmux {
namespace storage {
/* Chain request client */
class chain_request_client {
 public:
  typedef chain_request_serviceClient thrift_client;

  chain_request_client() = default;

  /**
   * @brief Constructor
   * @param host Socket host
   * @param port Socket port number
   * @param block_id Block id
   */

  explicit chain_request_client(const std::string &host, int32_t port, int32_t block_id);

  /**
   * @brief Destructor
   * Close transport and set block id to -1
   */

  ~chain_request_client();

  /**
   * @brief Open client connection
   * @param host Socket host number
   * @param port Socket port number
   * @param block_id Block id number
   */

  void connect(const std::string &host, int port, int32_t block_id);

  /**
   * @brief Close client connection
   * Set block id to -1
   */

  void disconnect();

  /**
   * @brief Check if transport is opened
   * @return Bool value, true if opened
   */

  bool is_connected() const;

  /**
   * @brief Fetch end point
   * @return End point string: host + " : " + port
   */

  std::string endpoint();

  /**
   * @brief Fetch protocol
   * @return Protocol
   */

  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol() const;

  /**
   * @brief Send a request
   * @param seq Sequence id
   * @param cmd_id Command id
   * @param arguments Arguments
   */

  void request(const sequence_id &seq, int32_t cmd_id, const std::vector<std::string> &arguments);

  /**
   * @brief Run command
   * @param _return Return value
   * @param cmd_id Command id
   * @param arguments Arguments
   */

  void run_command(std::vector<std::string> &_return, int32_t cmd_id, const std::vector<std::string> &arguments);

 private:
  /* Client host number */
  std::string host_;
  /* Client port number */
  int32_t port_{};
  /* Block id */
  int32_t block_id_{};
  /* Socket */
  std::shared_ptr<apache::thrift::transport::TSocket> socket_{};
  /* Transport */
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  /* Protocol */
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  /* Chain request service client */
  std::shared_ptr<thrift_client> client_{};
};

}
}

#endif //MMUX_CHAIN_REQUEST_CLIENT_H