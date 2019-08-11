#ifndef JIFFY_CHAIN_REQUEST_CLIENT_H
#define JIFFY_CHAIN_REQUEST_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "jiffy/storage/service/block_request_service.h"

namespace jiffy {
namespace storage {

/* Chain request client */
class chain_request_client {
 public:
  typedef block_request_serviceClient thrift_client;

  chain_request_client() = default;

  /**
   * @brief Constructor
   * @param host Chain server hostname
   * @param port Port number
   * @param block_id Block identifier
   */

  explicit chain_request_client(const std::string &host, int32_t port, int32_t block_id);

  /**
   * @brief Destructor
   */

  ~chain_request_client();

  /**
   * @brief Connect server
   * @param host Chain server host number
   * @param port Port number
   * @param block_id Block identifier
   */

  void connect(const std::string &host, int port, int32_t block_id);

  /**
   * @brief Close connection
   */

  void disconnect();

  /**
   * @brief Check if transport is opened
   * @return Bool value, true if opened
   */

  bool is_connected() const;

  /**
   * @brief Fetch end point
   * @return End point string
   */

  std::string endpoint();

  /**
   * @brief Fetch protocol
   * @return Protocol
   */

  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol() const;

  /**
   * @brief Send a request
   * @param seq Sequence identifier
   * @param arguments Arguments
   */

  void request(const sequence_id &seq, const std::vector<std::string> &arguments);

  /**
   * @brief Run command
   * @param _return Return value
   * @param arguments Arguments
   */

  void run_command(std::vector<std::string> &_return, const std::vector<std::string> &arguments);

 private:
  /* Server hostname */
  std::string host_;
  /* Client port number */
  int32_t port_{};
  /* Block identifier */
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

#endif //JIFFY_CHAIN_REQUEST_CLIENT_H
