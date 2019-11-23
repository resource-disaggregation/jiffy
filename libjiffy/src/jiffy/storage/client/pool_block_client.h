#ifndef JIFFY_POOL_BLOCK_CLIENT_H
#define JIFFY_POOL_BLOCK_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "jiffy/storage/service/block_request_service.h"
#include "jiffy/storage/service/block_response_service.h"
#include "jiffy/utils/client_cache.h"

namespace jiffy {
namespace storage {
/* Block client class */
class pool_block_client {
 public:
  /* Command response reader class */
  class command_response_reader {
   public:
    command_response_reader() = default;

    /**
     * @brief Constructor
     * @param prot Protocol
     */

    explicit command_response_reader(std::shared_ptr<apache::thrift::protocol::TProtocol> prot);

    /**
     * @brief Receive response
     * @param out Response result to be returned
     * @return Client sequence number
     */

    int64_t recv_response(std::vector<std::string> &out);

   private:
    /* Thrift protocol */
    std::shared_ptr<apache::thrift::protocol::TProtocol> prot_;
    /* Thrift protocol */
    apache::thrift::protocol::TProtocol *iprot_{};
  };

  typedef block_request_serviceClient thrift_client;
  typedef utils::client_cache<thrift_client> client_cache;

  pool_block_client() = default;

  /**
   * @brief Destructor
   */

  ~pool_block_client();

  /**
   * @brief Fetch client identifier
   * @return Client identifier
   */

  int64_t get_client_id();

  /**
   * @brief Connect block server
   * @param hostname Block server hostname
   * @param port Port number
   * @param block_id Block identifier
   * @param timeout_ms Timeout
   */

  void connect(const std::string &hostname, int port, int timeout_ms = 1000);

  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol();

  /**
   * @brief Disconnect server
   */

  void disconnect();

  /**
   * @brief Check if connection is opened
   * @return Bool value, true if connection is opened
   */

  bool is_connected() const;

  /**
   * @brief Register client identifier with block identifier and return command response reader
   * @param client_id Client identifier
   * @return Command response reader
   */

  command_response_reader get_command_response_reader(int64_t client_id);

  /**
   * @brief Request command
   * @param seq Sequence identifier
   * @param args Command arguments
   */

  void command_request(const sequence_id &seq, const std::vector<std::string> &args, int block_id);

  /**
   * @brief Send run command
   * @param block_id Block identifier
   * @param arguments Command arguments
   */
  void send_run_command(const int32_t block_id, const std::vector<std::string> &arguments);

  /**
   * @brief Receive response from run command
   * @param _return Response
   */
  void recv_run_command(std::vector<std::string> &_return);

 private:
  /* Transport */
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  /* Protocol */
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  /* Block request client */
  std::shared_ptr<thrift_client> client_{};
};

}
}


#endif //JIFFY_POOL_BLOCK_CLIENT_H
