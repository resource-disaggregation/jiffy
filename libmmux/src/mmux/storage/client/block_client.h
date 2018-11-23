#ifndef MMUX_BLOCK_CLIENT_H
#define MMUX_BLOCK_CLIENT_H

#include <thrift/transport/TSocket.h>
#include <libcuckoo/cuckoohash_map.hh>
#include "../service/block_request_service.h"
#include "../service/block_response_service.h"
#include "../../utils/client_cache.h"

namespace mmux {
namespace storage {
/* Block client class */
class block_client {
 public:
  /* Command response reader class */
  class command_response_reader {
   public:
    command_response_reader() = default;

    /**
     * @brief Constructor
     * @param prot Thrift protocol
     */

    explicit command_response_reader(std::shared_ptr<apache::thrift::protocol::TProtocol> prot);

    /**
     * @brief Response receiver
     * @param out Response result
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

  block_client() = default;

  /**
   * @brief Destructor
   */

  ~block_client();

  /**
   * @brief Fetch client id
   * @return Client id number
   */

  int64_t get_client_id();

  /**
   * @brief Connect host
   * @param hostname Host name
   * @param port Port number
   * @param block_id Block id
   * @param timeout_ms Timeout
   */

  void connect(const std::string &hostname, int port, int block_id, int timeout_ms = 1000);

  /**
   * @brief Disconnect host
   */

  void disconnect();

  /**
   * @brief Check if connection is opened
   * @return Bool value
   */

  bool is_connected() const;

  /**
   * @brief Register client id with block id and return command_response reader
   * @param client_id Client id number
   * @return Command response reader
   */

  command_response_reader get_command_response_reader(int64_t client_id);

  /**
   * @brief Request command
   * @param seq Sequence id number
   * @param cmd_id Command id number
   * @param args Arguments
   */
  void command_request(const sequence_id &seq, int32_t cmd_id, const std::vector<std::string> &args);

 private:
  /* Transport */
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  /* Protocol */
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  /* Block request client */
  std::shared_ptr<thrift_client> client_{};
  /* Block id */
  int block_id_{-1};
};

}
}

#endif //MMUX_BLOCK_CLIENT_H
