#ifndef JIFFY_BROKER_CLIENT_H
#define JIFFY_BROKER_CLIENT_H

#include <atomic>
#include <queue>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransport.h>
#include <thrift/protocol/TProtocol.h>
#include <thrift/protocol/TMultiplexedProtocol.h>
#include "jiffy/storage/service/block_request_service.h"
#include "jiffy/storage/notification/blocking_queue.h"

namespace jiffy {
namespace storage {

/* Block listener class */
class block_listener {
 public:
  typedef block_request_serviceClient thrift_client;

  typedef std::pair<std::string, std::string> notification_t;
  typedef blocking_queue<notification_t> mailbox_t;

  /**
   * @brief Destructor
   */

  ~block_listener();

  /**
   * @brief Constructor
   * @param host Notification server hostname
   * @param port Port number
   * @param controls Controls
   */

  block_listener(const std::string &host,
                 int port,
                 mailbox_t &controls);

  /**
   * @brief Connect notification server
   * @param host Notification server hostname
   * @param port Port number
   */

  void connect(const std::string &host, int port);

  /**
   * @brief Disconnect server
   */

  void disconnect();

  /**
   * @brief Fetch protocol
   * @return Protocol
   */

  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol();

  /**
   * @brief Subscribe for block on operation types
   * @param block_id Block identifier
   * @param ops Operation type
   */

  void subscribe(int32_t block_id, const std::vector<std::string> &ops);

  /**
   * @brief Unsubscribe for block on operation types
   * @param block_id Block identifier
   * @param ops Operation type
   */

  void unsubscribe(int32_t block_id, const std::vector<std::string> &ops);

 private:
  /* Control mailbox
   * The control mailbox is a log for subscribe and
   * unsubscribe control operations
   */
  mailbox_t &controls_;

  /* Socket */
  std::shared_ptr<apache::thrift::transport::TSocket> socket_{};
  /* Transport */
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  /* Protocol */
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  /* Notification service client */
  std::shared_ptr<thrift_client> client_{};
};

}
}

#endif //JIFFY_BROKER_CLIENT_H
