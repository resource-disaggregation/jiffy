#ifndef JIFFY_KV_MANAGEMENT_RPC_CLIENT_H
#define JIFFY_KV_MANAGEMENT_RPC_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "storage_management_service.h"
#include "../chain_module.h"

namespace jiffy {
namespace storage {
/* Storage management client class */
class storage_management_client {
 public:
  typedef storage_management_serviceClient thrift_client;
  storage_management_client() = default;

  /**
   * @brief Destructor
   */

  ~storage_management_client();

  /**
   * @brief Constructor
   * @param host Storage management server hostname
   * @param port Port number
   */

  storage_management_client(const std::string &host, int port);

  /**
   * @brief Connect
   * @param host Storage management server hostname
   * @param port Port number
   */

  void connect(const std::string &host, int port);

  /**
   * @brief Disconnect
   */

  void disconnect();

  /**
   * @brief Setup block
   * @param block_id Block identifier
   * @param path File path
   * @param chain Chain block names
   * @param role Block role
   * @param next_block_name Next block's name
   */

  void setup_block(int32_t block_id,
                   const std::string &path,
                   const std::string &partition_type,
                   const std::string &partition_name,
                   const std::string &partition_metadata,
                   const std::vector<std::string> &chain,
                   int32_t role,
                   const std::string &next_block_name);

  /**
   * @brief Fetch block path
   * @param block_id Block identifier
   * @return Block path
   */

  std::string path(int32_t block_id);

  /**
   * @brief Write data back to persistent storage if dirty
   * @param block_id Block identifier
   * @param backing_path Backing path
   */

  void sync(int32_t block_id, const std::string &backing_path);

  /**
   * @brief Load data from persistent storage to block
   * @param block_id Block identifier
   * @param backing_path Backing path
   */

  void load(int32_t block_id, const std::string &backing_path);

  /**
   * @brief Dump data back to persistent storage and clear the block
   * @param block_id Block identifier
   * @param backing_path Backing path
   */

  void dump(int32_t block_id, const std::string &backing_path);

  /**
   * @brief Reset block
   * @param block_id Block identifier
   */

  void reset(int32_t block_id);

  /**
   * @brief Fetch storage capacity
   * @param block_id Block identifier
   * @return Storage capacity
   */

  int64_t storage_capacity(int32_t block_id);

  /**
   * @brief Fetch storage size
   * @param block_id Block identifier
   * @return Storage size
   */

  int64_t storage_size(int32_t block_id);

  /**
   * @brief Resend pending request
   * @param block_id Block identifier
   */

  void resend_pending(int32_t block_id);

  /**
   * @brief Forward all key and value to next block
   * @param block_id Block identifier
   */

  void forward_all(int32_t block_id);

 private:
  /* Socket */
  std::shared_ptr<apache::thrift::transport::TSocket> socket_{};
  /* Transport */
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  /* Protocol */
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  /* Storage management service client */
  std::shared_ptr<thrift_client> client_{};
};

}
}

#endif //JIFFY_KV_MANAGEMENT_RPC_CLIENT_H
