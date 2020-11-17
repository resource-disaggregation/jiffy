#ifndef JIFFY_KV_MANAGEMENT_RPC_CLIENT_H
#define JIFFY_KV_MANAGEMENT_RPC_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "storage_management_service.h"
#include "jiffy/storage/chain_module.h"

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
   * @brief Create partition
   * @param block_id Block identifier
   * @param type Partition type
   * @param name Partition name
   * @param metadata Partition metadata
   * @param conf Partition configuration parameters
   */
  void create_partition(int32_t block_id,
                        const std::string &type,
                        const std::string &name,
                        const std::string &metadata,
                        const std::map<std::string, std::string> &conf,
                        const int32_t block_seq_no);

  /**
   * @brief Setup chain
   * @param block_id Block identifier
   * @param path Path associated with partition
   * @param chain Replica chain for the partition
   * @param role Role of the partition in the chain
   * @param next_block_id Identifier for the next block in the chain
   */
  void setup_chain(int32_t block_id, const std::string &path,
                   const std::vector<std::string> &chain, int32_t role,
                   const std::string &next_block_id,
                   const int32_t block_seq_no);

  /**
   * @brief Reset block
   * @param block_id Block identifier
   */

  void destroy_partition(int32_t block_id, const int32_t block_seq_no);

  /**
   * @brief Fetch block path
   * @param block_id Block identifier
   * @return Block path
   */

  std::string path(int32_t block_id, const int32_t block_seq_no);

  /**
   * @brief Write data back to persistent storage if dirty
   * @param block_id Block identifier
   * @param backing_path Backing path
   */

  void sync(int32_t block_id, const std::string &backing_path, const int32_t block_seq_no);

  /**
   * @brief Load data from persistent storage to block
   * @param block_id Block identifier
   * @param backing_path Backing path
   */

  void load(int32_t block_id, const std::string &backing_path, const int32_t block_seq_no);

  /**
   * @brief Dump data back to persistent storage and clear the block
   * @param block_id Block identifier
   * @param backing_path Backing path
   */

  void dump(int32_t block_id, const std::string &backing_path, const int32_t block_seq_no);

  /**
   * @brief Fetch storage capacity
   * @param block_id Block identifier
   * @return Storage capacity
   */

  int64_t storage_capacity(int32_t block_id, const int32_t block_seq_no);

  /**
   * @brief Fetch storage size
   * @param block_id Block identifier
   * @return Storage size
   */

  int64_t storage_size(int32_t block_id, const int32_t block_seq_no);

  /**
   * @brief Resend pending request
   * @param block_id Block identifier
   */

  void resend_pending(int32_t block_id, const int32_t block_seq_no);

  /**
   * @brief Forward all key and value to next block
   * @param block_id Block identifier
   */

  void forward_all(int32_t block_id, const int32_t block_seq_no);

  /**
   * @brief Update partition data and metadata
   * @param block_id Block identifier
   * @param partition_name New partition name
   * @param partition_metadata New partition metadata
   */

  void update_partition(const int32_t block_id,
                        const std::string &partition_name,
                        const std::string &partition_metadata,
                        const int32_t block_seq_no);

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
