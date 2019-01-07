#ifndef MMUX_KV_MANAGEMENT_RPC_CLIENT_H
#define MMUX_KV_MANAGEMENT_RPC_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "storage_management_service.h"
#include "../chain_module.h"

namespace mmux {
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
   * @param slot_begin Begin slot
   * @param slot_end End slot
   * @param chain Chain block names
   * @param auto_scale Bool value, true if auto_scale is on
   * @param role Block role
   * @param next_block_name Next block's name
   */

  void setup_block(int32_t block_id,
                   const std::string &path,
                   int32_t slot_begin,
                   int32_t slot_end,
                   const std::vector<std::string> &chain,
                   bool auto_scale,
                   int32_t role,
                   const std::string &next_block_name);

  /**
   * @brief Fetch slot range for particular block
   * @param block_id Block identifier
   * @return Pair containing the beginning and end slots for the block
   */

  std::pair<int32_t, int32_t> slot_range(int32_t block_id);

  /**
   * @brief Set block to be exporting
   * @param block_id Block identifier
   * @param target_block_name Target block name
   * @param slot_begin Exporting begin slot
   * @param slot_end Exporting end slot
   */

  void set_exporting(int32_t block_id,
                     const std::vector<std::string> &target_block_name,
                     int32_t slot_begin,
                     int32_t slot_end);

  /**
   * @brief Set block to be importing
   * @param block_id Block identifier
   * @param slot_begin Importing begin slot
   * @param slot_end Importing end slot
   */

  void set_importing(int32_t block_id, int32_t slot_begin, int32_t slot_end);

  /**
   * @brief Setup the block and set importing
   * @param block_id Block identifier
   * @param path File path
   * @param slot_begin Importing begin slot
   * @param slot_end Importing end slot
   * @param chain Chain block names
   * @param role Block role
   * @param next_block_name Next block's name
   */

  void setup_and_set_importing(int32_t block_id,
                               const std::string &path,
                               int32_t slot_begin,
                               int32_t slot_end,
                               const std::vector<std::string> &chain,
                               int32_t role,
                               const std::string &next_block_name);

  /**
   * @brief Set block back to regular
   * @param block_id Block identifier
   * @param slot_begin Slot begin
   * @param slot_end Slot end
   */

  void set_regular(int32_t block_id, int32_t slot_begin, int32_t slot_end);

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

  /**
   * @brief Export slots
   * @param block_id Block identifier
   */

  void export_slots(int32_t block_id);

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

#endif //MMUX_KV_MANAGEMENT_RPC_CLIENT_H