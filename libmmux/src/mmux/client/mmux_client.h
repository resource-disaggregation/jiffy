#ifndef MMUX_MMUX_CLIENT_H
#define MMUX_MMUX_CLIENT_H

#include <string>
#include "../directory/directory_ops.h"
#include "../directory/client/lease_renewal_worker.h"
#include "../storage/client/kv_client.h"
#include "../storage/client/kv_listener.h"

namespace mmux {
namespace client {
/* mmux_client class */
class mmux_client {
 public:
  /**
   * @brief Constructor
   * @param host Directory client & lease client host
   * @param dir_port Directory client port number
   * @param lease_port Lease port number
   */

  mmux_client(const std::string &host, int dir_port, int lease_port);

  /**
   * @brief Fetch directory client
   * @return Directory client
   */

  std::shared_ptr<directory::directory_client> fs();

  /**
   * @brief Fetch lease renewal worker
   * @return Lease renewal worker
   */

  directory::lease_renewal_worker &lease_worker();

  /**
   * @brief Begin scope, add path to lease worker
   * @param path Path
   */

  void begin_scope(const std::string &path);

  /**
   * @brief End scope, remove path from lease worker
   * @param path Path
   */

  void end_scope(const std::string &path);

  /**
   * @brief Create key value client
   * @param path File path
   * @param backing_path File backing path
   * @param num_blocks Number of blocks
   * @param chain_length Replication chain length
   * @param flags Flags
   * @param permissions Permissions
   * @param tags Tags
   * @return KV_client
   */

  std::shared_ptr<storage::kv_client> create(const std::string &path,
                                             const std::string &backing_path,
                                             size_t num_blocks = 1,
                                             size_t chain_length = 1,
                                             int32_t flags = 0,
                                             int32_t permissions = directory::perms::all(),
                                             const std::map<std::string, std::string> &tags = {});

  /**
   * @brief Open file, begin lease and create kv_client
   * @param path Path
   * @return KV_client
   */

  std::shared_ptr<storage::kv_client> open(const std::string &path);

  /**
   * @brief Open or create, begin lease and create kv_client
   * @param path Path
   * @param backing_path Backing path
   * @param num_blocks Number of blocks
   * @param chain_length Chain length
   * @param flags Flags
   * @param permissions Permissions
   * @param tags Tags
   * @return KV_client
   */

  std::shared_ptr<storage::kv_client> open_or_create(const std::string &path,
                                                     const std::string &backing_path,
                                                     size_t num_blocks = 1,
                                                     size_t chain_length = 1,
                                                     int32_t flags = 0,
                                                     int32_t permissions = directory::perms::all(),
                                                     const std::map<std::string, std::string> &tags = {});
  /**
   * @brief Open a file and start kv_listener
   * @param path Path
   * @return KV_listener
   */

  std::shared_ptr<storage::kv_listener> listen(const std::string &path);

  /**
   * @brief End scope of path
   * @param path Path
   */

  void close(const std::string &path);

  /**
   * @brief End scope and remove it from directory
   * @param path Path
   */

  void remove(const std::string &path);

  /**
   * @brief Write all dirty blocks back to persistent storage
   * @param path File path
   * @param dest File backing path
   */

  void sync(const std::string &path, const std::string &dest);

  /**
   * @brief Write all dirty blocks back to persistent storage and clear the block
   * @param path File path
   * @param dest File backing path
   */

  void dump(const std::string &path, const std::string &dest);

  /**
   * @brief Load blocks from persistent storage
   * @param path File path
   * @param dest File backing path
   */

  void load(const std::string &path, const std::string &dest);

 private:
  /* Directory client */
  std::shared_ptr<directory::directory_client> fs_;
  /* Lease worker */
  directory::lease_renewal_worker lease_worker_;
};

}
}

#endif //MMUX_MMUX_CLIENT_H
