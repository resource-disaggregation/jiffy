#ifndef JIFFY_JIFFY_CLIENT_H
#define JIFFY_JIFFY_CLIENT_H

#include <string>
#include "../directory/directory_ops.h"
#include "../directory/client/lease_renewal_worker.h"
#include "jiffy/storage/client/hash_table_client.h"
#include "jiffy/storage/client/hash_table_listener.h"

namespace jiffy {
namespace client {

/* Jiffy client class */
class jiffy_client {
 public:
  /**
   * @brief Constructor
   * @param host Directory client hostname
   * @param dir_port Directory client port number
   * @param lease_port Lease worker port number
   */

  jiffy_client(const std::string &host, int dir_port, int lease_port);

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
   * @param path File Path
   */

  void begin_scope(const std::string &path);

  /**
   * @brief End scope, remove path from lease worker
   * @param path File Path
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
   * @return Key value client
   */

  std::shared_ptr<storage::hash_table_client> create_hash_table(const std::string &path,
                                                                const std::string &backing_path,
                                                                int32_t num_blocks = 1,
                                                                int32_t chain_length = 1,
                                                                int32_t flags = 0,
                                                                int32_t permissions = directory::perms::all(),
                                                                const std::map<std::string, std::string> &tags = {});

  /**
   * @brief Open file, begin lease
   * @param path File path
   * @return Key value client
   */

  std::shared_ptr<storage::hash_table_client> open(const std::string &path);

  /**
   * @brief Open or create, begin lease
   * @param path File path
   * @param backing_path File backing path
   * @param num_blocks Number of blocks
   * @param chain_length Replication chain length
   * @param flags Flags
   * @param permissions Permissions
   * @param tags Tags
   * @return Key value client
   */

  std::shared_ptr<storage::hash_table_client> open_or_create_hash_table(const std::string &path,
                                                                        const std::string &backing_path,
                                                                        int32_t num_blocks = 1,
                                                                        int32_t chain_length = 1,
                                                                        int32_t flags = 0,
                                                                        int32_t permissions = directory::perms::all(),
                                                                        const std::map<std::string,
                                                                                       std::string> &tags = {});
  /**
   * @brief Open a file and start key value listener
   * @param path File path
   * @return Key value listener
   */

  std::shared_ptr<storage::hash_table_listener> listen(const std::string &path);

  /**
   * @brief End scope of file
   * @param path File path
   */

  void close(const std::string &path);

  /**
   * @brief End scope and remove file from directory
   * @param path File path
   */

  void remove(const std::string &path);

  /**
   * @brief Write all dirty blocks back to persistent storage
   * @param path File path
   * @param dest File backing path
   */

  void sync(const std::string &path, const std::string &dest);

  /**
   * @brief Write all dirty blocks back to persistent storage and clear the blocks
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

#endif //JIFFY_JIFFY_CLIENT_H
