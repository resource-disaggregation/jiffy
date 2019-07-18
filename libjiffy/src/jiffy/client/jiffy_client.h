#ifndef JIFFY_JIFFY_CLIENT_H
#define JIFFY_JIFFY_CLIENT_H

#include <string>
#include "jiffy/directory/directory_ops.h"
#include "jiffy/directory/client/lease_renewal_worker.h"
#include "jiffy/storage/client/hash_table_client.h"
#include "jiffy/storage/client/file_reader.h"
#include "jiffy/storage/client/file_writer.h"
#include "jiffy/storage/client/fifo_queue_client.h"
#include "jiffy/storage/client/data_structure_listener.h"

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
   * @brief Create hash table
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
   * @brief Create file
   * @param path File path
   * @param backing_path File backing path
   * @param num_blocks Number of blocks
   * @param chain_length Replication chain length
   * @param flags Flags
   * @param permissions Permissions
   * @param tags Tags
   * @return File writer
   */
  std::shared_ptr<storage::file_writer> create_file(const std::string &path,
                                                    const std::string &backing_path,
                                                    int32_t num_blocks = 1,
                                                    int32_t chain_length = 1,
                                                    int32_t flags = 0,
                                                    int32_t permissions = directory::perms::all(),
                                                    const std::map<std::string, std::string> &tags = {});

  /**
   * @brief Create fifo queue
   * @param path File path
   * @param backing_path File backing path
   * @param num_blocks Number of blocks
   * @param chain_length Replication chain length
   * @param flags Flags
   * @param permissions Permissions
   * @param tags Tags
   * @return File writer
   */
  std::shared_ptr<storage::fifo_queue_client> create_fifo_queue(const std::string &path,
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
  std::shared_ptr<storage::hash_table_client> open_hash_table(const std::string &path);

  /**
   * @brief Open file, begin lease
   * @param path File path
   * @return File reader client
   */
  std::shared_ptr<storage::file_reader> open_file_reader(const std::string &path);

  /**
   * @brief Open file, begin lease
   * @param path File path
   * @return File writer client
   */
  std::shared_ptr<storage::file_writer> open_file_writer(const std::string &path);

  /**
   * @brief Open file, begin lease
   * @param path File path
   * @return Fifo queue client
   */
  std::shared_ptr<storage::fifo_queue_client> open_fifo_queue(const std::string &path);

  /**
   * @brief Open or create hash table
   * @param path File path
   * @param backing_path File backing path
   * @param num_blocks Number of blocks
   * @param chain_length Replication chain length
   * @param flags Flags
   * @param permissions Permissions
   * @param tags Tags
   * @return Hash table client
   */
  std::shared_ptr<storage::hash_table_client> open_or_create_hash_table(const std::string &path,
                                                                        const std::string &backing_path,
                                                                        int32_t num_blocks = 1,
                                                                        int32_t chain_length = 1,
                                                                        int timeout_ms = 1000,
                                                                        int32_t flags = 0,
                                                                        int32_t permissions = directory::perms::all(),
                                                                        const std::map<std::string,
                                                                                       std::string> &tags = {});

  /**
   * @brief Open or create file
   * @param path File path
   * @param backing_path File backing path
   * @param num_blocks Number of blocks
   * @param chain_length Replication chain length
   * @param flags Flags
   * @param permissions Permissions
   * @param tags Tags
   * @return File client
   */
  std::shared_ptr<storage::file_writer> open_or_create_file(const std::string &path,
                                                            const std::string &backing_path,
                                                            int32_t num_blocks = 1,
                                                            int32_t chain_length = 1,
                                                            int32_t flags = 0,
                                                            int32_t permissions = directory::perms::all(),
                                                            const std::map<std::string,
                                                                           std::string> &tags = {});

  /**
   * @brief Open or create fifo queue
   * @param path File path
   * @param backing_path File backing path
   * @param num_blocks Number of blocks
   * @param chain_length Replication chain length
   * @param flags Flags
   * @param permissions Permissions
   * @param tags Tags
   * @return Fifo queue client
   */
  std::shared_ptr<storage::fifo_queue_client> open_or_create_fifo_queue(const std::string &path,
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
  std::shared_ptr<storage::data_structure_listener> listen(const std::string &path);

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