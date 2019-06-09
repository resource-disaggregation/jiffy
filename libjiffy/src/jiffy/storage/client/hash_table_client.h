#ifndef JIFFY_HASH_TABLE_CLIENT_H
#define JIFFY_HASH_TABLE_CLIENT_H

#include "jiffy/directory/client/directory_client.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/client_cache.h"
#include "jiffy/storage/client/data_structure_client.h"
#include "jiffy/storage/hashtable/hash_table_ops.h"

namespace jiffy {
namespace storage {

/* Hash table client */
class hash_table_client : public data_structure_client {
 public:
  /**
   * @brief Constructor
   * Store all replica chain and their begin slot
   * @param fs Directory service
   * @param path Key value block path
   * @param status Data status
   * @param timeout_ms Timeout
   */

  hash_table_client(std::shared_ptr<directory::directory_interface> fs,
                    const std::string &path,
                    const directory::data_status &status,
                    int timeout_ms = 1000);
  virtual ~hash_table_client() = default;
  /**
   * @brief Refresh the slot and blocks from directory service
   */

  void refresh() override;

  /**
   * @brief Put key value pair
   * @param key Key
   * @param value Value
   * @return Response of the command
   */

  std::string put(const std::string &key, const std::string &value);

  /**
   * @brief Get value for specified key
   * @param key Key
   * @return Response of the command
   */

  std::string get(const std::string &key);

  /**
   * @brief Update key value pair
   * @param key Key
   * @param value Value
   * @return Response of the command
   */

  std::string update(const std::string &key, const std::string &value);

  /**
   * @brief Remove key value pair
   * @param key Key
   * @return Response of the command
   */

  std::string remove(const std::string &key);

  /**
   * @brief Put in batch
   * @param kvs Key value batch
   * @return Response of batch command
   */

  std::vector<std::string> put(std::vector<std::string> &kvs);

  /**
   * @brief Get in batch
   * @param keys Key batch
   * @return Response of batch command
   */

  std::vector<std::string> get(std::vector<std::string> &keys);

  /**
   * @brief Update in batch
   * @param kvs Key value batch
   * @return Response of batch command
   */

  std::vector<std::string> update(std::vector<std::string> &kvs);

  /**
   * @brief Remove in batch
   * @param keys Key batch
   * @return Response of batch command
   */

  std::vector<std::string> remove(std::vector<std::string> &keys);

  /**
   * @brief Fetch number of keys
   * @return Number of keys
   */
  std::size_t num_keys();
 private:
  /**
   * @brief Fetch block identifier for particular key
   * @param key Key
   * @return Block identifier
   */

  std::size_t block_id(const std::string &key);

  /**
   * @brief Run same operation in batch
   * @param id Operation identifier
   * @param args Operation arguments
   * @param args_per_op Argument per operation
   * @return
   */

  std::vector<std::string> batch_command(const hash_table_cmd_id &id,
                                         const std::vector<std::string> &args,
                                         size_t args_per_op);

  /**
   * @brief Handle command in redirect case
   * @param cmd_id Command identifier
   * @param args Command arguments
   * @param response Response to be collected
   */

  void handle_redirect(int32_t cmd_id, const std::vector<std::string> &args, std::string &response) override;

  /**
   * @brief Handle multiple commands in redirect case
   * @param cmd_id Command identifier
   * @param args Command arguments
   * @param responses Responses to be collected
   */

  void handle_redirects(int32_t cmd_id,
                        std::vector<std::string> &args,
                        std::vector<std::string> &responses) override;

  /* Redo times */
  std::size_t redo_times = 0;
  /* Map from slot begin to replica chain client pointer */
  std::map<int32_t, std::shared_ptr<replica_chain_client>> blocks_;
};

}
}

#endif //JIFFY_HASH_TABLE_CLIENT_H
