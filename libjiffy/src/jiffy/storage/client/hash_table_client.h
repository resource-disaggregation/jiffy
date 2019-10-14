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

  /**
   * @brief Destructor
   */
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
  void put(const std::string &key, const std::string &value);

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
   * @brief Put key value pair, update if key exists
   * @param key Key
   * @param value Value
   * @return Response of the command
   */
  std::string upsert(const std::string &key, const std::string &value);

  /**
   * @brief Remove key value pair
   * @param key Key
   * @return Response of the command
   */
  std::string remove(const std::string &key);

  /**
   * @brief Check if key exists
   * @param key Key
   * @return Bool, true if key exists
   */
  bool exists(const std::string &key);


 private:
  /**
   * @brief Fetch block identifier for particular key
   * @param key Key
   * @return Block identifier
   */

  std::size_t block_id(const std::string &key);

  /**
   * @brief Handle command in redirect case
   * @param args Command arguments
   * @param _return Response to be collected
   */

  void handle_redirect(std::vector<std::string> &_return, const std::vector<std::string> &args) override;

  /* Redo times */
  std::size_t redo_times_ = 0;

  /* Map from slot begin to replica chain client pointer */
  std::map<int32_t, std::shared_ptr<replica_chain_client>> blocks_;

  /* Caching created connections */
  std::map<std::string, std::shared_ptr<replica_chain_client>> redirect_blocks_;
};

}
}

#endif //JIFFY_HASH_TABLE_CLIENT_H
