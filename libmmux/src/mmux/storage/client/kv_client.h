#ifndef MMUX_KV_CLIENT_H
#define MMUX_KV_CLIENT_H

#include "../../directory/client/directory_client.h"
#include "../../utils/client_cache.h"
#include "replica_chain_client.h"

namespace mmux {
namespace storage {
/* Redo when exception class
 * Redo whenever exception happens */
class redo_error : public std::exception {
 public:
  redo_error() {}
};

/* Key value storage client */
class kv_client {
 public:
  /* Locked client class */
  class locked_client {
   public:
    typedef replica_chain_client::locked_client locked_block_t;
    typedef std::shared_ptr<locked_block_t> locked_block_ptr_t;

    typedef replica_chain_client block_t;
    typedef std::shared_ptr<block_t> block_ptr_t;

    /**
     * @brief Constructor
     * @param parent Key value client parent
     */

    locked_client(kv_client &parent);

    /**
     * @brief Unlock the key value client
     */

    void unlock();

    /**
     * @brief Put
     * @param key Key
     * @param value Value
     * @return Response of the command
     */

    std::string put(const std::string &key, const std::string &value);

    /**
     * @brief Get
     * @param key Key
     * @return Response of the command
     */

    std::string get(const std::string &key);

    /**
     * @brief Update
     * @param key Key
     * @param value Value
     * @return Response of the command
     */


    std::string update(const std::string &key, const std::string &value);

    /**
     * @brief Remove
     * @param key Key
     * @return Response of the command
     */


    std::string remove(const std::string &key);

    /**
     * @brief Put in batch
     * @param kvs Key value batch
     * @return Responses of the batch commands
     */

    std::vector<std::string> put(const std::vector<std::string> &kvs);

    /**
     * @brief Get in batch
     * @param keys Key batch
     * @return Responses of the batch commands
     */

    std::vector<std::string> get(const std::vector<std::string> &keys);

    /**
     * @brief Update in batch
     * @param kvs Key value batch
     * @return Responses of the batch commands
     */

    std::vector<std::string> update(const std::vector<std::string> &kvs);

    /**
     * @brief Remove in batch
     * @param keys Key batch
     * @return Responses of the batch commands
     */

    std::vector<std::string> remove(const std::vector<std::string> &keys);

    /**
     * @brief Fetch number of keys
     * @return Key number
     */

    size_t num_keys();
   private:

    /**
     * @brief Handle command in redirect case
     * @param cmd_id Command identifier
     * @param args Command arguments
     * @param response Response to be collected
     */

    void handle_redirect(int32_t cmd_id, const std::vector<std::string> &args, std::string &response);

    /**
     * @brief Handle batch commands in redirect case
     * @param cmd_id Command identifier
     * @param args Command arguments
     * @param response Response to be collected
     */


    void handle_redirects(int32_t cmd_id, const std::vector<std::string> &args, std::vector<std::string> &responses);

    /* Parent key value client */
    kv_client &parent_;
    /* Blocks */
    std::vector<locked_block_ptr_t> blocks_;
    /* Redirect blocks */
    std::vector<block_ptr_t> redirect_blocks_;
    /* Locked redirect blocks */
    std::vector<locked_block_ptr_t> locked_redirect_blocks_;
    /* New block */
    std::vector<locked_block_ptr_t> new_blocks_;
  };

  /**
   * @brief Constructor
   * Store all replica chain and their begin slot
   * @param fs Directory service
   * @param path Key value block path
   * @param status Data status
   * @param timeout_ms Timeout
   */

  kv_client(std::shared_ptr<directory::directory_interface> fs,
            const std::string &path,
            const directory::data_status &status,
            int timeout_ms = 1000);

  /**
   * @brief Refresh the slot and blocks from directory service
   */

  void refresh();

  /**
   * @brief Fetch data status
   * @return Data status
   */

  directory::data_status &status();

  /**
   * @brief Lock key value client
   * @return Locked client
   */

  std::shared_ptr<locked_client> lock();

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

  std::vector<std::string> put(const std::vector<std::string> &kvs);

  /**
   * @brief Get in batch
   * @param keys Key batch
   * @return Response of batch command
   */

  std::vector<std::string> get(const std::vector<std::string> &keys);

  /**
   * @brief Update in batch
   * @param kvs Key value batch
   * @return Response of batch command
   */

  std::vector<std::string> update(const std::vector<std::string> &kvs);

  /**
   * @brief Remove in batch
   * @param keys Key batch
   * @return Response of batch command
   */

  std::vector<std::string> remove(const std::vector<std::string> &keys);
 private:
  /**
   * @brief Fetch block identifier for particular key
   * @param key Key
   * @return Block identifier
   */

  size_t block_id(const std::string &key);

  /**
   * @brief Run same operation in batch
   * @param id Operation identifier
   * @param args Operation arguments
   * @param args_per_op Argument per operation
   * @return
   */

  std::vector<std::string> batch_command(const kv_op_id &id, const std::vector<std::string> &args, size_t args_per_op);

  /**
   * @brief Handle command in redirect case
   * @param cmd_id Command identifier
   * @param args Command arguments
   * @param response Response to be collected
   */

  void handle_redirect(int32_t cmd_id, const std::vector<std::string> &args, std::string &response);

  /**
   * @brief Handle multiple commands in redirect case
   * @param cmd_id Command identifier
   * @param args Command arguments
   * @param responses Responses to be collected
   */

  void handle_redirects(int32_t cmd_id, const std::vector<std::string> &args, std::vector<std::string> &responses);
  /* Directory client */
  std::shared_ptr<directory::directory_interface> fs_;
  /* Key value block path */
  std::string path_;
  /* Data status */
  directory::data_status status_;
  /* Key value blocks, each block only save a replica chain client */
  std::vector<std::shared_ptr<replica_chain_client>> blocks_;
  /* Slot begin of the blocks */
  std::vector<int32_t> slots_;
  /* Time out*/
  int timeout_ms_;
};

}
}

#endif //MMUX_KV_CLIENT_H