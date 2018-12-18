#ifndef MMUX_REPLICA_CHAIN_CLIENT_H
#define MMUX_REPLICA_CHAIN_CLIENT_H

#include <map>
#include "block_client.h"
#include "../kv/kv_block.h"
#include "../../directory/client/directory_client.h"

namespace mmux {
namespace storage {
/* Replication chain client class
 * This class only considers the two most important
 * blocks in the chain(i.e. head and tail)*/
class replica_chain_client {
 public:
  typedef block_client *client_ref;
  /* Locked client class */
  class locked_client {
   public:
    /**
     * @brief Constructor
     * Lock parent, if needs redirect, set redirecting true and
     * take down redirect chain
     * @param parent Replication chain to be locked
     */

    locked_client(replica_chain_client &parent);

    /**
     * @brief Destructor, unlock the client
     */

    ~locked_client();

    /**
     * @brief Unlock the client
     */

    void unlock();

    /**
     * @brief Fetch directory replication chain class
     * @return Directory replication chain class
     */

    const directory::replica_chain &chain();

    /**
     * @brief Fetch if redirected
     * @return Bool value, true if redirected
     */

    bool redirecting() const;

    /**
     * @brief Fetch redirect chain block names
     * @return Vector of redirect chain block names
     */

    const std::vector<std::string> &redirect_chain();

    /**
     * @brief Send command
     * @param cmd_id Command id
     * @param args Command arguments
     */

    void send_command(int32_t cmd_id, const std::vector<std::string> &args);

    /**
     * @brief Receive response
     * @return Response
     */

    std::vector<std::string> recv_response();

    /**
     * @brief Run command on replication chain
     * @param cmd_id Command id
     * @param args Command arguments
     * @return Response of the command
     */

    std::vector<std::string> run_command(int32_t cmd_id, const std::vector<std::string> &args);

    /**
     * @brief Run command on redirect replication chain
     * @param cmd_id Command id
     * @param args Command arguments
     * @return Response of the command
     */

    std::vector<std::string> run_command_redirected(int32_t cmd_id, const std::vector<std::string> &args);

   private:
    /* Parent replication chain client */
    replica_chain_client &parent_;
    /* Bool value, true if redirecting */
    bool redirecting_;
    /* Redirect chain name */
    std::vector<std::string> redirect_chain_;
  };

  /**
   * @brief Constructor
   * @param fs directory interface
   * @param path Replication chain path
   * @param chain Directory replication chain class
   * @param timeout_ms Timeout, default for 1000
   */

  explicit replica_chain_client(std::shared_ptr<directory::directory_interface> fs,
                                const std::string &path,
                                const directory::replica_chain &chain,
                                int timeout_ms = 1000);

  /**
   * @brief Destructor, disconnect the client
   */

  ~replica_chain_client();

  /**
   * @brief Fetch directory replication chain class
   * @return Directory replication chain class
   */

  const directory::replica_chain &chain() const;

  /**
   * @brief Lock this replication chain client
   * @return Client after locked
   */

  std::shared_ptr<locked_client> lock();

  /**
   * @brief Check if head and tail of replication chain is connected
   * @return Bool value, true if both connected
   */

  bool is_connected() const;

  /**
   * @brief Send out command via command client
   * For each command id, we either save tail block client or
   * head block client into command client, so we can use
   * command id directly to locate the right block
   * @param cmd_id Command id
   * @param args Command arguments
   */

  void send_command(int32_t cmd_id, const std::vector<std::string> &args);

  /**
   * @brief Receive response of command
   * Check whether response equals seq_.client_seq_no
   * @return Response
   */

  std::vector<std::string> recv_response();

  /**
   * @brief Run command, first send command to the correct block(head || tail)
   * Then receive the response
   * @param cmd_id Command id
   * @param args Command argument
   * @return Response of the command
   */

  std::vector<std::string> run_command(int32_t cmd_id, const std::vector<std::string> &args);

  /**
   * @brief Sent command with a redirect symbol at the back of the arguments
   * @param cmd_id Command id
   * @param args Command arguments
   * @return Response of the command
   */

  std::vector<std::string> run_command_redirected(int32_t cmd_id, const std::vector<std::string> &args);
 private:

  /**
   * @brief Connect replication chain client to directory replication chain class
   * @param chain Directory replication chain class
   * @param timeout_ms time out
   */
  void connect(const directory::replica_chain &chain, int timeout_ms = 0);

  /**
   * @brief Disconnect both head block and tail block
   */

  void disconnect();
  /* Directory client */
  std::shared_ptr<directory::directory_interface> fs_;
  /* Replication chain path */
  std::string path_;
  /* Sequence id */
  sequence_id seq_;
  /* Directory replication chain structure */
  directory::replica_chain chain_;
  /* Block client, head of the chain
   * Set after connection */
  block_client head_;
  /* Block client, tail of the chain
   * Set after connection */
  block_client tail_;
  /* Command response reader */
  block_client::command_response_reader response_reader_;
  /* Clients for each commands */
  std::vector<client_ref> cmd_client_;
  /* Bool value, true if request is in flight */
  bool in_flight_;
  /* Time out */
  int timeout_ms_;
};

}
}

#endif //MMUX_REPLICA_CHAIN_CLIENT_H
