#ifndef MMUX_REPLICA_CHAIN_CLIENT_H
#define MMUX_REPLICA_CHAIN_CLIENT_H

#include <map>
#include "block_client.h"
#include "../kv/kv_block.h"
#include "../../directory/client/directory_client.h"

namespace mmux {
namespace storage {
/* */
class replica_chain_client {
 public:
  typedef block_client *client_ref;
  /* */
  class locked_client {
   public:
    /**
     * @brief
     * @param parent
     */

    locked_client(replica_chain_client &parent);

    /**
     * @brief
     */

    ~locked_client();

    /**
     * @brief
     */

    void unlock();

    /**
     * @brief
     * @return
     */

    const directory::replica_chain &chain();

    /**
     * @brief
     * @return
     */

    bool redirecting() const;

    /**
     * @brief
     * @return
     */

    const std::vector<std::string> &redirect_chain();

    /**
     * @brief
     * @param cmd_id
     * @param args
     */

    void send_command(int32_t cmd_id, const std::vector<std::string> &args);

    /**
     * @brief
     * @return
     */

    std::vector<std::string> recv_response();

    /**
     * @brief
     * @param cmd_id
     * @param args
     * @return
     */

    std::vector<std::string> run_command(int32_t cmd_id, const std::vector<std::string> &args);

    /**
     * @brief
     * @param cmd_id
     * @param args
     * @return
     */

    std::vector<std::string> run_command_redirected(int32_t cmd_id, const std::vector<std::string> &args);

   private:
    /* */
    replica_chain_client &parent_;
    /* */
    bool redirecting_;
    /* */
    std::vector<std::string> redirect_chain_;
  };

  /**
   * @brief
   * @param fs
   * @param path
   * @param chain
   * @param timeout_ms
   */

  explicit replica_chain_client(std::shared_ptr<directory::directory_interface> fs,
                                const std::string &path,
                                const directory::replica_chain &chain,
                                int timeout_ms = 1000);

  /**
   * @brief
   */

  ~replica_chain_client();

  /**
   * @brief
   * @return
   */

  const directory::replica_chain &chain() const;

  /**
   * @brief
   * @return
   */

  std::shared_ptr<locked_client> lock();

  /**
   * @brief
   * @return
   */

  bool is_connected() const;

  /**
   * @brief
   * @param cmd_id
   * @param args
   */

  void send_command(int32_t cmd_id, const std::vector<std::string> &args);

  /**
   * @brief
   * @return
   */

  std::vector<std::string> recv_response();

  /**
   * @brief
   * @param cmd_id
   * @param args
   * @return
   */

  std::vector<std::string> run_command(int32_t cmd_id, const std::vector<std::string> &args);

  /**
   * @brief
   * @param cmd_id
   * @param args
   * @return
   */

  std::vector<std::string> run_command_redirected(int32_t cmd_id, const std::vector<std::string> &args);
 private:

  /**
   * @brief
   * @param chain
   * @param timeout_ms
   */
  void connect(const directory::replica_chain &chain, int timeout_ms = 0);

  /**
   * @brief
   */

  void disconnect();
  /* */
  std::shared_ptr<directory::directory_interface> fs_;
  /* */
  std::string path_;
  /* */
  sequence_id seq_;
  /* */
  directory::replica_chain chain_;
  /* */
  block_client head_;
  /* */
  block_client tail_;
  /* */
  block_client::command_response_reader response_reader_;
  /* */
  std::vector<client_ref> cmd_client_;
  /* */
  bool in_flight_;
  /* */
  int timeout_ms_;
};

}
}

#endif //MMUX_REPLICA_CHAIN_CLIENT_H
