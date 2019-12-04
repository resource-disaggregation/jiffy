#ifndef JIFFY_REPLICA_CHAIN_CLIENT_H
#define JIFFY_REPLICA_CHAIN_CLIENT_H

#include <map>
#include "block_client.h"
#include "jiffy/directory/client/directory_client.h"
#include "jiffy/storage/command.h"

namespace jiffy {
namespace storage {

/* Replica chain client class */
class replica_chain_client {
 public:
  typedef block_client *client_ref;
  /**
   * @brief Constructor
   * @param fs Directory interface
   * @param path File path
   * @param chain Directory replica chain
   * @param timeout_ms Timeout
   */

  explicit replica_chain_client(std::shared_ptr<directory::directory_interface> fs,
                                const std::string &path,
                                const directory::replica_chain &chain,
                                const command_map &OPS,
                                int timeout_ms = 1000);

  /**
   * @brief Destructor
   */

  ~replica_chain_client();

  /**
   * @brief Fetch directory replica chain
   * @return Directory replica chain
   */

  const directory::replica_chain &chain() const;

  /**
   * @brief Check if head and tail of replica chain is connected
   * @return Bool value, true if both connected
   */

  bool is_connected() const;

  /**
   * @brief Send out command
   * For each command, we either save tail block client or
   * head block client into command client, so we can use
   * command identifier to locate the right block
   * @param args Command arguments
   */
  void send_command(const std::vector<std::string> &args);

  /**
   * @brief Receive response of command
   * Check whether response equals client sequence number
   * @return Response
   */
  std::vector<std::string> recv_response();

  /**
   * @brief Run command, first send command to the correct block(head or tail)
   * Then receive the response
   * @param cmd_id Command identifier
   * @param args Command argument
   * @return Response of the command
   */
  std::vector<std::string> run_command(const std::vector<std::string> &args);

  /**
   * @brief Sent command with a redirect symbol at the back of the arguments
   * @param cmd_id Command identifier
   * @param args Command arguments
   * @return Response of the command
   */

  std::vector<std::string> run_command_redirected(const std::vector<std::string> &args);

  /**
   * @brief Set replica chain name and metadata
   * @param name Replica chain name
   * @param metadata Replica chain metadata
   */
  void set_chain_name_metadata(std::string &name, std::string &metadata);

 private:

  /**
   * @brief Connect replica chain client to directory replica chain
   * @param chain Directory replica chain
   * @param timeout_ms time out
   */
  void connect(const directory::replica_chain &chain, int timeout_ms = 0);

  /**
   * @brief Disconnect both head block and tail block
   */

  void disconnect();
  /* Directory client */
  std::shared_ptr<directory::directory_interface> fs_;
  /* File path */
  std::string path_;
  /* Sequence identifier */
  sequence_id seq_;
  /* Directory replica chain structure */
  directory::replica_chain chain_;
  /* Block client, head of the chain */
  block_client head_;
  /* Block client, tail of the chain
   * Set after connection */
  block_client tail_;
  /* Command response reader */
  block_client::command_response_reader response_reader_;
  /* Clients for each commands */
  std::unordered_map<std::string, client_ref> cmd_client_;
  /* Bool value, true if request is in flight */
  bool in_flight_;
  /* Time out */
  int timeout_ms_;
  /* Operations for the data structure */
  command_map OPS_;
  /* Bool indicating if the command is accessor type */
  bool accessor_;
  /* Bool indicating if send run command throws an exception */
  bool send_run_command_exception_;
};

}
}

#endif //JIFFY_REPLICA_CHAIN_CLIENT_H
