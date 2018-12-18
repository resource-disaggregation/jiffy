#ifndef MMUX_BLOCK_REQUEST_HANDLER_H
#define MMUX_BLOCK_REQUEST_HANDLER_H

#include <atomic>

#include "block_request_service.h"
#include "block_response_client.h"
#include "../chain_module.h"

namespace mmux {
namespace storage {
/* Block request handler class
 * Inherited from block_request_serviceIf */
class block_request_handler : public block_request_serviceIf {
 public:

  /**
   * @brief Constructor
   * @param client Block response client
   * @param client_id_gen Client id generator
   * @param blocks Data blocks
   */

  explicit block_request_handler(std::shared_ptr<block_response_client> client,
                                 std::atomic<int64_t> &client_id_gen,
                                 std::vector<std::shared_ptr<chain_module>> &blocks);

  /**
   * @brief Fetch current client_id_gen_ and add one to the atomic pointer
   * @return Current client_id_gen_
   */

  int64_t get_client_id() override;

  /**
   * @brief Register the client with the block
   * Save the block id and client id to the request handler and
   * add client to the block response client map
   * @param block_id Block id number
   * @param client_id Client id number
   */

  void register_client_id(int32_t block_id, int64_t client_id) override;

  /**
   * @brief Request an command, starting from either the head or tail of the chain
   * @param seq Sequence id
   * @param block_id Block id number
   * @param cmd_id Command id number
   * @param arguments Arguments
   */

  void command_request(const sequence_id &seq,
                       int32_t block_id,
                       int32_t cmd_id,
                       const std::vector<std::string> &arguments) override;

  /**
   * @brief Fetch the handler's registered block id
   * @return Registered block id number
   */

  int32_t registered_block_id() const;

  /**
   * @brief Fetch the handler's registered client id
   * @return Registered client id number
   */

  int64_t registered_client_id() const;

 private:
  /* Block response client */
  std::shared_ptr<block_response_client> client_;
  /* Registered block id */
  int32_t registered_block_id_;
  /* Registered client id */
  int64_t registered_client_id_;
  /* Client id generator */
  std::atomic<int64_t> &client_id_gen_;
  /* Data blocks */
  std::vector<std::shared_ptr<chain_module>> &blocks_;
};

}
}

#endif //MMUX_BLOCK_REQUEST_HANDLER_H
