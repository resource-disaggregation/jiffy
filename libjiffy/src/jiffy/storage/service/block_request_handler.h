#ifndef JIFFY_BLOCK_REQUEST_HANDLER_H
#define JIFFY_BLOCK_REQUEST_HANDLER_H

#include <atomic>
#include <jiffy/storage/notification/notification_response_client.h>

#include "block_request_service.h"
#include "block_response_client.h"
#include "jiffy/storage/block.h"

namespace jiffy {
namespace storage {

/* Block request handler class
 * Inherited from block_request_serviceIf */
class block_request_handler : public block_request_serviceIf {
 public:

  /**
   * @brief Constructor
   * @param prot Block response client
   * @param client_id_gen Client identifier generator
   * @param blocks Data blocks
   */
  explicit block_request_handler(std::shared_ptr<::apache::thrift::protocol::TProtocol> prot,
                                 std::atomic<int64_t> &client_id_gen,
                                 std::map<int, std::shared_ptr<block>> &blocks);

  /**
   * @brief Fetch client identifier and add one to the atomic pointer
   * @return Client identifier
   */
  int64_t get_client_id() override;

  /**
   * @brief Register the client with the block
   * Save the block identifier and client identifier to the request handler and
   * add client to the block response client map
   * @param block_id Block identifier
   * @param client_id Client identifier
   */
  void register_client_id(int32_t block_id, int64_t client_id) override;

  /**
   * @brief Request a command, starting from either the head or tail of the chain
   * @param seq Sequence identifier
   * @param block_id Block identifier
   * @param args Command arguments
   */
  void command_request(const sequence_id &seq, int32_t block_id, const std::vector<std::string> &args) override;

  /**
   * @brief Fetch the registered block identifier
   * @return Registered block identifier
   */
  int32_t registered_block_id() const;

  /**
   * @brief Fetch the registered client identifier
   * @return Registered client identifier
   */
  int64_t registered_client_id() const;

  /**
   * @brief Send chain request
   * @param seq Sequence identifier
   * @param block_id Block identifier
   * @param args Command arguments
   */
  void chain_request(const sequence_id &seq,
                     const int32_t block_id,
                     const std::vector<std::string> &args) override;

  /**
   * @brief Run command on data block
   * @param _return Return status
   * @param block_id Block identifier
   * @param args Command arguments
   */
  void run_command(std::vector<std::string> &_return,
                   const int32_t block_id,
                   const std::vector<std::string> &args) override;

  /**
   * @brief Subscribe to a block for given operations
   * This function adds all pairs of block and operations in local subscription set
   * and adds all the operation with the subscription service client to the subscription map
   * of the block
   * @param block_id Block identifier
   * @param ops Operations
   */
  void subscribe(int32_t block_id,
      const std::vector<std::string> &ops) override;

  /**
   * @brief Unsubscribe to the block for given operations
   * This function takes down all the operations that are unsubscribed
   * and clears local subscription, then it removes the subscription in
   * the block's subscription map
   * @param block_id Block identifier, if block identifier is -1, find block identifier in local subscription
   * @param ops Operations, if operation is empty, clear all in local subscription
   */
  void unsubscribe(int32_t block_id, const std::vector<std::string> &ops) override;

 private:
  /* Local subscription set for pairs of partition and operation */
  std::set<std::pair<int32_t, std::string>> local_subs_;
  /* Protocol */
  std::shared_ptr<::apache::thrift::protocol::TProtocol> prot_;
  /* Block response client */
  std::shared_ptr<block_response_client> client_;
  /* Notification response client */
  std::shared_ptr<notification_response_client> notification_client_;
  /* Registered partition identifier */
  int32_t registered_block_id_;
  /* Registered client identifier */
  int64_t registered_client_id_;
  /* Client identifier generator */
  std::atomic<int64_t> &client_id_gen_;
  /* Data blocks */
  std::map<int, std::shared_ptr<block>> &blocks_;
};

}
}

#endif //JIFFY_BLOCK_REQUEST_HANDLER_H
