#ifndef MMUX_CHAIN_REQUEST_HANDLER_H
#define MMUX_CHAIN_REQUEST_HANDLER_H

#include "chain_request_service.h"
#include "../chain_module.h"
#include "chain_response_service.h"

namespace mmux {
namespace storage {
/* Chain request handler class
 * Inherited from chain_request_serviceIf */
class chain_request_handler : public chain_request_serviceIf {
 public:
  /**
   * @brief Constructor
   * @param prot Protocol
   * @param blocks Data blocks
   */

  explicit chain_request_handler(std::shared_ptr<::apache::thrift::protocol::TProtocol> prot,
                                 std::vector<std::shared_ptr<chain_module>> &blocks);

  /**
   * @brief Send chain request
   * @param seq Sequence identifier
   * @param block_id Block identifier
   * @param cmd_id Command identifier
   * @param arguments Command arguments
   */

  void chain_request(const sequence_id &seq,
                     int32_t block_id,
                     int32_t cmd_id,
                     const std::vector<std::string> &arguments) override;
  /**
   * @brief Run command on data block
   * @param _return Return status
   * @param block_id Block identifier
   * @param cmd_id Command identifier
   * @param arguments Command arguments
   */

  void run_command(std::vector<std::string> &_return,
                   const int32_t block_id,
                   const int32_t cmd_id,
                   const std::vector<std::string> &arguments) override;
 private:
  /* Data blocks*/
  std::vector<std::shared_ptr<chain_module>> &blocks_;
  /* Protocol */
  std::shared_ptr<::apache::thrift::protocol::TProtocol> prot_;
};

}
}

#endif //MMUX_CHAIN_REQUEST_HANDLER_H