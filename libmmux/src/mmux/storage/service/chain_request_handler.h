#ifndef MMUX_CHAIN_REQUEST_HANDLER_H
#define MMUX_CHAIN_REQUEST_HANDLER_H

#include "chain_request_service.h"
#include "../chain_module.h"
#include "chain_response_service.h"

namespace mmux {
namespace storage {

class chain_request_handler : public chain_request_serviceIf {
 public:
  explicit chain_request_handler(std::shared_ptr<::apache::thrift::protocol::TProtocol> prot,
                                 std::vector<std::shared_ptr<chain_module>> &blocks);

  void chain_request(const sequence_id &seq,
                     int32_t block_id,
                     int32_t cmd_id,
                     const std::vector<std::string> &arguments) override;
  void run_command(std::vector<std::string> &_return,
                   const int32_t block_id,
                   const int32_t cmd_id,
                   const std::vector<std::string> &arguments) override;
 private:
  std::vector<std::shared_ptr<chain_module>> &blocks_;
  std::shared_ptr<::apache::thrift::protocol::TProtocol> prot_;
};

}
}

#endif //MMUX_CHAIN_REQUEST_HANDLER_H
