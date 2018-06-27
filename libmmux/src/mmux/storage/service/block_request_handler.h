#ifndef MMUX_BLOCK_REQUEST_HANDLER_H
#define MMUX_BLOCK_REQUEST_HANDLER_H

#include <atomic>

#include "block_request_service.h"
#include "block_response_client.h"
#include "../chain_module.h"

namespace mmux {
namespace storage {

class block_request_handler : public block_request_serviceIf {
 public:
  explicit block_request_handler(std::shared_ptr<block_response_client> client,
                                 std::atomic<int64_t> &client_id_gen,
                                 std::vector<std::shared_ptr<chain_module>> &blocks);

  int64_t get_client_id() override;
  void register_client_id(int32_t block_id, int64_t client_id) override;
  void command_request(const sequence_id &seq,
                       int32_t block_id,
                       int32_t cmd_id,
                       const std::vector<std::string> &arguments) override;

  int32_t registered_block_id() const;
  int64_t registered_client_id() const;

 private:
  std::shared_ptr<block_response_client> client_;
  int32_t registered_block_id_;
  int64_t registered_client_id_;
  std::atomic<int64_t> &client_id_gen_;
  std::vector<std::shared_ptr<chain_module>> &blocks_;
};

}
}

#endif //MMUX_BLOCK_REQUEST_HANDLER_H
