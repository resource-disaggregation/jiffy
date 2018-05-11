#include "block_request_handler.h"

namespace mmux {
namespace storage {

block_request_handler::block_request_handler(std::shared_ptr<block_response_client> client,
                                             std::atomic<int64_t> &client_id_gen,
                                             std::vector<std::shared_ptr<chain_module>> &blocks)
    : client_(std::move(client)),
      registered_block_id_(-1),
      registered_client_id_(-1),
      client_id_gen_(client_id_gen),
      blocks_(blocks) {}

int64_t block_request_handler::get_client_id() {
  return client_id_gen_.fetch_add(1L);
}

void block_request_handler::register_client_id(const int32_t block_id, const int64_t client_id) {
  registered_client_id_ = client_id;
  registered_block_id_ = block_id;
  blocks_.at(static_cast<std::size_t>(block_id))->clients().add_client(client_id, client_);
}

void block_request_handler::command_request(const sequence_id &seq,
                                            const int32_t block_id,
                                            const int32_t cmd_id,
                                            const std::vector<std::string> &arguments) {
  blocks_.at(static_cast<std::size_t>(block_id))->request(seq, cmd_id, arguments);
}

int32_t block_request_handler::registered_block_id() const {
  return registered_block_id_;
}

int64_t block_request_handler::registered_client_id() const {
  return registered_client_id_;
}

}
}