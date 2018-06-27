#include "chain_request_handler.h"

using namespace ::apache::thrift::protocol;

namespace mmux {
namespace storage {

chain_request_handler::chain_request_handler(std::shared_ptr<TProtocol> prot,
                                             std::vector<std::shared_ptr<chain_module>> &blocks)
    : blocks_(blocks), prot_(std::move(prot)) {}

void chain_request_handler::chain_request(const sequence_id &seq,
                                          const int32_t block_id,
                                          const int32_t cmd_id,
                                          const std::vector<std::string> &arguments) {
  const auto &b = blocks_.at(static_cast<std::size_t>(block_id));
  if (!b->is_set_prev()) {
    b->reset_prev(prot_);
  }
  b->chain_request(seq, cmd_id, arguments);
}

void chain_request_handler::run_command(std::vector<std::string> &_return,
                                        const int32_t block_id,
                                        const int32_t cmd_id,
                                        const std::vector<std::string> &arguments) {
  blocks_.at(static_cast<std::size_t>(block_id))->run_command(_return, cmd_id, arguments);
}

}
}
