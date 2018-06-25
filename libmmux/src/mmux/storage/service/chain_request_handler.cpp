#include "chain_request_handler.h"

using namespace ::apache::thrift::protocol;

namespace mmux {
namespace storage {

chain_request_handler::chain_request_handler(std::shared_ptr<TProtocol> prot,
                                             std::vector<std::shared_ptr<chain_module>> &blocks)
    : blocks_(blocks), prot_(std::move(prot)) {}

void chain_request_handler::chain_request(const sequence_id &seq,
                                          int32_t block_id,
                                          const std::vector<int32_t> &cmd_ids,
                                          const std::vector<std::vector<std::string>> &arguments) {
  const auto &b = blocks_.at(static_cast<std::size_t>(block_id));
  if (!b->is_set_prev()) {
    b->reset_prev(prot_);
  }
  b->chain_request(seq, cmd_ids, arguments);
}

void chain_request_handler::run_command(std::vector<std::vector<std::string>> &_return,
                                        const int32_t block_id,
                                        const std::vector<int32_t> &cmd_ids,
                                        const std::vector<std::vector<std::string>> &arguments) {
  blocks_.at(static_cast<std::size_t>(block_id))->run_commands(_return, cmd_ids, arguments);
}

}
}
