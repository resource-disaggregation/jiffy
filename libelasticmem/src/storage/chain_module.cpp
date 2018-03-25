#include "chain_module.h"

namespace elasticmem {
namespace storage {

void chain_module::run_command_chain(std::vector<std::string> &_return,
                                     int32_t oid,
                                     const std::vector<std::string> &args) {
  if (is_accessor(oid) && !is_tail()) {
    throw std::logic_error("Called accessor operation " + op_name(oid) + " on non-tail node");
  }
  run_command(_return, oid, args);
  if (is_mutator(oid)) {
    if (!is_tail()) {
      assert(next_ != nullptr);
      if (next_->is_connected()) {
        int32_t seq_id = next_->client.send_command(oid, args);
        add_pending(seq_id, oid, args);
        std::vector<std::string> next_response;
        next_->client.recv_command_result(seq_id, next_response);
#ifdef DEBUG
        // Check that the responses are the same
        if (!std::equal(_return.begin(), _return.end(), next_response.begin())) {
          throw std::logic_error("Response from next node does not match local response");
        }
#endif
        remove_pending(seq_id);
      } else {
        // TODO: Add to pending requests
      }
    }
  }
}

}
}