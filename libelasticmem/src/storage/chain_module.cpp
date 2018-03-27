#include "chain_module.h"

namespace elasticmem {
namespace storage {

void chain_module::run_command_chain(std::vector<std::string> &_return,
                                     int64_t seq_no,
                                     int32_t oid,
                                     const std::vector<std::string> &args) {
  if (is_accessor(oid) && !is_tail()) {
    throw std::logic_error("Called accessor operation " + op_name(oid) + " on non-tail node");
  }
  run_command(_return, oid, args);
  if (is_head()) {
    assert(seq_no == -1);
    seq_no = incr_seq_no();
  }
  if (is_mutator(oid)) {
    if (!is_tail()) {
      assert(next_ != nullptr);
      if (next_->is_connected()) {
        int32_t seq_id = next_->send_command(seq_no, oid, args);
        add_pending(seq_no, oid, args);
        std::vector<std::string> next_response;
        try {
          next_->recv_command_result(seq_id, next_response);
        } catch (...) {
          throw chain_exception("FAILURE " + next_->endpoint());
        }
#ifdef DEBUG
        // Check that the responses are the same
        if (!std::equal(_return.begin(), _return.end(), next_response.begin())) {
          throw std::logic_error("Response from next node does not match local response");
        }
#endif
        remove_pending(seq_no);
      } else {
        add_pending(seq_no, oid, args);
        throw chain_exception("FAILURE " + next_->endpoint());
      }
    }
  }
}

}
}