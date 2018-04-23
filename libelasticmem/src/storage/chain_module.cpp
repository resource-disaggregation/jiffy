#include "chain_module.h"
#include "../utils/logger.h"

namespace elasticmem {
namespace storage {

using namespace utils;

void chain_module::resend_pending() {
  auto ops = pending_.lock_table();
  try {
    for (const auto &op: ops) {
      next_->request(op.second.seq, op.second.op_id, op.second.args);
    }
  } catch (...) {
    ops.unlock();
    std::rethrow_exception(std::current_exception());
  }
  ops.unlock();
}

void chain_module::ack(const sequence_id &seq) {
  remove_pending(seq);
  assert(prev_ != nullptr);
  if (!is_head()) {
    prev_->ack(seq);
  }
}

void chain_module::reset_next(const std::string &next_block) {
  next_->reset(next_block);
}

void chain_module::reset_next_and_listen(const std::string &next_block) {
  auto protocol = next_->reset(next_block);
  if (protocol) {
    auto handler = std::make_shared<chain_response_handler>(this);
    auto processor = std::make_shared<chain_response_serviceProcessor>(handler);
    if (response_processor_.joinable())
      response_processor_.join();
    response_processor_ = std::thread([processor, protocol] {
      while (true) {
        try {
          if (!processor->process(protocol, protocol, nullptr)) {
            break;
          }
        } catch (std::exception &e) {
          break;
        }
      }
    });
  }
}

void chain_module::request(sequence_id seq, int32_t oid, const std::vector<std::string> &args) {
  if (is_accessor(oid) && !is_tail()) {
    LOG(log_level::error) << "Called accessor operation " << op_name(oid) << " on non-tail node";
    return;
  }
  std::vector<std::string> result;
  if (state() == block_state::importing) {
    if (args.back() == "!redirected") {
      run_command(result, oid, args);
    } else {
      LOG(log_level::info) << "Received request without redirection on importing block";
      result.emplace_back("!block_moved");
    }
  } else {
    // Let specific block implementation handle exporting cases
    run_command(result, oid, args);
  }
  if (is_mutator(oid)) {
    if (!is_tail()) {
      assert(next_ != nullptr);
      {
        std::lock_guard<std::mutex> lock(request_mtx_); // Ensures FIFO order w.r.t. seq.server_seq_no
        if (is_head()) {
          seq.server_seq_no = ++chain_seq_no_;
        }
        next_->request(seq, oid, args);
      }
      add_pending(seq, oid, args);
    } else {
      clients().respond_client(seq, result);
      subscriptions().notify(op_name(oid), args[0]); // TODO: Fix
      ack(seq);
    }
  } else {
    clients().respond_client(seq, result);
    subscriptions().notify(op_name(oid), args[0]); // TODO: Fix
  }
}

}
}