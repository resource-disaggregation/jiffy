#include "chain_module.h"
#include "../utils/logger.h"
#include "kv/kv_block.h"
#include "../utils/time_utils.h"

namespace mmux {
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
  if (!is_head()) {
    if (prev_ == nullptr) {
      LOG(log_level::error) << "Invalid state: Previous is null";
    }
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
  if (!is_head() && !is_tail()) {
    LOG(log_level::error) << "Invalid state: Direct request on a mid node";
    return;
  }

  std::vector<std::string> result;
  run_command(result, oid, args);
  if (is_tail()) {
    clients().respond_client(seq, result);
    subscriptions().notify(op_name(oid), args[0]); // TODO: Fix
  } else {
    if (is_accessor(oid)) {
      LOG(log_level::error) << "Invalid state: Accessor request on non-tail node";
      return;
    }
    {
      std::lock_guard<std::mutex> lock(request_mtx_); // Ensures atomic use of chain client handle
      seq.server_seq_no = ++chain_seq_no_;
      next_->request(seq, oid, args);
    }
  }
  add_pending(seq, oid, args);
}

void chain_module::chain_request(const sequence_id& seq, int32_t oid, const std::vector<std::string> &args) {
  if (is_head()) {
    LOG(log_level::error) << "Invalid state: Chain request " << op_name(oid) << " on head node";
    return;
  }
  if (is_accessor(oid)) {
    LOG(log_level::error) << "Invalid state: Accessor " << op_name(oid) << " as chain request";
    return;
  }

  std::vector<std::string> result;
  run_command(result, oid, args);

  if (is_tail()) {
    clients().respond_client(seq, result);
    subscriptions().notify(op_name(oid), args[0]); // TODO: Fix
    ack(seq);
  } else {
    // Do not need a lock since this is the only thread handling chain requests
    next_->request(seq, oid, args);
  }
}

}
}