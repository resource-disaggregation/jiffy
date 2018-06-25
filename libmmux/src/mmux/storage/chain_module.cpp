#include "chain_module.h"
#include "../utils/logger.h"
#include "kv/kv_block.h"

namespace mmux {
namespace storage {

using namespace utils;

void chain_module::resend_pending() {
  auto ops = pending_.lock_table();
  try {
    for (const auto &op: ops) {
      next_->request(op.second.seq, op.second.op_ids, op.second.args);
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

void chain_module::request(sequence_id seq,
                           const std::vector<int32_t> &oids,
                           const std::vector<std::vector<std::string>> &args) {
  if (!is_head() && !is_tail()) {
    LOG(log_level::error) << "Invalid state: Direct request on a mid node";
    return;
  }

  std::vector<std::vector<std::string>> result;
  run_commands(result, oids, args);
  if (is_tail()) {
    clients().respond_client(seq, result);
    for (size_t i = 0; i < oids.size(); i++) {
      subscriptions().notify(op_name(oids[i]), args[i][0]); // TODO: Fix
    }
  } else {
    {
      std::lock_guard<std::mutex> lock(request_mtx_); // Ensures atomic use of chain client handle
      seq.server_seq_no = ++chain_seq_no_;
      next_->request(seq, oids, args);
    }
  }
  add_pending(seq, oids, args);
}

void chain_module::chain_request(const sequence_id &seq,
                                 const std::vector<int32_t> &oids,
                                 const std::vector<std::vector<std::string>> &args) {
  if (is_head()) {
    LOG(log_level::error) << "Invalid state: Chain request on head node";
    return;
  }

  std::vector<std::vector<std::string>> result;
  run_commands(result, oids, args);

  if (is_tail()) {
    clients().respond_client(seq, result);
    for (size_t i = 0; i < oids.size(); i++) {
      subscriptions().notify(op_name(oids[i]), args[i][0]); // TODO: Fix
    }
    ack(seq);
  } else {
    // Do not need a lock since this is the only thread handling chain requests
    next_->request(seq, oids, args);
  }
}

}
}