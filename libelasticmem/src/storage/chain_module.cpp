#include "chain_module.h"
#include "../utils/logger.h"
#include "kv/kv_block.h"
#include "../utils/time_utils.h"

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
  auto t1 = time_utils::now_us();
  std::vector<std::string> result;
  run_command(result, oid, args);
  auto t2 = time_utils::now_us();
  LOG(log_level::info) << "run_command took " << (t2 - t1) << " us";
  if (is_mutator(oid)) {
    if (!is_tail()) {
      t1 = time_utils::now_us();
      assert(next_ != nullptr);
      {
        std::lock_guard<std::mutex> lock(request_mtx_); // Ensures FIFO order w.r.t. seq.server_seq_no
        if (is_head()) {
          seq.server_seq_no = ++chain_seq_no_;
        }
        next_->request(seq, oid, args);
      }
      add_pending(seq, oid, args);
      t2 = time_utils::now_us();
      LOG(log_level::info) << "forwarding took " << (t2 - t1) << " us";
    } else {
      t1 = time_utils::now_us();
      clients().respond_client(seq, result);
      subscriptions().notify(op_name(oid), args[0]); // TODO: Fix
      ack(seq);
      t2 = time_utils::now_us();
      LOG(log_level::info) << "responding took " << (t2 - t1) << " us";
    }
  } else {
    clients().respond_client(seq, result);
    subscriptions().notify(op_name(oid), args[0]); // TODO: Fix
  }
}

}
}