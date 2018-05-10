#include "replica_chain_client.h"
#include "../manager/detail/block_name_parser.h"
#include "../kv/kv_block.h"

namespace mmux {
namespace storage {

replica_chain_client::replica_chain_client(const std::vector<std::string> &chain) {
  seq_.client_id = -1;
  seq_.client_seq_no = 0;
  connect(chain);
  for (auto &op: KV_OPS) {
    cmd_client_.push_back(op.type == block_op_type::accessor ? &tail_ : &head_);
  }
}

replica_chain_client::~replica_chain_client() {
  disconnect();
}

void replica_chain_client::disconnect() {
  head_.disconnect();
  tail_.disconnect();
}

const std::vector<std::string> &replica_chain_client::chain() const {
  return chain_;
}

void replica_chain_client::connect(const std::vector<std::string> &chain) {
  chain_ = chain;
  auto h = block_name_parser::parse(chain_.front());
  head_.connect(h.host, h.service_port, h.id);
  seq_.client_id = head_.get_client_id();
  if (chain.size() == 1) {
    tail_ = head_;
  } else {
    auto t = block_name_parser::parse(chain_.back());
    tail_.connect(t.host, t.service_port, t.id);
  }
  response_reader_ = tail_.get_command_response_reader(seq_.client_id);
}

std::string replica_chain_client::get(const std::string &key) {
  return run_command(kv_op_id::get, {key}).front();
}

std::string replica_chain_client::num_keys() {
  return run_command(kv_op_id::num_keys, {}).front();
}

std::string replica_chain_client::put(const std::string &key, const std::string &value) {
  return run_command(kv_op_id::put, {key, value}).front();
}

std::string replica_chain_client::remove(const std::string &key) {
  return run_command(kv_op_id::remove, {key}).front();
}

std::string replica_chain_client::update(const std::string &key, const std::string &value) {
  return run_command(kv_op_id::update, {key, value}).front();
}

std::vector<std::string> replica_chain_client::run_command(int32_t cmd_id, const std::vector<std::string> &args) {
  int64_t op_seq = seq_.client_seq_no;
  cmd_client_[cmd_id]->command_request(seq_, cmd_id, args);
  ++(seq_.client_seq_no);
  auto it = response_cache_.find(op_seq);
  if (it != response_cache_.end()) {
    auto res = *it;
    response_cache_.erase(it);
    return res.second;
  }
  while (true) {
    std::vector<std::string> ret;
    int64_t recv_seq = response_reader_.recv_response(ret);
    if (recv_seq == op_seq) {
      return ret;
    } else {
      response_cache_[op_seq] = ret;
    }
  }
}

}
}