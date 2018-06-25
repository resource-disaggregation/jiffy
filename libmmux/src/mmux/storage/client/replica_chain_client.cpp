#include "replica_chain_client.h"
#include "../manager/detail/block_name_parser.h"
#include "../kv/kv_block.h"

namespace mmux {
namespace storage {

replica_chain_client::replica_chain_client(const std::vector<std::string> &chain, int timeout_ms) : in_flight_(false) {
  seq_.client_id = -1;
  seq_.client_seq_no = 0;
  connect(chain, timeout_ms);
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

void replica_chain_client::connect(const std::vector<std::string> &chain, int timeout_ms) {
  chain_ = chain;
  auto h = block_name_parser::parse(chain_.front());
  head_.connect(h.host, h.service_port, h.id, timeout_ms);
  seq_.client_id = head_.get_client_id();
  if (chain.size() == 1) {
    tail_ = head_;
  } else {
    auto t = block_name_parser::parse(chain_.back());
    tail_.connect(t.host, t.service_port, t.id, timeout_ms);
  }
  response_reader_ = tail_.get_command_response_reader(seq_.client_id);
}

void replica_chain_client::send_lock() {
  send_command(&head_, {kv_op_id::lock}, {{}});
}

void replica_chain_client::send_unlock() {
  send_command(&head_, {kv_op_id::unlock}, {{}});
}

std::string replica_chain_client::get(const std::string &key) {
  return run_command(&tail_, {kv_op_id::get}, {{key}}).front().front();
}

std::string replica_chain_client::put(const std::string &key, const std::string &value) {
  return run_command(&head_, {kv_op_id::put}, {{key, value}}).front().front();
}

std::string replica_chain_client::remove(const std::string &key) {
  return run_command(&head_, {kv_op_id::remove}, {{key}}).front().front();
}

std::string replica_chain_client::update(const std::string &key, const std::string &value) {
  return run_command(&head_, {kv_op_id::update}, {{key, value}}).front().front();
}

void replica_chain_client::lock() {
  run_command(&head_, {kv_op_id::lock}, {{}});
}

void replica_chain_client::unlock() {
  run_command(&head_, {kv_op_id::unlock}, {{}});
}

std::string replica_chain_client::redirected_get(const std::string &key) {
  return run_command(&tail_, {kv_op_id::get}, {{key, "!redirected"}}).front().front();
}

std::string replica_chain_client::redirected_put(const std::string &key, const std::string &value) {
  return run_command(&head_, {kv_op_id::put}, {{key, value, "!redirected"}}).front().front();
}

std::string replica_chain_client::redirected_remove(const std::string &key) {
  return run_command(&head_, {kv_op_id::remove}, {{key, "!redirected"}}).front().front();
}

std::string replica_chain_client::redirected_update(const std::string &key, const std::string &value) {
  return run_command(&head_, {kv_op_id::update}, {{key, value, "!redirected"}}).front().front();
}

void replica_chain_client::send_command(block_client *client,
                                        const std::vector<int32_t> &cmd_ids,
                                        const std::vector<std::vector<std::string>> &args) {
  if (in_flight_) {
    throw std::length_error("Cannot have more than one request in-flight");
  }
  client->command_request(seq_, cmd_ids, args);
  in_flight_ = true;
}

std::vector<std::vector<std::string>> replica_chain_client::recv_response() {
  std::vector<std::vector<std::string>> ret;
  int64_t rseq = response_reader_.recv_response(ret);
  if (rseq != seq_.client_seq_no) {
    throw std::logic_error("SEQ: Expected=" + std::to_string(seq_.client_seq_no) + " Received=" + std::to_string(rseq));
  }
  seq_.client_seq_no++;
  in_flight_ = false;
  return ret;
}

std::vector<std::vector<std::string>> replica_chain_client::run_command(block_client *client,
                                                                        const std::vector<int32_t> &cmd_id,
                                                                        const std::vector<std::vector<std::string>> &args) {
  send_command(client, cmd_id, args);
  return recv_response();
}

std::string replica_chain_client::lget(const std::string &key) {
  return run_command(&tail_, {kv_op_id::locked_get}, {{key}}).front().front();
}

std::string replica_chain_client::lput(const std::string &key, const std::string &value) {
  return run_command(&head_, {kv_op_id::locked_put}, {{key, value}}).front().front();
}

std::string replica_chain_client::lremove(const std::string &key) {
  return run_command(&head_, {kv_op_id::locked_remove}, {{key}}).front().front();
}

std::string replica_chain_client::lupdate(const std::string &key, const std::string &value) {
  return run_command(&head_, {kv_op_id::locked_update}, {{key, value}}).front().front();
}

std::vector<std::string> replica_chain_client::lget(const std::vector<std::string> &keys) {
  return run_command(&tail_, {kv_op_id::locked_get}, {keys}).front();
}

std::vector<std::string> replica_chain_client::lput(const std::vector<std::string> &kvs) {
  return run_command(&head_, {kv_op_id::locked_put}, {kvs}).front();
}

std::vector<std::string> replica_chain_client::lremove(const std::vector<std::string> &keys) {
  return run_command(&head_, {kv_op_id::locked_remove}, {keys}).front();
}

std::vector<std::string> replica_chain_client::lupdate(const std::vector<std::string> &kvs) {
  return run_command(&head_, {kv_op_id::locked_update}, {kvs}).front();
}

std::vector<std::vector<std::string>> replica_chain_client::run_command(int32_t cmd_id,
                                                                        const std::vector<std::string> &args) {
  return run_command(cmd_client_[cmd_id], {cmd_id}, {args});
}

}
}