#include "block_chain_client.h"
#include "../manager/detail/block_name_parser.h"

namespace elasticmem {
namespace storage {

block_chain_client::block_chain_client(const std::vector<std::string> &chain) {
  seq_.client_id = -1;
  seq_.client_seq_no = 0;
  connect(chain);
  cmd_client_ = {&tail_, &head_, &head_, &head_};
}

block_chain_client::~block_chain_client() {
  disconnect();
}

void block_chain_client::disconnect() {
  head_.disconnect();
  tail_.disconnect();
  if (response_processor_.joinable())
    response_processor_.join();
}

const std::vector<std::string> &block_chain_client::chain() const {
  return chain_;
}

void block_chain_client::connect(const std::vector<std::string> &chain) {
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
  response_processor_ = tail_.add_response_listener(seq_.client_id, promises_);
}

std::future<std::string> block_chain_client::get(const std::string &key) {
  return run_command(0, {key});
}

std::future<std::string> block_chain_client::put(const std::string &key, const std::string &value) {
  return run_command(1, {key, value});
}

std::future<std::string> block_chain_client::remove(const std::string &key) {
  return run_command(2, {key});
}

std::future<std::string> block_chain_client::update(const std::string &key, const std::string &value) {
  return run_command(3, {key, value});
}

std::future<std::string> block_chain_client::run_command(int32_t cmd_id, const std::vector<std::string> &args) {
  int64_t op_seq = seq_.client_seq_no;
  auto event = std::make_shared<std::promise<std::string>>();
  promises_.insert(op_seq, event);
  cmd_client_[cmd_id]->command_request(seq_, cmd_id, args);
  ++(seq_.client_seq_no);
  return event->get_future();
}

}
}