#include "block_response_client_map.h"
#include "../../utils/logger.h"

namespace jiffy {
namespace storage {

using namespace utils;

block_response_client_map::block_response_client_map() : clients_(0) {
}

void block_response_client_map::add_client(int64_t client_id, std::shared_ptr<block_response_client> client) {
  clients_.insert(client_id, client);
}

void block_response_client_map::remove_client(int64_t client_id) {
  clients_.erase(client_id);
}

void block_response_client_map::respond_client(const sequence_id &seq, const std::vector<std::string> &result) {
  if (seq.client_id == -1)
    return;
  bool found = clients_.update_fn(seq.client_id, [&](std::shared_ptr<block_response_client> &client) {
    client->response(seq, result);
  });
  if (!found)
    LOG(log_level::warn) << "Cannot respond to client since client id " << seq.client_id << " is not registered...";
}

void block_response_client_map::clear() {
  clients_.clear();
}

void block_response_client_map::send_failure() {
  sequence_id fail;
  fail.__set_client_id(-2);
  fail.__set_client_seq_no(-2);
  fail.__set_client_id(-2);
  for (const auto &x : clients_.lock_table()) {
    try {
      x.second->response(fail, {});
    } catch (std::exception &e) {
      continue;
    }
  }
}

}
}
