#include "chain_response_client.h"

namespace jiffy {
namespace storage {

chain_response_client::chain_response_client(std::shared_ptr<::apache::thrift::protocol::TProtocol> prot) {
  reset_prot(std::move(prot));
}

void chain_response_client::reset_prot(std::shared_ptr<::apache::thrift::protocol::TProtocol> prot) {
  client_ = std::make_unique<jiffy::storage::chain_response_client::thrift_client>(prot);
}

bool chain_response_client::is_set() const {
  return client_ != nullptr;
}

void chain_response_client::ack(const sequence_id &seq) {
  client_->chain_ack(seq);
}

}
}
