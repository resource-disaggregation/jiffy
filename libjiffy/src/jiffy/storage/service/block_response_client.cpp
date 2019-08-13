#include "block_response_client.h"

#include <thrift/protocol/TBinaryProtocol.h>
#include <iostream>

using namespace ::apache::thrift::protocol;

namespace jiffy {
namespace storage {

block_response_client::block_response_client(std::shared_ptr<TProtocol> protocol)
    : client_(std::make_shared<thrift_client>(protocol)) {}

void block_response_client::response(const sequence_id &seq, const std::vector<std::string> &result) {
  client_->response(seq, result);
}

}
}
