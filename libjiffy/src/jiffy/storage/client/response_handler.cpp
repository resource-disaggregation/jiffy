#include <iostream>
#include "response_handler.h"

namespace jiffy {
namespace storage {

response_handler::response_handler(mailbox_t &responses)
    : responses_(responses) {}

void response_handler::notification(const std::string &op, const std::string &data) {
  throw std::logic_error("response handler cannot handle notification.");
}

void response_handler::control(response_type type, const std::vector<std::string> &ops, const std::string &error) {
  throw std::logic_error("response handler cannot handle control messages.");
}

void response_handler::response(const sequence_id &, const std::vector<std::string> &) {
  throw std::logic_error("response handler cannot handle query responses.");
}

void response_handler::chain_ack(const sequence_id &) {
  throw std::logic_error("response handler cannot handle chain acknowledgements.");
}

}
}
