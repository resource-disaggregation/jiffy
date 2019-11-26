#include <iostream>
#include "response_handler.h"
#include "jiffy/utils/logger.h"

namespace jiffy {
namespace storage {

using namespace utils;

response_handler::response_handler(std::vector<std::shared_ptr<mailbox_t>> &responses)
    : responses_(responses) {}

void response_handler::notification(const std::string &, const std::string &) {
  throw std::logic_error("response handler cannot handle notification.");
}

void response_handler::control(response_type, const std::vector<std::string> &, const std::string &) {
  throw std::logic_error("response handler cannot handle control messages.");
}

void response_handler::response(const sequence_id &seq, const std::vector<std::string> &response) {
  LOG(log_level::info) << "Response handler doing some stuff";
  responses_[seq.client_id]->push(response);
}

void response_handler::chain_ack(const sequence_id &) {
  throw std::logic_error("response handler cannot handle chain acknowledgements.");
}

}
}
