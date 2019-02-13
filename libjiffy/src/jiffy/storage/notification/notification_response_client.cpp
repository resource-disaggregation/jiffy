#include "notification_response_client.h"

namespace jiffy {
namespace storage {

notification_response_client::notification_response_client(std::shared_ptr<::apache::thrift::protocol::TProtocol> prot)
    : client_(prot) {}

void notification_response_client::notification(const std::string &op, const std::string &data) {
  client_.notification(op, data);
}

void notification_response_client::control(const response_type type,
                                           const std::vector<std::string> &ops,
                                           const std::string &error) {
  client_.control(type, ops, error);
}

}
}
