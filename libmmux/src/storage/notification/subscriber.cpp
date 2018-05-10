#include "subscriber.h"

#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <iostream>

namespace mmux {
namespace storage {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

subscriber::~subscriber() {
  if (transport_ != nullptr)
    disconnect();
  if (notification_worker_.joinable())
    notification_worker_.join();
}

subscriber::subscriber(const std::string &host, int port) {
  connect(host, port);
  auto handler = std::make_shared<notification_handler>(mailbox_, response_, response_cond_);
  auto processor = std::make_shared<subscription_serviceProcessor>(handler);
  notification_worker_ = std::thread([=] {
    while (true) {
      try {
        if (!processor->process(protocol_, protocol_, nullptr)) {
          break;
        }
      } catch (std::exception &e) {
        break;
      }
    }
  });
}

void subscriber::connect(const std::string &host, int port) {
  socket_ = std::make_shared<TSocket>(host, port);
  transport_ = std::shared_ptr<TTransport>(new TBufferedTransport(socket_));
  protocol_ = std::shared_ptr<TProtocol>(new TBinaryProtocol(transport_));
  client_ = std::make_shared<thrift_client>(protocol_);
  transport_->open();
}

void subscriber::disconnect() {
  if (transport_->isOpen()) {
    transport_->close();
  }
}

void subscriber::subscribe(int32_t block_id, const std::vector<std::string> &ops) {
  client_->subscribe(block_id, ops);
  std::unique_lock<std::mutex> response_lock{response_mtx_};
  response_cond_.wait(response_lock, [&] {
    return response_.load() != -1;
  });
  if (response_ != 0) {
    throw std::runtime_error("Subscription failed");
  }
  response_ = -1;
}

void subscriber::unsubscribe(int32_t block_id, const std::vector<std::string> &ops) {
  client_->unsubscribe(block_id, ops);
  std::unique_lock<std::mutex> response_lock{response_mtx_};
  response_cond_.wait(response_lock, [&] {
    return response_ != -1;
  });
  if (response_ != 0) {
    throw std::runtime_error("Subscription failed");
  }
  response_ = -1;
}

notification_handler::notification_handler(blocking_queue<std::pair<std::string, std::string>> &mailbox,
                                           std::atomic_int &response,
                                           std::condition_variable &response_cond)
    : mailbox_(mailbox), response_(response), response_cond_(response_cond) {}

void notification_handler::notification(const std::string &op, const std::string &data) {
  mailbox_.push(std::make_pair(op, data));
}

void notification_handler::success(const response_type, const std::vector<std::string> &) {
  response_ = 0;
  response_cond_.notify_all();
}

void notification_handler::error(const response_type, const std::string &) {
  response_ = 1;
  response_cond_.notify_all();
}

}
}