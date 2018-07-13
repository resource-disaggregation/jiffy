#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include "heartbeat_sender.h"
#include "../../utils/logger.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

namespace mmux {
namespace storage {

using namespace utils;

heartbeat_sender::heartbeat_sender(const std::string sender_endpoint,
                                   const std::string dir_host,
                                   int heartbeat_port,
                                   uint64_t heartbeat_period_ms)
    : heartbeat_period_(heartbeat_period_ms) {
  hb_.sender = sender_endpoint;
  socket_ = std::make_shared<TSocket>(dir_host, heartbeat_port);
  transport_ = std::shared_ptr<TTransport>(new TBufferedTransport(socket_));
  protocol_ = std::shared_ptr<TProtocol>(new TBinaryProtocol(transport_));
  client_ = std::make_shared<directory::heartbeat_serviceClient>(protocol_);
  transport_->open();
}

heartbeat_sender::~heartbeat_sender() {
  if (transport_ != nullptr && transport_->isOpen()) {
    transport_->close();
  }
  stop_.store(true);
  if (worker_.joinable())
    worker_.join();
}

void heartbeat_sender::start() {
  worker_ = std::move(std::thread([&] {
    while (!stop_.load()) {
      LOG(log_level::trace) << "Looking for expired leases...";
      auto start = std::chrono::steady_clock::now();
      try {
        hb_.timestamp = std::time(nullptr);
        client_->ping(hb_);
      } catch (std::exception &e) {
        LOG(error) << "Exception: " << e.what();
      }
      auto end = std::chrono::steady_clock::now();
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

      auto time_to_wait = std::chrono::duration_cast<std::chrono::milliseconds>(heartbeat_period_ - elapsed);
      if (time_to_wait > std::chrono::milliseconds::zero()) {
        std::this_thread::sleep_for(time_to_wait);
      }
    }
  }));
}

void heartbeat_sender::stop() {
  stop_.store(true);
}

}
}
