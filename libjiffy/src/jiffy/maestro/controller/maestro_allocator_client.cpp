#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include "maestro_allocator_client.h"

namespace jiffy {
  namespace maestro {

    using namespace ::apache::thrift;
    using namespace ::apache::thrift::protocol;
    using namespace ::apache::thrift::transport;

    maestro_allocator_client::~maestro_allocator_client() {
      if (transport_ != nullptr)
        disconnect();
    }

    maestro_allocator_client::maestro_allocator_client(const std::string &hostname, int port) {
      connect(hostname, port);
    }

    void maestro_allocator_client::connect(const std::string &hostname, int port) {
      socket_ = std::make_shared<TSocket>(hostname, port);
      transport_ = std::shared_ptr<TTransport>(new TBufferedTransport(socket_));
      protocol_ = std::shared_ptr<TProtocol>(new TBinaryProtocol(transport_));
      client_ = std::make_shared<thrift_client>(protocol_);
      transport_->open();
    }

    void maestro_allocator_client::disconnect() {
      if (transport_->isOpen()) {
        transport_->close();
      }
    }

    std::vector<std::string> maestro_allocator_client::allocate(const std::string &tenantID, int64_t numBlocks) {
      std::vector<std::string> blocks;
      client_->allocate(blocks, tenantID, numBlocks);
      return blocks;
    }

    void maestro_allocator_client::deallocate(const std::string &tenentID, const std::vector<std::string> &blocks) {
      client_->deallocate(tenentID, blocks);
    }

  }
}