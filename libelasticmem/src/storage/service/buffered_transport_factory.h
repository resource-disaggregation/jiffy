#ifndef ELASTICMEM_BUFFERED_TRANSPORT_H
#define ELASTICMEM_BUFFERED_TRANSPORT_H

#include <thrift/transport/TTransport.h>
#include <thrift/transport/TBufferTransports.h>

namespace elasticmem {
namespace storage {

class BufferedTransportFactory : public apache::thrift::transport::TTransportFactory {
 public:
  explicit BufferedTransportFactory(uint32_t buffer_size) : buffer_size_(buffer_size) {}

  virtual ~BufferedTransportFactory() {}

  /**
   * Wraps the transport into a buffered one.
   */
  std::shared_ptr<apache::thrift::transport::TTransport> getTransport(std::shared_ptr<apache::thrift::transport::TTransport> trans) override {
    return std::shared_ptr<apache::thrift::transport::TTransport>(new apache::thrift::transport::TBufferedTransport(
        trans,
        buffer_size_));
  }

 private:
  uint32_t buffer_size_;
};
}
}

#endif //ELASTICMEM_BUFFERED_TRANSPORT_H
