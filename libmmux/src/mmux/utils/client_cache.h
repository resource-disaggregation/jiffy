#ifndef MMUX_CLIENT_CACHE_H
#define MMUX_CLIENT_CACHE_H

#include <iostream>
#include <string>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

namespace mmux {
namespace utils {

template<typename C>
class client_cache {
 public:
  typedef std::pair<std::string, int> key_type;
  typedef std::tuple<std::shared_ptr<apache::thrift::transport::TTransport>,
                     std::shared_ptr<apache::thrift::protocol::TProtocol>,
                     std::shared_ptr<C>> value_type;
  typedef std::map<key_type, value_type> cache_type;

  client_cache() {}

  ~client_cache() {
    for (auto const &entry: cache_) {
      if (std::get<0>(entry.second)->isOpen()) {
        std::get<0>(entry.second)->close();
      }
    }
  }

  value_type get(const std::string &host, int port) {
    using namespace apache::thrift;
    key_type key(host, port);
    typename cache_type::iterator it;
    if ((it = cache_.find(key)) != cache_.end()) {
      return it->second;
    }
    auto sock = std::make_shared<transport::TSocket>(host, port);
    auto transport = std::shared_ptr<transport::TTransport>(new transport::TBufferedTransport(sock));
    auto prot = std::shared_ptr<protocol::TProtocol>(new protocol::TBinaryProtocol(transport));
    auto client = std::make_shared<C>(prot);
    transport::TTransportException ex;
    size_t n_attempts = 3;
    while (n_attempts > 0) {
      try {
        transport->open();
        break;
      } catch (transport::TTransportException &e) {
        ex = e;
        n_attempts--;
        continue;
      }
    }
    if (n_attempts == 0)
      throw ex;
    cache_.insert(std::make_pair(key, std::make_tuple(transport, prot, client)));
    return cache_.at(key);
  }

 private:
  cache_type cache_;
};

}
}

#endif //MMUX_CLIENT_CACHE_H
