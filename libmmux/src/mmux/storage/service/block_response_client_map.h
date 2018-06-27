#ifndef MMUX_BLOCK_RESPONSE_CLIENT_MAP_H
#define MMUX_BLOCK_RESPONSE_CLIENT_MAP_H

#include <cstdint>
#include <libcuckoo/cuckoohash_map.hh>
#include "block_response_client.h"

namespace mmux {
namespace storage {

class block_response_client_map {
 public:
  block_response_client_map();

  void add_client(int64_t client_id, std::shared_ptr<block_response_client> client);

  void remove_client(int64_t client_id);

  void respond_client(const sequence_id &seq, const std::vector<std::string> &result);

  void clear();

 private:
  cuckoohash_map<int64_t, std::shared_ptr<block_response_client>> clients_;
};

}
}

#endif //MMUX_BLOCK_RESPONSE_CLIENT_MAP_H
