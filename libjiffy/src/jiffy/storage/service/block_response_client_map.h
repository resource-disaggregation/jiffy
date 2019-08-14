#ifndef JIFFY_BLOCK_RESPONSE_CLIENT_MAP_H
#define JIFFY_BLOCK_RESPONSE_CLIENT_MAP_H

#include <cstdint>
#include <libcuckoo/cuckoohash_map.hh>
#include "block_response_client.h"

namespace jiffy {
namespace storage {
/* Block response client map class
 * This map records all the client that sent request to the partition.
 * It maps data from client identifier to partition response client
 */
class block_response_client_map {
 public:

  /**
   * @brief Constructor
   */

  block_response_client_map();

  /**
   * @brief Add a client to the map
   * @param client_id Client identifier
   * @param client Block response client
   */

  void add_client(int64_t client_id, std::shared_ptr<block_response_client> client);

  /**
   * @brief Remove a client from the map
   * @param client_id Client identifier
   */

  void remove_client(int64_t client_id);

  /**
   * @brief Respond to the client
   * @param seq Request sequence identifier
   * @param result Request result
   */

  void respond_client(const sequence_id &seq, const std::vector<std::string> &result);

  /**
   * @brief Clear the map
   */

  void clear();

  /**
   * @brief Send failing request when block is deleted
   */
  void send_failure();

 private:
  /* Response client map */
  cuckoohash_map<int64_t, std::shared_ptr<block_response_client>> clients_;
};

}
}

#endif //JIFFY_BLOCK_RESPONSE_CLIENT_MAP_H
