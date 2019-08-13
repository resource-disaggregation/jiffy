#ifndef JIFFY_SUBSCRIPTION_MAP_H
#define JIFFY_SUBSCRIPTION_MAP_H

#include <string>
#include <unordered_map>
#include <mutex>
#include "notification_response_client.h"

namespace jiffy {
namespace storage {

/* Subscription map class
 * This map records all the clients that are waiting for a specific operation
 *  on the partition. When the operation is done, the partition will send a notification
 *  in order to let the client get the right data at right time
 */
class subscription_map {
 public:
  /**
   * @brief Constructor
   */

  subscription_map();

  /**
   * @brief Add operations to subscription map
   * @param ops Operations
   * @param client Subscription service client
   */

  void add_subscriptions(const std::vector<std::string> &ops, const std::shared_ptr<notification_response_client>& client);

  /**
   * @brief Remove a subscription from subscription map
   * @param ops Operations
   * @param client Subscription service client
   * @param inform Bool value. true if informed
   */

  void remove_subscriptions(const std::vector<std::string> &ops,
                            const std::shared_ptr<notification_response_client>& client,
                            bool inform = true);

  /**
   * @brief Notify all the waiting clients of the operation
   * @param op Operation
   * @param msg Message to be sent to waiting clients
   */

  void notify(const std::string &op, const std::string &msg);

  /**
   * @brief Clear the subscription map
   */

  void clear();

  /**
   * @brief Send failure message to all clients to end their connections
   */
  void end_connections();

 private:
  /* Subscription map */
  std::map<std::string, std::set<std::shared_ptr<notification_response_client>>> subs_{};
};

}
}

#endif //JIFFY_SUBSCRIPTION_MAP_H
