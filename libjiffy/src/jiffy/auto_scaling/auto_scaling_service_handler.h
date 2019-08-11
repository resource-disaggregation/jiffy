#ifndef JIFFY_AUTO_SCALING_RPC_SERVICE_HANDLER_H
#define JIFFY_AUTO_SCALING_RPC_SERVICE_HANDLER_H

#include <jiffy/storage/client/replica_chain_client.h>
#include "auto_scaling_service.h"
#include "jiffy/storage/client/replica_chain_client.h"

namespace jiffy {
namespace auto_scaling {

/* Auto scaling service handler class */
class auto_scaling_service_handler : public auto_scaling_serviceIf {
 public:
  /**
   * @brief Constructor
   * @param directory_host Directory server host name
   * @param directory_port Directory server port number
   */
  explicit auto_scaling_service_handler(const std::string& directory_host, int directory_port);

  /**
   * @brief Auto scaling handling function
   * @param cur_chain Current replica chain
   * @param path Path
   * @param conf Configuration map
   */
  void auto_scaling(const std::vector<std::string> &cur_chain,
                    const std::string &path,
                    const std::map<std::string, std::string> &conf) override;

 private:
  /**
   * @brief Packs chain into a single string
   * @param chain Replica chain
   * @return Packed string
   */
  static std::string pack(const directory::replica_chain &chain);

  /**
   * @brief Make exceptions
   * @param e exception
   * @return Auto scaling exceptions
   */
  static auto_scaling_exception make_exception(std::exception &e);

  /**
   * @brief Make exceptions
   * @param msg Exception message
   * @return Auto scaling exceptions
   */
  static auto_scaling_exception make_exception(const std::string &msg);

  /**
   * @brief Find viable target partition for hash table merge
   * @param merge_target Target partition for hash table merge
   * @param fs Directory service client
   * @param path Path for hash table
   * @param storage_capacity Storage capacity for each partition
   * @param slot_beg Beginning of merge slot range
   * @param slot_end End of merge slot range
   * @return True if merge candidate is found, false otherwise
   */
  static bool find_merge_target(directory::replica_chain &merge_target,
                                const std::shared_ptr<directory::directory_client>& fs,
                                const std::string &path,
                                size_t storage_capacity,
                                int32_t slot_beg,
                                int32_t slot_end);

  /**
   * @brief Data transfer for hash table
   * @param src Source partition client
   * @param dst Destination partition client
   * @param slot_beg Beginning of slot range for data transfer
   * @param slot_end End of slot range for data transfer
   * @param batch_size Batch size for data transfer
   */
  static void hash_table_transfer_data(const std::shared_ptr<storage::replica_chain_client>& src,
                                       const std::shared_ptr<storage::replica_chain_client>& dst,
                                       size_t slot_beg,
                                       size_t slot_end,
                                       size_t batch_size = 2);
  /* Directory server host name */
  std::string directory_host_;
  /* Directory server port number */
  int directory_port_;
};

}
}

#endif //JIFFY_AUTO_SCALING_RPC_SERVICE_HANDLER_H
