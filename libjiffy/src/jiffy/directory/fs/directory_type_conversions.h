#ifndef JIFFY_DIRECTORY_TYPE_CONVERSIONS_H
#define JIFFY_DIRECTORY_TYPE_CONVERSIONS_H

#include "directory_service_types.h"
#include "../directory_ops.h"

namespace jiffy {
namespace directory {
/**
 * Directory type conversion class
 * Aim to interact with RPC
 */
class directory_type_conversions {
 public:

  /**
   * @brief Convert replica chain to RPC
   * @param chain Replica chain
   * @return RPC replica chain
   */

  static rpc_replica_chain to_rpc(const replica_chain &chain) {
    rpc_replica_chain rpc;
    rpc.block_ids = chain.block_ids;
    rpc.name = chain.name;
    rpc.metadata = chain.metadata;
    rpc.storage_mode = (rpc_storage_mode) chain.mode;
    return rpc;
  }

  /**
   * @brief Convert RPC replica chain to replica chain
   * @param rpc RPC replica chain
   * @return Replica chain
   */

  static replica_chain from_rpc(const rpc_replica_chain &rpc) {
    replica_chain chain(rpc.block_ids, static_cast<storage_mode>(rpc.storage_mode));
    chain.name = rpc.name;
    chain.metadata = rpc.metadata;
    return chain;
  }

  /**
   * @brief Convert data status to RPC data status
   * @param status Data status
   * @return RPC data status
   */

  static rpc_data_status to_rpc(const data_status &status) {
    rpc_data_status rpc;
    rpc.type = status.type();
    rpc.backing_path = status.backing_path();
    rpc.chain_length = static_cast<int32_t>(status.chain_length());
    rpc.flags = status.flags();
    for (const auto &blk: status.data_blocks()) {
      rpc.data_blocks.push_back(to_rpc(blk));
    }
    rpc.tags = status.get_tags();
    return rpc;
  }

  /**
   * @brief Convert file status to RPC file status
   * @param status File status
   * @return RPC file status
   */

  static rpc_file_status to_rpc(const file_status &status) {
    rpc_file_status rpc;
    rpc.type = (rpc_file_type) status.type();
    rpc.last_write_time = status.last_write_time();
    rpc.permissions = status.permissions()();
    return rpc;
  }

  /**
   * @brief Convert directory entry to RPC directory entry
   * @param entry Directory entry
   * @return RPC directory entry
   */

  static rpc_dir_entry to_rpc(const directory_entry &entry) {
    rpc_dir_entry rpc;
    rpc.name = entry.name();
    rpc.status.permissions = entry.permissions()();
    rpc.status.type = (rpc_file_type) entry.type();
    rpc.status.last_write_time = entry.last_write_time();
    return rpc;
  }

  /**
   * @brief Convert RPC data status to data status
   * @param rpc RPC data status
   * @return Data status
   */

  static data_status from_rpc(const rpc_data_status &rpc) {
    std::vector<replica_chain> data_blocks;
    for (const auto &blk: rpc.data_blocks) {
      data_blocks.push_back(from_rpc(blk));
    }
    return data_status(rpc.type,
                       rpc.backing_path,
                       static_cast<size_t>(rpc.chain_length),
                       data_blocks,
                       rpc.flags,
                       rpc.tags);
  }

  /**
   * @brief Convert RPC file status to file status
   * @param rpc RPC file status
   * @return File status
   */

  static file_status from_rpc(const rpc_file_status &rpc) {
    return file_status(static_cast<file_type>(rpc.type),
                       perms(static_cast<uint16_t>(rpc.permissions)),
                       static_cast<uint64_t>(rpc.last_write_time));
  }

  /**
   * @brief Convert RPC directory entry to directory entry
   * @param rpc RPC directory entry
   * @return Directory entry
   */

  static directory_entry from_rpc(const rpc_dir_entry &rpc) {
    return directory_entry(rpc.name, from_rpc(rpc.status));
  }
};

}
}

#endif //JIFFY_DIRECTORY_TYPE_CONVERSIONS_H
