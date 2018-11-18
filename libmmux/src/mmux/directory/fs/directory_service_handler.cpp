#include "directory_service_handler.h"
#include "directory_type_conversions.h"

namespace mmux {
namespace directory {

/**
 * @brief Construction function of directory_service_handler class
 * @param shard server's directory tree
 */

directory_service_handler::directory_service_handler(std::shared_ptr<directory_tree> shard)
    : shard_(std::move(shard)) {}

/**
 * @brief Create directory
 * @param path directory path
 */

void directory_service_handler::create_directory(const std::string &path) {
  try {
    shard_->create_directory(path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Create multiple directories
 * @param path directory paths
 */

void directory_service_handler::create_directories(const std::string &path) {
  try {
    shard_->create_directories(path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Open a file given file path
 * @param _return rpc data status to be collected
 * @param path file path
 */

void directory_service_handler::open(rpc_data_status &_return, const std::string &path) {
  try {
    _return = directory_type_conversions::to_rpc(shard_->open(path));
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Create file
 * @param _return rpc data status to be collected
 * @param path file path
 * @param backing_path backing path
 * @param num_blocks number of blocks
 * @param chain_length replication chain length
 * @param flags flag arguments
 * @param permissions file permission set
 * @param tags tag arguments
 */

void directory_service_handler::create(rpc_data_status &_return,
                                       const std::string &path,
                                       const std::string &backing_path,
                                       int32_t num_blocks,
                                       int32_t chain_length,
                                       int32_t flags,
                                       int32_t permissions,
                                       const std::map<std::string, std::string> &tags) {
  try {
    _return = directory_type_conversions::to_rpc(shard_->create(path,
                                                                backing_path,
                                                                (size_t) num_blocks,
                                                                (size_t) chain_length,
                                                                flags,
                                                                permissions,
                                                                tags));
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Open or create a file
 * Open file if file exists
 * If not, create it
 * @param _return rpc data status to be collected
 * @param path file path
 * @param backing_path file backing path
 * @param num_blocks number of blocks
 * @param chain_length replication chain length
 * @param flags flag arguments
 * @param permissions file permission set
 * @param tags tag arguments
 */

void directory_service_handler::open_or_create(rpc_data_status &_return,
                                               const std::string &path,
                                               const std::string &backing_path,
                                               int32_t num_blocks,
                                               int32_t chain_length,
                                               int32_t flags,
                                               int32_t permissions,
                                               const std::map<std::string, std::string> &tags) {
  try {
    _return = directory_type_conversions::to_rpc(shard_->open_or_create(path,
                                                                        backing_path,
                                                                        (size_t) num_blocks,
                                                                        (size_t) chain_length,
                                                                        flags,
                                                                        permissions,
                                                                        tags));
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Check if the file exists
 * @param path file path
 * @return bool value
 */

bool directory_service_handler::exists(const std::string &path) {
  try {
    return shard_->exists(path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Fetch last write time of file
 * @param path file path
 * @return last write time
 */

int64_t directory_service_handler::last_write_time(const std::string &path) {
  try {
    return static_cast<int64_t>(shard_->last_write_time(path));
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Set permissions of a file
 * @param path file path
 * @param prms permission
 * @param opts permission options replace, add, remove
 */

void directory_service_handler::set_permissions(const std::string &path,
                                                rpc_perms prms,
                                                rpc_perm_options opts) {
  try {
    shard_->permissions(path, perms((uint16_t) prms), (perm_options) opts);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Fetch permission of a file
 * @param path file path
 * @return permission
 */

rpc_perms directory_service_handler::get_permissions(const std::string &path) {
  try {
    return static_cast<rpc_perms>(shard_->permissions(path)());
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Remove file
 * @param path file path
 */

void directory_service_handler::remove(const std::string &path) {
  try {
    shard_->remove(path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Remove all files under given directory
 * @param path directory path
 */

void directory_service_handler::remove_all(const std::string &path) {
  try {
    shard_->remove_all(path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Write all dirty blocks back to persistent storage
 * @param path file path
 * @param backing_path file backing path
 */

void directory_service_handler::sync(const std::string &path, const std::string &backing_path) {
  try {
    shard_->sync(path, backing_path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Write all dirty blocks back to persistent storage and clear the block
 * @param path file path
 * @param backing_path file backing path
 */

void directory_service_handler::dump(const std::string &path, const std::string &backing_path) {
  try {
    shard_->dump(path, backing_path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Load blocks from persistent storage
 * @param path
 * @param backing_path
 */

void directory_service_handler::load(const std::string &path, const std::string &backing_path) {
  try {
    shard_->load(path, backing_path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Rename a file
 * @param old_path original file path
 * @param new_path new file path
 */

void directory_service_handler::rename(const std::string &old_path, const std::string &new_path) {
  try {
    shard_->rename(old_path, new_path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Fetch file status
 * @param _return file status to be collected
 * @param path file path
 */

void directory_service_handler::status(rpc_file_status &_return, const std::string &path) {
  try {
    _return = directory_type_conversions::to_rpc(shard_->status(path));
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Collect all entries of files in the directory
 * @param _return rpc directory entries to be collected
 * @param path directory path
 */

void directory_service_handler::directory_entries(std::vector<rpc_dir_entry> &_return, const std::string &path) {
  try {
    auto out = shard_->directory_entries(path);
    for (const auto &o: out)
      _return.push_back(directory_type_conversions::to_rpc(o));
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Collect all entries of files in the directory recursively
 * @param _return rpc directory entries to be collected
 * @param path directory path
 */

void directory_service_handler::recursive_directory_entries(std::vector<rpc_dir_entry> &_return,
                                                            const std::string &path) {
  try {
    auto out = shard_->recursive_directory_entries(path);
    for (const auto &o: out)
      _return.push_back(directory_type_conversions::to_rpc(o));
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Collect data status
 * @param _return rpc data status to be collected
 * @param path file path
 */

void directory_service_handler::dstatus(rpc_data_status &_return, const std::string &path) {
  try {
    _return = directory_type_conversions::to_rpc(shard_->dstatus(path));
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Add tags to the file status
 * @param path file path
 * @param tags
 */

void directory_service_handler::add_tags(const std::string &path, const std::map<std::string, std::string> &tags) {
  try {
    shard_->add_tags(path, tags);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Check if path is regular file
 * @param path file path
 * @return bool value
 */

bool directory_service_handler::is_regular_file(const std::string &path) {
  try {
    return shard_->is_regular_file(path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Check if path is directory
 * @param path directory path
 * @return bool value
 */

bool directory_service_handler::is_directory(const std::string &path) {
  try {
    return shard_->is_directory(path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * TODO should be a spelling mistake here resolve : reslove
 * @brief Resolve failures using replication chain
 * @param _return rpc replication chain to be collected
 * @param path file path
 * @param chain replication chain
 */

void directory_service_handler::reslove_failures(rpc_replica_chain &_return,
                                                 const std::string &path,
                                                 const rpc_replica_chain &chain) {
  try {
    auto ret = shard_->resolve_failures(path, directory_type_conversions::from_rpc(chain));
    _return = directory_type_conversions::to_rpc(ret);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
    catch(directory_serviceClient.)
  }
}

/**
 * @brief Add a new replication to chain
 * @param _return replication chain status
 * @param path file path
 * @param chain replication chain
 */

void directory_service_handler::add_replica_to_chain(rpc_replica_chain &_return, const std::string &path,
                                                     const rpc_replica_chain &chain) {
  try {
    auto ret = shard_->add_replica_to_chain(path, directory_type_conversions::from_rpc(chain));
    _return = directory_type_conversions::to_rpc(ret);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Add block to file
 * @param path file path
 */

void directory_service_handler::add_block_to_file(const std::string &path) {
  try {
    shard_->add_block_to_file(path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Split block hash range
 * @param path file path
 * @param slot_begin split begin range
 * @param slot_end split end range
 */

void directory_service_handler::split_slot_range(const std::string &path,
                                                 const int32_t slot_begin,
                                                 const int32_t slot_end) {
  try {
    shard_->split_slot_range(path, slot_begin, slot_end);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Merge slot hash range
 * @param path file path
 * @param slot_begin merge begin range
 * @param slot_end merge end range
 */

void directory_service_handler::merge_slot_range(const std::string &path,
                                                 const int32_t slot_begin,
                                                 const int32_t slot_end) {
  try {
    shard_->merge_slot_range(path, slot_begin, slot_end);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

/**
 * @brief Make exceptions
 * @param ex exception
 * @return exception message
 */

directory_service_exception directory_service_handler::make_exception(directory_ops_exception &ex) const {
  directory_service_exception e;
  e.msg = ex.what();
  return e;
}

}
}
