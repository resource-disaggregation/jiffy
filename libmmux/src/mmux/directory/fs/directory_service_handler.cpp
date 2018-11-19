#include "directory_service_handler.h"
#include "directory_type_conversions.h"

namespace mmux {
namespace directory {

/**
 * @brief Constructor
 * @param shard Server's directory tree
 */

directory_service_handler::directory_service_handler(std::shared_ptr<directory_tree> shard)
    : shard_(std::move(shard)) {}

/**
 * @brief Create directory
 * @param path Directory path
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
 * @param path Directory paths
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
 * @param _return Rpc data status to be collected
 * @param path File path
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
 * @param _return Rpc data status to be collected
 * @param path File path
 * @param backing_path Backing path
 * @param num_blocks Number of blocks
 * @param chain_length Replication chain length
 * @param flags Flag arguments
 * @param permissions File permission set
 * @param tags Tag arguments
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
 * @param _return Rpc data status to be collected
 * @param path File path
 * @param backing_path File backing path
 * @param num_blocks Number of blocks
 * @param chain_length Replication chain length
 * @param flags Flag arguments
 * @param permissions File permission set
 * @param tags Tag arguments
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
 * @param path File path
 * @return Bool value
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
 * @param path File path
 * @return Last write time
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
 * @param path File path
 * @param prms Permission
 * @param opts Permission options replace, add, remove
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
 * @param path File path
 * @return Permission
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
 * @param path File path
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
 * @param path Directory path
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
 * @param path File path
 * @param backing_path File backing path
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
 * @param path File path
 * @param backing_path File backing path
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
 * @param path File path
 * @param backing_path File backing path
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
 * @param old_path Original file path
 * @param new_path New file path
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
 * @param _return File status to be collected
 * @param path File path
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
 * @param _return Rpc directory entries to be collected
 * @param path Directory path
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
 * @param _return Rpc directory entries to be collected
 * @param path Directory path
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
 * @param _return Rpc data status to be collected
 * @param path File path
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
 * @param path File path
 * @param tags Tags
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
 * @param path File path
 * @return Bool value
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
 * @param path Directory path
 * @return Bool value
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
 * @param _return Rpc replication chain to be collected
 * @param path File path
 * @param chain Replication chain
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
 * @param _return Replication chain status
 * @param path File path
 * @param chain Replication chain
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
 * @param path File path
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
 * @param path File path
 * @param slot_begin Split begin range
 * @param slot_end Split end range
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
 * @param path File path
 * @param slot_begin Merge begin range
 * @param slot_end Merge end range
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
 * @param ex Exception
 * @return Exception message
 */

directory_service_exception directory_service_handler::make_exception(directory_ops_exception &ex) const {
  directory_service_exception e;
  e.msg = ex.what();
  return e;
}

}
}
