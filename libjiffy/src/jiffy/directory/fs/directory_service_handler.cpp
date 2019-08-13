#include "directory_service_handler.h"
#include "directory_type_conversions.h"

namespace jiffy {
namespace directory {

directory_service_handler::directory_service_handler(std::shared_ptr<directory_tree> shard)
    : shard_(std::move(shard)) {}

void directory_service_handler::create_directory(const std::string &path) {
  try {
    shard_->create_directory(path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::create_directories(const std::string &path) {
  try {
    shard_->create_directories(path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::open(rpc_data_status &_return, const std::string &path) {
  try {
    _return = directory_type_conversions::to_rpc(shard_->open(path));
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::create(rpc_data_status &_return,
                                       const std::string &path,
                                       const std::string &type,
                                       const std::string &backing_path,
                                       int32_t num_blocks,
                                       int32_t chain_length,
                                       int32_t flags,
                                       int32_t permissions,
                                       const std::vector<std::string> &block_names,
                                       const std::vector<std::string> &block_metadata,
                                       const std::map<std::string, std::string> &tags) {
  try {
    _return = directory_type_conversions::to_rpc(shard_->create(path, type, backing_path, num_blocks, chain_length,
                                                                flags, permissions, block_names, block_metadata, tags));
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::open_or_create(rpc_data_status &_return,
                                               const std::string &path,
                                               const std::string &type,
                                               const std::string &backing_path,
                                               int32_t num_blocks,
                                               int32_t chain_length,
                                               int32_t flags,
                                               int32_t permissions,
                                               const std::vector<std::string> &block_names,
                                               const std::vector<std::string> &block_metadata,
                                               const std::map<std::string, std::string> &tags) {
  try {
    _return = directory_type_conversions::to_rpc(shard_->open_or_create(path, type, backing_path, num_blocks,
                                                                        chain_length, flags, permissions, block_names,
                                                                        block_metadata, tags));
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

bool directory_service_handler::exists(const std::string &path) {
  try {
    return shard_->exists(path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

int64_t directory_service_handler::last_write_time(const std::string &path) {
  try {
    return static_cast<int64_t>(shard_->last_write_time(path));
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::set_permissions(const std::string &path,
                                                rpc_perms prms,
                                                rpc_perm_options opts) {
  try {
    shard_->permissions(path, perms((uint16_t) prms), (perm_options) opts);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

rpc_perms directory_service_handler::get_permissions(const std::string &path) {
  try {
    return static_cast<rpc_perms>(shard_->permissions(path)());
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::remove(const std::string &path) {
  try {
    shard_->remove(path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::remove_all(const std::string &path) {
  try {
    shard_->remove_all(path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::sync(const std::string &path, const std::string &backing_path) {
  try {
    shard_->sync(path, backing_path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::dump(const std::string &path, const std::string &backing_path) {
  try {
    shard_->dump(path, backing_path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::load(const std::string &path, const std::string &backing_path) {
  try {
    shard_->load(path, backing_path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::rename(const std::string &old_path, const std::string &new_path) {
  try {
    shard_->rename(old_path, new_path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::status(rpc_file_status &_return, const std::string &path) {
  try {
    _return = directory_type_conversions::to_rpc(shard_->status(path));
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::directory_entries(std::vector<rpc_dir_entry> &_return, const std::string &path) {
  try {
    auto out = shard_->directory_entries(path);
    for (const auto &o: out)
      _return.push_back(directory_type_conversions::to_rpc(o));
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

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

void directory_service_handler::dstatus(rpc_data_status &_return, const std::string &path) {
  try {
    _return = directory_type_conversions::to_rpc(shard_->dstatus(path));
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::add_tags(const std::string &path, const std::map<std::string, std::string> &tags) {
  try {
    shard_->add_tags(path, tags);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

bool directory_service_handler::is_regular_file(const std::string &path) {
  try {
    return shard_->is_regular_file(path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

bool directory_service_handler::is_directory(const std::string &path) {
  try {
    return shard_->is_directory(path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::reslove_failures(rpc_replica_chain &_return,
                                                 const std::string &path,
                                                 const rpc_replica_chain &chain) {
  try {
    auto ret = shard_->resolve_failures(path, directory_type_conversions::from_rpc(chain));
    _return = directory_type_conversions::to_rpc(ret);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::add_replica_to_chain(rpc_replica_chain &_return, const std::string &path,
                                                     const rpc_replica_chain &chain) {
  try {
    auto ret = shard_->add_replica_to_chain(path, directory_type_conversions::from_rpc(chain));
    _return = directory_type_conversions::to_rpc(ret);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::add_data_block(rpc_replica_chain &_return,
                                               const std::string &path,
                                               const std::string &partition_name,
                                               const std::string &partition_metadata) {
  try {
    auto ret = shard_->add_block(path, partition_name, partition_metadata);
    _return = directory_type_conversions::to_rpc(ret);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::remove_data_block(const std::string &path, const std::string &partition_name) {
  try {
    shard_->remove_block(path, partition_name);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::request_partition_data_update(const std::string &path,
                                                              const std::string &old_partition_name,
                                                              const std::string &new_partition_name,
                                                              const std::string &partition_metadata) {
  try {
    shard_->update_partition(path, old_partition_name, new_partition_name, partition_metadata);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

int64_t directory_service_handler::get_storage_capacity(const std::string &path, const std::string &partition_name) {
  try {
    return shard_->get_capacity(path, partition_name);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

directory_service_exception directory_service_handler::make_exception(directory_ops_exception &ex) const {
  directory_service_exception e;
  e.msg = ex.what();
  return e;
}

}
}
