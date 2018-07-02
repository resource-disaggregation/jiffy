#include "directory_service_handler.h"
#include "directory_type_conversions.h"

namespace mmux {
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
                                       const std::string &backing_path,
                                       int32_t num_blocks,
                                       int32_t chain_length,
                                       int32_t flags) {
  try {
    _return = directory_type_conversions::to_rpc(shard_->create(path,
                                                                backing_path,
                                                                (size_t) num_blocks,
                                                                (size_t) chain_length,
                                                                flags));
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::open_or_create(rpc_data_status &_return,
                                               const std::string &path,
                                               const std::string &backing_path,
                                               int32_t num_blocks,
                                               int32_t chain_length,
                                               int32_t flags) {
  try {
    _return = directory_type_conversions::to_rpc(shard_->open_or_create(path,
                                                                        backing_path,
                                                                        (size_t) num_blocks,
                                                                        (size_t) chain_length,
                                                                        flags));
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

void directory_service_handler::add_block_to_file(const std::string &path) {
  try {
    shard_->add_block_to_file(path);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::split_slot_range(const std::string &path,
                                                 const int32_t slot_begin,
                                                 const int32_t slot_end) {
  try {
    shard_->split_slot_range(path, slot_begin, slot_end);
  } catch (directory_ops_exception &e) {
    throw make_exception(e);
  }
}

void directory_service_handler::merge_slot_range(const std::string &path,
                                                 const int32_t slot_begin,
                                                 const int32_t slot_end) {
  try {
    shard_->merge_slot_range(path, slot_begin, slot_end);
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
