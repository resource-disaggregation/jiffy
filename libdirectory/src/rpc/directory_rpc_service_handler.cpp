#include "directory_rpc_service_handler.h"

namespace elasticmem {
namespace directory {

directory_rpc_service_handler::directory_rpc_service_handler(std::shared_ptr<directory_service_shard> shard)
    : shard_(std::move(shard)) {}

void directory_rpc_service_handler::create_directory(const std::string &path) {
  try {
    shard_->create_directory(path);
  } catch (directory_service_exception &e) {
    throw make_exception(e);
  }
}

void directory_rpc_service_handler::create_directories(const std::string &path) {
  try {
    shard_->create_directories(path);
  } catch (directory_service_exception &e) {
    throw make_exception(e);
  }
}

void directory_rpc_service_handler::create_file(const std::string &path) {
  try {
    shard_->create_file(path);
  } catch (directory_service_exception &e) {
    throw make_exception(e);
  }
}

bool directory_rpc_service_handler::exists(const std::string &path) {
  try {
    return shard_->exists(path);
  } catch (directory_service_exception &e) {
    throw make_exception(e);
  }
}

int64_t directory_rpc_service_handler::file_size(const std::string &path) {
  try {
    return static_cast<int64_t>(shard_->file_size(path));
  } catch (directory_service_exception &e) {
    throw make_exception(e);
  }
}
int64_t directory_rpc_service_handler::last_write_time(const std::string &path) {
  try {
    return static_cast<int64_t>(shard_->last_write_time(path));
  } catch (directory_service_exception &e) {
    throw make_exception(e);
  }
}
void directory_rpc_service_handler::set_permissions(const std::string &path,
                                                    rpc_perms prms,
                                                    rpc_perm_options opts) {
  try {
    shard_->permissions(path, perms((uint16_t) prms), (perm_options) opts);
  } catch (directory_service_exception &e) {
    throw make_exception(e);
  }
}

rpc_perms directory_rpc_service_handler::get_permissions(const std::string &path) {
  try {
    return static_cast<rpc_perms>(shard_->permissions(path)());
  } catch (directory_service_exception &e) {
    throw make_exception(e);
  }
}

void directory_rpc_service_handler::remove(const std::string &path) {
  try {
    shard_->remove(path);
  } catch (directory_service_exception &e) {
    throw make_exception(e);
  }
}

void directory_rpc_service_handler::remove_all(const std::string &path) {
  try {
    shard_->remove_all(path);
  } catch (directory_service_exception &e) {
    throw make_exception(e);
  }
}

void directory_rpc_service_handler::rename(const std::string &old_path, const std::string &new_path) {
  try {
    shard_->rename(old_path, new_path);
  } catch (directory_service_exception &e) {
    throw make_exception(e);
  }
}

void directory_rpc_service_handler::status(rpc_file_status &_return, const std::string &path) {
  try {
    auto s = shard_->status(path);
    _return.permissions = s.permissions()();
    _return.type = (rpc_file_type) s.type();
    _return.last_write_time = s.last_write_time();
  } catch (directory_service_exception &e) {
    throw make_exception(e);
  }
}

void directory_rpc_service_handler::directory_entries(std::vector<rpc_dir_entry> &_return, const std::string &path) {
  try {
    auto out = shard_->directory_entries(path);
    for (const auto &o: out) {
      rpc_dir_entry e;
      e.name = o.name();
      e.status.permissions = o.permissions()();
      e.status.type = (rpc_file_type) o.type();
      e.status.last_write_time = o.last_write_time();
      _return.push_back(e);
    }
  } catch (directory_service_exception &e) {
    throw make_exception(e);
  }
}

void directory_rpc_service_handler::recursive_directory_entries(std::vector<rpc_dir_entry> &_return,
                                                                const std::string &path) {
  try {
    auto out = shard_->recursive_directory_entries(path);
    for (const auto &o: out) {
      rpc_dir_entry e;
      e.name = o.name();
      e.status.permissions = o.permissions()();
      e.status.type = (rpc_file_type) o.type();
      e.status.last_write_time = o.last_write_time();
      _return.push_back(e);
    }
  } catch (directory_service_exception &e) {
    throw make_exception(e);
  }
}

void directory_rpc_service_handler::dstatus(rpc_data_status& _return, const std::string &path) {
  try {
    auto s = shard_->dstatus(path);
    _return.storage_mode = (rpc_storage_mode) s.mode();
    _return.data_nodes = s.data_nodes();
  } catch (directory_service_exception &e) {
    throw make_exception(e);
  }
}

rpc_storage_mode directory_rpc_service_handler::mode(const std::string &path) {
  try {
    return static_cast<rpc_storage_mode>(shard_->mode(path));
  } catch (directory_service_exception &e) {
    throw make_exception(e);
  }
}

void directory_rpc_service_handler::nodes(std::vector<std::string> &_return, const std::string &path) {
  try {
    _return = shard_->nodes(path);
  } catch (directory_service_exception &e) {
    throw make_exception(e);
  }
}

bool directory_rpc_service_handler::is_regular_file(const std::string &path) {
  try {
    return shard_->is_regular_file(path);
  } catch (directory_service_exception &e) {
    throw make_exception(e);
  }
}

bool directory_rpc_service_handler::is_directory(const std::string &path) {
  try {
    return shard_->is_directory(path);
  } catch (directory_service_exception &e) {
    throw make_exception(e);
  }
}

directory_rpc_service_exception directory_rpc_service_handler::make_exception(directory_service_exception &ex) const {
  directory_rpc_service_exception e;
  e.msg = ex.what();
  return e;
}

}
}
