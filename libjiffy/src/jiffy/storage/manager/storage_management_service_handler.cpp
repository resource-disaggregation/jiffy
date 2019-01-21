#include "storage_management_service_handler.h"

namespace jiffy {
namespace storage {

storage_management_service_handler::storage_management_service_handler(std::vector<std::shared_ptr<chain_module>> &blocks)
    : blocks_(blocks) {}

void storage_management_service_handler::setup_block(int32_t block_id,
                                                     const std::string &path,
                                                     const std::string &partition_type,
                                                     const std::string &partition_name,
                                                     const std::string &partition_metadata,
                                                     const std::vector<std::string> &chain,
                                                     int32_t chain_role,
                                                     const std::string &next_block_name) {
  try {
    // TODO: Allocate data structure of type partition_type
    blocks_.at(static_cast<std::size_t>(block_id))->setup(path,
                                                          partition_name,
                                                          partition_metadata,
                                                          chain,
                                                          static_cast<storage::chain_role>(chain_role),
                                                          next_block_name);
  } catch (std::exception &e) {
    throw make_exception(e);
  }
}

void storage_management_service_handler::get_path(std::string &_return, const int32_t block_id) {
  try {
    _return = blocks_.at(static_cast<std::size_t>(block_id))->path();
  } catch (std::exception &e) {
    throw make_exception(e);
  }
}

void storage_management_service_handler::dump(int32_t block_id, const std::string &backing_path) {
  try {
    blocks_.at(static_cast<std::size_t>(block_id))->dump(backing_path);
  } catch (std::exception &e) {
    throw make_exception(e);
  }
}

void storage_management_service_handler::sync(int32_t block_id, const std::string &backing_path) {
  try {
    blocks_.at(static_cast<std::size_t>(block_id))->sync(backing_path);
  } catch (std::exception &e) {
    throw make_exception(e);
  }
}

void storage_management_service_handler::load(int32_t block_id, const std::string &backing_path) {
  try {
    blocks_.at(static_cast<std::size_t>(block_id))->load(backing_path);
  } catch (std::exception &e) {
    throw make_exception(e);
  }
}

void storage_management_service_handler::reset(int32_t block_id) {
  try {
    blocks_.at(static_cast<std::size_t>(block_id))->reset();
  } catch (std::exception &e) {
    throw make_exception(e);
  }
}

int64_t storage_management_service_handler::storage_capacity(int32_t block_id) {
  try {
    return static_cast<int64_t>(blocks_.at(static_cast<std::size_t>(block_id))->storage_capacity());
  } catch (std::exception &e) {
    throw make_exception(e);
  }
}

int64_t storage_management_service_handler::storage_size(int32_t block_id) {
  try {
    return static_cast<int64_t>(blocks_.at(static_cast<std::size_t>(block_id))->storage_size());
  } catch (std::exception &e) {
    throw make_exception(e);
  }
}

void storage_management_service_handler::resend_pending(const int32_t block_id) {
  try {
    blocks_.at(static_cast<std::size_t>(block_id))->resend_pending();
  } catch (std::exception &e) {
    throw make_exception(e);
  }
}

void storage_management_service_handler::forward_all(const int32_t block_id) {
  try {
    blocks_.at(static_cast<std::size_t>(block_id))->forward_all();
  } catch (std::exception &e) {
    throw make_exception(e);
  }
}

storage_management_exception storage_management_service_handler::make_exception(std::exception &e) {
  storage_management_exception ex;
  ex.msg = e.what();
  return ex;
}

storage_management_exception storage_management_service_handler::make_exception(const std::string &msg) {
  storage_management_exception ex;
  ex.msg = msg;
  return ex;
}

}
}
