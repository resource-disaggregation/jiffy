#include "storage_management_service_handler.h"
#include "jiffy/utils/logger.h"

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

storage_management_service_handler::storage_management_service_handler(std::vector<std::shared_ptr<block>> &blocks)
    : blocks_(blocks) {}

void storage_management_service_handler::create_partition(int32_t block_id,
                                                          const std::string &type,
                                                          const std::string &name,
                                                          const std::string &metadata,
                                                          const std::map<std::string, std::string> &conf,
                                                          const int32_t block_seq_no) {
  try {
    throw std::logic_error("Not supposed to be called");
    block_seq_no_check(block_id, block_seq_no);
    if(block_seq_no > blocks_.at(static_cast<std::size_t>(block_id))->impl()->seq_no()) {
      auto old_path = blocks_.at(static_cast<std::size_t>(block_id))->impl()->path();
      // LOG(log_level::info) << "Syncing block: " << block_id << " to " << "local://home/midhul/jiffy_dump" << old_path;
      blocks_.at(static_cast<std::size_t>(block_id))->impl()->sync("local://home/midhul/jiffy_dump" + old_path);
      LOG(log_level::info) << "Updating block seq no, block: " << block_id << " " << block_seq_no;
    }
    blocks_.at(static_cast<std::size_t>(block_id))->setup(type, name, metadata, utils::property_map(conf));
    blocks_.at(static_cast<std::size_t>(block_id))->impl()->update_seq_no(block_seq_no);
  } catch (std::exception &e) {
    LOG(log_level::info) << "Caught exception: " << e.what();
    throw make_exception(e);
  }
}

void storage_management_service_handler::setup_chain(int32_t block_id,
                                                     const std::string &path,
                                                     const std::vector<std::string> &chain,
                                                     int32_t chain_role,
                                                     const std::string &next_block_name,
                                                     const int32_t block_seq_no) {
  try {
    throw std::logic_error("Not supposed to be called");
    block_seq_no_check(block_id, block_seq_no);
    blocks_.at(static_cast<std::size_t>(block_id))->impl()->setup(path, chain,
                                                                  static_cast<storage::chain_role>(chain_role),
                                                                  next_block_name);
  } catch (std::exception &e) {
    throw make_exception(e);
  }
}

void storage_management_service_handler::destroy_partition(int32_t block_id, const int32_t block_seq_no) {
  try {
    throw std::logic_error("Not supposed to be called");
    // block_seq_no_check(block_id, block_seq_no);
    auto blk = blocks_.at(static_cast<std::size_t>(block_id));
    auto partition = blk->impl();
    if(block_seq_no < partition->seq_no()) {
      LOG(log_level::info) << "Ignoring partition destory on stale seq no, block: " << block_id;
      return;
    }
    blocks_.at(static_cast<std::size_t>(block_id))->destroy();
  } catch (std::exception &e) {
    throw make_exception(e);
  }
}

void storage_management_service_handler::get_path(std::string &_return, const int32_t block_id, const int32_t block_seq_no) {
  try {
    block_seq_no_check(block_id, block_seq_no);
    _return = blocks_.at(static_cast<std::size_t>(block_id))->impl()->path();
  } catch (std::exception &e) {
    throw make_exception(e);
  }
}

void storage_management_service_handler::dump(int32_t block_id, const std::string &backing_path, const int32_t block_seq_no) {
  try {
    block_seq_no_check(block_id, block_seq_no);
    blocks_.at(static_cast<std::size_t>(block_id))->impl()->dump(backing_path);
  } catch (std::exception &e) {
    throw make_exception(e);
  }
}

void storage_management_service_handler::sync(int32_t block_id, const std::string &backing_path, const int32_t block_seq_no) {
  try {
    block_seq_no_check(block_id, block_seq_no);
    blocks_.at(static_cast<std::size_t>(block_id))->impl()->sync(backing_path);
  } catch (std::exception &e) {
    throw make_exception(e);
  }
}

void storage_management_service_handler::load(int32_t block_id, const std::string &backing_path, const int32_t block_seq_no) {
  try {
    block_seq_no_check(block_id, block_seq_no);
    blocks_.at(static_cast<std::size_t>(block_id))->impl()->load(backing_path);
  } catch (std::exception &e) {
    throw make_exception(e);
  }
}

int64_t storage_management_service_handler::storage_capacity(int32_t block_id, const int32_t block_seq_no) {
  try {
    block_seq_no_check(block_id, block_seq_no);
    return static_cast<int64_t>(blocks_.at(static_cast<std::size_t>(block_id))->capacity());
  } catch (std::exception &e) {
    throw make_exception(e);
  }
}

int64_t storage_management_service_handler::storage_size(int32_t block_id, const int32_t block_seq_no) {
  try {
    block_seq_no_check(block_id, block_seq_no);
    return static_cast<int64_t>(blocks_.at(static_cast<std::size_t>(block_id))->impl()->storage_size());
  } catch (std::exception &e) {
    throw make_exception(e);
  }
}

void storage_management_service_handler::resend_pending(const int32_t block_id, const int32_t block_seq_no) {
  try {
    block_seq_no_check(block_id, block_seq_no);
    blocks_.at(static_cast<std::size_t>(block_id))->impl()->resend_pending();
  } catch (std::exception &e) {
    throw make_exception(e);
  }
}

void storage_management_service_handler::forward_all(const int32_t block_id, const int32_t block_seq_no) {
  try {
    block_seq_no_check(block_id, block_seq_no);
    blocks_.at(static_cast<std::size_t>(block_id))->impl()->forward_all();
  } catch (std::exception &e) {
    throw make_exception(e);
  }
}

void storage_management_service_handler::update_partition_data(const int32_t block_id,
                                                               const std::string &partition_name,
                                                               const std::string &partition_metadata,
                                                               const int32_t block_seq_no) {
  try {
    block_seq_no_check(block_id, block_seq_no);
    blocks_.at(static_cast<std::size_t>(block_id))->impl()->set_name_and_metadata(partition_name, partition_metadata);
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

void storage_management_service_handler::block_seq_no_check(int32_t block_id, const int32_t block_seq_no) {
  auto blk = blocks_.at(static_cast<std::size_t>(block_id));
  auto partition = blk->impl();
    if(block_seq_no < partition->seq_no()) {
      throw std::runtime_error("Stale block sequence number");
    }
}

}
}
