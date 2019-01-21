#include "block_registration_service_handler.h"
#include "../../utils/logger.h"

namespace jiffy {
namespace directory {

using namespace utils;

block_registration_service_handler::block_registration_service_handler(std::shared_ptr<block_allocator> alloc)
    : alloc_(std::move(alloc)) {}

void block_registration_service_handler::add_blocks(const std::vector<std::string> &block_names) {
  try {
    LOG(log_level::info) << "Received advertisement for " << block_names.size() << " blocks";
    alloc_->add_blocks(block_names);
  } catch (std::out_of_range &e) {
    throw make_exception(e);
  }
}

void block_registration_service_handler::remove_blocks(const std::vector<std::string> &block_names) {
  try {
    LOG(log_level::info) << "Received retraction for " << block_names.size() << " blocks";
    alloc_->remove_blocks(block_names);
  } catch (std::out_of_range &e) {
    throw make_exception(e);
  }
}

block_registration_service_exception block_registration_service_handler::make_exception(const std::out_of_range &e) {
  block_registration_service_exception ex;
  ex.msg = e.what();
  return ex;
}

}
}
