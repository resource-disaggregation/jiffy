#include "kv_client.h"
#include "../../utils/logger.h"
#include "../../utils/string_utils.h"
#include "../kv/hash_slot.h"

namespace mmux {
namespace storage {

using namespace mmux::utils;

kv_client::kv_client(std::shared_ptr<directory::directory_ops> fs,
                     const std::string &path,
                     const directory::data_status &status) : fs_(std::move(fs)), path_(path), status_(status) {
  slots_.clear();
  blocks_.clear();
  for (const auto &block: status.data_blocks()) {
    slots_.push_back(block.slot_begin());
    blocks_.push_back(std::make_shared<replica_chain_client>(block.block_names));
  }
}

void kv_client::refresh() {
  status_ = fs_->dstatus(path_);
  LOG(log_level::info) << "Refreshing block mappings to " << status_.to_string();
  slots_.clear();
  blocks_.clear();
  for (const auto &block: status_.data_blocks()) {
    slots_.push_back(block.slot_begin());
    blocks_.push_back(std::make_shared<replica_chain_client>(block.block_names));
  }
}

void kv_client::put(const std::string &key, const std::string &value) {
  try {
    parse_response(blocks_[block_id(key)]->put(key, value));
  } catch (redo_error &e) {
    parse_response(blocks_[block_id(key)]->put(key, value));
  } catch (redirect_error &e) {
    parse_response(replica_chain_client(e.blocks()).put(key, value));
  }
}

std::string kv_client::get(const std::string &key) {
  try {
    return parse_response(blocks_[block_id(key)]->get(key));
  } catch (redo_error &e) {
    return parse_response(blocks_[block_id(key)]->get(key));
  } catch (redirect_error &e) {
    return parse_response(replica_chain_client(e.blocks()).get(key));
  }
}

std::string kv_client::update(const std::string &key, const std::string &value) {
  try {
    return parse_response(blocks_[block_id(key)]->update(key, value));
  } catch (redo_error &e) {
    return parse_response(blocks_[block_id(key)]->update(key, value));
  } catch (redirect_error &e) {
    return parse_response(replica_chain_client(e.blocks()).update(key, value));
  }
}

std::string kv_client::remove(const std::string &key) {
  try {
    return parse_response(blocks_[block_id(key)]->remove(key));
  } catch (redo_error &e) {
    return parse_response(blocks_[block_id(key)]->remove(key));
  } catch (redirect_error &e) {
    return parse_response(replica_chain_client(e.blocks()).remove(key));
  }
}

size_t kv_client::block_id(const std::string &key) {
  return static_cast<size_t>(std::upper_bound(slots_.begin(), slots_.end(), hash_slot::get(key)) - slots_.begin() - 1);
}

std::string kv_client::parse_response(const std::string &raw_response) {
  // Actionable response
  if (raw_response[0] == '!') {
    if (raw_response == "!ok") {
      return "ok";
    } else if (raw_response == "!key_not_found") {
      throw std::out_of_range("No such key");
    } else if (raw_response == "!duplicate_key") {
      throw std::domain_error("Key already exists");
    } else if (raw_response == "!args_error") {
      throw std::invalid_argument("Incorrect arguments");
    } else if (raw_response == "!block_moved") {
      refresh();
      throw redo_error();
    } else if (raw_response.substr(0, 9) == "!exporting") {
      std::vector<std::string> parts = string_utils::split(raw_response, '!');
      throw redirect_error(std::vector<std::string>(parts.begin() + 1, parts.end()));
    }
  }
  return raw_response;
}

}
}
