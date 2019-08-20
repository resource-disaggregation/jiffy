#include "hash_table_client.h"
#include "jiffy/utils/string_utils.h"
#include "jiffy/storage/hashtable/hash_slot.h"
#include <thread>
#include <cmath>

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

hash_table_client::hash_table_client(std::shared_ptr<directory::directory_interface> fs,
                                     const std::string &path,
                                     const directory::data_status &status,
                                     int timeout_ms)
    : data_structure_client(fs, path, status, timeout_ms) {
  blocks_.clear();
  for (auto &block: status.data_blocks()) {
    blocks_.emplace(std::make_pair(static_cast<int32_t>(std::stoi(utils::string_utils::split(block.name, '_')[0])),
                                   std::make_shared<replica_chain_client>(fs_, path_, block, HT_OPS, timeout_ms_)));
  }
}

void hash_table_client::refresh() {
  status_ = fs_->dstatus(path_);
  blocks_.clear();
  for (auto &block: status_.data_blocks()) {
    if (block.metadata != "split_importing" && block.metadata != "importing") {
      blocks_.emplace(std::make_pair(static_cast<int32_t>(std::stoi(utils::string_utils::split(block.name, '_')[0])),
                                     std::make_shared<replica_chain_client>(fs_, path_, block, HT_OPS, timeout_ms_)));
    }
  }
  redirect_blocks_.clear();
}

void hash_table_client::put(const std::string &key, const std::string &value) {
  std::vector<std::string> _return;
  std::vector<std::string> args{"put", key, value};
  bool redo;
  do {
    try {
      _return = blocks_[block_id(key)]->run_command(args);
      handle_redirect(_return, args);
      redo = false;
      redo_times_ = 0;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  THROW_IF_NOT_OK(_return);
}

std::string hash_table_client::get(const std::string &key) {
  std::vector<std::string> _return;
  std::vector<std::string> args{"get", key};
  bool redo;
  do {
    try {
      _return = blocks_[block_id(key)]->run_command(args);
      handle_redirect(_return, args);
      redo = false;
      redo_times_ = 0;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  THROW_IF_NOT_OK(_return);
  return _return[1];
}

std::string hash_table_client::update(const std::string &key, const std::string &value) {
  std::vector<std::string> _return;
  std::vector<std::string> args{"update", key, value};
  bool redo;
  do {
    try {
      _return = blocks_[block_id(key)]->run_command(args);
      handle_redirect(_return, args);
      redo = false;
      redo_times_ = 0;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  THROW_IF_NOT_OK(_return);
  return _return[1];
}

std::string hash_table_client::remove(const std::string &key) {
  std::vector<std::string> _return;
  std::vector<std::string> args{"remove", key};
  bool redo;
  do {
    try {
      _return = blocks_[block_id(key)]->run_command(args);
      handle_redirect(_return, args);
      redo = false;
      redo_times_ = 0;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  THROW_IF_NOT_OK(_return);
  return _return[1];
}

std::size_t hash_table_client::block_id(const std::string &key) {
  return static_cast<size_t>((*std::prev(blocks_.upper_bound(hash_slot::get(key)))).first);
}

void hash_table_client::handle_redirect(std::vector<std::string> &_return, const std::vector<std::string> &args) {
  while (_return[0] == "!exporting") {
    auto it = redirect_blocks_.find(_return[0] + _return[1]);
    if (it == redirect_blocks_.end()) {
      auto chain = directory::replica_chain(string_utils::split(_return[1], '!'));
      auto client = std::make_shared<replica_chain_client>(fs_, path_, chain, HT_OPS, 0);
      redirect_blocks_.emplace(std::make_pair(_return[0] + _return[1], client_cache_slot(_return[2], std::stoi(_return[3]), std::stoi(_return[4]), client)));
      _return = client->run_command_redirected(args);
    } else {
      _return = it->second->run_command_redirected(args);
    }

  }
  if (_return[0] == "!block_moved") {
    auto hash = hash_slot::get(args[1]);
    for(const auto &x : redirect_blocks_) {
      auto slot_range = string_utils::split(x.second.slot_range_, '_', 2);
      if(hash >= std::stoi(slot_range[0]) && hash < std::stoi(slot_range[1])) {
        auto it = blocks_.find(std::stoi(slot_range[0]));
        if(it == blocks_.end() && !x.second.merging_) {
          blocks_.emplace(std::make_pair(std::stoi(slot_range[0]), x.second.client_));
        } else if(x.second.merging_) {
          if(x.second.merge_direction_) {
            auto node = *(std::next(it));
            blocks_.erase(std::next(it));
            blocks_.insert(std::make_pair(std::stoi(slot_range[0]), node.second));
          } 
          blocks_.erase(it);
        } else {
          continue;
        }
        throw redo_error();
      }
    }
    refresh();
    throw redo_error();
  }
  if (_return[0] == "!full") {
    std::this_thread::sleep_for(std::chrono::milliseconds((int) redo_times_));
    redo_times_++;
    throw redo_error();
  }
}

}
}