#include "hash_table_client.h"
#include "jiffy/utils/string_utils.h"
#include "jiffy/storage/hashtable/hash_slot.h"
#include "jiffy/utils/logger.h"
#include "jiffy/utils/time_utils.h"
#include <thread>
#include <cmath>

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

hash_table_client::hash_table_client(std::shared_ptr<directory::directory_interface> fs,
                                     const std::string &path,
                                     const directory::data_status &status,
                                     connection_pool & pool,
                                     int timeout_ms)
    : data_structure_client(fs, path, status, pool, timeout_ms) {
  blocks_.clear();
  std::vector<std::string> block_init;
  for(auto & block: status.data_blocks()) {
    block_init.emplace_back(block.block_ids[0]);
  }
  pool_.init(block_init); // TODO we need to figure this out?
  for (auto &block: status.data_blocks()) {
    blocks_.emplace(std::make_pair(static_cast<int32_t>(std::stoi(utils::string_utils::split(block.name, '_')[0])),
                                   std::make_shared<pool_replica_chain_client>(fs_,
                                                                               pool_,
                                                                               path_,
                                                                               block,
                                                                               HT_OPS,
                                                                               timeout_ms_)));
    //std::make_shared<replica_chain_client>(fs_, path_, block, HT_OPS, timeout_ms_)));
  }
}

void hash_table_client::refresh() {
  LOG(log_level::info) << "Refreshing";
  auto start = time_utils::now_us();
  bool redo;
  do {
    status_ = fs_->dstatus(path_);
    blocks_.clear();
    redirect_blocks_.clear();
    try {
      for (auto &block: status_.data_blocks()) {
        if (block.metadata != "split_importing" && block.metadata != "importing") {
          LOG(log_level::info) << "Creating connections for !!!!" << block.to_string();
          blocks_.emplace(std::make_pair(static_cast<int32_t>(std::stoi(utils::string_utils::split(block.name,
                                                                                                   '_')[0])),
                                         std::make_shared<pool_replica_chain_client>(fs_,
                                                                                     pool_,
                                                                                     path_,
                                                                                     block,
                                                                                     HT_OPS,
                                                                                     timeout_ms_)));
//                                        std::make_shared<replica_chain_client>(fs_,
//                                                      path_,
//                                                      block,
//                                                      HT_OPS,
//                                                      timeout_ms_)));
        }
      }
      redo = false;
    } catch (std::exception &e) {
      LOG(log_level::info) << "Refreshing the connection due to: " << e.what();
      redo = true;
    }
  } while (redo);
  auto end = time_utils::now_us();
  LOG(log_level::info) << "Refresh taking time " << end - start << " " << start << " " << end;

}

void hash_table_client::put(const std::string &key, const std::string &value) {
  std::vector<std::string> _return;
  std::vector<std::string> args{"put", key, value};
  bool redo;
  do {
    try {
      _return = blocks_[block_id(key)]->run_command(args);
      LOG(log_level::info) << "Returning: " << _return.front();
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
  return _return[0];
}

std::string hash_table_client::upsert(const std::string &key, const std::string &value) {
  std::vector<std::string> _return;
  std::vector<std::string> args{"upsert", key, value};
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
  return _return[0];
}

bool hash_table_client::exists(const std::string &key) {
  std::vector<std::string> _return;
  std::vector<std::string> args{"exists", key};
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
  return _return[0] == "!ok";
}

std::size_t hash_table_client::block_id(const std::string &key) {
  return static_cast<size_t>((*std::prev(blocks_.upper_bound(hash_slot::get(key)))).first);
}

void hash_table_client::handle_redirect(std::vector<std::string> &_return, const std::vector<std::string> &args) {
  while (_return[0] == "!exporting") {
    auto args_copy = args;
    if (args[0] == "update" || args[0] == "upsert") {
      args_copy.emplace_back(_return[2]);
      args_copy.emplace_back(_return[3]);
    }

//    auto it = redirect_blocks_.find(_return[0] + _return[1]);
//    if (it == redirect_blocks_.end()) {
//      auto chain = directory::replica_chain(string_utils::split(_return[1], '!'));
//      auto client = std::make_shared<replica_chain_client>(fs_, path_, chain, HT_OPS, 0);
//      redirect_blocks_.emplace(std::make_pair(_return[0] + _return[1], client));
//      do {
//        _return = client->run_command_redirected(args_copy);
//      } while (_return[0] == "!redo");
//    } else {
//      do {
//        _return = it->second->run_command_redirected(args_copy);
//      } while (_return[0] == "!redo");
//    }

    auto chain = directory::replica_chain(string_utils::split(_return[1], '!'));
    LOG(log_level::info) << "Redirecting to block: " << chain.to_string();
    bool found = false;
    std::string redirected_id;
    std::shared_ptr<pool_replica_chain_client> redirected_client;
    for (const auto &clients: blocks_) { // FIXME storage mode only use default
      if (clients.second->chain().to_string() == chain.to_string()) {
        found = true;
        //redirected_id = utils::string_utils::split(chains.name, '_')[0];
        redirected_client = clients.second;
        //LOG(log_level::info) << "Redirecting to this block " << chain.to_string();
      }
    }
    if (found) {
      do {
        //LOG(log_level::info) << "The information of the redirected block: " << redirected_client->chain().to_string();
        _return = redirected_client->run_command_redirected(args_copy);
      } while (_return[0] == "!redo");
    } else {
      auto it = redirect_blocks_.find(_return[0] + _return[1]);
      if (it == redirect_blocks_.end()) {
        auto client = std::make_shared<pool_replica_chain_client>(fs_, pool_, path_, chain, HT_OPS, 0);
        redirect_blocks_.emplace(std::make_pair(_return[0] + _return[1], client));
        do {
          _return = client->run_command_redirected(args_copy);
        } while (_return[0] == "!redo");
      } else {
        do {
          _return = it->second->run_command_redirected(args_copy);
        } while (_return[0] == "!redo");

      }
    }
  }
  if (_return[0] == "!block_moved") {
    refresh();
    throw redo_error();
  }
  if (_return[0] == "!full") {
    std::this_thread::sleep_for(std::chrono::milliseconds((int) redo_times_));
    redo_times_++;
    throw redo_error();
  }
  if (_return[0] == "!redo") {
    throw redo_error();
  }
}

}
}
