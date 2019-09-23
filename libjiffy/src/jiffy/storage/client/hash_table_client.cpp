#include "hash_table_client.h"
#include "jiffy/utils/string_utils.h"
#include "jiffy/storage/hashtable/hash_slot.h"
#include "jiffy/utils/logger.h"
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
  redirect_bool = false;
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
  bool tmp;
  do {
    try {
        auto start = time_utils::now_us();
      _return = blocks_[block_id(key)]->run_command(args);
        auto end_1 = time_utils::now_us();
      handle_redirect(_return, args);
      tmp = redirect_bool;
      redirect_bool = false;
        auto end_2 = time_utils::now_us();
        //LOG(log_level::info) << "time: " << end_1 - start << " " << end_2 - end_1;
      redo = false;
      redo_times_ = 0;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  //THROW_IF_NOT_OK(_return);
  if(_return[0] == "!key_not_found")
    return "!ok!" + std::to_string(tmp);
  return _return[1] + "!" + std::to_string(tmp);
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
  if(_return[0] == "!exporting" && args[0] == "get") {
    redirect_bool = true;
  }
	while(_return[0] == "!block_moved" || _return[0] == "!exporting") {
	  bool redo_flag = false;
    if (_return[0] == "!block_moved") {
      try {
        if(_return.size() == 1) {
          redirect_bool = false;
          throw std::logic_error("do some refreshing");
        }
        auto slot_range = string_utils::split(_return[1], '_', 2);
        if (!std::stoi(_return[2])) {  // split
          auto it = redirect_blocks_.find(_return[3]);
          if (it != redirect_blocks_.end()) {
            blocks_.emplace(std::make_pair(std::stoi(slot_range[0]), it->second));
            redirect_blocks_.erase(it);
          } else {
            auto chain = directory::replica_chain(string_utils::split(_return[3], '!'));
            auto client = std::make_shared<replica_chain_client>(fs_, path_, chain, HT_OPS, 0);
            blocks_.emplace(std::make_pair(std::stoi(slot_range[0]), client));
          }
	        auto beg = _return.cbegin();
	        _return = std::vector<std::string>(beg + 5, beg + _return.size());
	        if(_return[0] == "!block_moved")
		        redo_flag = true;
        } else {
          auto it = blocks_.find(std::stoi(slot_range[0]));
          if (std::stoi(_return[4])) {
            if(it == blocks_.end())
              throw std::logic_error("do some refreshing");
            auto client = std::next(it)->second;
            blocks_.erase(std::next(it));
            blocks_.erase(it);
            blocks_.insert(std::make_pair(std::stoi(slot_range[0]), client));
          } else {
              blocks_.erase(it);
          }
	        redo_flag = true;
        }
      } catch (std::exception &e) {
        LOG(log_level::error) << "This refresh should never be called " << e.what();
        refresh();
        redirect_bool = false;
        throw redo_error();
      }
    	if(redo_flag) {
          redirect_bool = false;
          throw redo_error();
    	}
    }
  while (_return[0] == "!exporting") {
    auto it = redirect_blocks_.find(_return[1]);
    if (it == redirect_blocks_.end()) {
      auto merge_it = blocks_.find(std::stoi(_return[2]));
      if (merge_it == blocks_.end()) {
        auto chain = directory::replica_chain(string_utils::split(_return[1], '!'));
        auto client = std::make_shared<replica_chain_client>(fs_, path_, chain, HT_OPS, 0);
        redirect_blocks_.emplace(std::make_pair(_return[1], client));
        _return = client->run_command_redirected(args);
      } else {
        if(std::stoi(_return[3]))
          _return = std::next(merge_it)->second->run_command_redirected(args);
        else
          _return = std::prev(merge_it)->second->run_command_redirected(args);
      }
    } else {
      _return = it->second->run_command_redirected(args);
    }
  }
}
  if (_return[0] == "!full") {
    std::this_thread::sleep_for(std::chrono::milliseconds((int) redo_times_));
    redo_times_++;
    redirect_bool = false;
    throw redo_error();
  }
}

}
}
