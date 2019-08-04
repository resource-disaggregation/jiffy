#include "hash_table_client.h"
#include "jiffy/utils/logger.h"
#include "jiffy/utils/string_utils.h"
#include "jiffy/storage/hashtable/hash_slot.h"
#include <thread>
#include <cmath>
#include "jiffy/utils/time_utils.h"

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

hash_table_client::hash_table_client(std::shared_ptr<directory::directory_interface> fs,
                                     const std::string &path,
                                     const directory::data_status &status,
                                     int timeout_ms)
    : data_structure_client(fs, path, status, KV_OPS, timeout_ms) {
  blocks_.clear();
  LOG(log_level::info) << "Init hash table client with " << status.data_blocks().size();
  for (auto &block: status.data_blocks()) {
    blocks_.emplace(std::make_pair(static_cast<int32_t>(std::stoi(utils::string_utils::split(block.name, '_')[0])),
                                   std::make_shared<replica_chain_client>(fs_, path_, block, KV_OPS, timeout_ms_)));
  }
}

void hash_table_client::refresh() {
	auto start = time_utils::now_us();
  status_ = fs_->dstatus(path_);
	auto end = time_utils::now_us();
	LOG(log_level::info) << "Fetch blocks from directory server " << end - start;
  blocks_.clear();
  for (auto &block: status_.data_blocks()) {
    if (block.metadata != "importing" && block.metadata != "split_importing") {
      blocks_.emplace(std::make_pair(static_cast<int32_t>(std::stoi(utils::string_utils::split(block.name, '_')[0])),
                                     std::make_shared<replica_chain_client>(fs_, path_, block, KV_OPS, timeout_ms_)));
    }
  }
  redirect_blocks_.clear();
}

std::string hash_table_client::put(const std::string &key, const std::string &value) {
	    LOG(log_level::info) << "Put " << key << " " << time_utils::now_us();
  std::string _return;
  std::vector<std::string> args{key, value};
  bool redo;
  do {
    try {
      _return = blocks_[block_id(key)]->run_command(hash_table_cmd_id::ht_put, args).front();
      handle_redirect(hash_table_cmd_id::ht_put, args, _return);
      redo = false;
      redo_times = 0;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::string hash_table_client::get(const std::string &key) {
	    LOG(log_level::info) << "Get " << key << " " << time_utils::now_us();
  std::string _return;
  std::vector<std::string> args{key};
  bool redo;
  do {
    try {
      _return = blocks_[block_id(key)]->run_command(hash_table_cmd_id::ht_get, args).front();
      handle_redirect(hash_table_cmd_id::ht_get, args, _return);
      redo = false;
      redo_times = 0;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::string hash_table_client::update(const std::string &key, const std::string &value) {
  std::string _return;
  std::vector<std::string> args{key, value};
  bool redo;
  do {
    try {
      _return = blocks_[block_id(key)]->run_command(hash_table_cmd_id::ht_update, args).front();
      handle_redirect(hash_table_cmd_id::ht_update, args, _return);
      redo = false;
      redo_times = 0;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::string hash_table_client::remove(const std::string &key) {
  std::string _return;
  std::vector<std::string> args{key};
  bool redo;
  do {
    try {
      _return = blocks_[block_id(key)]->run_command(hash_table_cmd_id::ht_remove, args).front();
      handle_redirect(hash_table_cmd_id::ht_remove, args, _return);
      redo = false;
      redo_times = 0;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::size_t hash_table_client::block_id(const std::string &key) {
  return static_cast<size_t>((*std::prev(blocks_.upper_bound(hash_slot::get(key)))).first);
}

void hash_table_client::handle_redirect(int32_t cmd_id, const std::vector<std::string> &args, std::string &response) {
  if (response.substr(0, 10) == "!exporting") {
	 auto start = time_utils::now_us(); 
    typedef std::vector<std::string> list_t;
    do {
	    
	    auto it = redirect_blocks_.find(response);
	    if(it == redirect_blocks_.end()) {
		auto parts = string_utils::split(response, '!');
      		auto chain = list_t(parts.begin() + 2, parts.end());
      		LOG(log_level::info) << "Connecting replica_chain_client " << time_utils::now_us();
      auto client = std::make_shared<replica_chain_client>(fs_,
                                      path_,
                                      directory::replica_chain(chain),
                                      KV_OPS,
                                      0);
      redirect_blocks_.emplace(std::make_pair(response, client));
      response = client->run_command_redirected(cmd_id, args).front();
	    } else {
		    response = it->second->run_command_redirected(cmd_id, args).front();
	    }
	 auto end = time_utils::now_us(); 
	 LOG(log_level::info) << "Time spent on auto_scaling " << end - start << " " << end;
    } while (response.substr(0, 10) == "!exporting");
  }
  if (response == "!block_moved") {
	 auto start = time_utils::now_us(); 
    refresh();
	 auto end = time_utils::now_us(); 
	 LOG(log_level::info) << "Time spent on refreshing " << end - start;
    throw redo_error();
  }
  if (response == "!full") {
    //std::this_thread::sleep_for(std::chrono::milliseconds((int) (std::pow(2, redo_times))));
    //LOG(log_level::info) << "Redo for " << std::pow(2, redo_times) << cmd_id << " " << args.front();
    std::this_thread::sleep_for(std::chrono::milliseconds((int)redo_times));
    LOG(log_level::info) << "Redo for " << redo_times << cmd_id << " " << args.front();
    redo_times++;
    throw redo_error();
  }
}

}
}
