#include "hash_table_client.h"
#include "jiffy/utils/logger.h"
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
    : data_structure_client(fs, path, status, KV_OPS, timeout_ms) {
  blocks_.clear();
  for (auto &block: status.data_blocks()) {
    blocks_.emplace(std::make_pair(static_cast<int32_t>(std::stoi(utils::string_utils::split(block.name, '_')[0])),
                                   std::make_shared<replica_chain_client>(fs_, path_, block, KV_OPS, timeout_ms_)));
  }
}

void hash_table_client::refresh() {
  status_ = fs_->dstatus(path_);
  blocks_.clear();
  for (auto &block: status_.data_blocks()) {
    if (block.metadata != "split_importing" && block.metadata != "importing") {
      blocks_.emplace(std::make_pair(static_cast<int32_t>(std::stoi(utils::string_utils::split(block.name, '_')[0])),
                                     std::make_shared<replica_chain_client>(fs_, path_, block, KV_OPS, timeout_ms_)));
    }
  }
}

std::string hash_table_client::put(const std::string &key, const std::string &value) {
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

std::vector<std::string> hash_table_client::put(std::vector<std::string> &kvs) {
  if (kvs.size() % 2 != 0) {
    throw std::invalid_argument("Incorrect number of arguments");
  }
  std::vector<std::string> _return;
  for (size_t i = 0; i < kvs.size(); i += 2) {
    _return.emplace_back(put(kvs[i], kvs[i + 1]));
  }
  return _return;
}

std::vector<std::string> hash_table_client::get(std::vector<std::string> &keys) {
  std::vector<std::string> _return;
  for (auto &key : keys) {
    _return.emplace_back(get(key));
  }
  return _return;
}

std::vector<std::string> hash_table_client::update(std::vector<std::string> &kvs) {
  if (kvs.size() % 2 != 0) {
    throw std::invalid_argument("Incorrect number of arguments");
  }
  std::vector<std::string> _return;
  for (size_t i = 0; i < kvs.size(); i += 2) {
    _return.emplace_back(update(kvs[i], kvs[i + 1]));
  }
  return _return;
}

std::vector<std::string> hash_table_client::remove(std::vector<std::string> &keys) {
  std::vector<std::string> _return;
  for (auto &key :keys) {
    _return.emplace_back(remove(key));
  }
  return _return;
}

std::size_t hash_table_client::num_keys() {
  for (auto &block : blocks_) {
    block.second->send_command(hash_table_cmd_id::ht_num_keys, {});
  }
  size_t n = 0;
  for (auto &block : blocks_) {
    n += std::stoll(block.second->recv_response().front());
  }
  return n;
}

std::size_t hash_table_client::block_id(const std::string &key) {
  return static_cast<size_t>((*std::prev(blocks_.upper_bound(hash_slot::get(key)))).first);
}

std::vector<std::string> hash_table_client::batch_command(const hash_table_cmd_id &op,
                                                          const std::vector<std::string> &args,
                                                          size_t args_per_op) {
  // Split arguments
  if (args.size() % args_per_op != 0)
    throw std::invalid_argument("Incorrect number of arguments");

  std::map<int32_t, std::vector<std::string>> block_args;
  std::map<int32_t, std::vector<size_t>> positions;
  size_t num_ops = args.size() / args_per_op;
  for (size_t i = 0; i < num_ops; i++) {
    auto id = block_id(args[i * args_per_op]);
    if (block_args.find(id) == block_args.end()) {
      block_args.emplace(std::make_pair(id, std::vector<std::string>{}));
    }
    if (positions.find(id) == positions.end()) {
      positions.emplace(std::make_pair(id, std::vector<size_t>{}));
    }
    for (size_t j = 0; j < args_per_op; j++)
      block_args[id].push_back(args[i * args_per_op + j]);
    positions[id].push_back(i);
  }

  for (auto &block: blocks_) {
    if (!block_args[block.first].empty())
      block.second->send_command(op, block_args[block.first]);
  }

  std::vector<std::string> results(num_ops);
  for (auto &block: blocks_) {
    if (!block_args[block.first].empty()) {
      auto res = block.second->recv_response();
      for (size_t j = 0; j < res.size(); j++) {
        results[positions[block.first][j]] = res[j];
      }
    }
  }

  return results;
}

void hash_table_client::handle_redirect(int32_t cmd_id, const std::vector<std::string> &args, std::string &response) {
  if (response.substr(0, 10) == "!exporting") {
    typedef std::vector<std::string> list_t;
    do {
      auto parts = string_utils::split(response, '!');
      auto chain = list_t(parts.begin() + 2, parts.end());
      response = replica_chain_client(fs_,
                                      path_,
                                      directory::replica_chain(chain),
                                      KV_OPS,
                                      0).run_command_redirected(cmd_id, args).front();
    } while (response.substr(0, 10) == "!exporting");
  }
  if (response == "!block_moved") {
    refresh();
    throw redo_error();
  }
  if (response == "!full") {
    std::this_thread::sleep_for(std::chrono::milliseconds((int) (std::pow(2, redo_times))));
    redo_times++;
    throw redo_error();
  }
}

void hash_table_client::handle_redirects(int32_t cmd_id,
                                         std::vector<std::string> &args,
                                         std::vector<std::string> &responses) {
  size_t n_ops = responses.size();
  size_t n_op_args = args.size() / n_ops;
  std::vector<std::string> redo_args;
  bool refresh_flag = false;
  bool redo_flag = false;
  for (size_t i = 0; i < responses.size(); i++) {
    auto &response = responses[i];
    if (response.substr(0, 10) == "!exporting") {
      typedef std::vector<std::string> list_t;
      list_t op_args(args.begin() + i * n_op_args, args.begin() + (i + 1) * n_op_args);
      do {
        auto parts = string_utils::split(response, '!');
        auto chain = list_t(parts.begin() + 2, parts.end());
        response = replica_chain_client(fs_,
                                        path_,
                                        directory::replica_chain(chain),
                                        KV_OPS,
                                        0).run_command_redirected(cmd_id, op_args).front();
      } while (response.substr(0, 10) == "!exporting");
    }
    if (response == "!block_moved") {
      refresh_flag = true;
      redo_flag = true;
      for (size_t j = 0; j < n_op_args; j++)
        redo_args.push_back(args[i * n_op_args + j]);
    }
    if (response == "!full") {
      redo_flag = true;
      for (size_t j = 0; j < n_op_args; j++)
        redo_args.push_back(args[i * n_op_args + j]);
    }
  }
  if (redo_flag) {
    if (refresh_flag) {
      refresh();
    }
    args = redo_args;
    throw redo_error();
  }
}

}
}