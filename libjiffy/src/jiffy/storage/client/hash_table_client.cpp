#include "hash_table_client.h"
#include "jiffy/utils/logger.h"
#include "jiffy/utils/string_utils.h"
#include "jiffy/storage/hashtable/hash_slot.h"
#include <thread>
#include <cmath>

namespace jiffy {
namespace storage {

using namespace jiffy::utils;
std::size_t redo_times = 0;

hash_table_client::hash_table_client(std::shared_ptr<directory::directory_interface> fs,
                                     const std::string &path,
                                     const directory::data_status &status,
                                     int timeout_ms)
    : data_structure_client(fs, path, status, KV_OPS, timeout_ms) {
  slots_.clear();
  for (const auto &block: status.data_blocks()) {
    slots_.push_back(std::stoull(utils::string_utils::split(block.name, '_')[0]));
  }
}

void hash_table_client::refresh() {
  status_ = fs_->dstatus(path_);
  slots_.clear();
  blocks_.clear();
  for (const auto &block: status_.data_blocks()) {
    slots_.push_back(std::stoull(utils::string_utils::split(block.name, '_')[0]));
    blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, KV_OPS, timeout_ms_));
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
      redo_times++;
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
      //LOG(log_level::info) << "The return value is" << _return;
      handle_redirect(hash_table_cmd_id::ht_remove, args, _return);
      redo = false;
      redo_times = 0;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::vector<std::string> hash_table_client::put(const std::vector<std::string> &kvs) {
  if (kvs.size() % 2 != 0) {
    throw std::invalid_argument("Incorrect number of arguments");
  }
  std::vector<std::string> _return;
  bool redo;
  do {
    try {
      _return = batch_command(hash_table_cmd_id::ht_put, kvs, 2);
      handle_redirects(hash_table_cmd_id::ht_put, kvs, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::vector<std::string> hash_table_client::get(const std::vector<std::string> &keys) {
  std::vector<std::string> _return;
  bool redo;
  do {
    try {
      _return = batch_command(hash_table_cmd_id::ht_get, keys, 1);
      handle_redirects(hash_table_cmd_id::ht_get, keys, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::vector<std::string> hash_table_client::update(const std::vector<std::string> &kvs) {
  if (kvs.size() % 2 != 0) {
    throw std::invalid_argument("Incorrect number of arguments");
  }
  std::vector<std::string> _return;
  bool redo;
  do {
    try {
      _return = batch_command(hash_table_cmd_id::ht_update, kvs, 2);
      handle_redirects(hash_table_cmd_id::ht_update, kvs, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::vector<std::string> hash_table_client::remove(const std::vector<std::string> &keys) {
  std::vector<std::string> _return;
  bool redo;
  do {
    try {
      _return = batch_command(hash_table_cmd_id::ht_remove, keys, 1);
      handle_redirects(hash_table_cmd_id::ht_remove, keys, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::size_t hash_table_client::num_keys() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    blocks_[i]->send_command(hash_table_cmd_id::ht_num_keys, {});
  }
  size_t n = 0;
  for (size_t i = 0; i < blocks_.size(); i++) {
    n += std::stoll(blocks_[i]->recv_response().front());
  }
  return n;
}

size_t hash_table_client::block_id(const std::string &key) {
  auto hash = hash_slot::get(key);
  int max_value = -1;
  size_t idx;

  // TODO fix this
  for(auto x = slots_.begin(); x != slots_.end(); x++) {
    if(*x <= hash && *x > max_value) {
      max_value = *x;
      idx = static_cast<size_t>(x - slots_.begin());
    }
  }
  return idx;
 // return static_cast<size_t>(std::upper_bound(slots_.begin(), slots_.end(), hash_slot::get(key)) - slots_.begin() - 1);
}

std::vector<std::string> hash_table_client::batch_command(const hash_table_cmd_id &op,
                                                          const std::vector<std::string> &args,
                                                          size_t args_per_op) {
  // Split arguments
  if (args.size() % args_per_op != 0)
    throw std::invalid_argument("Incorrect number of arguments");

  std::vector<std::vector<std::string>> block_args(blocks_.size());
  std::vector<std::vector<size_t>> positions(blocks_.size());
  size_t num_ops = args.size() / args_per_op;
  for (size_t i = 0; i < num_ops; i++) {
    auto id = block_id(args[i * args_per_op]);
    for (size_t j = 0; j < args_per_op; j++)
      block_args[id].push_back(args[i * args_per_op + j]);
    positions[id].push_back(i);
  }

  for (size_t i = 0; i < blocks_.size(); i++) {
    if (!block_args[i].empty())
      blocks_[i]->send_command(op, block_args[i]);
  }

  std::vector<std::string> results(num_ops);
  for (size_t i = 0; i < blocks_.size(); i++) {
    if (!block_args[i].empty()) {
      auto res = blocks_[i]->recv_response();
      for (size_t j = 0; j < res.size(); j++) {
        results[positions[i][j]] = res[j];
      }
    }
  }

  return results;
}

void hash_table_client::handle_redirect(int32_t cmd_id, const std::vector<std::string> &args, std::string &response) {
  if (response.substr(0, 10) == "!exporting") {
    //LOG(log_level::info) << "exporting redirect";
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
    //LOG(log_level::info) << "block_moved, refreshing";
    refresh();
    throw redo_error();
  }
  if(response == "!full") {
    //LOG(log_level::info) << "putting the client to sleep to let auto_scaling run first for 2^" << redo_times << " milliseconds";
    std::this_thread::sleep_for(std::chrono::milliseconds((int)(std::pow(2, redo_times))));
    redo_times++;
    throw redo_error();
  }
}

void hash_table_client::handle_redirects(int32_t cmd_id,
                                         const std::vector<std::string> &args,
                                         std::vector<std::string> &responses) {
  size_t n_ops = responses.size();
  size_t n_op_args = args.size() / n_ops;
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
      refresh();
      throw redo_error();
    }
    if(response == "!full") {
      std::this_thread::sleep_for(std::chrono::milliseconds((int)(std::pow(2, redo_times))));
      throw redo_error();
    }
  }
}

}
}