#include "btree_client.h"
#include "jiffy/utils/logger.h"
#include "jiffy/utils/string_utils.h"
#include <thread>

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

btree_client::btree_client(std::shared_ptr<directory::directory_interface> fs,
                           const std::string &path,
                           const directory::data_status &status,
                           int timeout_ms)
    : data_structure_client(fs, path, status, BTREE_OPS, timeout_ms) {
  slots_.clear();
  for (const auto &block: status.data_blocks()) {
    slots_.push_back(utils::string_utils::split(block.name, '_')[0]);
    blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, BTREE_OPS, timeout_ms_));
  }
}

void btree_client::refresh() {
  status_ = fs_->dstatus(path_);
  //LOG(log_level::info) << "Refreshing partition mappings";
  slots_.clear();
  blocks_.clear();
  for (const auto &block: status_.data_blocks()) {
    slots_.push_back(utils::string_utils::split(block.name, '_')[0]);
    blocks_.push_back(std::make_shared<replica_chain_client>(fs_, path_, block, BTREE_OPS, timeout_ms_));
  }
}

std::string btree_client::put(const std::string &key, const std::string &value) {
  std::string _return;
  std::vector<std::string> args{key, value};
  bool redo;
  do {
    try {
      _return = blocks_[block_id(key)]->run_command(btree_cmd_id::bt_put, args).front();
      handle_redirect(btree_cmd_id::bt_put, args, _return);
      redo = false;
      redo_times = 0;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::string btree_client::get(const std::string &key) {
  std::string _return;
  std::vector<std::string> args{key};
  //auto start1 =  std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  //std::cout << start1 << " " << key << std::endl;
  bool redo;
  do {
    try {
      //auto start2 =  std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
      _return = blocks_[block_id(key)]->run_command(btree_cmd_id::bt_get, args).front();
      //auto stop2 =  std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
      //LOG(log_level::info) << "run command:: " << stop2 - start2;
      //if(stop2 - start2 > 20)
      //  std::cout << "1 " << stop2 - start2 << std::endl;
      //auto start3 =  std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
      handle_redirect(btree_cmd_id::bt_get, args, _return);
      //auto stop3 =  std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
      //LOG(log_level::info) << "handling redirect " << stop3 - start3;
      //if(stop3 - start3 > 20)
      //    std::cout << "2 " << stop3 - start3 << std::endl;
      redo = false;
      redo_times = 0;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  //auto stop1 =  std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  //LOG(log_level::info) << "Takes time for client:: " << stop1 - start1;
  //if(stop1 - start1 > 20)
  //std::cout << "3 " << stop1 - start1 << std::endl;
  return _return;
}

std::string btree_client::update(const std::string &key, const std::string &value) {
  std::string _return;
  std::vector<std::string> args{key, value};
  bool redo;
  do {
    try {
      _return = blocks_[block_id(key)]->run_command(btree_cmd_id::bt_update, args).front();
      handle_redirect(btree_cmd_id::bt_update, args, _return);
      redo = false;
      redo_times = 0;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::string btree_client::remove(const std::string &key) {
  std::string _return;
  std::vector<std::string> args{key};
  bool redo;
  do {
    try {
      _return = blocks_[block_id(key)]->run_command(btree_cmd_id::bt_remove, args).front();
      handle_redirect(btree_cmd_id::bt_remove, args, _return);
      redo = false;
      redo_times = 0;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::vector<std::string> btree_client::range_lookup(const std::string &begin_range,
                                                    const std::string &end_range) {
  std::vector<std::string> _return;
  std::vector<std::string> args{begin_range, end_range};
  bool redo;
  do {
    try {
      // TODO this block id may be complicated to find
      _return = blocks_[0]->run_command(btree_cmd_id::bt_range_lookup,
                                        args);// TODO this is a hot fix since we assume that there is only one replica chain currently
      handle_redirect(btree_cmd_id::bt_remove, args, _return.front());
      redo = false;
      redo_times = 0;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::string btree_client::range_count(const std::string &begin_range,
                                      const std::string &end_range) {
  std::string _return;
  std::vector<std::string> args{begin_range, end_range};
  bool redo;
  do {
    try {
      _return = blocks_[0]->run_command(btree_cmd_id::bt_range_count, args).front();
      handle_redirect(btree_cmd_id::bt_remove, args, _return);
      redo = false;
      redo_times = 0;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}
// TODO batch command should be fixed
std::vector<std::string> btree_client::put(std::vector<std::string> &kvs) {
  if (kvs.size() % 2 != 0) {
    throw std::invalid_argument("Incorrect number of arguments");
  }
  std::vector<std::string> _return;
  bool redo;
  do {
    try {
      _return = batch_command(btree_cmd_id::bt_put, kvs, 2);
      //handle_redirects(btree_cmd_id::bt_put, kvs, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::vector<std::string> btree_client::get(std::vector<std::string> &keys) {
  std::vector<std::string> _return;
  bool redo;
  do {
    try {
      _return = batch_command(btree_cmd_id::bt_get, keys, 1);
      //handle_redirects(btree_cmd_id::bt_get, keys, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::vector<std::string> btree_client::update(std::vector<std::string> &kvs) {
  if (kvs.size() % 2 != 0) {
    throw std::invalid_argument("Incorrect number of arguments");
  }
  std::vector<std::string> _return;
  bool redo;
  do {
    try {
      _return = batch_command(btree_cmd_id::bt_update, kvs, 2);
      //handle_redirects(btree_cmd_id::bt_update, kvs, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::vector<std::string> btree_client::remove(std::vector<std::string> &keys) {
  std::vector<std::string> _return;
  bool redo;
  do {
    try {
      _return = batch_command(btree_cmd_id::bt_remove, keys, 1);
      // handle_redirects(btree_cmd_id::bt_remove, keys, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::vector<std::string> btree_client::range_lookup(std::vector<std::string> args) {
  if (args.size() % 2 != 0) {
    throw std::invalid_argument("Incorrect number of arguments");
  }
  std::vector<std::string> _return;
  bool redo;
  do {
    try {
      _return = batch_command(btree_cmd_id::bt_range_lookup, args, 2);
      //  handle_redirects(btree_cmd_id::bt_range_lookup, keys, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::vector<std::string> btree_client::range_count(std::vector<std::string> args) {
  if (args.size() % 2 != 0) {
    throw std::invalid_argument("Incorrect number of arguments");
  }
  std::vector<std::string> _return;
  bool redo;
  do {
    try {
      _return = batch_command(btree_cmd_id::bt_range_count, args, 2);
      //  handle_redirects(btree_cmd_id::bt_range_lookup, keys, _return);
      redo = false;
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

//TODO fix this function
size_t btree_client::block_id(const std::string &key) {
  std::string max_value;
  size_t idx = 0;

  // TODO fix this for range functions
  for (auto x = slots_.begin(); x != slots_.end(); x++) {
    if (*x <= key && *x > max_value) {
      max_value = *x;
      idx = static_cast<size_t>(x - slots_.begin());
    }
  }
  return idx;
}

std::vector<std::string> btree_client::batch_command(const btree_cmd_id &op,
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

void btree_client::handle_redirect(int32_t cmd_id, const std::vector<std::string> &args, std::string &response) {
  if (response.substr(0, 10) == "!exporting") {
    typedef std::vector<std::string> list_t;
    do {
      auto parts = string_utils::split(response, '!');
      auto chain = list_t(parts.begin() + 2, parts.end());
      response = replica_chain_client(fs_,
                                      path_,
                                      directory::replica_chain(chain),
                                      BTREE_OPS,
                                      0).run_command_redirected(cmd_id, args).front();
    } while (response.substr(0, 10) == "!exporting");
  }
  if (response == "!block_moved") {
    refresh();
    throw redo_error();
  }
  if (response == "!full") {
    //LOG(log_level::info) << "putting the client to sleep to let auto_scaling run first for 2^" << redo_times << " milliseconds";
    std::this_thread::sleep_for(std::chrono::milliseconds((int) (std::pow(2, redo_times))));
    redo_times++;
    throw redo_error();
  }
}

void btree_client::handle_redirects(int32_t cmd_id,
                                    std::vector<std::string> &args,
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
                                        BTREE_OPS,
                                        0).run_command_redirected(cmd_id, op_args).front();
      } while (response.substr(0, 10) == "!exporting");
    }
    if (response == "!block_moved") {
      refresh();
      throw redo_error();
    }
  }
}

}
}
