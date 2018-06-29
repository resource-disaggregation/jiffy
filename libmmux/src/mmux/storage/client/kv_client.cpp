#include "kv_client.h"
#include "../../utils/logger.h"
#include "../../utils/string_utils.h"
#include "../kv/hash_slot.h"

namespace mmux {
namespace storage {

using namespace mmux::utils;

kv_client::kv_client(std::shared_ptr<directory::directory_ops> fs,
                     const std::string &path,
                     const directory::data_status &status,
                     int timeout_ms)
    : fs_(std::move(fs)), path_(path), status_(status) {
  slots_.clear();
  blocks_.clear();
  for (const auto &block: status.data_blocks()) {
    slots_.push_back(block.slot_begin());
    blocks_.push_back(std::make_shared<replica_chain_client>(block.block_names, timeout_ms));
  }
}

directory::data_status &kv_client::status() {
  return status_;
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

std::string kv_client::put(const std::string &key, const std::string &value) {
  std::string _return;
  std::vector<std::string> args{key, value};
  bool redo = false;
  do {
    try {
      _return = blocks_[block_id(key)]->run_command(kv_op_id::put, args).front();
      handle_redirect(kv_op_id::put, args, _return);
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::string kv_client::get(const std::string &key) {
  std::string _return;
  std::vector<std::string> args{key};
  bool redo = false;
  do {
    try {
      _return = blocks_[block_id(key)]->run_command(kv_op_id::get, args).front();
      handle_redirect(kv_op_id::get, args, _return);
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::string kv_client::update(const std::string &key, const std::string &value) {
  std::string _return;
  std::vector<std::string> args{key, value};
  bool redo = false;
  do {
    try {
      _return = blocks_[block_id(key)]->run_command(kv_op_id::update, args).front();
      handle_redirect(kv_op_id::update, args, _return);
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::string kv_client::remove(const std::string &key) {
  std::string _return;
  std::vector<std::string> args{key};
  bool redo = false;
  do {
    try {
      _return = blocks_[block_id(key)]->run_command(kv_op_id::remove, args).front();
      handle_redirect(kv_op_id::remove, args, _return);
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::vector<std::string> kv_client::put(const std::vector<std::string> &kvs) {
  std::vector<std::string> _return;
  bool redo = false;
  do {
    try {
      _return = batch_command(kv_op_id::put, kvs, 2);
      handle_redirects(kv_op_id::put, kvs, _return);
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::vector<std::string> kv_client::get(const std::vector<std::string> &keys) {
  std::vector<std::string> _return;
  bool redo = false;
  do {
    try {
      _return = batch_command(kv_op_id::get, keys, 2);
      handle_redirects(kv_op_id::get, keys, _return);
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::vector<std::string> kv_client::update(const std::vector<std::string> &kvs) {
  std::vector<std::string> _return;
  bool redo = false;
  do {
    try {
      _return = batch_command(kv_op_id::update, kvs, 2);
      handle_redirects(kv_op_id::update, kvs, _return);
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

std::vector<std::string> kv_client::remove(const std::vector<std::string> &keys) {
  std::vector<std::string> _return;
  bool redo = false;
  do {
    try {
      _return = batch_command(kv_op_id::remove, keys, 2);
      handle_redirects(kv_op_id::remove, keys, _return);
    } catch (redo_error &e) {
      redo = true;
    }
  } while (redo);
  return _return;
}

size_t kv_client::block_id(const std::string &key) {
  return static_cast<size_t>(std::upper_bound(slots_.begin(), slots_.end(), hash_slot::get(key)) - slots_.begin() - 1);
}

std::vector<std::string> kv_client::batch_command(const kv_op_id &op,
                                                  const std::vector<std::string> &args,
                                                  size_t args_per_op) {
  // Split arguments
  if (args.size() % args_per_op != 0)
    throw std::invalid_argument("Incorrect number of arguments");

  std::vector<std::vector<std::string>> block_args(blocks_.size());
  std::vector<std::vector<size_t>> positions(blocks_.size());
  size_t num_ops = args.size() / args_per_op;
  for (size_t i = 0; i < num_ops; i++) {
    auto id = block_id(args[i * num_ops]);
    for (size_t j = 0; j < num_ops; j++)
      block_args[id].push_back(args[i * num_ops + j]);
    positions[id].push_back(i);
  }

  for (size_t i = 0; i < blocks_.size(); i++)
    blocks_[i]->send_command(op, block_args[i]);

  std::vector<std::string> results(num_ops);
  for (size_t i = 0; i < blocks_.size(); i++) {
    auto res = blocks_[i]->recv_response();
    for (size_t j = 0; j < res.size(); j++) {
      results[positions[i][j]] = res[j];
    }
  }

  return results;
}

void kv_client::handle_redirect(int32_t cmd_id, const std::vector<std::string> &args, std::string &response) {
  if (response.substr(0, 9) == "!exporting") {
    typedef std::vector<std::string> list_t;
    do {
      auto parts = string_utils::split(response, '!');
      auto chain = list_t(parts.begin() + 1, parts.end());
      response = replica_chain_client(std::move(chain)).run_command_redirected(cmd_id, args).front();
    } while (response.substr(0, 9) == "!exporting");
  }
  if (response == "!block_moved") {
    refresh();
    throw redo_error();
  }
}

void kv_client::handle_redirects(int32_t cmd_id,
                                 const std::vector<std::string> &args,
                                 std::vector<std::string> &responses) {
  size_t n_ops = responses.size();
  size_t n_op_args = args.size() / n_ops;
  for (size_t i = 0; i < responses.size(); i++) {
    auto &response = responses[i];
    if (response.substr(0, 9) == "!exporting") {
      typedef std::vector<std::string> list_t;
      list_t op_args(args.begin() + i * n_op_args, args.begin() + (i + 1) * n_op_args);
      do {
        auto parts = string_utils::split(response, '!');
        auto chain = list_t(parts.begin() + 1, parts.end());
        response = replica_chain_client(std::move(chain)).run_command_redirected(cmd_id, op_args).front();
      } while (response.substr(0, 9) == "!exporting");
    }
    if (response == "!block_moved") {
      refresh();
      throw redo_error();
    }
  }
}

kv_client::locked_client::locked_client(kv_client &parent) : parent_(parent) {
  blocks_.resize(parent_.blocks_.size());
  for (size_t i = 0; i < blocks_.size(); i++) {
    blocks_[i] = std::make_shared<block_t>(std::move(parent_.blocks_[i]->lock()));
  }
}

void kv_client::locked_client::unlock() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    blocks_[i]->unlock();
  }
}

std::string kv_client::locked_client::put(const std::string &key, const std::string &value) {
  std::vector<std::string> args{key, value};
  auto _return = blocks_[parent_.block_id(key)]->run_command(kv_op_id::locked_put, args).front();
  handle_redirect(kv_op_id::locked_put, args, _return);
  return _return;
}

std::string kv_client::locked_client::get(const std::string &key) {
  std::vector<std::string> args{key};
  auto _return = blocks_[parent_.block_id(key)]->run_command(kv_op_id::locked_get, args).front();
  handle_redirect(kv_op_id::locked_get, args, _return);
  return _return;
}

std::string kv_client::locked_client::update(const std::string &key, const std::string &value) {
  std::vector<std::string> args{key, value};
  auto _return = blocks_[parent_.block_id(key)]->run_command(kv_op_id::locked_update, args).front();
  handle_redirect(kv_op_id::locked_update, args, _return);
  return _return;
}

std::string kv_client::locked_client::remove(const std::string &key) {
  std::vector<std::string> args{key};
  auto _return = blocks_[parent_.block_id(key)]->run_command(kv_op_id::locked_remove, args).front();
  handle_redirect(kv_op_id::locked_remove, args, _return);
  return _return;
}

std::vector<std::string> kv_client::locked_client::put(const std::vector<std::string> &kvs) {
  auto _return = parent_.batch_command(kv_op_id::locked_put, kvs, 2);
  handle_redirects(kv_op_id::locked_put, kvs, _return);
  return _return;
}

std::vector<std::string> kv_client::locked_client::get(const std::vector<std::string> &keys) {
  auto _return = parent_.batch_command(kv_op_id::locked_get, keys, 2);
  handle_redirects(kv_op_id::locked_get, keys, _return);
  return _return;
}

std::vector<std::string> kv_client::locked_client::update(const std::vector<std::string> &kvs) {
  auto _return = parent_.batch_command(kv_op_id::locked_update, kvs, 2);
  handle_redirects(kv_op_id::locked_update, kvs, _return);
  return _return;
}

std::vector<std::string> kv_client::locked_client::remove(const std::vector<std::string> &keys) {
  auto _return = parent_.batch_command(kv_op_id::locked_remove, keys, 2);
  handle_redirects(kv_op_id::locked_remove, keys, _return);
  return _return;
}
void kv_client::locked_client::handle_redirect(int32_t cmd_id,
                                               const std::vector<std::string> &args,
                                               std::string &response) {
  if (response.substr(0, 9) == "!exporting") {
    typedef std::vector<std::string> list_t;
    do {
      auto parts = string_utils::split(response, '!');
      auto chain = list_t(parts.begin() + 1, parts.end());
      for (size_t i = 0; i < blocks_.size(); i++) {
        const auto &client_chain = parent_.blocks_[i]->chain();
        if (client_chain == chain) {
          response = blocks_[i]->run_command_redirected(cmd_id, args).front();
        }
      }
      response = replica_chain_client(std::move(chain)).run_command_redirected(cmd_id, args).front();
    } while (response.substr(0, 9) == "!exporting");
  }
  // There can be !block_moved response, since:
  // (1) No new exports can start while the kv is locked
  // (2) Ongoing exports cannot finish while the kv is locked
}
void kv_client::locked_client::handle_redirects(int32_t cmd_id,
                                                const std::vector<std::string> &args,
                                                std::vector<std::string> &responses) {
  size_t n_ops = responses.size();
  size_t n_op_args = args.size() / n_ops;
  for (size_t i = 0; i < responses.size(); i++) {
    auto &response = responses[i];
    if (response.substr(0, 9) == "!exporting") {
      typedef std::vector<std::string> list_t;
      list_t op_args(args.begin() + i * n_op_args, args.begin() + (i + 1) * n_op_args);
      do {
        auto parts = string_utils::split(response, '!');
        auto chain = list_t(parts.begin() + 1, parts.end());
        for (size_t j = 0; j < blocks_.size(); j++) {
          const auto &client_chain = parent_.blocks_[j]->chain();
          if (client_chain == chain) {
            response = blocks_[j]->run_command_redirected(cmd_id, args).front();
          }
        }
        response = replica_chain_client(std::move(chain)).run_command_redirected(cmd_id, op_args).front();
      } while (response.substr(0, 9) == "!exporting");
    }
  }
  // There can be !block_moved response, since:
  // (1) No new exports can start while the kv is locked
  // (2) Ongoing exports cannot finish while the kv is locked
}

}
}
