#include <thrift/transport/TTransportException.h>
#include "pool_replica_chain_client.h"
#include "jiffy/utils/string_utils.h"
#include "jiffy/utils/logger.h"
#include "jiffy/storage/command.h"
#include "jiffy/storage/manager/detail/block_id_parser.h"

namespace jiffy {
namespace storage {

using namespace utils;

pool_replica_chain_client::pool_replica_chain_client(std::shared_ptr<directory::directory_interface> fs,
                                           connection_pool & pool,
                                           const std::string &path,
                                           const directory::replica_chain &chain,
                                           const command_map &OPS,
                                           int timeout_ms) : fs_(fs), pool_(pool), path_(path), in_flight_(false), OPS_(OPS) {
  seq_.client_id = -1;
  seq_.client_seq_no = 0;
  accessor_ = false;
  send_run_command_exception_ = false;
  connect(chain, timeout_ms);
  for (auto &op: OPS) {
    cmd_client_[op.first] = op.second.is_accessor() ? &tail_ : &head_;
  }
}

pool_replica_chain_client::~pool_replica_chain_client() {
  disconnect();
}

void pool_replica_chain_client::disconnect() {
  pool_.release_connection(head_id_);
  if(head_id_ != tail_id_)
    pool_.release_connection(tail_id_);
}

const directory::replica_chain &pool_replica_chain_client::chain() const {
  return chain_;
}

void pool_replica_chain_client::connect(const directory::replica_chain &chain, int timeout_ms) {
  auto start = time_utils::now_us();
  chain_ = chain;
  timeout_ms_ = timeout_ms;
  auto h = block_id_parser::parse(chain_.block_ids.front());
  head_id_ = static_cast<std::size_t>(h.id);
  LOG(log_level::info) << "Head id: " << head_id_;
  head_ = pool_.request_connection(h);
  LOG(log_level::info) << "Hey 1";
  seq_.client_id = head_.client_id;
  LOG(log_level::info) << "Hey 2";
  if (chain_.block_ids.size() == 1) {
    LOG(log_level::info) << "Hey 3";
    tail_ = head_;
    LOG(log_level::info) << "Hey 4";
    tail_id_ = head_id_;
  } else {
    auto t = block_id_parser::parse(chain_.block_ids.back());
    tail_id_ = static_cast<std::size_t>(t.id);
    LOG(log_level::info) << "Tail id: " << tail_id_;
    tail_ = pool_.request_connection(t);
  }
  auto start1 = time_utils::now_us();
  LOG(log_level::info) << "Hey 5";
  //response_reader_ = tail_.connection->get_command_response_reader(seq_.client_id);
  response_reader_ = tail_.response_reader;
  auto end = time_utils::now_us();
  LOG(log_level::info) << "Connecting takes time: " << start1 - start;
  LOG(log_level::info) << "Fetching the command response reader takes time: " << end - start1;
  in_flight_ = false;
}

void pool_replica_chain_client::send_command(const std::vector<std::string> &args) {
  if (in_flight_) {
    throw std::length_error("Cannot have more than one request in-flight");
  }
  if (OPS_[args[0]].is_accessor()) {
    try {
      accessor_ = true;
      cmd_client_.at(args.front())->connection->send_run_command(std::stoi(string_utils::split(chain_.tail(), ':').back()), args);
    } catch (std::exception &e) {
      send_run_command_exception_ = true;
    }
  } else {
    cmd_client_.at(args.front())->connection->command_request(seq_, args);
  }
  in_flight_ = true;
}

std::vector<std::string> pool_replica_chain_client::recv_response() {
  std::vector<std::string> ret;
  int64_t rseq;
  if (accessor_) {
    if (send_run_command_exception_) {
      ret.emplace_back("!block_moved");
    } else {
      try {
        tail_.connection->recv_run_command(ret);
      } catch (std::exception &e) {
        if (!send_run_command_exception_) {
          ret.emplace_back("!block_moved");
        }
      }
    }
    send_run_command_exception_ = false;
  } else {
    rseq = response_reader_.recv_response(ret);
    if (rseq != seq_.client_seq_no) {
      throw std::logic_error(
          "SEQ: Expected=" + std::to_string(seq_.client_seq_no) + " Received=" + std::to_string(rseq));
    }
  }
  seq_.client_seq_no++;
  in_flight_ = false;
  accessor_ = false;
  return ret;
}

std::vector<std::string> pool_replica_chain_client::run_command(const std::vector<std::string> &args) {
  std::vector<std::string> response;
  bool retry = false;
  while (response.empty()) {
    try {
      send_command(args);
      response = recv_response();
      if (retry && response[0] == "!duplicate_key") { // TODO: This is hash table specific logic
        response[0] = "!ok";
      }
    } catch (apache::thrift::transport::TTransportException &e) {
      LOG(log_level::info) << "Error in connection to chain: " << e.what();
      LOG(log_level::info) << args.front() << " " << chain_.name;
      for (const auto &x : chain_.block_ids)
        LOG(log_level::info) << x;
      connect(fs_->resolve_failures(path_, chain_), timeout_ms_);
      retry = true;
    } catch (std::logic_error &e) { // TODO: This is very iffy, we need to fix this
      response.clear();
      response.emplace_back("!block_moved");
      break;
    }
  }
  return response;
}

std::vector<std::string> pool_replica_chain_client::run_command_redirected(const std::vector<std::string> &args) {
  auto args_copy = args;
  if (args_copy.back() != "!redirected")
    args_copy.emplace_back("!redirected");
  send_command(args_copy);
  return recv_response();
}

void pool_replica_chain_client::set_chain_name_metadata(std::string &name, std::string &metadata) {
  chain_.name = name;
  chain_.metadata = metadata;
}

bool pool_replica_chain_client::is_connected() const {
  return head_.connection->is_connected() && tail_.connection->is_connected();
}

}
}
