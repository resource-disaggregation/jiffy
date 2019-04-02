#include "auto_scaling_service_handler.h"
#include "jiffy/utils/logger.h"
#include "jiffy/directory/client/directory_client.h"
#include "jiffy/storage/msgqueue/msg_queue_ops.h"
#include "jiffy/utils/logger.h"
#include <thread>
namespace jiffy {
namespace auto_scaling {

using namespace jiffy::utils;

auto_scaling_service_handler::auto_scaling_service_handler(const std::string directory_host, int directory_port) : directory_host_(directory_host), directory_port_(directory_port) {}

void auto_scaling_service_handler::auto_scaling(const std::vector<std::string> & current_replica_chain, const std::string& path, const std::map<std::string, std::string> & conf) {
  std::string scaling_type = conf.find("type")->second;
  if(scaling_type == "msg_queue") {
    LOG(log_level::info) << "Into msg queue auto_scaling function";
    LOG(log_level::info) << directory_host_ << " " << directory_port_;
    std::string dst_partition_name = conf.find("next_partition_name")->second;
    auto fs = std::make_shared<directory::directory_client>(directory_host_, directory_port_);
    auto dst_replica_chain = fs->add_block(path, dst_partition_name, "regular");
    std::string next_target_string = "";
    for(const auto &block: dst_replica_chain.block_ids) {
      next_target_string += (block + "!");
    }
    next_target_string.pop_back();
    LOG(log_level::info) << "Next target" << next_target_string;
    LOG(log_level::info) << "See if it can get here 1";
    LOG(log_level::info) << "current thread 2" << std::this_thread::get_id();
    auto src = std::make_shared<storage::replica_chain_client>(fs, path, current_replica_chain, storage::MSG_QUEUE_OPS);
    LOG(log_level::info) << "See if it can get here 2";
    std::vector<std::string> args;
    args.emplace_back(next_target_string);
    //src->run_command(storage::msg_queue_cmd_id::mq_update_partition, args);
    LOG(log_level::info) << "Running command finishes";
  }
}

auto_scaling_exception auto_scaling_service_handler::make_exception(std::exception &e) {
  auto_scaling_exception ex;
  ex.msg = e.what();
  return ex;
}

auto_scaling_exception auto_scaling_service_handler::make_exception(const std::string &msg) {
  auto_scaling_exception ex;
  ex.msg = msg;
  return ex;
}

}
}
