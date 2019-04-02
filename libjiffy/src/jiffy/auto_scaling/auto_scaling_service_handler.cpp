#include "auto_scaling_service_handler.h"
#include "jiffy/utils/logger.h"
#include "jiffy/directory/client/directory_client.h"
#include "jiffy/storage/msgqueue/msg_queue_ops.h"
#include "jiffy/storage/hashtable/hash_table_ops.h"
#include "jiffy/utils/logger.h"
#include <thread>
namespace jiffy {
namespace auto_scaling {

using namespace jiffy::utils;

auto_scaling_service_handler::auto_scaling_service_handler(const std::string directory_host, int directory_port)
    : directory_host_(directory_host), directory_port_(directory_port) {}

void auto_scaling_service_handler::auto_scaling(const std::vector<std::string> &current_replica_chain,
                                                const std::string &path,
                                                const std::map<std::string, std::string> &conf) {
  std::string scaling_type = conf.find("type")->second;
  auto fs = std::make_shared<directory::directory_client>(directory_host_, directory_port_);


  if (scaling_type == "msg_queue") {
    std::string dst_partition_name = conf.find("next_partition_name")->second;
    auto dst_replica_chain = fs->add_block(path, dst_partition_name, "regular");
    std::string next_target_string = "";
    for (const auto &block: dst_replica_chain.block_ids) {
      next_target_string += (block + "!");
    }
    next_target_string.pop_back();
    auto src = std::make_shared<storage::replica_chain_client>(fs, path, current_replica_chain, storage::MSG_QUEUE_OPS);
    std::vector<std::string> args;
    args.emplace_back(next_target_string);
    src->run_command(storage::msg_queue_cmd_id::mq_update_partition, args);



  } else if (scaling_type == "hash_table_split") {
    LOG(log_level::info) << "Into hash table split function";
    std::int32_t slot_range_begin = std::stoi(conf.find("slot_range_begin")->second);
    std::int32_t slot_range_end = std::stoi(conf.find("slot_range_end")->second);
    auto split_range_begin = (slot_range_begin + slot_range_end) / 2;
    auto split_range_end = slot_range_end;
    std::string dst_partition_name = std::to_string(split_range_begin) + "_" + std::to_string(split_range_end);
    std::string src_partition_name = std::to_string(slot_range_begin) + "_" + std::to_string(split_range_begin);
    auto dst_replica_chain = fs->add_block(path, dst_partition_name, "importing");
    std::string export_target_str_ = "";
    for (const auto &block: dst_replica_chain.block_ids) {
      export_target_str_ += (block + "!");
    }
    export_target_str_.pop_back();
    std::string current_name = std::to_string(slot_range_begin) + "_" + std::to_string(slot_range_end);
    std::vector<std::string> src_before_args;
    std::vector<std::string> dst_before_args;
    src_before_args.push_back(current_name);
    src_before_args.emplace_back("exporting$" + dst_partition_name + "$" + export_target_str_);
    dst_before_args.push_back(dst_partition_name);
    dst_before_args.emplace_back("importing$" + dst_partition_name);
    std::vector<std::string> src_after_args;
    std::vector<std::string> dst_after_args;
    src_after_args.push_back(src_partition_name);
    src_after_args.emplace_back("regular");
    dst_after_args.push_back(dst_partition_name);
    dst_after_args.emplace_back("regular");
    auto src = std::make_shared<storage::replica_chain_client>(fs, path, current_replica_chain, storage::KV_OPS);
    src->run_command(storage::hash_table_cmd_id::ht_update_partition, src_before_args);
    auto dst = std::make_shared<storage::replica_chain_client>(fs, path, dst_replica_chain, storage::KV_OPS);
    dst->run_command(storage::hash_table_cmd_id::ht_update_partition, dst_before_args);
    bool has_more = true;
    std::size_t split_batch_size = 1024;
    std::size_t tot_split_keys = 0;
    while (has_more) {
      // Read data to split
      std::vector<std::string> split_data;
      std::vector<std::string> args;
      args.emplace_back(std::to_string(split_range_begin));
      args.emplace_back(std::to_string(split_range_end));
      args.emplace_back(std::to_string(split_batch_size));
      split_data = src->run_command(storage::hash_table_cmd_id::ht_get_range_data, args);
      if (split_data.empty()) {
        break;
      } else if (split_data.size() < split_batch_size) {
        has_more = false;
      }
      auto split_keys = split_data.size() / 2;
      tot_split_keys += split_keys;
      LOG(log_level::info) << "Read " << split_keys << " keys to split";

      // Add redirected argument so that importing chain does not ignore our request
      split_data.emplace_back("!redirected");

      // Write data to dst partition
      dst->run_command(storage::hash_table_cmd_id::ht_put, split_data);

      // Remove data from src partition
      std::vector<std::string> remove_keys;
      split_data.pop_back(); // Remove !redirected argument
      std::size_t n_split_items = split_data.size();
      for (std::size_t i = 0; i < n_split_items; i++) {
        if (i % 2) {
          remove_keys.push_back(split_data.back());
        }
        split_data.pop_back();
      }
      assert(remove_keys.size() == split_keys);
      LOG(log_level::info) << "Sending " << remove_keys.size() << " split keys to remove";
      auto ret = src->run_command(storage::hash_table_cmd_id::ht_remove, remove_keys);
      LOG(log_level::info) << "Removed " << remove_keys.size() << " split keys";
    }
    // Finalize slot range split
    std::string old_name = current_name;
    fs->update_partition(path, old_name, src_partition_name, "regular");
    src->run_command(storage::hash_table_cmd_id::ht_update_partition, src_after_args);
    dst->run_command(storage::hash_table_cmd_id::ht_update_partition, dst_after_args);
    LOG(log_level::info) << "Exported slot range (" << split_range_begin << ", " << split_range_end << ")";




  } else if (scaling_type == "hash_table_merge") {





  } else if (scaling_type == "btree_split") {

  } else if (scaling_type == "btree_merge") {

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
