#include "auto_scaling_service_handler.h"
#include "jiffy/utils/logger.h"
#include "jiffy/directory/client/directory_client.h"
#include "jiffy/storage/msgqueue/msg_queue_ops.h"
#include "jiffy/storage/hashtable/hash_table_ops.h"
#include "jiffy/utils/logger.h"
#include "jiffy/utils/string_utils.h"
#include <thread>
#include <mutex>
#include <chrono>

std::mutex mtx;
namespace jiffy {
namespace auto_scaling {

using namespace utils;

auto_scaling_service_handler::auto_scaling_service_handler(const std::string directory_host, int directory_port)
    : directory_host_(directory_host), directory_port_(directory_port) {}

void auto_scaling_service_handler::auto_scaling(const std::vector<std::string> &current_replica_chain,
                                                const std::string &path,
                                                const std::map<std::string, std::string> &conf) {
  mtx.lock();
  std::string scaling_type = conf.find("type")->second;
  auto fs = std::make_shared<directory::directory_client>(directory_host_, directory_port_);
  if (scaling_type == "msg_queue") {
    auto start = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    //LOG(log_level::info) << "Start message queue auto_scaling: " << start;
    std::string dst_partition_name = conf.find("next_partition_name")->second;
    auto dst_replica_chain = fs->add_block(path, dst_partition_name, "regular");
    auto finish_adding_replica_chain = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    //LOG(log_level::info) << "Finish adding replica chain for msg queue: " << finish_adding_replica_chain;
    std::string next_target_string = "";
    for (const auto &block: dst_replica_chain.block_ids) {
      next_target_string += (block + "!");
    }
    next_target_string.pop_back();
    auto src = std::make_shared<storage::replica_chain_client>(fs, path, current_replica_chain, storage::MSG_QUEUE_OPS);
    std::vector<std::string> args;
    args.emplace_back(next_target_string);
    src->run_command(storage::msg_queue_cmd_id::mq_update_partition, args);
    auto finish_updating_partition = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    //LOG(log_level::info) << "Finish updating partition for msg queue: " << finish_updating_partition;
    LOG(log_level::info) << "===== " << "Message queue auto_scaling" << " ======";
    LOG(log_level::info) << "\t Total time: " << finish_updating_partition - start << " ms";
    LOG(log_level::info) << "\t Add replica chain: " << finish_adding_replica_chain - start << " ms";
    LOG(log_level::info) << "\t Update partition: " << finish_updating_partition - finish_adding_replica_chain << " ms";

  } else if (scaling_type == "hash_table_split") {
    auto start = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    //LOG(log_level::info) << "Start hash table split auto_scaling: " << start;
    std::int32_t slot_range_begin = std::stoi(conf.find("slot_range_begin")->second);
    std::int32_t slot_range_end = std::stoi(conf.find("slot_range_end")->second);
    auto split_range_begin = (slot_range_begin + slot_range_end) / 2;
    auto split_range_end = slot_range_end;
    std::string dst_partition_name = std::to_string(split_range_begin) + "_" + std::to_string(split_range_end);
    std::string src_partition_name = std::to_string(slot_range_begin) + "_" + std::to_string(split_range_begin);
    auto dst_replica_chain = fs->add_block(path, dst_partition_name, "importing");
    auto finish_adding_replica_chain = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    //LOG(log_level::info) << "Finish adding replica chain for hash table splitting: " << finish_adding_replica_chain;
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
    auto finish_updating_partition_before = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    //LOG(log_level::info) << "Finish updating partition for hash table before splitting: " << finish_updating_partition_before;
    bool has_more = true;
    std::size_t split_batch_size = 4;
    std::size_t tot_split_keys = 0;
    std::vector<std::string> args;
    args.emplace_back(std::to_string(split_range_begin));
    args.emplace_back(std::to_string(split_range_end));
    args.emplace_back(std::to_string(split_batch_size));
    while (has_more) {
      // Read data to split
      LOG(log_level::info) << "INTO THIS FUNCTION 1 *****************************";
      std::vector<std::string> split_data;
      LOG(log_level::info) << "INTO THIS FUNCTION 2 *****************************";
      split_data = src->run_command(storage::hash_table_cmd_id::ht_get_range_data, args);
      LOG(log_level::info) << "INTO THIS FUNCTION 3 *****************************";
      if (split_data.back() == "!empty") {
        LOG(log_level::info) << "INTO THIS FUNCTION 4 *****************************";
        break;
      } else if (split_data.size() < split_batch_size) {
        has_more = false;
      }
      LOG(log_level::info) << "INTO THIS FUNCTION 5 *****************************";
      auto split_keys = split_data.size() / 2;
      tot_split_keys += split_keys;
      //LOG(log_level::info) << "Read " << split_keys << " keys to split";

      // Add redirected argument so that importing chain does not ignore our request
      split_data.emplace_back("!redirected");
      LOG(log_level::info) << "INTO THIS FUNCTION 6 *****************************";
      // Write data to dst partition
      dst->run_command(storage::hash_table_cmd_id::ht_put, split_data);
      LOG(log_level::info) << "INTO THIS FUNCTION 7 *****************************";

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
      LOG(log_level::info) << "INTO THIS FUNCTION 8 *****************************";

      LOG(log_level::info) << "Sending " << remove_keys.size() << " split keys to remove";
      auto ret = src->run_command(storage::hash_table_cmd_id::ht_scale_remove, remove_keys);
      LOG(log_level::info) << "INTO THIS FUNCTION 9 *****************************";
      LOG(log_level::info) << "Removed " << remove_keys.size() << " split keys";
    }
    LOG(log_level::info) << "INTO THIS FUNCTION 10 *****************************";
    auto finish_data_transmission = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    //LOG(log_level::info) << "Finish data transmission: " << finish_data_transmission;
    // Finalize slot range split
    std::string old_name = current_name;
    src->run_command(storage::hash_table_cmd_id::ht_update_partition, src_after_args);
    dst->run_command(storage::hash_table_cmd_id::ht_update_partition, dst_after_args);
    auto finish_updating_partition_after = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    //LOG(log_level::info) << "Finish updating partition for hash table after splitting: " << finish_updating_partition_after;
    fs->update_partition(path, old_name, src_partition_name, "regular");
    auto finish_updating_partition_dir = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    //LOG(log_level::info) << "Finish updating mapping on directory server for hash table after splitting: " << finish_updating_partition_dir;
    LOG(log_level::info) << "Exported slot range (" << split_range_begin << ", " << split_range_end << ")";
    LOG(log_level::info) << "===== " << "Hash table splitting" << " ======";
    LOG(log_level::info) << "\t Total time: " << finish_updating_partition_dir - start << " ms";
    LOG(log_level::info) << "\t Add replica chain: " << finish_adding_replica_chain - start << " ms";
    LOG(log_level::info) << "\t Update partition before splitting: " << finish_updating_partition_before - finish_adding_replica_chain << " ms";
    LOG(log_level::info) << "\t Finish data transmission: " << finish_data_transmission - finish_updating_partition_before  << " ms";
    LOG(log_level::info) << "\t Update partition after splitting: " << finish_updating_partition_after - finish_data_transmission << " ms";
    LOG(log_level::info) << "\t Update mapping on directory server " << finish_updating_partition_dir - finish_updating_partition_after << " ms";



  } else if (scaling_type == "hash_table_merge") {
    auto start = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    //LOG(log_level::info) << "Start hash table merge auto_scaling: " << start;
    double threshold_hi_ = std::stod(conf.find("threshold_hi_")->second);
    std::string name;
    auto replica_set = fs->dstatus(path).data_blocks();
    for(auto &i: replica_set) {
      if(i.block_ids == current_replica_chain)
        name = i.name;
    }
    //LOG(log_level::info) << "INTO THIS FUNCTION  2 ***************************** " << name;
    auto src = std::make_shared<storage::replica_chain_client>(fs, path, current_replica_chain, storage::KV_OPS);
    //LOG(log_level::info) << "INTO THIS FUNCTION  3 *****************************";
    if(name == "0_65536") {
      std::vector<std::string> src_after_args;
      // We don't need to update the src partition cause it will be deleted anyway
      src_after_args.push_back(name);
      src_after_args.emplace_back("regular$" + name);
      src->run_command(storage::hash_table_cmd_id::ht_update_partition, src_after_args);
      return;
    }
    std::vector<std::string> ret = src->run_command(storage::hash_table_cmd_id::ht_get_storage_size, {});
    auto storage_size = static_cast<std::size_t>(std::stoi(ret.front()));
    auto storage_capacity = static_cast<std::size_t>(std::stoi(ret.back()));
    //LOG(log_level::info) << "*************************************************************";
    //LOG(log_level::info) << "Storage_size " << storage_size;
    //LOG(log_level::info) << "Storage_capacity" << storage_capacity;
    //LOG(log_level::info) << "**************************************************************";
    std::vector<std::string> slot_range = string_utils::split(name, '_', 2);
    std::int32_t merge_range_begin = std::stoi(slot_range[0]);
    std::int32_t merge_range_end = std::stoi(slot_range[1]);
    directory::replica_chain merge_target;
    bool able_to_merge = true;
    size_t find_min_size = static_cast<size_t>(static_cast<double>(storage_capacity)) + 1;
    for (auto &i : replica_set) {
      if (i.fetch_slot_range().first == merge_range_end || i.fetch_slot_range().second == merge_range_begin) {
        auto client = std::make_shared<storage::replica_chain_client>(fs, path, i, storage::KV_OPS, 0);
        auto size =
            static_cast<size_t>(std::stoi(client->run_command(storage::hash_table_cmd_id::ht_get_storage_size,
                                                              {}).front()));
        auto
            metadata_status = client->run_command(storage::hash_table_cmd_id::ht_get_metadata, {}).front();
        if (size + storage_size < static_cast<size_t>(static_cast<double>(storage_capacity) * threshold_hi_)
            && size < find_min_size) {
          if (metadata_status == "importing" || metadata_status == "exporting") {
            LOG(log_level::info) << "This should be throwing an error " << metadata_status;
            throw std::logic_error("Replica chain already involved in re-partitioning");
          }
          merge_target = i;
          find_min_size = size;
          able_to_merge = false;
        }
      }
    }
    if (able_to_merge) {
      throw std::logic_error("Adjacent partitions are not found or full");
    }
    auto finish_finding_chain_to_merge = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    //LOG(log_level::info) << "Found replica chain to merge: " << finish_finding_chain_to_merge;

    // Connect two replica chains
   // auto src = std::make_shared<storage::replica_chain_client>(fs, path, current_replica_chain, storage::KV_OPS);
    auto dst = std::make_shared<storage::replica_chain_client>(fs, path, merge_target, storage::KV_OPS);
    std::string dst_partition_name;
    if (merge_target.fetch_slot_range().first == merge_range_end)
      dst_partition_name =
          std::to_string(merge_range_begin) + "_" + std::to_string(merge_target.fetch_slot_range().second);
    else
      dst_partition_name =
          std::to_string(merge_target.fetch_slot_range().first) + "_" + std::to_string(merge_range_end);

    std::string export_target_str_ = "";
    for (const auto &block: merge_target.block_ids) {
      export_target_str_ += (block + "!");
    }
    export_target_str_.pop_back();
    std::vector<std::string> src_before_args;
    std::vector<std::string> dst_before_args;
    src_before_args.push_back(name);
    src_before_args.emplace_back("exporting$" + dst_partition_name + "$" + export_target_str_);
    dst_before_args.push_back(merge_target.name);
    dst_before_args.emplace_back("importing$" + name);
    src->run_command(storage::hash_table_cmd_id::ht_update_partition, src_before_args);
    dst->run_command(storage::hash_table_cmd_id::ht_update_partition, dst_before_args);
    auto finish_update_partition_before = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    //LOG(log_level::info) << "Finish updating partition before hash table merge: " << finish_update_partition_before;
    bool has_more = true;
    std::size_t merge_batch_size = 4;
    std::size_t tot_merge_keys = 0;
    std::vector<std::string> args;
    args.emplace_back(std::to_string(merge_range_begin));
    args.emplace_back(std::to_string(merge_range_end));
    args.emplace_back(std::to_string(static_cast<int32_t>(merge_batch_size)));
    while (has_more) {
      // Read data to merge
      std::vector<std::string> merge_data;
      //LOG(log_level::info) << "Look here 1";
      merge_data = src->run_command(storage::hash_table_cmd_id::ht_get_range_data, args);
      //LOG(log_level::info) << "Look here 2 " << " merge_data_size: " << merge_data.size();
      if (merge_data.back() == "!empty") {
        break;
      } else if (merge_data.size() < merge_batch_size) {
        has_more = false;
      }
      //LOG(log_level::info) << "Look here 3";

      auto merge_keys = merge_data.size() / 2;
      tot_merge_keys += merge_keys;
      //LOG(log_level::info) << "Read " << merge_keys << " keys to merge";

      // Add redirected argument so that importing chain does not ignore our request
      merge_data.push_back("!redirected");
      // Write data to dst partition
      dst->run_command(storage::hash_table_cmd_id::ht_put, merge_data);
      //LOG(log_level::info) << "Sent " << merge_keys << " keys";

      // Remove data from src partition
      std::vector<std::string> remove_keys;
      merge_data.pop_back(); // Remove !redirected argument
      std::size_t n_merge_items = merge_data.size();
      for (std::size_t i = 0; i < n_merge_items; i++) {
        if (i % 2) {
          remove_keys.push_back(merge_data.back());
        }
        merge_data.pop_back();
      }
      assert(remove_keys.size() == merge_keys);
      src->run_command(storage::hash_table_cmd_id::ht_scale_remove, remove_keys);
      //LOG(log_level::info) << "Removed " << remove_keys.size() << " merged keys";
    }
    //LOG(log_level::info) << "Look here 4";
    // Finalize slot range split
    // Update directory mapping
    auto finish_data_transmission = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    //LOG(log_level::info) << "Finish data transmission for hash table merge: " << finish_data_transmission;
    fs->update_partition(path, merge_target.name, dst_partition_name, "regular");
    auto finish_update_partition_dir = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    //LOG(log_level::info) << "Finish updating mapping on directory server after hash table merge: " << finish_update_partition_dir;
    //LOG(log_level::info) << "Look here 5";
    //Setting name and metadata for src and dst
    std::vector<std::string> src_after_args;
    std::vector<std::string> dst_after_args;
    // We don't need to update the src partition cause it will be deleted anyway
    dst_after_args.push_back(dst_partition_name);
    dst_after_args.emplace_back("regular$" + name);
    dst->run_command(storage::hash_table_cmd_id::ht_update_partition, dst_after_args);
    auto finish_update_partition_after = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    //LOG(log_level::info) << "Finish updating partition after hash table merge: " << finish_update_partition_after;
    LOG(log_level::info) << "Merged slot range (" << merge_range_begin << ", " << merge_range_end << ")";

    LOG(log_level::info) << "===== " << "Hash table merging" << " ======";
    LOG(log_level::info) << "\t Total time: " << finish_update_partition_after - start << " ms";
    LOG(log_level::info) << "Found replica chain to merge: " << finish_finding_chain_to_merge << " ms";
    LOG(log_level::info) << "\t Update partition before splitting: " << finish_update_partition_before - finish_finding_chain_to_merge << " ms";
    LOG(log_level::info) << "\t Finish data transmission: " << finish_data_transmission - finish_update_partition_before  << " ms";
    LOG(log_level::info) << "\t Update mapping on directory server " << finish_update_partition_dir - finish_data_transmission << " ms";
    LOG(log_level::info) << "\t Update partition after splitting: " << finish_update_partition_after - finish_data_transmission << " ms";


  } else if (scaling_type == "btree_split") {

  } else if (scaling_type == "btree_merge") {

  }
  mtx.unlock(); // Using global lock because we want to avoid merging and splitting happening in the same time
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
