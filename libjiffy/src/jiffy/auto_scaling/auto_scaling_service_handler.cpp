#include "auto_scaling_service_handler.h"
#include "jiffy/utils/logger.h"
#include "jiffy/directory/client/directory_client.h"
#include "jiffy/storage/file/file_ops.h"
#include "jiffy/storage/hashtable/hash_table_ops.h"
#include "jiffy/storage/fifoqueue/fifo_queue_ops.h"
#include "jiffy/utils/logger.h"
#include "jiffy/utils/string_utils.h"
#include <thread>
#include <mutex>
#include <chrono>

namespace jiffy {
namespace auto_scaling {

using namespace utils;
using namespace storage;

/* Mutex to make sure auto_scaling server handles one request at a time */
std::mutex MTX;

#define LOCK MTX.lock()
#define UNLOCK MTX.unlock()
#define UNLOCK_AND_RETURN \
  MTX.unlock();		  \
  return
#define UNLOCK_AND_THROW(ex)  \
  MTX.unlock();   	      \
  throw make_exception(ex)

auto_scaling_service_handler::auto_scaling_service_handler(const std::string directory_host, int directory_port)
    : directory_host_(directory_host), directory_port_(directory_port) {}

void auto_scaling_service_handler::auto_scaling(const std::vector<std::string> &cur_chain,
                                                const std::string &path,
                                                const std::map<std::string, std::string> &conf) {
  //LOCK;
  std::string scaling_type = conf.find("type")->second;
  auto fs = std::make_shared<directory::directory_client>(directory_host_, directory_port_);
  if (scaling_type == "file") {
    // Add replica chain at directory server
    auto start = time_utils::now_us();
    auto dst_name = conf.find("next_partition_name")->second;
    auto dst_replica_chain = fs->add_block(path, dst_name, "regular");
    auto finish_adding_replica_chain = time_utils::now_us();

    // Update source partition
    auto src = std::make_shared<replica_chain_client>(fs, path, cur_chain, FILE_OPS);
    src->run_command(file_cmd_id::file_update_partition, {pack(dst_replica_chain)});
    auto finish_updating_partition = time_utils::now_us();

    // Log auto-scaling info
    LOG(log_level::info) << "===== " << "File auto_scaling" << " ======";
    LOG(log_level::info) << "\t Start " << start;
    LOG(log_level::info) << "\t Add_replica_chain: " << finish_adding_replica_chain;
    LOG(log_level::info) << "\t Update_partition: " << finish_updating_partition;
    LOG(log_level::info) << " " << start << " " << finish_updating_partition - start << " "
                         << finish_adding_replica_chain - start << " "
                         << finish_updating_partition - finish_adding_replica_chain;

  } else if (scaling_type == "hash_table_split") {
    // Add replica chain at directory server
    auto start = time_utils::now_us();
    auto slot_range_beg = std::stoi(conf.find("slot_range_begin")->second);
    auto slot_range_end = std::stoi(conf.find("slot_range_end")->second);
    auto split_range_beg = (slot_range_beg + slot_range_end) / 2;
    auto split_range_end = slot_range_end;
    auto dst_name = std::to_string(split_range_beg) + "_" + std::to_string(split_range_end);
    auto src_name = std::to_string(slot_range_beg) + "_" + std::to_string(split_range_beg);
    // Making it split importing since we don't want the client to refresh and add this block
    auto dst_replica_chain = fs->add_block(path, dst_name, "split_importing");
    auto finish_adding_replica_chain = time_utils::now_us();

    // Update source and destination partitions before transfer
    std::string exp_target = pack(dst_replica_chain);
    std::string cur_name = std::to_string(slot_range_beg) + "_" + std::to_string(slot_range_end);
    auto src = std::make_shared<replica_chain_client>(fs, path, cur_chain, KV_OPS);
    src->run_command(hash_table_cmd_id::ht_update_partition, {cur_name, "exporting$" + dst_name + "$" + exp_target});
    auto dst = std::make_shared<replica_chain_client>(fs, path, dst_replica_chain, KV_OPS);
    dst->run_command(hash_table_cmd_id::ht_update_partition, {dst_name, "importing$" + dst_name});
    auto finish_updating_partition_before = time_utils::now_us();

    // Transfer the data from source to destination
    bool has_more = true;
    std::size_t split_batch_size = 2; // TODO: parameterize
    std::size_t tot_split_keys = 0;
    std::vector<std::string> args{std::to_string(split_range_beg),
                                  std::to_string(split_range_end),
                                  std::to_string(split_batch_size)};
    while (has_more) {
      // Read data to split
      std::vector<std::string> split_data;
      split_data = src->run_command(hash_table_cmd_id::ht_get_range_data, args);
      if (split_data.back() == "!empty") {
        break;
      } else if (split_data.size() < split_batch_size) {
        has_more = false;
      }
      auto split_keys = split_data.size() / 2;
      tot_split_keys += split_keys;
      // Add redirected argument so that importing chain does not ignore our request
      split_data.emplace_back("!redirected");
      // Write data to dst partition
      dst->run_command(hash_table_cmd_id::ht_put, split_data);
      // Remove data from src partition
      std::vector<std::string> remove_keys;
      split_data.pop_back(); // Remove !redirected argument
      auto n_split_items = split_data.size();
      for (std::size_t i = 0; i < n_split_items; i++) {
        if (i % 2) remove_keys.push_back(split_data.back());
        split_data.pop_back();
      }
      assert(remove_keys.size() == split_keys);
      auto ret = src->run_command(hash_table_cmd_id::ht_scale_remove, remove_keys);
    }
    auto finish_data_transmission = time_utils::now_us();

    // Finalize slot range split at directory server
    std::string old_name = cur_name;
    fs->update_partition(path, old_name, src_name, "regular");
    fs->update_partition(path, dst_name, dst_name, "regular");
    auto finish_updating_partition_dir = time_utils::now_us();

    // Update partitions after data transfer
    src->run_command(hash_table_cmd_id::ht_update_partition, {src_name, "regular"});
    dst->run_command(hash_table_cmd_id::ht_update_partition, {dst_name, "regular"});
    auto finish_updating_partition_after = time_utils::now_us();

    // Log auto-scaling info
    LOG(log_level::info) << "Exported slot range (" << split_range_beg << ", " << split_range_end << ")";
    LOG(log_level::info) << "===== " << "Hash table splitting" << " ======";
    LOG(log_level::info) << "\t start: " << start;
    LOG(log_level::info) << "\t Add_replica_chain: " << finish_adding_replica_chain;
    LOG(log_level::info) << "\t Update_partition_before_splitting: " << finish_updating_partition_before;
    LOG(log_level::info) << "\t Finish_data_transmission: " << finish_data_transmission;
    LOG(log_level::info) << "\t Update_mapping_on_directory_server " << finish_updating_partition_dir;
    LOG(log_level::info) << "\t Update_partition_after_splitting: " << finish_updating_partition_after;
    LOG(log_level::info) << " S " << start << " " << finish_updating_partition_after - start << " "
                         << finish_adding_replica_chain - start << " "
                         << finish_updating_partition_before - finish_adding_replica_chain << " "
                         << finish_data_transmission - finish_updating_partition_before << " "
                         << finish_updating_partition_dir - finish_data_transmission << " "
                         << finish_updating_partition_after - finish_updating_partition_dir;

  } else if (scaling_type == "hash_table_merge") {
    // Find a merge target
    LOCK;
    auto start = time_utils::now_us();
    auto src = std::make_shared<replica_chain_client>(fs, path, cur_chain, KV_OPS);
    auto name = src->run_command(hash_table_cmd_id::ht_update_partition, {"merging", "merging"}).front();
    if (name == "!fail") {
	    LOG(log_level::info) << " Fetching partition name failed";
      UNLOCK_AND_THROW("Partition is under auto_scaling");
    }
    auto storage_capacity = static_cast<std::size_t>(std::stoi(conf.find("storage_capacity")->second));
    auto replica_set = fs->dstatus(path).data_blocks();
    auto slot_range = string_utils::split(name, '_', 2);
    auto merge_range_beg = std::stoi(slot_range[0]);
    auto merge_range_end = std::stoi(slot_range[1]);
    directory::replica_chain merge_target;
    bool able_to_merge = true;
    size_t find_min_size = storage_capacity + 1;
    for (auto &i : replica_set) {
      if (i.fetch_slot_range().first == merge_range_end || i.fetch_slot_range().second == merge_range_beg) {
	      std::shared_ptr<replica_chain_client> client;
	      try {
        	client = std::make_shared<replica_chain_client>(fs, path, i, KV_OPS, 0);
	      } catch (apache::thrift::transport::TTransportException &e) {
		      continue;
	      }
        auto ret = client->run_command(hash_table_cmd_id::ht_get_storage_size, {}).front();
        if (ret == "!block_moved") continue;

        auto size = static_cast<size_t>(std::stoi(ret));
        if (size < static_cast<size_t>(storage_capacity * 0.5) && size < find_min_size) {
          merge_target = i;
          find_min_size = size;
          able_to_merge = false;
        }
      }
    }
    if (able_to_merge) {
	    LOG(log_level::info) << "Failed to find merge target";
      src->run_command(hash_table_cmd_id::ht_update_partition, {name, "regular$" + name});
      UNLOCK_AND_THROW("Adjacent partitions are not found or full");
    }
    auto finish_finding_chain_to_merge = time_utils::now_us();

    // Connect the two replica chains
    std::shared_ptr<replica_chain_client> dst;
    try {
    	dst = std::make_shared<replica_chain_client>(fs, path, merge_target, KV_OPS);
    } catch (apache::thrift::transport::TTransportException &e) {
	LOG(log_level::info) << "The merge target chain has been deleted";
      src->run_command(hash_table_cmd_id::ht_update_partition, {name, "regular$" + name});
      UNLOCK_AND_RETURN;
    }
    //auto dst_name = (merge_target.fetch_slot_range().first == merge_range_end) ?
    //                std::to_string(merge_range_beg) + "_" + std::to_string(merge_target.fetch_slot_range().second) :
    //                std::to_string(merge_target.fetch_slot_range().first) + "_" + std::to_string(merge_range_end);
    auto exp_target = pack(merge_target);
    auto dst_old_name = dst->run_command(hash_table_cmd_id::ht_update_partition, {merge_target.name, "importing$" + name}).front();

    // We don't need to update the src partition cause it will be deleted anyway
    if (dst_old_name == "!fail") {
	    LOG(log_level::info) << "Unable to save the destination block";
      src->run_command(hash_table_cmd_id::ht_update_partition, {name, "regular$" + name});
      UNLOCK_AND_RETURN;
    }
    UNLOCK;
    auto dst_slot_range = string_utils::split(dst_old_name, '_', 2);
    auto dst_name = (std::stoi(dst_slot_range[0]) == merge_range_end) ?
                    std::to_string(merge_range_beg) + "_" + dst_slot_range[1]:
                    dst_slot_range[0] + "_" + std::to_string(merge_range_end);
    src->run_command(hash_table_cmd_id::ht_update_partition, {name, "exporting$" + name + "$" + exp_target});
    auto finish_update_partition_before = time_utils::now_us();

    // Transfer data from source to destination
    bool has_more = true;
    std::size_t merge_batch_size = 2;
    std::size_t tot_merge_keys = 0;
    std::vector<std::string> args{std::to_string(merge_range_beg),
                                  std::to_string(merge_range_end),
                                  std::to_string(merge_batch_size)};
    while (has_more) {
      // Read data to merge
      auto merge_data = src->run_command(hash_table_cmd_id::ht_get_range_data, args);
      if (merge_data.back() == "!empty") {
        break;
      } else if (merge_data.size() < merge_batch_size) {
        has_more = false;
      }
      auto merge_keys = merge_data.size() / 2;
      tot_merge_keys += merge_keys;
      // Add redirected argument so that importing chain does not ignore our request
      merge_data.emplace_back("!redirected");
      // Write data to dst partition
      dst->run_command(hash_table_cmd_id::ht_put, merge_data);
      // Remove data from src partition
      std::vector<std::string> remove_keys;
      merge_data.pop_back(); // Remove !redirected argument
      std::size_t n_merge_items = merge_data.size();
      for (std::size_t i = 0; i < n_merge_items; i++) {
        if (i % 2) remove_keys.push_back(merge_data.back());
        merge_data.pop_back();
      }
      assert(remove_keys.size() == merge_keys);
      src->run_command(hash_table_cmd_id::ht_scale_remove, remove_keys);
    }
    auto finish_data_transmission = time_utils::now_us();

    // Update partition at directory server
    fs->update_partition(path, dst_old_name, dst_name, "regular");
    auto finish_update_partition_dir = time_utils::now_us();

    // Setting name and metadata for src and dst
    // We don't need to update the src partition cause it will be deleted anyway
    dst->run_command(hash_table_cmd_id::ht_update_partition, {dst_name, "regular$" + name});
    auto finish_update_partition_after = time_utils::now_us();

    // Log auto-scaling info
    LOG(log_level::info) << "Merged slot range (" << merge_range_beg << ", " << merge_range_end << ")";
    LOG(log_level::info) << "===== " << "Hash table merging" << " ======";
    LOG(log_level::info) << "\t Start: " << start;
    LOG(log_level::info) << "\t Found_replica_chain_to_merge: " << finish_finding_chain_to_merge;
    LOG(log_level::info) << "\t Update_partition_before_splitting: " << finish_update_partition_before;
    LOG(log_level::info) << "\t Finish_data_transmission: " << finish_data_transmission;
    LOG(log_level::info) << "\t Update_mapping_on_directory_server: " << finish_update_partition_dir;
    LOG(log_level::info) << "\t Update_partition_after_splitting: " << finish_update_partition_after;
    LOG(log_level::info) << " M " << start << " " << finish_update_partition_after - start << " "
                         << finish_finding_chain_to_merge - start << " "
                         << finish_update_partition_before - finish_finding_chain_to_merge << " "
                         << finish_data_transmission - finish_update_partition_before << " "
                         << finish_update_partition_dir - finish_data_transmission << " "
                         << finish_update_partition_after - finish_update_partition_dir;

    //UNLOCK;
  } else if (scaling_type == "fifo_queue_add") {
    // Add a replica chain at directory server
    auto start = time_utils::now_us();
    auto dst_name = conf.find("next_partition_name")->second;
    auto dst_replica_chain = fs->add_block(path, dst_name, "regular");
    auto finish_adding_replica_chain = time_utils::now_us();

    // Update source partition
    auto src = std::make_shared<replica_chain_client>(fs, path, cur_chain, FIFO_QUEUE_OPS);
    src->run_command(fifo_queue_cmd_id::fq_update_partition, {pack(dst_replica_chain)});
    auto finish_updating_partition = time_utils::now_us();

    // Log auto-scaling info
    LOG(log_level::info) << "===== " << "Fifo queue auto_scaling" << " ======";
    LOG(log_level::info) << "\t Start " << start;
    LOG(log_level::info) << "\t Add_replica_chain: " << finish_adding_replica_chain;
    LOG(log_level::info) << "\t Update_partition: " << finish_updating_partition;
    LOG(log_level::info) << "A " << start << " " << finish_updating_partition - start << " "
                         << finish_adding_replica_chain - start << " "
                         << finish_updating_partition - finish_adding_replica_chain;

  } else if (scaling_type == "fifo_queue_delete") {
    // Remove block at directory server
    auto start = time_utils::now_us();
    auto cur_name = conf.find("current_partition_name")->second;
    fs->remove_block(path, cur_name);
    auto finish_removing_replica_chain = time_utils::now_us();

    // Log auto-scaling info
    LOG(log_level::info) << "D " << start << " " << finish_removing_replica_chain - start;
  }
  //UNLOCK; // Using global lock because we want to avoid merging and splitting happening in the same time
}

std::string auto_scaling_service_handler::pack(const directory::replica_chain &chain) {
  std::string packed_str;
  for (const auto &block: chain.block_ids) {
    packed_str += (block + "!");
  }
  packed_str.pop_back();
  return packed_str;
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
