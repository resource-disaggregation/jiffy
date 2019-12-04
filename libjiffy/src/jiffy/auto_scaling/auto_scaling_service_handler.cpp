#include "auto_scaling_service_handler.h"
#include "jiffy/utils/logger.h"
#include "jiffy/directory/client/directory_client.h"
#include "jiffy/storage/file/file_ops.h"
#include "jiffy/storage/hashtable/hash_table_ops.h"
#include "jiffy/storage/fifoqueue/fifo_queue_ops.h"
#include "jiffy/utils/string_utils.h"
#include <mutex>
#include <chrono>
#include <jiffy/storage/client/data_structure_client.h>

namespace jiffy {
namespace auto_scaling {

using namespace utils;
using namespace storage;

/* Mutex to make sure auto_scaling server handles one request at a time */
std::mutex MTX;

#define LOCK MTX.lock()
#define UNLOCK MTX.unlock()
#define UNLOCK_AND_RETURN \
  MTX.unlock();           \
  return
#define UNLOCK_AND_THROW(ex)  \
  MTX.unlock();               \
  throw make_exception(ex)

auto_scaling_service_handler::auto_scaling_service_handler(const std::string &directory_host, int directory_port)
    : directory_host_(directory_host), directory_port_(directory_port) {}

void auto_scaling_service_handler::auto_scaling(const std::vector<std::string> &cur_chain,
                                                const std::string &path,
                                                const std::map<std::string, std::string> &conf) {
  std::string scaling_type = conf.find("type")->second;
  auto fs = std::make_shared<directory::directory_client>(directory_host_, directory_port_);
  if (scaling_type == "file") {
    LOG(log_level::info) << "Auto-scaling file";
    auto dst_name = std::stoi(conf.find("next_partition_name")->second);
    auto chain_to_add = std::stoul(conf.find("partition_num")->second);
    std::vector<directory::replica_chain> chain_vector(chain_to_add + 1);
    std::shared_ptr<replica_chain_client> src_vector;
    std::vector<std::string> args{"update_partition"};
    auto start = time_utils::now_us();
    for(std::size_t i = 0; i < chain_to_add; i++) {
      // Add replica chain at directory server
      args.push_back(pack(fs->add_block(path, std::to_string(dst_name + i), "regular")));
    }
    auto finish_adding_replica_chain = time_utils::now_us();
    // Update source partition
    src_vector = std::make_shared<replica_chain_client>(fs, path, cur_chain, FILE_OPS);
    src_vector->run_command(args);

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
    LOG(log_level::info) << "Auto-scaling hash table (split)";

    auto slot_range_beg = std::stoi(conf.find("slot_range_begin")->second);
    auto slot_range_end = std::stoi(conf.find("slot_range_end")->second);
    auto split_range_beg = (slot_range_beg + slot_range_end) / 2;
    auto split_range_end = slot_range_end;
    auto dst_name = std::to_string(split_range_beg) + "_" + std::to_string(split_range_end);
    auto src_name = std::to_string(slot_range_beg) + "_" + std::to_string(split_range_beg);

    // Add replica chain at directory server
    auto start = time_utils::now_us();
    // Making it split importing since we don't want the client to refresh and add this block
    auto dst_chain = fs->add_block(path, dst_name, "split_importing");
    auto finish_adding_replica_chain = time_utils::now_us();

    // Update source and destination partitions before transfer
    auto exp_target = pack(dst_chain);
    auto cur_name = std::to_string(slot_range_beg) + "_" + std::to_string(slot_range_end);
    auto src = std::make_shared<replica_chain_client>(fs, path, cur_chain, HT_OPS);
    auto dst = std::make_shared<replica_chain_client>(fs, path, dst_chain, HT_OPS);
    dst->run_command({"update_partition", dst_name, "importing$" + dst_name});
    src->run_command({"update_partition", cur_name, "exporting$" + dst_name + "$" + exp_target});
    auto finish_updating_partition_before = time_utils::now_us();

    // Transfer the data from source to destination
    hash_table_transfer_data(src, dst, split_range_beg, split_range_end);
    auto finish_data_transmission = time_utils::now_us();

    // Finalize slot range split at directory server
    const auto &old_name = cur_name;
    fs->update_partition(path, old_name, src_name, "regular");
    fs->update_partition(path, dst_name, dst_name, "regular");
    auto finish_updating_partition_dir = time_utils::now_us();

    // Update partitions after data transfer
    src->run_command({"update_partition", src_name, "regular"});
    dst->run_command({"update_partition", dst_name, "regular"});
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
    LOCK;
    LOG(log_level::info) << "Auto-scaling hash table (merge)";

    auto storage_capacity = std::stoull(conf.at("storage_capacity"));

    // Find a merge target
    std::shared_ptr<replica_chain_client> src;
    auto start = time_utils::now_us();
    try {
      src = std::make_shared<replica_chain_client>(fs, path, cur_chain, HT_OPS);
    } catch (std::exception &e) {
      UNLOCK_AND_THROW("Unable to connect to src partition");
    }
    auto update_resp = src->run_command({"update_partition", "merging", "merging"}); // TODO: Why "merging" twice?
    if (update_resp[0] == "!fail") {
      UNLOCK_AND_THROW("Partition is under auto_scaling");
    }
    auto name = update_resp[1];
    auto slot_range = string_utils::split(name, '_', 2);
    auto merge_range_beg = std::stoi(slot_range[0]);
    auto merge_range_end = std::stoi(slot_range[1]);

    directory::replica_chain merge_target;
    if (!find_merge_target(merge_target, fs, path, storage_capacity, merge_range_beg, merge_range_end)) {
      src->run_command({"update_partition", name, "regular$" + name});
      UNLOCK_AND_THROW("Adjacent partitions are not found or full");
    }
    auto finish_finding_chain_to_merge = time_utils::now_us();

    // Connect the two replica chains
    std::shared_ptr<replica_chain_client> dst;
    std::string dst_old_name;
    try {
      dst = std::make_shared<replica_chain_client>(fs, path, merge_target, HT_OPS);
      dst->send_command({"update_partition", merge_target.name, "importing$" + name});
      dst_old_name = dst->recv_response().front();
    } catch (std::exception &e) {
      src->run_command({"update_partition", name, "regular$" + name});
      UNLOCK_AND_RETURN;
    }

    auto exp_target = pack(merge_target);


    // We don't need to update the src partition cause it will be deleted anyway
    if (dst_old_name == "!fail") {
      src->run_command({"update_partition", name, "regular$" + name});
      UNLOCK_AND_RETURN;
    }
    UNLOCK;
    auto dst_slot_range = string_utils::split(dst_old_name, '_', 2);
    auto dst_name = (std::stoi(dst_slot_range[0]) == merge_range_end) ?
                    std::to_string(merge_range_beg) + "_" + dst_slot_range[1]:
                    dst_slot_range[0] + "_" + std::to_string(merge_range_end);
    src->run_command({"update_partition", name, "exporting$" + name + "$" + exp_target});
    auto finish_update_partition_before = time_utils::now_us();

    // Transfer data from source to destination
    hash_table_transfer_data(src, dst, merge_range_beg, merge_range_end);
    auto finish_data_transmission = time_utils::now_us();

    // Update partition at directory server
    fs->update_partition(path, dst_old_name, dst_name, "regular");
    auto finish_update_partition_dir = time_utils::now_us();

    // Setting name and metadata for src and dst
    // We don't need to update the src partition cause it will be deleted anyway
    dst->run_command({"update_partition", dst_name, "regular"});
    auto finish_update_partition_after = time_utils::now_us();

    // Remove the merged chain
    fs->remove_block(path, name);

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

  } else if (scaling_type == "fifo_queue_add") {
    LOG(log_level::info) << "Auto-scaling fifo queue (add)";

    // Add a replica chain at directory server
    auto start = time_utils::now_us();
    auto dst_name = conf.find("next_partition_name")->second;
    auto dst_replica_chain = fs->add_block(path, dst_name, "regular");
    auto finish_adding_replica_chain = time_utils::now_us();

    // Update source partition
    auto src = std::make_shared<replica_chain_client>(fs, path, cur_chain, FQ_CMDS);
    src->run_command({"update_partition", pack(dst_replica_chain)});
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
    LOG(log_level::info) << "Auto-scaling fifo queue (remove)";

    // Remove block at directory server
    auto start = time_utils::now_us();
    auto cur_name = conf.find("current_partition_name")->second;
    fs->remove_block(path, cur_name);
    auto finish_removing_replica_chain = time_utils::now_us();

    // Log auto-scaling info
    LOG(log_level::info) << "D " << start << " " << finish_removing_replica_chain - start;
  }
}

void auto_scaling_service_handler::hash_table_transfer_data(const std::shared_ptr<storage::replica_chain_client> &src,
                                                            const std::shared_ptr<storage::replica_chain_client> &dst,
                                                            size_t slot_beg,
                                                            size_t slot_end,
                                                            size_t batch_size) {

  // Transfer the data from source to destination
  bool has_more = true;
  std::vector<std::string> read_args{"get_range_data",
                                     std::to_string(slot_beg),
                                     std::to_string(slot_end),
                                     std::to_string(batch_size)};
  while (has_more) {
    // Read data to split
    auto write_args = src->run_command(read_args);
    if (write_args.back() == "!empty") {
      break;
    } else if (write_args.size() < batch_size) {
      has_more = false;
    }

    // write_args.insert(write_args.begin(), "scale_put"); // Add scale_put command name
    write_args[0] = "scale_put";

    // Write data to dst partition
    auto response = dst->run_command(write_args);

    // Remove data from src partition
    auto transfer_size = write_args.size() - 1; // Account for "scale_put" command name

    std::vector<std::string> remove_args{"scale_remove"};
    for (std::size_t i = 0; i < transfer_size; i++) {
      if (i % 2) {
        remove_args.push_back(write_args.back());
      }
      write_args.pop_back();
    }
    assert(write_args.size() == 1);
    assert(remove_args.size() == transfer_size / 2 + 1);
    auto ret = src->run_command(remove_args);
  }
}

bool auto_scaling_service_handler::find_merge_target(directory::replica_chain &merge_target,
                                                     const std::shared_ptr<directory::directory_client> &fs,
                                                     const std::string &path,
                                                     size_t storage_capacity,
                                                     int32_t slot_beg,
                                                     int32_t slot_end) {
  bool able_to_merge = false;
  auto replica_set = fs->dstatus(path).data_blocks();
  size_t find_min_size = storage_capacity + 1;
  for (auto &r : replica_set) {
    if (r.fetch_slot_range().first == slot_end || r.fetch_slot_range().second == slot_beg) {
      std::shared_ptr<replica_chain_client> client;
      try {
        client = std::make_shared<replica_chain_client>(fs, path, r, HT_OPS, 0);
        auto ret = client->run_command({"get_storage_size"});
        if (ret[0] == "!block_moved") continue;
        auto size = std::stoull(ret[1]);
        if (size < (storage_capacity / 2) && size < find_min_size) {
          merge_target = r;
          find_min_size = size;
          able_to_merge = true;
        }
      } catch (std::exception &e) {
        continue;
      }
    }
  }
  return able_to_merge;
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
