#include <jiffy/utils/string_utils.h>
#include "msg_queue_partition.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/logger.h"
#include "jiffy/persistent/persistent_store.h"
#include "jiffy/storage/partition_manager.h"
#include "jiffy/storage/msgqueue/msg_queue_ops.h"
#include "jiffy/directory/client/directory_client.h"
#include "jiffy/directory/directory_ops.h"

namespace jiffy {
namespace storage {

using namespace utils;

msg_queue_partition::msg_queue_partition(block_memory_manager *manager,
                                         const std::string &name,
                                         const std::string &metadata,
                                         const utils::property_map &conf,
                                         const std::string &directory_host,
                                         const int directory_port)
    : chain_module(manager, name, metadata, MSG_QUEUE_OPS),
      partition_(build_allocator<msg_type>()),
      splitting_(false),
      merging_(false),
      dirty_(false),
    // export_slot_range_(0, -1),
    // import_slot_range_(0, -1),// TODO remove export slot range for now since we don't know what is the value beyond the string
      directory_host_(directory_host),
      directory_port_(directory_port) {
  auto ser = conf.get("msgqueue.serializer", "csv");
  if (ser == "binary") {
    ser_ = std::make_shared<csv_serde>();
  } else if (ser == "csv") {
    ser_ = std::make_shared<binary_serde>();
  } else {
    throw std::invalid_argument("No such serializer/deserializer " + ser);
  }
  threshold_hi_ = conf.get_as<double>("msgqueue.capacity_threshold_hi", 0.95);
  threshold_lo_ = conf.get_as<double>("msgqueue.capacity_threshold_lo", 0.00);
  auto_scale_ = conf.get_as<bool>("msgqueue.auto_scale", true);
  LOG(log_level::info) << "Partition name: " << name_;
}




std::string msg_queue_partition::send(const msg_type &message, bool indirect) {
  //LOG(log_level::info) << "Sending new message = " << message;
  std::unique_lock<std::shared_mutex> lock(operation_mtx_);
  partition_.push_back(message);
  return "!ok";
}

msg_type msg_queue_partition::receive(std::string position, bool indirect) {
  auto pos = std::stoi(position);
  if(pos < 0) throw std::invalid_argument("receive position invalid");
  if(pos < size()) {
    std::unique_lock<std::shared_mutex> lock(operation_mtx_);
    return partition_[pos];
  }
  return "!key_not_found";

}

std::string msg_queue_partition::clear() {
  std::unique_lock<std::shared_mutex> lock(operation_mtx_);
  partition_.clear();
  return "!ok";
}

/* TODO update this when adding auto scaling
std::string msg_queue_partition::update_partition(const std::string new_name, const std::string new_metadata) {

  name(new_name);
  auto s = utils::string_utils::split(new_metadata, '$');
  std::string status = s.front();

  if (status == "exporting") {
    export_target(s[2]);
    auto range = utils::string_utils::split(s[1], '_');
    export_slot_range(std::stoi(range[0]), std::stoi(range[1]));
  } else if (status == "importing") {
    auto range = utils::string_utils::split(s[1], '_');
    import_slot_range(std::stoi(range[0]), std::stoi(range[1]));
  } else {
    if (metadata() == "importing") {
      if (import_slot_range().first != slot_range().first || import_slot_range().second != slot_range().second) {
        auto fs = std::make_shared<directory::directory_client>(directory_host_, directory_port_);
        fs->remove_block(path(), s[1]);
      }
    }
    export_slot_range(0, -1);
    import_slot_range(0, -1);
  }
  metadata(status);
  return "!ok";

}
//TODO change this function
std::string msg_queue_partition::get_storage_size() {

  return std::to_string(bytes_.load());

}

std::string msg_queue_partition::get_metadata() {
  return metadata();
}
  */

void msg_queue_partition::run_command(std::vector<std::string> &_return,
                                      int32_t cmd_id,
                                      const std::vector<std::string> &args) {
  bool redirect = !args.empty() && args.back() == "!redirected";
  size_t nargs = redirect ? args.size() - 1 : args.size();
  switch (cmd_id) {
    case msg_queue_cmd_id::mq_send:
      for (const msg_type &msg: args)
        _return.emplace_back(send(msg, redirect));
      break;
    case msg_queue_cmd_id::mq_receive:
      for (const auto &pos: args)
        _return.emplace_back(receive(pos, redirect));
      break;
    case msg_queue_cmd_id::mq_clear:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
          _return.emplace_back(clear());
      }
      break;
      /* TODO need to add auto scaling function in the future
    case msg_queue_cmd_id::update_partition:
      if (nargs != 2) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(update_partition(args[0], args[1]));
      }
      break;
    case msg_queue_cmd_id::get_storage_size:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(get_storage_size());
      }
      break;
    case msg_queue_cmd_id::get_metadata:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(get_metadata());
      }
      break;
*/
    default:throw std::invalid_argument("No such operation id " + std::to_string(cmd_id));
  }
  if (is_mutator(cmd_id)) {
    dirty_ = true;
  }
  bool expected = false;
  if (auto_scale_.load() && is_mutator(cmd_id) && overload() && metadata_ != "exporting"
      && metadata_ != "importing" && is_tail()
      && splitting_.compare_exchange_strong(expected, true)) {
    // Ask directory server to split this slot range
    LOG(log_level::info) << "Overloaded partition; storage = " << bytes_.load() << " capacity = "
                         << manager_->mb_capacity();
    try {

      splitting_ = false;
      LOG(log_level::info) << "Not supporting auto_scaling currently";
      /* TODO add auto scaling logic for msg queue here
      // TODO: currently the split and merge use similar logic but using redundant coding, should combine them after passing all the test
      splitting_ = true;
      LOG(log_level::info) << "Requested slot range split";

      auto split_range_begin = (slot_range_.first + slot_range_.second) / 2;
      auto split_range_end = slot_range_.second;
      std::string dst_partition_name = std::to_string(split_range_begin) + "_" + std::to_string(split_range_end);
      auto fs = std::make_shared<directory::directory_client>(directory_host_, directory_port_);
      LOG(log_level::info) << "host !!!!!!!" << directory_host_ << " port " << directory_port_;
      auto dst_replica_chain =
          fs->add_block(path(), dst_partition_name, "importing");

      LOG(log_level::info) << "Look here!!!!!!!";

      // TODO check if add_block succeed, might not be enough capacity in extreme situation
      auto src = std::make_shared<replica_chain_client>(fs, path(), chain(), 0);
      auto dst = std::make_shared<replica_chain_client>(fs, path(), dst_replica_chain, 0);


      std::string src_partition_name = std::to_string(slot_range_.first) + "_" + std::to_string(split_range_begin);
      set_exporting(dst_replica_chain.block_ids,
                    split_range_begin,
                    split_range_end);

      std::vector<std::string> src_before_args;
      std::vector<std::string> dst_before_args;
      src_before_args.push_back(name());
      src_before_args.emplace_back("exporting$" + dst_partition_name + "$" + export_target_str());
      dst_before_args.push_back(dst_partition_name);
      dst_before_args.emplace_back("importing$" + dst_partition_name);
      src->send_command(hash_table_cmd_id::update_partition, src_before_args);
      dst->send_command(hash_table_cmd_id::update_partition, dst_before_args);
      src->recv_response();
      dst->recv_response();
      LOG(log_level::info) << "Look here 1";
      bool has_more = true;
      std::size_t split_batch_size = 1024;
      std::size_t tot_split_keys = 0;
      while (has_more) {
        // Lock source and destination blocks
        if (role() == chain_role::singleton) {
          dst->send_command(hash_table_cmd_id::lock, {});
          lock();
          dst->recv_response();
        } else {
          src->send_command(hash_table_cmd_id::lock, {});
          dst->send_command(hash_table_cmd_id::lock, {});
          src->recv_response();
          dst->recv_response();
        }
        // Read data to split
        std::vector<std::string> split_data;
        locked_get_data_in_slot_range(split_data,
                                      split_range_begin,
                                      split_range_end,
                                      static_cast<int32_t>(split_batch_size));
        if (split_data.size() == 0) {
          if (role() == chain_role::singleton) {
            dst->send_command(hash_table_cmd_id::unlock, {});
            unlock();
            dst->recv_response();
          } else {
            src->send_command(hash_table_cmd_id::unlock, {});
            dst->send_command(hash_table_cmd_id::unlock, {});
            src->recv_response();
            dst->recv_response();
          }
          break;
        } else if (split_data.size() < split_batch_size) {
          has_more = false;
        }

        LOG(log_level::info) << "Look here 2";
        auto split_keys = split_data.size() / 2;
        tot_split_keys += split_keys;
        LOG(log_level::info) << "Read " << split_keys << " keys to split";

        // Add redirected argument so that importing chain does not ignore our request
        split_data.emplace_back("!redirected");

        // Write data to dst partition
        dst->run_command(hash_table_cmd_id::locked_put, split_data);
        LOG(log_level::info) << "Sent " << split_keys << " keys";

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
        src->run_command(hash_table_cmd_id::locked_remove, remove_keys);
        LOG(log_level::info) << "Removed " << remove_keys.size() << " split keys";

        // Unlock source and destination blocks
        if (role() == chain_role::singleton) {
          dst->send_command(hash_table_cmd_id::unlock, {});
          unlock();
          dst->recv_response();
        } else {
          src->send_command(hash_table_cmd_id::unlock, {});
          dst->send_command(hash_table_cmd_id::unlock, {});
          src->recv_response();
          dst->recv_response();
        }
      }
      // Finalize slot range split
      LOG(log_level::info) << "Look here 3";
      // Update directory mapping
      std::string old_name = name();
      fs->update_partition(path(), old_name, src_partition_name, "regular");

      //Setting name and metadata for src and dst
      std::vector<std::string> src_after_args;
      std::vector<std::string> dst_after_args;
      src_after_args.push_back(src_partition_name);
      src_after_args.emplace_back("regular");
      dst_after_args.push_back(dst_partition_name);
      dst_after_args.emplace_back("regular");
      src->send_command(hash_table_cmd_id::update_partition, src_after_args);
      dst->send_command(hash_table_cmd_id::update_partition, dst_after_args);
      src->recv_response();
      dst->recv_response();
      LOG(log_level::info) << "Exported slot range (" << split_range_begin << ", " << split_range_end << ")";
      splitting_ = false;
      merging_ = false;
       */

    } catch (std::exception &e) {
      splitting_ = false;
      LOG(log_level::warn) << "Split slot range failed: " << e.what();
    }
    LOG(log_level::info) << "After split storage: " << manager_->mb_used() << " capacity: " << manager_->mb_capacity();
  }
  expected = false;
  if (auto_scale_.load() && //cmd_id == b_tree_cmd_id::bt_remove &&
      underload()
      && metadata_ != "exporting"
      && metadata_ != "importing" && //slot_end() != MAX_KEY &&
      is_tail()
      && merging_.compare_exchange_strong(expected, true)) {
    // Ask directory server to split this slot range
    LOG(log_level::info) << "Underloaded partition; storage = " << bytes_.load() << " capacity = "
                         << manager_->mb_capacity();
    try {
      merging_ = false;
      LOG(log_level::info) << "Currently does not support auto_scaling";
      // TODO add auto_scaling logic here
      /*
      merging_ = true;
      LOG(log_level::info) << "Requested slot range merge";

      auto merge_range_begin = slot_range_.first;
      auto merge_range_end = slot_range_.second;
      auto fs = std::make_shared<directory::directory_client>(directory_host_, directory_port_);
      auto replica_set = fs->dstatus(path()).data_blocks();

      directory::replica_chain merge_target;
      bool able_to_merge = true;
      size_t find_min_size = static_cast<size_t>(static_cast<double>(manager_->mb_capacity())) + 1;
      for (auto &i : replica_set) {
        if (i.fetch_slot_range().first == slot_range().second || i.fetch_slot_range().second == slot_range().first) {
          auto client = std::make_shared<replica_chain_client>(fs, path_, i, 0);
          auto size =
              static_cast<size_t>(std::stoi(client->run_command(hash_table_cmd_id::get_storage_size, {}).front()));
          auto metadata_status = client->run_command(hash_table_cmd_id::get_metadata, {}).front();
          if (size + storage_size() < static_cast<size_t>(static_cast<double>(manager_->mb_capacity()) * threshold_hi_)
              && size < find_min_size) {
            if(metadata_status == "importing" || metadata_status == "exporting")
              throw std::logic_error("Replica chain already involved in re-partitioning");
            merge_target = i;
            find_min_size = size;
            able_to_merge = false;
          }
        }
      }
      if (able_to_merge) {
        throw std::logic_error("Adjacent partitions are not found or full");
      }

      // Connect two replica chains
      auto src = std::make_shared<replica_chain_client>(fs, path_, chain(), 0);
      auto dst = std::make_shared<replica_chain_client>(fs, path_, merge_target, 0);
      std::string dst_partition_name;
      if (merge_target.fetch_slot_range().first == slot_range().second)
        dst_partition_name =
            std::to_string(slot_range().first) + "_" + std::to_string(merge_target.fetch_slot_range().second);
      else
        dst_partition_name =
            std::to_string(merge_target.fetch_slot_range().first) + "_" + std::to_string(slot_range().second);

      set_exporting(merge_target.block_ids,
                    merge_range_begin,
                    merge_range_end);

      std::vector<std::string> src_before_args;
      std::vector<std::string> dst_before_args;
      src_before_args.push_back(name());
      src_before_args.emplace_back("exporting$" + dst_partition_name + "$" + export_target_str());
      dst_before_args.push_back(merge_target.name);
      dst_before_args.emplace_back("importing$" + name());
      src->send_command(hash_table_cmd_id::update_partition, src_before_args);
      dst->send_command(hash_table_cmd_id::update_partition, dst_before_args);
      src->recv_response();
      dst->recv_response();

      bool has_more = true;
      std::size_t merge_batch_size = 1024;
      std::size_t tot_merge_keys = 0;
      while (has_more) {
        // Lock source and destination blocks
        if (role() == chain_role::singleton) {
          dst->send_command(hash_table_cmd_id::lock, {});
          lock();
          dst->recv_response();
        } else {
          src->send_command(hash_table_cmd_id::lock, {});
          dst->send_command(hash_table_cmd_id::lock, {});
          src->recv_response();
          dst->recv_response();
        }
        // Read data to merge
        std::vector<std::string> merge_data;
        locked_get_data_in_slot_range(merge_data,
                                      merge_range_begin,
                                      merge_range_end,
                                      static_cast<int32_t>(merge_batch_size));
        if (merge_data.size() == 0) {
          if (role() == chain_role::singleton) {
            dst->send_command(hash_table_cmd_id::unlock, {});
            unlock();
            dst->recv_response();
          } else {
            src->send_command(hash_table_cmd_id::unlock, {});
            dst->send_command(hash_table_cmd_id::unlock, {});
            src->recv_response();
            dst->recv_response();
          }
          break;
        } else if (merge_data.size() < merge_batch_size) {
          has_more = false;
        }

        auto merge_keys = merge_data.size() / 2;
        tot_merge_keys += merge_keys;
        LOG(log_level::trace) << "Read " << merge_keys << " keys to merge";

        // Add redirected argument so that importing chain does not ignore our request
        merge_data.emplace_back("!redirected");

        // Write data to dst partition
        dst->run_command(hash_table_cmd_id::locked_put, merge_data);
        LOG(log_level::trace) << "Sent " << merge_keys << " keys";

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
        src->run_command(hash_table_cmd_id::locked_remove, remove_keys);
        LOG(log_level::trace) << "Removed " << remove_keys.size() << " merged keys";

        // Unlock source and destination blocks
        if (role() == chain_role::singleton) {
          dst->send_command(hash_table_cmd_id::unlock, {});
          unlock();
          dst->recv_response();
        } else {
          src->send_command(hash_table_cmd_id::unlock, {});
          dst->send_command(hash_table_cmd_id::unlock, {});
          src->recv_response();
          dst->recv_response();
        }
      }

      // Finalize slot range split

      // Update directory mapping
      fs->update_partition(path(), merge_target.name, dst_partition_name, "regular");

      //Setting name and metadata for src and dst
      std::vector<std::string> src_after_args;
      std::vector<std::string> dst_after_args;
      // We don't need to update the src partition cause it will be deleted anyway
      //src_after_args.push_back(src_partition_name);
      //src_after_args.emplace_back("regular");
      dst_after_args.push_back(dst_partition_name);
      dst_after_args.emplace_back("regular$" + name());
      //src->send_command(hash_table_cmd_id::update_partition, src_after_args);
      dst->send_command(hash_table_cmd_id::update_partition, dst_after_args);
      //src->recv_response();
      dst->recv_response();
      LOG(log_level::info) << "Merged slot range (" << merge_range_begin << ", " << merge_range_end << ")";
      splitting_ = false;
      merging_ = false;
*/
    } catch (std::exception &e) {
      merging_ = false;
      LOG(log_level::warn) << "Merge slot range failed: " << e.what();
    }
  }
}

std::size_t msg_queue_partition::size() const {
  return partition_.size();
}

bool msg_queue_partition::empty() const {
  return partition_.empty();
}

bool msg_queue_partition::is_dirty() const {
  return dirty_.load();
}

void msg_queue_partition::load(const std::string &path) {
  auto remote = persistent::persistent_store::instance(path, ser_);
  auto decomposed = persistent::persistent_store::decompose_path(path);
  //remote->read<msg_queue_type>(decomposed.second, partition_); TODO fix this, currently template function only supports hash tables
}

bool msg_queue_partition::sync(const std::string &path) {
  bool expected = true;
  if (dirty_.compare_exchange_strong(expected, false)) {
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    //remote->write<msg_queue_type>(partition_, decomposed.second); TODO fix this, currently template function only supports hash tables
    return true;
  }
  return false;
}

bool msg_queue_partition::dump(const std::string &path) {
  std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
  bool expected = true;
  bool flushed = false;
  if (dirty_.compare_exchange_strong(expected, false)) {
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    //remote->write<msg_queue_type>(partition_, decomposed.second); TODO fix this, currently template function only supports hash tables
    flushed = true;
  }
  partition_.clear();
  next_->reset("nil");
  path_ = "";
  // clients().clear();
  sub_map_.clear();
  chain_ = {};
  role_ = singleton;
  splitting_ = false;
  merging_ = false;
  dirty_ = false;
  return flushed;
}

void msg_queue_partition::forward_all() {
  int64_t i = 0;
  for (auto it = partition_.begin(); it != partition_.end(); it++) {
    std::vector<std::string> result;
   // run_command_on_next(result, msg_queue_cmd_id::_put, {it.key(), (*it).second}); TODO fix this
    ++i;
  }
}

bool msg_queue_partition::overload() {
  return manager_->mb_used() > static_cast<size_t>(static_cast<double>(manager_->mb_capacity()) * threshold_hi_);
}

bool msg_queue_partition::underload() {
  return manager_->mb_used() < static_cast<size_t>(static_cast<double>(manager_->mb_capacity()) * threshold_lo_);
}

REGISTER_IMPLEMENTATION("msgqueue", msg_queue_partition);

}
}