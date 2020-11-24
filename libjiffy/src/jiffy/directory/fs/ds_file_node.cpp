#include "jiffy/storage/chain_module.h"
#include "jiffy/utils/logger.h"
#include "jiffy/utils/directory_utils.h"
#include "jiffy/directory/fs/ds_file_node.h"

namespace jiffy {
namespace directory {

ds_file_node::ds_file_node(const std::string &name)
    : ds_node(name, file_status(file_type::regular, perms(perms::all), utils::time_utils::now_ms())),
      dstatus_{} {}

ds_file_node::ds_file_node(const std::string &name,
                           const std::string &type,
                           const std::string &backing_path,
                           std::size_t chain_length,
                           std::vector<replica_chain> blocks,
                           int32_t flags,
                           int32_t permissions,
                           const std::map<std::string, std::string> &tags) :
    ds_node(name,
            file_status(file_type::regular, perms(static_cast<uint16_t>(permissions)), utils::time_utils::now_ms())),
    dstatus_(type, backing_path, chain_length, std::move(blocks), flags, tags) {}

const data_status &ds_file_node::dstatus() const {
  std::unique_lock<std::mutex> lock(mtx_);
  return dstatus_;
}

void ds_file_node::dstatus(const data_status &status) {
  std::unique_lock<std::mutex> lock(mtx_);
  dstatus_ = status;
}

std::vector<storage_mode> ds_file_node::mode() const {
  std::unique_lock<std::mutex> lock(mtx_);
  return dstatus_.mode();
}

void ds_file_node::mode(size_t i, const storage_mode &m) {
  std::unique_lock<std::mutex> lock(mtx_);
  dstatus_.mode(i, m);
}

void ds_file_node::mode(const storage_mode &m) {
  std::unique_lock<std::mutex> lock(mtx_);
  dstatus_.mode(m);
}

const std::string &ds_file_node::backing_path() const {
  std::unique_lock<std::mutex> lock(mtx_);
  return dstatus_.backing_path();
}

void ds_file_node::backing_path(const std::string &prefix) {
  std::unique_lock<std::mutex> lock(mtx_);
  dstatus_.backing_path(prefix);
}

std::size_t ds_file_node::chain_length() const {
  std::unique_lock<std::mutex> lock(mtx_);
  return dstatus_.chain_length();
}

void ds_file_node::chain_length(std::size_t chain_length) {
  std::unique_lock<std::mutex> lock(mtx_);
  dstatus_.chain_length(chain_length);
}

void ds_file_node::add_tag(const std::string &key, const std::string &value) {
  std::unique_lock<std::mutex> lock(mtx_);
  dstatus_.add_tag(key, value);
}

void ds_file_node::add_tags(const std::map<std::string, std::string> &tags) {
  std::unique_lock<std::mutex> lock(mtx_);
  dstatus_.add_tags(tags);
}

std::string ds_file_node::get_tag(const std::string &key) const {
  std::unique_lock<std::mutex> lock(mtx_);
  return dstatus_.get_tag(key);
}

const std::map<std::string, std::string> &ds_file_node::get_tags() const {
  std::unique_lock<std::mutex> lock(mtx_);
  return dstatus_.get_tags();
}

std::int32_t ds_file_node::flags() const {
  std::unique_lock<std::mutex> lock(mtx_);
  return dstatus_.flags();
}

void ds_file_node::flags(std::int32_t flags) {
  std::unique_lock<std::mutex> lock(mtx_);
  dstatus_.flags(flags);
}

bool ds_file_node::is_pinned() const {
  std::unique_lock<std::mutex> lock(mtx_);
  return dstatus_.is_pinned();
}

bool ds_file_node::is_mapped() const {
  std::unique_lock<std::mutex> lock(mtx_);
  return dstatus_.is_mapped();
}

bool ds_file_node::is_static_provisioned() const {
  std::unique_lock<std::mutex> lock(mtx_);
  return dstatus_.is_static_provisioned();
}

const std::vector<replica_chain> &ds_file_node::data_blocks() const {
  std::unique_lock<std::mutex> lock(mtx_);
  return dstatus_.data_blocks();
}

void ds_file_node::sync(const std::string &backing_path,
                        const std::shared_ptr<storage::storage_management_ops> &storage) {
  std::unique_lock<std::mutex> lock(mtx_);
  for (const auto &block: dstatus_.data_blocks()) {
    std::string block_backing_path = backing_path;
    utils::directory_utils::push_path_element(block_backing_path, block.name);
    if (block.mode == storage_mode::in_memory || block.mode == storage_mode::in_memory_grace)
      storage->sync(block.tail(), block_backing_path);
  }
}

void ds_file_node::dump(std::vector<std::string> &cleared_blocks,
                        const std::string &backing_path,
                        const std::shared_ptr<storage::storage_management_ops> &storage) {
  std::unique_lock<std::mutex> lock(mtx_);
  for (const auto &block: dstatus_.data_blocks()) {
    for (size_t i = 0; i < dstatus_.chain_length(); i++) {
      if (i == dstatus_.chain_length() - 1) {
        std::string block_backing_path = backing_path;
        utils::directory_utils::push_path_element(block_backing_path, block.name);
        storage->dump(block.tail(), block_backing_path);
        dstatus_.mark_dumped(i);
      } else {
        storage->destroy_partition(block.block_ids[i]);
      }
      cleared_blocks.push_back(block.block_ids[i]);
    }
  }
}

void ds_file_node::load(const std::string &path,
                        const std::string &backing_path,
                        const std::shared_ptr<storage::storage_management_ops> &storage,
                        const std::shared_ptr<block_allocator> &allocator) {
  std::unique_lock<std::mutex> lock(mtx_);

  auto num_blocks = dstatus_.data_blocks().size();
  auto chain_length = dstatus_.chain_length();
  std::string tenant_id = directory_utils::get_root(directory_utils::get_parent_path(path));
  for (std::size_t i = 0; i < num_blocks; ++i) {
    replica_chain chain(allocator->allocate(chain_length, {}, tenant_id), storage_mode::in_memory);
    assert(chain.block_ids.size() == chain_length);
    chain.metadata = "regular";
    using namespace storage;
    if (chain_length == 1) {
      std::string block_backing_path = backing_path;
      utils::directory_utils::push_path_element(block_backing_path, chain.name);
      storage->create_partition(chain.block_ids[0], dstatus_.type(), dstatus_.data_blocks()[i].name, "regular", dstatus_.get_tags());
      storage->setup_chain(chain.block_ids[0], path, chain.block_ids, chain_role::singleton, "nil");
      storage->load(chain.block_ids[0], block_backing_path);
      dstatus_.mark_loaded(i, chain.block_ids);
    } else {
      std::string block_backing_path = backing_path;
      utils::directory_utils::push_path_element(block_backing_path, chain.name);
      for (std::size_t j = 0; j < chain_length; ++j) {
        std::string block_name = chain.block_ids[j];
        std::string next_block_name = (j == chain_length - 1) ? "nil" : chain.block_ids[j + 1];
        int32_t role = (j == 0) ? chain_role::head : (j == chain_length - 1) ? chain_role::tail : chain_role::mid;
        storage->create_partition(block_name, dstatus_.type(), chain.name, chain.metadata, get_tags());
        storage->setup_chain(block_name, path, chain.block_ids, role, next_block_name);
        storage->load(block_name, block_backing_path);
      }
      dstatus_.mark_loaded(i, chain.block_ids);
    }
  }
}

bool ds_file_node::handle_lease_expiry(std::vector<std::string> &cleared_blocks,
                                       const std::shared_ptr<storage::storage_management_ops>& storage) {
  std::unique_lock<std::mutex> lock(mtx_);
  if (!dstatus_.is_pinned()) {
    using namespace utils;
    LOG(log_level::info) << "Clearing storage for " << name();
    if (dstatus_.is_mapped()) {
      for (const auto &block: dstatus_.data_blocks()) {
        for (size_t i = 0; i < dstatus_.chain_length(); i++) {
          if (i == dstatus_.chain_length() - 1) {
            std::string block_backing_path = dstatus_.backing_path();
            utils::directory_utils::push_path_element(block_backing_path, block.name);
            storage->dump(block.tail(), block_backing_path);
            dstatus_.mode(i, storage_mode::on_disk);
          } else {
            storage->destroy_partition(block.block_ids[i]);
          }
          cleared_blocks.push_back(block.block_ids[i]);
        }
      }
      return false; // Clear the blocks, but don't delete the path
    } else {
      for (const auto &block: dstatus_.data_blocks()) {
        for (const auto &block_name: block.block_ids) {
          storage->destroy_partition(block_name);
          cleared_blocks.push_back(block_name);
        }
      }
    }
    return true; // Clear the blocks and delete the path
  }
  return false; // Don't clear the blocks or delete the path
}

size_t ds_file_node::num_blocks() const {
  std::unique_lock<std::mutex> lock(mtx_);
  return dstatus_.data_blocks().size();
}

replica_chain ds_file_node::add_data_block(const std::string &path,
                                           const std::string &partition_name,
                                           const std::string &partition_metadata,
                                           const std::shared_ptr<storage::storage_management_ops> &storage,
                                           const std::shared_ptr<block_allocator> &allocator) {
  using namespace utils;
  std::unique_lock<std::mutex> lock(mtx_);
  std::string tenant_id = directory_utils::get_root(directory_utils::get_parent_path(path));
  replica_chain chain(allocator->allocate(static_cast<size_t>(dstatus_.chain_length()), {}, tenant_id), storage_mode::in_memory);
  chain.name = partition_name;
  chain.metadata = partition_metadata;
  assert(chain.block_ids.size() == chain_length);
  dstatus_.add_data_block(chain);
  using namespace storage;
  if (dstatus_.chain_length() == 1) {
    storage->create_partition(chain.block_ids[0], dstatus_.type(), chain.name, chain.metadata, dstatus_.get_tags());
    storage->setup_chain(chain.block_ids[0], path, chain.block_ids, chain_role::singleton, "nil");
  } else {
    for (size_t j = 0; j < dstatus_.chain_length(); ++j) {
      std::string block_id = chain.block_ids[j];
      std::string next_block_id = (j == dstatus_.chain_length() - 1) ? "nil" : chain.block_ids[j + 1];
      int32_t
          role = (j == 0) ? chain_role::head : (j == dstatus_.chain_length() - 1) ? chain_role::tail : chain_role::mid;
      storage->create_partition(block_id, dstatus_.type(), chain.name, chain.metadata, dstatus_.get_tags());
      storage->setup_chain(block_id, path, chain.block_ids, role, next_block_id);
    }
  }
  return chain;
}

void ds_file_node::remove_block(const std::string &partition_name,
                                const std::shared_ptr<storage::storage_management_ops> &storage,
                                const std::shared_ptr<block_allocator> &allocator,
                                const std::string &tenant_id) {
  std::unique_lock<std::mutex> lock(mtx_);
  replica_chain block;
  if (!dstatus_.remove_data_block(partition_name, block)) {
    throw directory_ops_exception("No partition with name " + partition_name);
  }
  for (const auto &id: block.block_ids) {
    storage->destroy_partition(id);
  }
  allocator->free(block.block_ids, tenant_id);
}

void ds_file_node::update_data_status_partition(const std::string &old_name,
                                                const std::string &new_name,
                                                const std::string &metadata) {
  dstatus_.set_partition_name(old_name, new_name);
  dstatus_.set_partition_metadata(new_name, metadata);
}

}
}