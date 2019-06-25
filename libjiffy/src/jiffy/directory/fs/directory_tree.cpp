#include "directory_tree.h"

#include "../../utils/retry_utils.h"

namespace jiffy {
namespace directory {

using namespace utils;

directory_tree::directory_tree(std::shared_ptr<block_allocator> allocator,
                               std::shared_ptr<storage::storage_management_ops> storage)
    : root_(std::make_shared<ds_dir_node>(std::string("/"))),
      allocator_(std::move(allocator)),
      storage_(std::move(storage)) {}

void directory_tree::create_directory(const std::string &path) {
  LOG(log_level::info) << "Creating directory " << path;
  std::string ptemp = path;
  std::string directory_name = directory_utils::pop_path_element(ptemp);
  auto parent = get_node_as_dir(ptemp);
  if (parent->get_child(directory_name) == nullptr) {
    parent->add_child(std::make_shared<ds_dir_node>(directory_name));
  }
}

void directory_tree::create_directories(const std::string &path) {
  LOG(log_level::info) << "Creating directory " << path;
  std::string p_so_far(root_->name());
  std::shared_ptr<ds_dir_node> dir_node = root_;
  for (auto &name: directory_utils::path_elements(path)) {
    directory_utils::push_path_element(p_so_far, name);
    std::shared_ptr<ds_node> child = dir_node->get_child(name);
    if (child == nullptr) {
      child = std::dynamic_pointer_cast<ds_node>(std::make_shared<ds_dir_node>(name));
      dir_node->add_child(child);
      dir_node = std::dynamic_pointer_cast<ds_dir_node>(child);
    } else {
      if (child->is_directory()) {
        dir_node = std::dynamic_pointer_cast<ds_dir_node>(child);
      } else {
        throw directory_ops_exception("Cannot create directory: " + p_so_far + " is a file.");
      }
    }
  }
}

data_status directory_tree::open(const std::string &path) {
  LOG(log_level::info) << "Opening file " << path;
  return dstatus(path);
}

data_status directory_tree::create(const std::string &path,
                                   const std::string &type,
                                   const std::string &backing_path,
                                   int32_t num_blocks,
                                   int32_t chain_length,
                                   int32_t flags,
                                   int32_t permissions,
                                   const std::vector<std::string> &partition_names,
                                   const std::vector<std::string> &partition_metadata,
                                   const std::map<std::string, std::string> &tags) {
  LOG(log_level::info) << "Creating file " << path << " with backing_path=" << backing_path << " num_blocks="
                       << num_blocks << ", chain_length=" << chain_length;
  if (num_blocks == 0) {
    throw directory_ops_exception("File cannot have zero blocks");
  }
  if (chain_length == 0) {
    throw directory_ops_exception("Chain length cannot be zero");
  }
  std::string filename = directory_utils::get_filename(path);

  if (filename == "." || filename == "/") {
    throw directory_ops_exception("Path is a directory: " + path);
  }
  std::string parent_path = directory_utils::get_parent_path(path);
  auto node = get_node_unsafe(parent_path);
  if (node == nullptr) {
    create_directories(parent_path);
    node = get_node_unsafe(parent_path);
  } else if (node->is_regular_file()) {
    throw directory_ops_exception(
        "Cannot create file in dir " + parent_path + ": " + node->name() + " is a file.");
  }

  auto parent = std::dynamic_pointer_cast<ds_dir_node>(node);

  std::vector<replica_chain> blocks;
  for (int32_t i = 0; i < num_blocks; ++i) {
    replica_chain chain(allocator_->allocate(static_cast<size_t>(chain_length), {}), storage_mode::in_memory);
    chain.name = partition_names[i];
    chain.metadata = partition_metadata[i];
    assert(chain.block_ids.size() == chain_length);
    blocks.push_back(chain);
    using namespace storage;
    if (chain_length == 1) {
      storage_->create_partition(chain.block_ids[0], type, chain.name, chain.metadata, tags);
      storage_->setup_chain(chain.block_ids[0], path, chain.block_ids, chain_role::singleton, "nil");
    } else {
      for (int32_t j = 0; j < chain_length; ++j) {
        std::string block_id = chain.block_ids[j];
        std::string next_block_id = (j == chain_length - 1) ? "nil" : chain.block_ids[j + 1];
        int32_t role = (j == 0) ? chain_role::head : (j == chain_length - 1) ? chain_role::tail : chain_role::mid;
        storage_->create_partition(block_id, type, chain.name, chain.metadata, tags);
        storage_->setup_chain(block_id, path, chain.block_ids, role, next_block_id);
      }
    }
  }
  auto child = std::make_shared<ds_file_node>(filename, type, backing_path, chain_length, blocks, flags, permissions,
                                              tags);

  parent->add_child(child);

  return child->dstatus();
}

data_status directory_tree::open_or_create(const std::string &path,
                                           const std::string &type,
                                           const std::string &backing_path,
                                           int32_t num_blocks,
                                           int32_t chain_length,
                                           int32_t flags,
                                           int32_t permissions,
                                           const std::vector<std::string> &partition_names,
                                           const std::vector<std::string> &partition_metadata,
                                           const std::map<std::string, std::string> &tags) {

  LOG(log_level::info) << "Opening or creating file " << path << " with backing_path=" << backing_path << " num_blocks="
                       << num_blocks << ", chain_length=" << chain_length;
  std::string filename = directory_utils::get_filename(path);
  if (filename == "." || filename == "/") {
    throw directory_ops_exception("Path is a directory: " + path);
  }
  std::string parent_path = directory_utils::get_parent_path(path);
  auto node = get_node_unsafe(parent_path);
  if (node == nullptr) {
    create_directories(parent_path);
    node = get_node_unsafe(parent_path);
  } else if (node->is_regular_file()) {
    throw directory_ops_exception(
        "Cannot create file in dir " + parent_path + ": " + node->name() + " is a file.");
  }

  auto parent = std::dynamic_pointer_cast<ds_dir_node>(node);
  auto c = parent->get_child(filename);
  if (c != nullptr) {
    if (c->is_regular_file()) {
      return std::dynamic_pointer_cast<ds_file_node>(c)->dstatus();
    } else {
      throw directory_ops_exception("Cannot open or create " + path + ": is a directory");
    }
  }
  if (num_blocks == 0) {
    throw directory_ops_exception("File cannot have zero blocks");
  }

  if (chain_length == 0) {
    throw directory_ops_exception("Chain length cannot be zero");
  }
  std::vector<replica_chain> blocks;
  for (int32_t i = 0; i < num_blocks; ++i) {
    replica_chain chain(allocator_->allocate(static_cast<size_t>(chain_length), {}), storage_mode::in_memory);
    chain.name = partition_names[i];
    chain.metadata = partition_metadata[i];
    assert(chain.block_ids.size() == chain_length);
    blocks.push_back(chain);
    using namespace storage;
    if (chain_length == 1) {
      storage_->create_partition(chain.block_ids[0], type, chain.name, chain.metadata, tags);
      storage_->setup_chain(chain.block_ids[0], path, chain.block_ids, chain_role::singleton, "nil");
    } else {
      for (int32_t j = 0; j < chain_length; ++j) {
        std::string block_id = chain.block_ids[j];
        std::string next_block_id = (j == chain_length - 1) ? "nil" : chain.block_ids[j + 1];
        int32_t role = (j == 0) ? chain_role::head : (j == chain_length - 1) ? chain_role::tail : chain_role::mid;
        storage_->create_partition(block_id, type, chain.name, chain.metadata, tags);
        storage_->setup_chain(block_id, path, chain.block_ids, role, next_block_id);
      }
    }
  }
  auto child = std::make_shared<ds_file_node>(filename, type, backing_path, chain_length, blocks, flags, permissions,
                                              tags);
  parent->add_child(child);

  return child->dstatus();
}

bool directory_tree::exists(const std::string &path) const {
  return get_node_unsafe(path) != nullptr;
}

std::uint64_t directory_tree::last_write_time(const std::string &path) const {
  return get_node(path)->last_write_time();
}

perms directory_tree::permissions(const std::string &path) {
  return get_node(path)->permissions();
}

void directory_tree::permissions(const std::string &path, const perms &prms, perm_options opts) {
  auto node = get_node(path);
  perms p;
  switch (opts) {
    case perm_options::replace: {
      p = (prms & perms::mask);
      break;
    }
    case perm_options::add: {
      p = node->permissions() | (prms & perms::mask);
      break;
    }
    case perm_options::remove: {
      p = node->permissions() & ~(prms & perms::mask);
      break;
    }
  }
  LOG(log_level::info) << "Setting permissions for " << path << " to " << p;
  node->permissions(p);
}

void directory_tree::remove(const std::string &path) {
  LOG(log_level::info) << "Removing path " << path;
  if (path == "/") {
    if (root_->child_names().empty())
      return;
    throw directory_ops_exception("Directory not empty: " + path);
  }
  std::string ptemp = path;
  std::string child_name = directory_utils::pop_path_element(ptemp);
  auto parent = get_node_as_dir(ptemp);
  auto child = parent->get_child(child_name);
  if (child == nullptr) {
    throw directory_ops_exception("Path does not exist: " + path);
  }
  if (child->is_directory() && !std::dynamic_pointer_cast<ds_dir_node>(child)->empty()) {
    throw directory_ops_exception("Directory not empty: " + path);
  }
  parent->remove_child(child_name);
  std::vector<std::string> cleared_blocks;
  clear_storage(cleared_blocks, child);
  allocator_->free(cleared_blocks);
}

void directory_tree::remove_all(std::shared_ptr<ds_dir_node> parent, const std::string &child_name) {
  auto child = parent->get_child(child_name);
  if (child == nullptr) {
    throw directory_ops_exception("Node does not exist: " + child_name);
  }
  parent->remove_child(child_name);
  LOG(log_level::info) << "Removed child " << child_name;
  std::vector<std::string> cleared_blocks;
  clear_storage(cleared_blocks, child);
  LOG(log_level::info) << "Cleared all blocks " << child_name;
  allocator_->free(cleared_blocks);
}

void directory_tree::remove_all(const std::string &path) {
  LOG(log_level::info) << "Removing path " << path;
  if (path == "/") {
    auto parent = root_;
    auto children = root_->child_names();
    for (const auto &child_name: children) {
      remove_all(parent, child_name);
    }
    return;
  }
  std::string ptemp = path;
  std::string child_name = directory_utils::pop_path_element(ptemp);
  auto parent = get_node_as_dir(ptemp);
  remove_all(parent, child_name);
}

void directory_tree::sync(const std::string &path, const std::string &backing_path) {
  LOG(log_level::info) << "Syncing path " << path;
  get_node(path)->sync(backing_path, storage_);
}

void directory_tree::dump(const std::string &path, const std::string &backing_path) {
  LOG(log_level::info) << "Dumping path " << path;
  std::vector<std::string> cleared_blocks;
  get_node(path)->dump(cleared_blocks, backing_path, storage_);
  allocator_->free(cleared_blocks);
}

void directory_tree::load(const std::string &path, const std::string &backing_path) {
  LOG(log_level::info) << "Loading path " << path;
  get_node(path)->load(path, backing_path, storage_, allocator_);
}

void directory_tree::rename(const std::string &old_path, const std::string &new_path) {
  LOG(log_level::info) << "Renaming " << old_path << " to " << new_path;
  if (old_path == new_path)
    return;
  std::string ptemp = old_path;
  std::string old_child_name = directory_utils::pop_path_element(ptemp);
  auto old_parent = get_node_as_dir(ptemp);
  auto old_child = old_parent->get_child(old_child_name);
  if (old_child == nullptr) {
    throw directory_ops_exception("Path does not exist: " + old_path);
  }

  ptemp = new_path;
  std::string new_child_name = directory_utils::pop_path_element(ptemp);
  auto new_parent = get_node_as_dir(ptemp);
  auto new_child = new_parent->get_child(new_child_name);
  if (new_child != nullptr) {
    if (new_child->is_directory()) {
      new_parent = std::dynamic_pointer_cast<ds_dir_node>(new_child);
      new_child_name = old_child_name;
    } else {
      new_parent->remove_child(new_child_name);
      std::vector<std::string> cleared_blocks;
      clear_storage(cleared_blocks, new_child);
      allocator_->free(cleared_blocks);
    }
  }
  old_parent->remove_child(old_child->name());
  old_child->name(new_child_name);
  new_parent->add_child(old_child);
}

file_status directory_tree::status(const std::string &path) const {
  return get_node(path)->status();
}

std::vector<directory_entry> directory_tree::directory_entries(const std::string &path) {
  return get_node_as_dir(path)->entries();
}

std::vector<directory_entry> directory_tree::recursive_directory_entries(const std::string &path) {
  return get_node_as_dir(path)->recursive_entries();
}

data_status directory_tree::dstatus(const std::string &path) {
  return get_node_as_file(path)->dstatus();
}

void directory_tree::add_tags(const std::string &path, const std::map<std::string, std::string> &tags) {
  get_node_as_file(path)->add_tags(tags);
}

bool directory_tree::is_regular_file(const std::string &path) {
  return get_node(path)->is_regular_file();
}

bool directory_tree::is_directory(const std::string &path) {
  return get_node(path)->is_directory();
}

void directory_tree::touch(const std::string &path) {
  auto time = utils::time_utils::now_ms();
  auto node = touch_node_path(path, time);
  if (node == nullptr) {
    throw directory_ops_exception("Path does not exist: " + path);
  }
  touch(node, time);
}

replica_chain directory_tree::resolve_failures(const std::string &path, const replica_chain &chain) {
  // TODO: Replace replica_chain argument with chain id
  // FIXME: This is not thread safe
  std::size_t chain_length = chain.block_ids.size();
  std::vector<bool> failed(chain_length);
  LOG(log_level::info) << "Resolving failures for partition chain " << chain.to_string() << " @ " << path;
  bool mid_failure = false;
  auto node = get_node_as_file(path);
  auto dstatus = node->dstatus();
  auto blocks = dstatus.data_blocks();
  size_t chain_pos = blocks.size();
  for (size_t i = 0; i < blocks.size(); i++) {
    if (chain == blocks.at(i)) {
      chain_pos = i;
    }
  }
  if (chain_pos == blocks.size()) {
    throw directory_ops_exception("No such chain for path " + path);
  }
  std::vector<std::string> fixed_chain;
  for (std::size_t i = 0; i < chain_length; i++) {
    std::string block_id = chain.block_ids[i];
    auto parsed = storage::block_id_parser::parse(block_id);
    try {
      utils::retry_utils::retry(3, [parsed]() {
        using namespace ::apache::thrift::transport;
        TSocket m_sock(parsed.host, parsed.management_port);
        m_sock.open();
        LOG(log_level::info) << m_sock.getPeerHost() << ":" << m_sock.getPeerPort() << " is still live";
        m_sock.close();
        return true;
      });
      LOG(log_level::info) << "Block " << block_id << " is still live";
      fixed_chain.push_back(block_id);
    } catch (std::exception &) {
      LOG(log_level::warn) << "Block " << block_id << " has failed, removing it from chain";
      failed[i] = true;
      if (i > 0 && i < chain_length - 1 && failed[i] && !failed[i - 1]) {
        mid_failure = true;
      }
    }
  }

  if (fixed_chain.size() == chain_length) { // Nothing has failed, really
    return fixed_chain;
  }
  // Re-organize chain as needed
  using namespace storage;
  if (fixed_chain.empty()) {                       // All failed
    LOG(log_level::error) << "No blocks left in chain";
    throw directory_ops_exception("All blocks in the chain have failed.");
  } else if (fixed_chain.size() == 1) {            // All but one failed
    LOG(log_level::info) << "Only one partition has remained in chain; setting singleton role";
    storage_->setup_chain(fixed_chain[0], path, fixed_chain, chain_role::singleton, "nil");
  } else {                                         // More than one left
    LOG(log_level::info) << fixed_chain.size() << " blocks left in chain";
    for (std::size_t i = 0; i < fixed_chain.size(); ++i) {
      std::string block_id = fixed_chain[i];
      std::string next_block_id = (i == fixed_chain.size() - 1) ? "nil" : fixed_chain[i + 1];
      int32_t
          role = (i == 0) ? chain_role::head : (i == fixed_chain.size() - 1) ? chain_role::tail : chain_role::mid;
      LOG(log_level::info) << "Setting partition <" << block_id << ">: path=" << path << ", role=" << role
                           << ", next=" << next_block_id << ">";
      storage_->setup_chain(block_id, path, fixed_chain, role, next_block_id);
    }
    if (mid_failure) {
      LOG(log_level::info) << "Resending pending requests to resolve mid partition failure";
      storage_->resend_pending(fixed_chain[0]);
    }
  }
  dstatus.set_data_block(chain_pos, replica_chain(fixed_chain, storage_mode::in_memory));
  node->dstatus(dstatus);
  return dstatus.get_data_block(chain_pos);
}

replica_chain directory_tree::add_replica_to_chain(const std::string &path, const replica_chain &chain) {
  // TODO: Replace replica_chain argument with chain id
  // FIXME: This is not thread safe
  using namespace storage;
  LOG(log_level::info) << "Adding new replica to chain " << chain.to_string() << " @ " << path;

  auto node = get_node_as_file(path);
  auto dstatus = node->dstatus();
  auto blocks = dstatus.data_blocks();
  size_t chain_pos = blocks.size();
  for (size_t i = 0; i < blocks.size(); i++) {
    if (chain == blocks.at(i)) {
      chain_pos = i;
    }
  }
  if (chain_pos == blocks.size()) {
    throw directory_ops_exception("No such chain for path " + path);
  }

  auto new_blocks = allocator_->allocate(1, chain.block_ids);
  auto updated_chain = chain.block_ids;
  updated_chain.insert(updated_chain.end(), new_blocks.begin(), new_blocks.end());

  // Setup forwarding path
  LOG(log_level::info) << "Setting old tail partition <" << chain.block_ids.back() << ">: path=" << path
                       << ", role=" << chain_role::tail << ", next=" << new_blocks.front() << ">";
  storage_->setup_chain(chain.block_ids.back(), path, updated_chain, chain_role::tail, new_blocks.front());
  for (std::size_t i = chain.block_ids.size(); i < updated_chain.size(); i++) {
    std::string block_id = updated_chain[i];
    std::string next_block_id = (i == updated_chain.size() - 1) ? "nil" : updated_chain[i + 1];
    int32_t
        role = (i == 0) ? chain_role::head : (i == updated_chain.size() - 1) ? chain_role::tail : chain_role::mid;
    LOG(log_level::info) << "Setting partition <" << block_id << ">: path=" << path << ", role=" << role
                         << ", next=" << next_block_id << ">";
    // TODO: this is incorrect -- we shouldn't be setting the chain to updated_chain right now...
    storage_->create_partition(block_id, dstatus.type(), chain.name, chain.metadata, dstatus.get_tags());
    storage_->setup_chain(block_id, path, updated_chain, role, next_block_id);
  }

  LOG(log_level::info) << "Forwarding data from <" << chain.block_ids.back() << "> to <" << new_blocks.front() << ">";

  storage_->forward_all(chain.block_ids.back());

  LOG(log_level::info) << "Setting old tail partition <" << chain.block_ids.back() << ">: path=" << path
                       << ", role=" << chain_role::mid << ", next=" << new_blocks.front() << ">";
  storage_->setup_chain(chain.block_ids.back(), path, updated_chain, chain_role::mid, new_blocks.front());

  dstatus.set_data_block(chain_pos, replica_chain(updated_chain, storage_mode::in_memory));
  node->dstatus(dstatus);
  return dstatus.get_data_block(chain_pos);
}

void directory_tree::handle_lease_expiry(const std::string &path) {
  LOG(log_level::info) << "Handling expiry for " << path;
  std::string ptemp = path;
  std::string child_name = directory_utils::pop_path_element(ptemp);
  auto parent = get_node_as_dir(ptemp);
  std::vector<std::string> cleared_blocks;
  parent->handle_lease_expiry(cleared_blocks, child_name, storage_);
  if (!cleared_blocks.empty()) {
    LOG(log_level::info) << "Handled lease expiry, freeing blocks for " << path;
    allocator_->free(cleared_blocks);
  }
}

std::shared_ptr<ds_node> directory_tree::get_node_unsafe(const std::string &path) const {
  std::shared_ptr<ds_node> node = root_;
  for (auto &name: directory_utils::path_elements(path)) {
    if (!node->is_directory()) {
      return nullptr;
    } else {
      node = std::dynamic_pointer_cast<ds_dir_node>(node)->get_child(name);
      if (node == nullptr) {
        return nullptr;
      }
    }
  }
  return node;
}

std::shared_ptr<ds_node> directory_tree::get_node(const std::string &path) const {
  auto node = get_node_unsafe(path);
  if (node == nullptr) {
    throw directory_ops_exception("Path does not exist: " + path);
  }
  return node;
}

std::shared_ptr<ds_dir_node> directory_tree::get_node_as_dir(const std::string &path) const {
  auto node = get_node(path);
  if (node->is_regular_file()) {
    throw directory_ops_exception("Path corresponds to a file: " + path);
  }
  return std::dynamic_pointer_cast<ds_dir_node>(node);
}

std::shared_ptr<ds_file_node> directory_tree::get_node_as_file(const std::string &path) const {
  auto node = get_node(path);
  if (!node->is_regular_file()) {
    throw directory_ops_exception("Path corresponds to a directory: " + path);
  }
  return std::dynamic_pointer_cast<ds_file_node>(node);
}

std::shared_ptr<ds_node> directory_tree::touch_node_path(const std::string &path,
                                                         const std::uint64_t time) const {
  std::shared_ptr<ds_node> node = root_;
  for (auto &name: directory_utils::path_elements(path)) {
    if (!node->is_directory()) {
      return nullptr;
    } else {
      node = std::dynamic_pointer_cast<ds_dir_node>(node)->get_child(name);
      if (node == nullptr) {
        return nullptr;
      }
      node->last_write_time(time);
    }
  }
  return node;
}

void directory_tree::touch(std::shared_ptr<ds_node> node, std::uint64_t time) {
  node->last_write_time(time);
  if (node->is_regular_file()) {
    return;
  }
  auto dir = std::dynamic_pointer_cast<ds_dir_node>(node);
  for (const auto &child: *dir) {
    touch(child.second, time);
  }
}

void directory_tree::clear_storage(std::vector<std::string> &cleared_blocks, std::shared_ptr<ds_node> node) {
  if (node == nullptr)
    return;
  if (node->is_regular_file()) {
    auto file = std::dynamic_pointer_cast<ds_file_node>(node);
    auto s = file->dstatus();
    for (const auto &block: s.data_blocks()) {
      for (const auto &block_id: block.block_ids) {
        LOG(log_level::info) << "Destroying partition @ block " << block_id;
        storage_->destroy_partition(block_id);
        LOG(log_level::info) << "Destroyed partition @ block " << block_id;
        cleared_blocks.push_back(block_id);
      }
    }
  } else if (node->is_directory()) {
    auto dir = std::dynamic_pointer_cast<ds_dir_node>(node);
    LOG(log_level::info) << "Clearing directory " << dir->name();
    for (const auto &entry: *dir) {
      clear_storage(cleared_blocks, entry.second);
    }
  }
}

replica_chain directory_tree::add_block(const std::string &path,
                                        const std::string &partition_name,
                                        const std::string &partition_metadata) {
  LOG(log_level::info) << "Adding block with partition_name = " << partition_name << " and partition_metadata = "
                       << partition_metadata << " to file " << path;
  return get_node_as_file(path)->add_data_block(path, partition_name, partition_metadata, storage_, allocator_);
}

void directory_tree::remove_block(const std::string &path, const std::string &partition_name) {
  LOG(log_level::info) << "Removing block with partition_name = " << partition_name << " from file " << path;
  get_node_as_file(path)->remove_block(partition_name, storage_, allocator_);
}

void directory_tree::update_partition(const std::string &path,
                                      const std::string &old_partition_name,
                                      const std::string &new_partition_name,
                                      const std::string &partition_metadata) {
  LOG(log_level::info) << "Updating partition name = " << new_partition_name << " metadata = " << partition_metadata
                       << " for partition: " << old_partition_name << " of file " << path;
  auto replica_set = get_node_as_file(path)->dstatus().data_blocks();
  bool flag = true;
  for (auto &replica : replica_set) {
    if (replica.name == old_partition_name) {
      for (auto &block_name : replica.block_ids)
        storage_->update_partition(block_name, new_partition_name, partition_metadata);
      flag = false;
    }
  }
  if (flag)
    throw directory_ops_exception("Cannot find partition: " + old_partition_name + " under file: " + path);
  else
    get_node_as_file(path)->update_data_status_partition(old_partition_name, new_partition_name, partition_metadata);
}

int64_t directory_tree::get_capacity(const std::string &path, const std::string &partition_name) {
  LOG(log_level::info) << "Checking partition capacity with partition_name = " << partition_name << " of file " << path;
  auto replica_set = get_node_as_file(path)->dstatus().data_blocks();
  for (auto &replica :replica_set) {
    if (replica.name == partition_name)
      return storage_->storage_capacity(replica.block_ids.front());
  }
  throw directory_ops_exception("Cannot find partition: " + partition_name + " under file: " + path);
}

}
}
