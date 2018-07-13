#include "directory_tree.h"

#include "../../utils/retry_utils.h"

namespace mmux {
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
                                   const std::string &backing_path,
                                   std::size_t num_blocks,
                                   std::size_t chain_length,
                                   std::int32_t flags) {
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
  std::size_t slots_per_block = storage::block::SLOT_MAX / num_blocks;
  bool auto_scale = (flags & data_status::STATIC_PROVISIONED) != data_status::STATIC_PROVISIONED;
  for (std::size_t i = 0; i < num_blocks; ++i) {
    auto slot_begin = static_cast<int32_t>(i * slots_per_block);
    auto slot_end =
        i == (num_blocks - 1) ? storage::block::SLOT_MAX : static_cast<int32_t>((i + 1) * slots_per_block - 1);
    replica_chain chain(allocator_->allocate(chain_length, {}),
                        slot_begin,
                        slot_end,
                        chain_status::stable,
                        storage_mode::in_memory);
    assert(chain.block_names.size() == chain_length);
    blocks.push_back(chain);
    using namespace storage;
    if (chain_length == 1) {
      storage_->setup_block(chain.block_names[0],
                            path,
                            slot_begin,
                            slot_end,
                            chain.block_names,
                            auto_scale,
                            chain_role::singleton,
                            "nil");
    } else {
      for (std::size_t j = 0; j < chain_length; ++j) {
        std::string block_name = chain.block_names[j];
        std::string next_block_name = (j == chain_length - 1) ? "nil" : chain.block_names[j + 1];
        int32_t role = (j == 0) ? chain_role::head : (j == chain_length - 1) ? chain_role::tail : chain_role::mid;
        storage_->setup_block(block_name,
                              path,
                              slot_begin,
                              slot_end,
                              chain.block_names,
                              auto_scale,
                              role,
                              next_block_name);
      }
    }
  }
  auto child = std::make_shared<ds_file_node>(filename, backing_path, chain_length, blocks);
  child->backing_path(backing_path);
  child->flags(flags);
  parent->add_child(child);

  return child->dstatus();
}

data_status directory_tree::open_or_create(const std::string &path,
                                           const std::string &backing_path,
                                           std::size_t num_blocks,
                                           std::size_t chain_length,
                                           std::int32_t flags) {

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
  std::size_t slots_per_block = storage::block::SLOT_MAX / num_blocks;
  bool auto_scale = (flags & data_status::STATIC_PROVISIONED) != data_status::STATIC_PROVISIONED;
  for (std::size_t i = 0; i < num_blocks; ++i) {
    auto slot_begin = static_cast<int32_t>(i * slots_per_block);
    auto slot_end =
        i == (num_blocks - 1) ? storage::block::SLOT_MAX : static_cast<int32_t>((i + 1) * slots_per_block - 1);
    replica_chain chain(allocator_->allocate(chain_length, {}),
                        slot_begin,
                        slot_end,
                        chain_status::stable,
                        storage_mode::in_memory);
    assert(chain.block_names.size() == chain_length);
    blocks.push_back(chain);
    using namespace storage;
    if (chain_length == 1) {
      storage_->setup_block(chain.block_names[0],
                            path,
                            slot_begin,
                            slot_end,
                            chain.block_names,
                            auto_scale,
                            chain_role::singleton,
                            "nil");
    } else {
      for (std::size_t j = 0; j < chain_length; ++j) {
        std::string block_name = chain.block_names[j];
        std::string next_block_name = (j == chain_length - 1) ? "nil" : chain.block_names[j + 1];
        int32_t role = (j == 0) ? chain_role::head : (j == chain_length - 1) ? chain_role::tail : chain_role::mid;
        storage_->setup_block(block_name,
                              path,
                              slot_begin,
                              slot_end,
                              chain.block_names,
                              auto_scale,
                              role,
                              next_block_name);
      }
    }
  }
  auto child = std::make_shared<ds_file_node>(filename, backing_path, chain_length, blocks);
  child->backing_path(backing_path);
  child->flags(flags);
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

void directory_tree::remove_all(const std::string &path) {
  LOG(log_level::info) << "Removing path " << path;
  std::string ptemp = path;
  std::string child_name = directory_utils::pop_path_element(ptemp);
  auto parent = get_node_as_dir(ptemp);
  auto child = parent->get_child(child_name);
  if (child == nullptr) {
    throw directory_ops_exception("Path does not exist: " + path);
  }
  parent->remove_child(child_name);
  LOG(log_level::info) << "Removed child " << path;
  std::vector<std::string> cleared_blocks;
  clear_storage(cleared_blocks, child);
  LOG(log_level::info) << "Cleared all blocks " << path;
  allocator_->free(cleared_blocks);
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
  std::string child_name = directory_utils::pop_path_element(ptemp);
  auto old_parent = get_node_as_dir(ptemp);
  auto old_child = old_parent->get_child(child_name);
  if (old_child == nullptr) {
    throw directory_ops_exception("Path does not exist: " + old_path);
  }

  ptemp = new_path;
  child_name = directory_utils::pop_path_element(ptemp);
  auto new_parent = get_node_as_dir(ptemp);
  auto new_child = new_parent->get_child(child_name);
  if (new_child != nullptr) {
    if (new_child->is_directory())
      throw directory_ops_exception("New path is a non-empty directory: " + new_path);
    new_parent->remove_child(child_name);
  }
  old_parent->remove_child(old_child->name());
  old_child->name(child_name);
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
  std::size_t chain_length = chain.block_names.size();
  std::vector<bool> failed(chain_length);
  LOG(log_level::info) << "Resolving failures for block chain " << chain.to_string() << " @ " << path;
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
    std::string block_name = chain.block_names[i];
    auto parsed = storage::block_name_parser::parse(block_name);
    try {
      utils::retry_utils::retry(3, [parsed]() {
        using namespace ::apache::thrift::transport;
        TSocket m_sock(parsed.host, parsed.management_port);
        m_sock.open();
        LOG(log_level::info) << m_sock.getPeerHost() << ":" << m_sock.getPeerPort() << " is still live";
        m_sock.close();
        return true;
      });
      LOG(log_level::info) << "Block " << block_name << " is still live";
      fixed_chain.push_back(block_name);
    } catch (std::exception &) {
      LOG(log_level::warn) << "Block " << block_name << " has failed, removing it from chain";
      failed[i] = true;
      if (i > 0 && i < chain_length - 1 && failed[i] && !failed[i - 1]) {
        mid_failure = true;
      }
    }
  }
  // Re-organize chain as needed
  using namespace storage;
  bool auto_scale = !dstatus.is_static_provisioned();
  if (fixed_chain.empty()) {                       // All failed
    LOG(log_level::error) << "No blocks left in chain";
    throw directory_ops_exception("All blocks in the chain have failed.");
  } else if (fixed_chain.size() == 1) {            // All but one failed
    LOG(log_level::info) << "Only one block has remained in chain; setting singleton role";
    auto slot_range = storage_->slot_range(fixed_chain[0]);
    storage_->setup_block(fixed_chain[0],
                          path,
                          slot_range.first,
                          slot_range.second,
                          fixed_chain,
                          auto_scale,
                          chain_role::singleton,
                          "nil");
  } else {                                         // More than one left
    LOG(log_level::info) << fixed_chain.size() << " blocks left in chain";
    auto slot_range = storage_->slot_range(fixed_chain[0]);
    for (std::size_t i = 0; i < fixed_chain.size(); ++i) {
      std::string block_name = fixed_chain[i];
      std::string next_block_name = (i == fixed_chain.size() - 1) ? "nil" : fixed_chain[i + 1];
      int32_t role = (i == 0) ? chain_role::head : (i == fixed_chain.size() - 1) ? chain_role::tail : chain_role::mid;
      LOG(log_level::info) << "Setting block <" << block_name << ">: path=" << path << ", role=" << role << ", next="
                           << next_block_name << ">";
      storage_->setup_block(block_name,
                            path,
                            slot_range.first,
                            slot_range.second,
                            fixed_chain,
                            auto_scale,
                            role,
                            next_block_name);
    }
    if (mid_failure) {
      LOG(log_level::info) << "Resending pending requests to resolve mid block failure";
      storage_->resend_pending(fixed_chain[0]);
    }
  }
  dstatus.set_data_block(chain_pos, replica_chain(fixed_chain,
                                                  chain.slot_range.first,
                                                  chain.slot_range.second,
                                                  chain_status::stable,
                                                  storage_mode::in_memory));
  node->dstatus(dstatus);
  return dstatus.get_data_block(chain_pos);
}

replica_chain directory_tree::add_replica_to_chain(const std::string &path, const replica_chain &chain) {
  // TODO: Replace replica_chain argument with chain id
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
  bool auto_scale = !dstatus.is_static_provisioned();

  auto new_blocks = allocator_->allocate(1, chain.block_names);
  auto updated_chain = chain.block_names;
  updated_chain.insert(updated_chain.end(), new_blocks.begin(), new_blocks.end());

  // Setup forwarding path
  LOG(log_level::info) << "Setting old tail block <" << chain.block_names.back() << ">: path=" << path << ", role="
                       << chain_role::tail << ", next=" << new_blocks.front() << ">";
  auto slot_range = storage_->slot_range(chain.block_names.back());
  storage_->setup_block(chain.block_names.back(),
                        path,
                        slot_range.first,
                        slot_range.second,
                        updated_chain,
                        auto_scale,
                        chain_role::tail,
                        new_blocks.front());
  for (std::size_t i = chain.block_names.size(); i < updated_chain.size(); i++) {
    std::string block_name = updated_chain[i];
    std::string next_block_name = (i == updated_chain.size() - 1) ? "nil" : updated_chain[i + 1];
    int32_t role = (i == 0) ? chain_role::head : (i == updated_chain.size() - 1) ? chain_role::tail : chain_role::mid;
    LOG(log_level::info) << "Setting block <" << block_name << ">: path=" << path << ", role=" << role << ", next="
                         << next_block_name << ">";
    // TODO: this is incorrect -- we shouldn't be setting the chain to updated_chain right now...
    storage_->setup_block(block_name,
                          path,
                          slot_range.first,
                          slot_range.second,
                          updated_chain,
                          auto_scale,
                          role,
                          next_block_name);
  }

  LOG(log_level::info) << "Forwarding data from <" << chain.block_names.back() << "> to <" << new_blocks.front() << ">";

  storage_->forward_all(chain.block_names.back());

  LOG(log_level::info) << "Setting old tail block <" << chain.block_names.back() << ">: path=" << path << ", role="
                       << chain_role::mid << ", next=" << new_blocks.front() << ">";
  storage_->setup_block(chain.block_names.back(),
                        path,
                        slot_range.first,
                        slot_range.second,
                        updated_chain,
                        auto_scale,
                        chain_role::mid,
                        new_blocks.front());

  dstatus.set_data_block(chain_pos, replica_chain(updated_chain,
                                                  chain.slot_range.first,
                                                  chain.slot_range.second,
                                                  chain_status::stable,
                                                  storage_mode::in_memory));
  node->dstatus(dstatus);
  return dstatus.get_data_block(chain_pos);
}

void directory_tree::add_block_to_file(const std::string &path) {
  LOG(log_level::info) << "Adding new block to file " << path;
  auto node = get_node_as_file(path);
  auto ctx = node->setup_add_block(storage_, allocator_, path);
  std::thread([&, node, ctx] {
    auto start = time_utils::now_ms();
    storage_->export_slots(ctx.from_block.block_names.front());
    auto elapsed = time_utils::now_ms() - start;
    LOG(log_level::info) << "Finished export in " << elapsed << " ms";
    node->finalize_slot_range_split(storage_, ctx);
  }).detach();
}

void directory_tree::split_slot_range(const std::string &path, int32_t slot_begin, int32_t slot_end) {
  LOG(log_level::info) << "Splitting slot range (" << slot_begin << ", " << slot_end << ") @ " << path;
  auto node = get_node_as_file(path);
  auto ctx = node->setup_slot_range_split(storage_, allocator_, path, slot_begin, slot_end);
  std::thread([&, node, ctx] {
    auto start = time_utils::now_ms();
    storage_->export_slots(ctx.from_block.block_names.front());
    auto elapsed = time_utils::now_ms() - start;
    LOG(log_level::info) << "Finished export in " << elapsed << " ms";
    node->finalize_slot_range_split(storage_, ctx);
  }).detach();
}

void directory_tree::merge_slot_range(const std::string &path, int32_t slot_begin, int32_t slot_end) {
  LOG(log_level::info) << "Merging slot range (" << slot_begin << ", " << slot_end << ") @ " << path;
  auto node = get_node_as_file(path);
  auto ctx = node->setup_slot_range_merge(storage_, slot_begin, slot_end);
  std::thread([&, node, ctx] {
    auto start = time_utils::now_ms();
    storage_->export_slots(ctx.from_block.block_names.front());
    auto elapsed = time_utils::now_ms() - start;
    LOG(log_level::info) << "Finished export in " << elapsed << " ms";
    node->finalize_slot_range_merge(storage_, allocator_, ctx);
  }).detach();
}

void directory_tree::handle_lease_expiry(const std::string &path) {
  LOG(log_level::info) << "Handling expiry for " << path;
  std::string ptemp = path;
  std::string child_name = directory_utils::pop_path_element(ptemp);
  auto parent = get_node_as_dir(ptemp);
  std::vector<std::string> cleared_blocks;
  parent->handle_lease_expiry(cleared_blocks, child_name, storage_);
  LOG(log_level::info) << "Handled lease expiry, clearing freeing blocks for " << path;
  allocator_->free(cleared_blocks);
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

std::shared_ptr<ds_node> directory_tree::touch_node_path(const std::string &path, const std::uint64_t time) const {
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
      for (const auto &block_name: block.block_names) {
        LOG(log_level::info) << "Clearing block " << block_name;
        storage_->reset(block_name);
        LOG(log_level::info) << "Cleared block " << block_name;
        cleared_blocks.push_back(block_name);
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

}
}