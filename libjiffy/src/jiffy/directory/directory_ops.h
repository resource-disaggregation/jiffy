#ifndef JIFFY_DIRECTORY_OPS_H
#define JIFFY_DIRECTORY_OPS_H

#include <exception>
#include <string>
#include <algorithm>
#include <utility>
#include <vector>
#include <chrono>
#include <sstream>
#include <iostream>
#include <map>

namespace jiffy {
namespace directory {
/* Directory exception class */
class directory_ops_exception : public std::exception {
 public:

  /*
   * Constructor
   */

  explicit directory_ops_exception(std::string msg) : msg_(std::move(msg)) {}

  /*
   * Fetch exception message
   */

  char const *what() const noexcept override {
    return msg_.c_str();
  }
 private:
  /* Exception message */
  std::string msg_;
};
/* Permission class */
class perms {
 public:
  static const perms none;

  static const perms owner_read;
  static const perms owner_write;
  static const perms owner_exec;
  static const perms owner_all;

  static const perms group_read;
  static const perms group_write;
  static const perms group_exec;
  static const perms group_all;

  static const perms others_read;
  static const perms others_write;
  static const perms others_exec;
  static const perms others_all;

  static const perms all;

  static const perms set_uid;
  static const perms set_gid;
  static const perms sticky_bit;

  static const perms mask;

  /**
   * @brief Constructor
   */

  perms() : prms_(0) {}

  /**
   * @brief Constructor
   * @param prms permissions
   */

  explicit perms(uint16_t prms) : prms_(prms) {}

  /**
   * @brief Bitwise AND operator
   * @param p1 Permission
   * @param p2 Permission
   * @return Bitwise AND result
   */

  friend inline perms operator&(const perms &p1, const perms &p2) {
    return perms(p1.prms_ & p2.prms_);
  }

  /**
   * @brief Bitwise OR operator
   * @param p1 Permission
   * @param p2 Permission
   * @return Bitwise OR result
   */

  friend inline perms operator|(const perms &p1, const perms &p2) {
    return perms(p1.prms_ | p2.prms_);
  }

  /**
   * @brief XOR operator
   * @param p1 Permission
   * @param p2 Permission
   * @return XOR result
   */

  friend inline perms operator^(const perms &p1, const perms &p2) {
    return perms(p1.prms_ ^ p2.prms_);
  }

  /**
   * @brief AND assignment operator
   * @param p Permission
   * @return AND assignment result
   */

  perms &operator&=(const perms &p) {
    prms_ &= p.prms_;
    return *this;
  }

  /**
   * @brief OR assignment operator
   * @param p Permission
   * @return OR assignment result
   */

  perms &operator|=(const perms &p) {
    prms_ |= p.prms_;
    return *this;
  }

  /**
   * @brief XOR assignment operator
   * @param p Permission
   * @return XOR assignment result
   */

  perms &operator^=(const perms &p) {
    prms_ ^= p.prms_;
    return *this;
  }

  /**
   * @brief NOT operator
   * @param p Permission
   * @return NOT result
   */

  perms operator~() const {
    return perms(~prms_);
  }

  /**
   * @brief Assignment operator
   * @param p Permission
   * @return Assignment result
   */

  perms &operator=(uint16_t prms) {
    prms_ = prms;
    return *this;
  }

  /**
   * @brief Brackets operator
   * @param p Permission
   * @return Permission
   */

  const uint16_t &operator()() const {
    return prms_;
  }

  /**
   * @brief Equal Operator
   * @param p Permission
   * @return Bool value, true if two equal
   */

  bool operator==(const perms &other) const {
    return prms_ == other.prms_;
  }

  /**
   * @brief Not equal Operator
   * @param p Permission
   * @return bool value, true if two not equal
   */

  bool operator!=(const perms &other) const {
    return prms_ != other.prms_;
  }

  /**
   * @brief Output stream
   * @param out Output stream
   * @param p Permission
   * @return Output stream
   */

  friend std::ostream &operator<<(std::ostream &out, const perms &p) {
    out << ((p & owner_read) != none ? "r" : "-")
        << ((p & owner_write) != none ? "w" : "-")
        << ((p & owner_exec) != none ? "x" : "-")
        << ((p & group_read) != none ? "r" : "-")
        << ((p & group_write) != none ? "w" : "-")
        << ((p & group_exec) != none ? "x" : "-")
        << ((p & others_read) != none ? "r" : "-")
        << ((p & others_write) != none ? "w" : "-")
        << ((p & others_exec) != none ? "x" : "-");
    return out;
  }

 private:
  /* Permission */
  uint16_t prms_;
};

/* Permission options */
enum perm_options {
  replace = 0,
  add = 1,
  remove = 2
};

/* File types */
enum file_type {
  none = 0,
  regular = 1,
  directory = 2
};

/* Storage mode */
enum storage_mode {
  in_memory = 0,
  in_memory_grace = 1,
  on_disk = 2
};

/* Replica chain structure */
struct replica_chain {
  /* Partition name */
  std::string name;
  /* Block identifier */
  std::vector<std::string> block_ids;
  /* Partition metadata */
  std::string metadata;
  /* Storage mode */
  storage_mode mode;
  typedef std::pair<int32_t, int32_t> slot_range;
  /**
   * Default Constructor
   */
  replica_chain() : mode(storage_mode::in_memory) {}

  /**
   * Constructor
   * @param block_ids Block IDs
   * @param mode Storage mode
   */
  replica_chain(const std::vector<std::string> &block_ids, storage_mode mode) {
    this->block_ids = block_ids;
    this->mode = mode;
  }

  /**
   * Constructor
   * @param block_ids Block IDs
   */
  replica_chain(const std::vector<std::string> block_ids) : mode(storage_mode::in_memory) {
    this->block_ids = block_ids;
  }

  /**
   * Fetch head block name
   * @return Head block name
   */
  const std::string &head() const {
    return block_ids.front();
  }

  /**
   * Fetch tail block name
   * @return Tail block name
   */
  const std::string &tail() const {
    return block_ids.back();
  }

  /**
   * Convert replica chain to string
   * @return Replica chain string
   */
  std::string to_string() const {
    std::string out = "<";
    for (const auto &name: block_ids) {
      out += name + ", ";
    }
    out.pop_back();
    out.pop_back();
    out += "> :: { mode : " + std::to_string(mode) + " }";
    return out;
  }

  /**
   * Equal operator
   * @param other Replica chain to compare with
   * @return Bool value, true if equal
   */
  bool operator==(const replica_chain &other) const {
    return block_ids == other.block_ids;
  }

  /**
   * Not equal operator
   * @param other Replica chain to compare with
   * @return Bool value, true if not equal
   */
  bool operator!=(const replica_chain &other) const {
    return !(*this == other);
  }

  bool operator<(const replica_chain &other) const {
    return fetch_slot_range().first < other.fetch_slot_range().first;
  }

  /**
   * @brief Fetch slot range from name
   * @return Slot range pair
   */
  slot_range fetch_slot_range() const {
    std::string delimiter = "_";
    int32_t slot_begin = atoi(name.substr(0, name.find(delimiter)).c_str());
    int32_t slot_end = atoi(name.substr(name.find(delimiter) + 1, name.length()).c_str());
    return std::make_pair(slot_begin, slot_end);
  }
};

/* File status class */
class file_status {
 public:
  /**
   * Constructor
   */
  file_status()
      : type_(file_type::none),
        permissions_(),
        last_write_time_(0) {}
  /**
   * Constructor
   * @param type File type
   * @param prms File permissions
   * @param last_write_time Last write time
   */
  file_status(file_type type, const perms &prms, std::uint64_t last_write_time)
      : type_(type),
        permissions_(prms),
        last_write_time_(last_write_time) {}
  /**
   * Fetch file type
   * @return File type
   */
  const file_type &type() const {
    return type_;
  }
  /**
   * Fetch file permissions
   * @return File permissions
   */
  const perms &permissions() const {
    return permissions_;
  }

  /**
   * Set file permissions
   * @param prms File permissions
   */
  void permissions(const perms &prms) {
    permissions_ = prms;
  }

  /**
   * Fetch last write time
   * @return File last write time
   */
  const std::uint64_t &last_write_time() const {
    return last_write_time_;
  }

  /**
   * Set file last write time
   * @param time File last write time
   */
  void last_write_time(std::uint64_t time) {
    last_write_time_ = time;
  }

 private:
  /* File type */
  file_type type_;
  /* File permission */
  perms permissions_;
  /* Last write time */
  std::uint64_t last_write_time_;
};

/* Directory entry class */
class directory_entry {
 public:
  directory_entry() = default;

  /**
   * Constructor
   * @param name File name
   * @param status File status
   */
  directory_entry(std::string name, const file_status &status)
      : name_(std::move(name)),
        status_(status) {}
  /**
   * Fetch file name
   * @return File name
   */
  const std::string &name() const {
    return name_;
  }
  /**
   * Fetch file type
   * @return File type
   */
  const file_type &type() const {
    return status_.type();
  }
  /**
   * Fetch file permission
   * @return File permission
   */
  const perms &permissions() const {
    return status_.permissions();
  }
  /**
   * Fetch last write time
   * @return File last write time
   */
  const std::uint64_t &last_write_time() const {
    return status_.last_write_time();
  }

  /**
   * Fetch file status
   * @return File status
   */
  const file_status &status() const {
    return status_;
  }

 private:
  /* File name */
  std::string name_;
  /* File status */
  file_status status_;
};

/* Data status */
class data_status {
 public:
  static const std::int32_t PINNED = 0x01;
  static const std::int32_t STATIC_PROVISIONED = 0x02;
  static const std::int32_t MAPPED = 0x04;
  static const std::size_t MAX_NAME_LEN = 256;
  static const std::size_t MAX_METADATA_LEN = 8192;
  static const std::size_t MAX_TAG_KEYLEN = 256;
  static const std::size_t MAX_TAG_VALLEN = 256;
  static const std::size_t MAX_NUM_TAGS = 256;

  /**
   * Constructor
   */
  data_status() : chain_length_(1), flags_(0) {}

  /**
   * Constructor
   * @param type Data type
   * @param backing_path File backing path
   * @param chain_length Chain length
   * @param blocks Data blocks
   * @param flags Flags
   * @param tags Tags
   */
  data_status(std::string type,
              std::string backing_path,
              std::size_t chain_length,
              std::vector<replica_chain> blocks,
              int32_t flags,
              const std::map<std::string, std::string> &tags)
      : type_(type),
        backing_path_(std::move(backing_path)),
        chain_length_(chain_length),
        data_blocks_(std::move(blocks)),
        tags_(tags),
        flags_(flags) {
  }

  /**
   * @brief Get data type
   * @return The data type
   */
  const std::string &type() const {
    return type_;
  }
  /**
   * @brief Fetch data blocks
   * @return Data blocks
   */

  const std::vector<replica_chain> &data_blocks() const {
    return data_blocks_;
  }

  /**
   * @brief Fetch all block's storage mode
   * @return Storage modes
   */

  std::vector<storage_mode> mode() const {
    std::vector<storage_mode> modes(data_blocks_.size());
    for (size_t i = 0; i < data_blocks_.size(); i++) {
      modes[i] = data_blocks_[i].mode;
    }

    return modes;
  }

  /**
   * @brief Set storage mode for block
   * @param block_id Block identifier
   * @param mode Storage mode
   */

  void mode(size_t block_id, storage_mode mode) {
    data_blocks_.at(block_id).mode = mode;
  }

  /**
   * @brief Set storage mode for all blocks
   * @param mode Storage mode
   */

  void mode(storage_mode mode) {
    for (size_t i = 0; i < data_blocks_.size(); i++) {
      data_blocks_[i].mode = mode;
    }
  }

  /**
   * @brief Mark block as dumped
   * Mark on disk and clear
   * @param block_id Block identifier
   * @return Block names
   */

  std::vector<std::string> mark_dumped(size_t block_id) {
    data_blocks_.at(block_id).mode = storage_mode::on_disk;
    std::vector<std::string> chain = data_blocks_.at(block_id).block_ids;
    data_blocks_.at(block_id).block_ids.clear();
    return chain;
  }

  /**
   * @brief Mark block as loaded
   * @param block_id Block identifier
   * @param chain Block chain
   */

  void mark_loaded(size_t block_id, const std::vector<std::string> chain) {
    data_blocks_.at(block_id).mode = storage_mode::in_memory;
    data_blocks_.at(block_id).block_ids = chain;
  }

  /**
   * @brief Fetch backing path
   * @return Backing path
   */

  const std::string &backing_path() const {
    return backing_path_;
  }

  /**
   * @brief Set backing path
   * @param backing_path Backing path
   */

  void backing_path(const std::string &backing_path) {
    backing_path_ = backing_path;
  }

  /**
   * @brief Fetch chain length
   * @return Chain length
   */

  std::size_t chain_length() const {
    return chain_length_;
  }

  /**
   * @brief Set chain length
   * @param chain_length Chain length
   */

  void chain_length(std::size_t chain_length) {
    chain_length_ = chain_length;
  }

  /**
   * @brief Remove all data blocks
   */

  void remove_all_data_blocks() {
    data_blocks_.clear();
  }

  /**
   * @brief Find replica chain in data blocks
   * @param chain Replica chain
   * @return Replica chain offset
   */

  std::size_t find_replica_chain(const replica_chain &chain) {
    auto it = std::find(data_blocks_.begin(), data_blocks_.end(), chain);
    if (it == data_blocks_.end()) {
      throw std::logic_error("Could not find replica chain " + chain.to_string());
    }
    return static_cast<size_t>(it - data_blocks_.begin());
  }

  /**
   * @brief Add data block
   * @param block Block
   * @param i Data block offset
   */
  void add_data_block(const replica_chain &block, std::size_t i) {
    data_blocks_.insert(data_blocks_.begin() + i, block);
  }

  /**
   * @brief Add data block
   * @param block Block
   */
  void add_data_block(const replica_chain &block) {
    data_blocks_.push_back(block);
  }

  /**
   * @brief Remove data block
   * @param i Data block offset
   */

  void remove_data_block(std::size_t i) {
    data_blocks_.erase(data_blocks_.begin() + i);
  }

  /**
   * @brief Remove data block by partition name
   * @param partition_name Name of partition at data block.
   * @return True if removal was successful, false otherwise
   */
  bool remove_data_block(const std::string &partition_name, replica_chain &block) {
    std::vector<replica_chain>::iterator it;
    for (it = data_blocks_.begin(); it != data_blocks_.end(); ++it) {
      if (it->name == partition_name) {
        break;
      }
    }
    if (it != data_blocks_.end()) {
      block = *it;
      data_blocks_.erase(it);
      return true;
    }
    return false;
  }

  /**
   * @brief Set data block
   * @param i Data block offset
   * @param chain Replica chain
   */

  void set_data_block(std::size_t i, replica_chain &&chain) {
    data_blocks_[i] = std::move(chain);
  }

  /**
   * @brief Fetch data block
   * @param i Data block offset
   * @return Replica chain
   */

  const replica_chain &get_data_block(std::size_t i) const {
    return data_blocks_[i];
  }

  /**
   * @brief Get data block name.
   * @param i Data block offset
   * @return Data block name
   */

  const std::string &get_data_block_name(std::size_t i) const {
    return data_blocks_[i].name;
  }

  /**
   * @brief Set data block name
   * @param i Data block offset
   * @param name Data block name
   */

  void set_data_block_name(std::size_t i, const std::string &name) {
    if (name.size() > MAX_NAME_LEN) {
      throw directory_ops_exception("Data block name size > " + std::to_string(MAX_NAME_LEN));
    }
    data_blocks_[i].name = name;
  }

  /**
   * @brief Set new name for partition
   * @param old_name Old partition name
   * @param new_name New partition name
   */
  void set_partition_name(const std::string &old_name, const std::string &new_name) {
    for (auto &replica : data_blocks_) {
      if (replica.name == old_name) {
        replica.name = new_name;
        return;
      }
    }
    throw directory_ops_exception("Partition not found: " + old_name);
  }

  /**
   * @brief Fetch data block metadata
   * @param i Data block offset
   * @return Data block metadata
   */

  const std::string &get_data_block_metadata(std::size_t i) const {
    return data_blocks_[i].metadata;
  }

  /**
   * @brief Set data block metadata
   * @param i Data block offset
   * @param metadata Data block metadata
   */

  void set_data_block_metadata(std::size_t i, const std::string &metadata) {
    if (metadata.size() > MAX_METADATA_LEN) {
      throw directory_ops_exception("Data block metadata size > " + std::to_string(MAX_METADATA_LEN));
    }
    data_blocks_[i].metadata = metadata;
  }

  /**
   * @brief Set new metadata for partition
   * @param name Partition name
   * @param metadata New partition metadata
   */
  void set_partition_metadata(const std::string &name, const std::string &metadata) {
    for (auto &replica : data_blocks_) {
      if (replica.name == name) {
        replica.metadata = metadata;
        return;
      }
    }
    throw directory_ops_exception("Partition not found: " + name);
  }
  /**
   * @brief Add tag
   * @param key Key
   * @param value Value
   */

  void add_tag(const std::string &key, const std::string &value) {
    if (key.size() > MAX_TAG_KEYLEN) {
      throw directory_ops_exception("Tag key size > " + std::to_string(MAX_TAG_KEYLEN));
    }
    if (value.size() > MAX_TAG_VALLEN) {
      throw directory_ops_exception("Tag value size > " + std::to_string(MAX_TAG_VALLEN));
    }
    auto it = tags_.find(key);
    if (it != tags_.end()) {
      it->second = value;
    } else {
      if (tags_.size() + 1 > MAX_NUM_TAGS) {
        throw directory_ops_exception("Reached tag limit for file: " + std::to_string(MAX_NUM_TAGS));
      }
      tags_[key] = value;
    }
  }

  /**
   * @brief Add tags
   * @param tags Tags
   */

  void add_tags(const std::map<std::string, std::string> &tags) {
    for (const auto &tag: tags) {
      add_tag(tag.first, tag.second);
    }
  }

  /**
   * @brief Fetch tag for specific key
   * @param key Key
   * @return Tag for key
   */

  std::string get_tag(const std::string &key) const {
    if (tags_.find(key) != tags_.end())
      return tags_.at(key);
    throw directory_ops_exception("tag " + key + " not found");
  }

  /**
   * @brief Fetch all tags
   * @return Tags
   */

  const std::map<std::string, std::string> &get_tags() const {
    return tags_;
  }

  /**
   * @brief Form all data information into string
   * @return String of data information
   */

  std::string to_string() const {
    std::string out = "{ backing_path: " + backing_path_ + ", chain_length: " + std::to_string(chain_length_)
        + ", data_blocks: { ";
    for (const auto &chain: data_blocks_) {
      out += chain.to_string() + ", ";
    }
    out.pop_back();
    out.pop_back();
    out += " }}";
    return out;
  }

  /**
   * @brief Fetch flags
   * @return Flags
   */

  std::int32_t flags() const {
    return flags_;
  }

  /**
   * @brief Set flags
   * @param flags Flags
   */

  void flags(std::int32_t flags) {
    flags_ = flags;
  }

  /**
   * @brief Set pinned
   */

  void set_pinned() {
    flags_ |= PINNED;
  }

  /**
   * @brief Set static provisioned
   */

  void set_static_provisioned() {
    flags_ |= STATIC_PROVISIONED;
  }

  /**
   * @brief Set mapped
   */

  void set_mapped() {
    flags_ |= MAPPED;
  }

  /**
   * @brief Clear all flags
   */

  void clear_flags() {
    flags_ = 0;
  }

  /**
   * @brief Check if data is pinned
   * @return Bool value, true if data is pinned
   */

  bool is_pinned() const {
    return (flags_ & PINNED) == PINNED;
  }

  /**
   * @brief Check if data is static provisioned
   * @return Bool value, true if data is static provisioned
   */

  bool is_static_provisioned() const {
    return (flags_ & STATIC_PROVISIONED) == STATIC_PROVISIONED;
  }

  /**
   * @brief Check if data is mapped
   * @return Bool value, true if data is mapped
   */

  bool is_mapped() const {
    return (flags_ & MAPPED) == MAPPED;
  }

 private:
  /* Type data type */
  std::string type_;
  /* Backing path */
  std::string backing_path_;
  /* Replica chain */
  std::size_t chain_length_;
  /* Data blocks */
  std::vector<replica_chain> data_blocks_;
  /* Tags */
  std::map<std::string, std::string> tags_;
  /* Flags */
  std::int32_t flags_;
};

/* Directory operations virtual class */
class directory_ops {
 public:
  virtual ~directory_ops() = default;
  /**
   * @brief Create directory
   * @param Directory path
   */

  virtual void create_directory(const std::string &path) = 0;

  /**
   * @brief Create multiple directories
   * @param Directory path
   */

  virtual void create_directories(const std::string &path) = 0;

  /**
   * @brief Open a file given file path
   * @param path File path
   * @return Data status
   */

  virtual data_status open(const std::string &path) = 0;

  /**
   * @brief Create file
   * @param path File path
   * @param backing_path File backing path
   * @param num_blocks Number of blocks
   * @param chain_length Replica chain length
   * @param flags Flags
   * @param permissions File permissions
   * @param tags Tags
   * @return Data status
   */

  virtual data_status create(const std::string &path,
                             const std::string &type,
                             const std::string &backing_path,
                             int32_t num_blocks,
                             int32_t chain_length,
                             int32_t flags,
                             int32_t permissions,
                             const std::vector<std::string> &block_names,
                             const std::vector<std::string> &block_metadata,
                             const std::map<std::string, std::string> &tags) = 0;

  /**
   * @brief Open or create a file
   * Open file if file exists
   * If not, create it
   * @param path File path
   * @param backing_path File backing path
   * @param num_blocks Number of blocks
   * @param chain_length Replica chain length
   * @param flags Flags
   * @param permissions File permissions
   * @param tags Tags
   * @return File data status
   */

  virtual data_status open_or_create(const std::string &path,
                                     const std::string &type,
                                     const std::string &backing_path,
                                     int32_t num_blocks,
                                     int32_t chain_length,
                                     int32_t flags,
                                     int32_t permissions,
                                     const std::vector<std::string> &block_names,
                                     const std::vector<std::string> &block_metadata,
                                     const std::map<std::string, std::string> &tags) = 0;

  /**
   * @brief Check if the file exists
   * @param path File path
   * @return Bool value, true if file exists
   */

  virtual bool exists(const std::string &path) const = 0;

  /**
   * @brief Fetch last write time of file
   * @param path File path
   * @return Last write time
   */

  virtual std::uint64_t last_write_time(const std::string &path) const = 0;

  /**
   * @brief Fetch file permissions
   * @param path File path
   * @return File permissions
   */

  virtual perms permissions(const std::string &path) = 0;

  /**
   * @brief Set permissions of a file
   * @param path File path
   * @param prms Permission
   * @param opts Permission options
   */

  virtual void permissions(const std::string &path, const perms &permissions, perm_options opts) = 0;

  /**
   * @brief Remove file given path
   * @param path File path
   */

  virtual void remove(const std::string &path) = 0;

  /**
   * @brief Remove all files under given directory
   * @param path Directory path
   */

  virtual void remove_all(const std::string &path) = 0;

  /**
   * @brief Write all dirty blocks back to persistent storage
   * @param path File path
   * @param backing_path File backing path
   */

  virtual void sync(const std::string &path, const std::string &backing_path) = 0;

  /**
   * @brief Write all dirty blocks back to persistent storage and clear the block
   * @param path File path
   * @param backing_path File backing path
   */

  virtual void dump(const std::string &path, const std::string &backing_path) = 0;

  /**
   * @brief Load blocks from persistent storage
   * @param path File path
   * @param backing_path File backing path
   */

  virtual void load(const std::string &path, const std::string &backing_path) = 0;

  /**
   * @brief Rename a file
   * If new file path is a directory path, then put old path file under that directory.
   * If new file path is a file path, overwrite that file with old path file.
   * If new file path doesn't exist, put old path file in new path
   * @param old_path Original file path
   * @param new_path New file path
   */

  virtual void rename(const std::string &old_path, const std::string &new_path) = 0;

  /**
   * @brief Fetch file status
   * @param path File path
   * @return File status
   */

  virtual file_status status(const std::string &path) const = 0;

  /**
   * @brief Collect all entries of files in the directory
   * @param path Directory path
   * @return Directory entries
   */

  virtual std::vector<directory_entry> directory_entries(const std::string &path) = 0;

  /**
   * @brief Collect all entries of files in the directory recursively
   * @param path Directory path
   * @return Directory recursive entries
   */

  virtual std::vector<directory_entry> recursive_directory_entries(const std::string &path) = 0;

  /**
   * @brief Collect data status
   * @param path File path
   * @return Data status
   */

  virtual data_status dstatus(const std::string &path) = 0;

  /**
   * @brief Add tags to the file status
   * @param path File path
   * @param tags Tags
   */

  virtual void add_tags(const std::string &path, const std::map<std::string, std::string> &tags) = 0;

  /**
   * @brief Check if path is regular file
   * @param path File path
   * @return Bool value
   */

  virtual bool is_regular_file(const std::string &path) = 0;

  /**
   * @brief Check if path is directory
   * @param path File path
   * @return Bool value
   */

  virtual bool is_directory(const std::string &path) = 0;
};

/* Directory management operations virtual class */
class directory_management_ops {
 public:

  // Lease management

  /**
   * @brief Touch file or directory at given path
   * First touch all nodes along the path, then touch all nodes under the path
   * @param path File or directory path
   */
  virtual void touch(const std::string &path) = 0;

  /**
   * @brief Handle lease expiry
   * @param path File path
   */
  virtual void handle_lease_expiry(const std::string &path) = 0;

  // Chain replication

  /**
   * @brief Resolve failure
   * @param path File path
   * @param chain Replica chain
   * @return Replica chain
   */

  virtual replica_chain resolve_failures(const std::string &path, const replica_chain &chain) = 0;

  /**
   * @brief Add a new replica to the chain of the given path
   * @param path File path
   * @param chain Replica chain
   * @return Replica chain
   */

  virtual replica_chain add_replica_to_chain(const std::string &path, const replica_chain &chain) = 0;

  // Block allocation

  /**
   * @brief Add a block to file
   * @param path File path
   * @param partition_name Name of the partition at new block
   * @param partition_metadata Metadata of the partition at new block
   * @return Replica chain corresponding to the new block
   */

  virtual replica_chain add_block(const std::string &path,
                                  const std::string &partition_name,
                                  const std::string &partition_metadata) = 0;
      
  /**
   * @brief Remove block from the file
   * @param path File path
   * @param partition_name Name of the partition at the block.
   */

  virtual void remove_block(const std::string &path, const std::string &block_name) = 0;

  /**
   * @brief Update partition name and metadata
   * @param path File path
   * @param old_partition_name Old partition name
   * @param new_partition_name New partition name to be set
   * @param partition_metadata New partition metadata to be set
   */
  
  virtual void update_partition(const std::string &path,
                                const std::string &old_partition_name,
                                const std::string &new_partition_name,
                                const std::string &partition_metadata) = 0;
  
  /**
   * @brief Fetch the capacity of the partition
   * @param path File path
   * @param partition_name Partition name
   * @return Storage capacity
   */

  virtual int64_t get_capacity(const std::string &path, const std::string &partition_name) = 0;
};

class directory_interface : public directory_ops, public directory_management_ops {};

}
}

#endif //JIFFY_DIRECTORY_OPS_H
