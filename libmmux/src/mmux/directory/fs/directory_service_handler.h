#ifndef MMUX_DIRECTORY_RPC_SERVICE_HANDLER_H
#define MMUX_DIRECTORY_RPC_SERVICE_HANDLER_H

#include "directory_service.h"
#include "directory_tree.h"

namespace mmux {
namespace directory {
class directory_service_handler : public directory_serviceIf {
 public:
  explicit directory_service_handler(std::shared_ptr<directory_tree> shard);
  void create_directory(const std::string &path) override;
  void create_directories(const std::string &path) override;
  void open(rpc_data_status &_return, const std::string &path) override;
  void create(rpc_data_status &_return,
              const std::string &path,
              const std::string &backing_path,
              int32_t num_blocks,
              int32_t chain_length,
              int32_t flags) override;
  void open_or_create(rpc_data_status &_return,
                      const std::string &path,
                      const std::string &backing_path,
                      int32_t num_blocks,
                      int32_t chain_length,
                      int32_t flags) override;
  bool exists(const std::string &path) override;
  int64_t last_write_time(const std::string &path) override;
  void set_permissions(const std::string &path, rpc_perms perms, rpc_perm_options opts) override;
  rpc_perms get_permissions(const std::string &path) override;
  void remove(const std::string &path) override;
  void remove_all(const std::string &path) override;
  void rename(const std::string &old_path, const std::string &new_path) override;
  void status(rpc_file_status &_return, const std::string &path) override;
  void directory_entries(std::vector<rpc_dir_entry> &_return, const std::string &path) override;
  void recursive_directory_entries(std::vector<rpc_dir_entry> &_return, const std::string &path) override;
  void dstatus(rpc_data_status &_return, const std::string &path) override;
  bool is_regular_file(const std::string &path) override;
  bool is_directory(const std::string &path) override;
  void reslove_failures(rpc_replica_chain& _return, const std::string& path, const rpc_replica_chain& chain) override;
  void add_replica_to_chain(rpc_replica_chain &_return, const std::string &path, const rpc_replica_chain &chain) override;
  void add_block_to_file(const std::string &path) override;
  void split_slot_range(const std::string &path, int32_t slot_begin, int32_t slot_end) override;
  void merge_slot_range(const std::string &path, const int32_t slot_begin, const int32_t slot_end) override ;
  void sync(const std::string &path, const std::string &backing_path) override;
  void dump(const std::string &path, const std::string &backing_path) override;
  void load(const std::string &path, const std::string &backing_path) override;
 private:
  directory_service_exception make_exception(directory_ops_exception &ex) const;
  std::shared_ptr<directory_tree> shard_;
};
}
}

#endif //MMUX_DIRECTORY_RPC_SERVICE_HANDLER_H
