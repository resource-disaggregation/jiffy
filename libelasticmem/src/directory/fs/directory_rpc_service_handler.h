#ifndef ELASTICMEM_DIRECTORY_RPC_SERVICE_HANDLER_H
#define ELASTICMEM_DIRECTORY_RPC_SERVICE_HANDLER_H

#include "directory_rpc_service.h"
#include "directory_tree.h"

namespace elasticmem {
namespace directory {
class directory_rpc_service_handler : public directory_rpc_serviceIf {
 public:
  explicit directory_rpc_service_handler(std::shared_ptr<directory_tree> shard);
  void create_directory(const std::string &path) override;
  void create_directories(const std::string &path) override;
  void create_file(const std::string &path, const std::string &persistent_store_prefix) override;
  bool exists(const std::string &path) override;
  int64_t file_size(const std::string &path) override;
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
  rpc_storage_mode mode(const std::string &path) override;
  void persistent_store_prefix(std::string &_return, const std::string &path) override;
  void data_blocks(std::vector<std::string> &_return, const std::string &path) override;
  bool is_regular_file(const std::string &path) override;
  bool is_directory(const std::string &path) override;
 private:
  directory_rpc_service_exception make_exception(directory_service_exception &ex) const;
  std::shared_ptr<directory_tree> shard_;
};
}
}

#endif //ELASTICMEM_DIRECTORY_RPC_SERVICE_HANDLER_H
