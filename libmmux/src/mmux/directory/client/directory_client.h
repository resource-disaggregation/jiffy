#ifndef MMUX_DIRECTORY_CLIENT_H
#define MMUX_DIRECTORY_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "../directory_ops.h"
#include "../fs/directory_service.h"

namespace mmux {
namespace directory {

class directory_client : public directory_interface {
 public:
  typedef directory_serviceClient thrift_client;

  directory_client() = default;
  ~directory_client() override;
  directory_client(const std::string &hostname, int port);
  void connect(const std::string &hostname, int port);
  void disconnect();
  void create_directory(const std::string &path) override;
  void create_directories(const std::string &path) override;
  data_status open(const std::string &path) override;
  data_status create(const std::string &path,
                     const std::string &backing_path,
                     std::size_t num_blocks,
                     std::size_t chain_length,
                     std::int32_t flags) override;
  data_status open_or_create(const std::string &path,
                             const std::string &backing_path,
                             std::size_t num_blocks,
                             std::size_t chain_length,
                             std::int32_t flags) override;
  bool exists(const std::string &path) const override;
  std::uint64_t last_write_time(const std::string &path) const override;
  perms permissions(const std::string &path) override;
  void permissions(const std::string &path, const perms &prms, perm_options opts) override;
  void remove(const std::string &path) override;
  void remove_all(const std::string &path) override;
  void sync(const std::string &path, const std::string &backing_path) override;
  void rename(const std::string &old_path, const std::string &new_path) override;
  file_status status(const std::string &path) const override;
  std::vector<directory_entry> directory_entries(const std::string &path) override;
  std::vector<directory_entry> recursive_directory_entries(const std::string &path) override;
  data_status dstatus(const std::string &path) override;
  bool is_regular_file(const std::string &path) override;
  bool is_directory(const std::string &path) override;

  // Management Ops
  replica_chain resolve_failures(const std::string &path, const replica_chain &chain);
  replica_chain add_replica_to_chain(const std::string &path, const replica_chain &chain);
  void add_block_to_file(const std::string &path);
  void split_slot_range(const std::string &path, int32_t slot_begin, int32_t slot_end);
  void merge_slot_range(const std::string &path, int32_t slot_begin, int32_t slot_end);
  void dump(const std::string &path, const std::string &backing_path);
  void load(const std::string &path, const std::string &backing_path);
  virtual void touch(const std::string &path);
  virtual void handle_lease_expiry(const std::string &path);

 private:
  std::shared_ptr<apache::thrift::transport::TSocket> socket_{};
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  std::shared_ptr<thrift_client> client_{};
};

}
}

#endif //MMUX_DIRECTORY_CLIENT_H
