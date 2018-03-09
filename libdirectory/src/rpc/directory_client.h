#ifndef ELASTICMEM_DIRECTORY_CLIENT_H
#define ELASTICMEM_DIRECTORY_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "../directory_service.h"
#include "directory_rpc_service.h"

namespace elasticmem {
namespace directory {

class directory_client : public directory_service {
 public:
  typedef directory_rpc_serviceClient thrift_client;

  directory_client() = default;
  ~directory_client() override;
  directory_client(const std::string &hostname, int port);
  void connect(const std::string &hostname, int port);
  void disconnect();
  void create_directory(const std::string &path) override;
  void create_directories(const std::string &path) override;
  void create_file(const std::string &path) override;
  bool exists(const std::string &path) const override;
  std::size_t file_size(const std::string &path) const override;
  std::time_t last_write_time(const std::string &path) const override;
  perms permissions(const std::string &path) override;
  void permissions(const std::string &path, const perms &permsissions, perm_options opts) override;
  void remove(const std::string &path) override;
  void remove_all(const std::string &path) override;
  void rename(const std::string &old_path, const std::string &new_path) override;
  file_status status(const std::string &path) const override;
  std::vector<directory_entry> directory_entries(const std::string &path) override;
  std::vector<directory_entry> recursive_directory_entries(const std::string &path) override;
  data_status dstatus(const std::string &path) override;
  bool is_regular_file(const std::string &path) override;
  bool is_directory(const std::string &path) override;
  storage_mode mode(const std::string &path) override;
  std::vector<std::string> nodes(const std::string &path) override;

 private:
  std::shared_ptr<apache::thrift::transport::TSocket> socket_;
  std::shared_ptr<apache::thrift::transport::TTransport> transport_;
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_;
  std::shared_ptr<thrift_client> client_;
};

}
}

#endif //ELASTICMEM_DIRECTORY_CLIENT_H
