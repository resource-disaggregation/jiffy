#ifndef MMUX_BLOCK_CLIENT_H
#define MMUX_BLOCK_CLIENT_H

#include <thrift/transport/TSocket.h>
#include <libcuckoo/cuckoohash_map.hh>
#include "../service/block_request_service.h"
#include "../service/block_response_service.h"
#include "../../utils/client_cache.h"

namespace mmux {
namespace storage {

class block_client {
 public:
  class command_response_reader {
   public:
    command_response_reader() = default;

    explicit command_response_reader(std::shared_ptr<apache::thrift::protocol::TProtocol> prot)
        : prot_(std::move(prot)) {
      iprot_ = prot_.get();
    }

    int64_t recv_response(std::vector<std::string> &out) {
      using namespace ::apache::thrift::protocol;
      using namespace ::apache::thrift;
      int32_t rseqid = 0;
      std::string fname;
      TMessageType mtype;

      this->iprot_->readMessageBegin(fname, mtype, rseqid);
      if (mtype == T_EXCEPTION) {
        TApplicationException x;
        x.read(this->iprot_);
        this->iprot_->readMessageEnd();
        this->iprot_->getTransport()->readEnd();
        throw x;
      }
      block_response_service_response_args result;
      result.read(this->iprot_);
      this->iprot_->readMessageEnd();
      this->iprot_->getTransport()->readEnd();
      if (result.__isset.seq && result.__isset.result) {
        out = result.result;
        return result.seq.client_seq_no;
      }
      throw TApplicationException(TApplicationException::MISSING_RESULT, "Command failed: unknown result");
    }

   private:
    std::shared_ptr<apache::thrift::protocol::TProtocol> prot_;
    apache::thrift::protocol::TProtocol *iprot_{};
  };

  typedef block_request_serviceClient thrift_client;
  typedef utils::client_cache<thrift_client> client_cache;

  block_client() = default;
  ~block_client();
  int64_t get_client_id();
  void connect(const std::string &hostname, int port, int block_id, int timeout_ms = 0);
  void disconnect();
  bool is_connected() const;

  command_response_reader get_command_response_reader(int64_t client_id);
  void command_request(const sequence_id &seq, int32_t cmd_id, const std::vector<std::string> &args);

 private:
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  std::shared_ptr<thrift_client> client_{};
  int block_id_{-1};
  int timeout_ms_;
};

}
}

#endif //MMUX_BLOCK_CLIENT_H
