#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <iostream>

#include "pool_block_client.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

namespace jiffy {
namespace storage {

pool_block_client::~pool_block_client() {
  if (transport_ != nullptr)
    disconnect();
}

int64_t pool_block_client::get_client_id() {
  return client_->get_client_id();
}

void pool_block_client::connect(const std::string &host, int port, int timeout_ms) {
  auto sock = std::make_shared<TSocket>(host, port);
  if (timeout_ms > 0)
    sock->setRecvTimeout(timeout_ms);
  transport_ = std::shared_ptr<TTransport>(new TFramedTransport(sock));
  protocol_ = std::shared_ptr<TProtocol>(new TBinaryProtocol(transport_));
  client_ = std::make_shared<thrift_client>(protocol_);
  transport_->open();
}

std::shared_ptr<apache::thrift::protocol::TProtocol> pool_block_client::protocol() {
  return protocol_;
}

pool_block_client::command_response_reader pool_block_client::get_command_response_reader(int64_t client_id) {
  client_->register_client_id(0, client_id);
  return pool_block_client::command_response_reader(protocol_);
}

void pool_block_client::disconnect() {
  if (is_connected()) {
    transport_->close();
  }
}

bool pool_block_client::is_connected() const {
  if (transport_ == nullptr) return false;
  return transport_->isOpen();
}

void pool_block_client::command_request(const sequence_id &seq, const std::vector<std::string> &args, int block_id) {
  client_->command_request(seq, block_id, args);
}

void pool_block_client::send_run_command(const int32_t block_id, const std::vector<std::string> &arguments) {
  client_->send_run_command(block_id, arguments);
}

void pool_block_client::recv_run_command(std::vector<std::string> &_return) {
  client_->recv_run_command(_return);
}

void pool_block_client::register_client_id(int64_t client_id) {
  client_->register_client_id(0, client_id);

}

pool_block_client::command_response_reader::command_response_reader(std::shared_ptr<apache::thrift::protocol::TProtocol> prot)
    : prot_(std::move(prot)) {
  iprot_ = prot_.get();
}

int64_t pool_block_client::command_response_reader::recv_response(std::vector<std::string> &out) {
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
bool pool_block_client::command_response_reader::is_set() {
  return prot_ != nullptr;
}

}
}
