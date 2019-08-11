#ifndef JIFFY_CHAIN_MODULE_H
#define JIFFY_CHAIN_MODULE_H

#include <atomic>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include <libcuckoo/cuckoohash_map.hh>
#include <shared_mutex>
#include "jiffy/storage/client/block_client.h"
#include "jiffy/storage/manager/detail/block_id_parser.h"
#include "jiffy/storage/partition.h"
#include "jiffy/storage/chain/chain_request_client.h"
#include "jiffy/storage/chain/chain_response_client.h"
#include "jiffy/storage/notification/subscription_map.h"
#include "jiffy/storage/service/block_response_client_map.h"

namespace jiffy {
namespace storage {

/*
 * Chain roles
 * We mark out the special rule singleton if there is only one partition for the chain
 * i.e. we don't use chain replication in this case
 * The head would deal with update request and the tail would deal with query request
 * and also generate the respond
 */

enum chain_role {
  singleton = 0,
  head = 1,
  mid = 2,
  tail = 3
};

/*
 * Chain operation
 */

struct chain_op {
  /* Command sequence identifier */
  sequence_id seq;
  /* Command arguments */
  std::vector<std::string> args;
};

/* Connection to next chain module */
class next_chain_module_cxn {
 public:
  next_chain_module_cxn() = default;

  /**
   * @brief Constructor
   * @param block_name Next block name
   */

  explicit next_chain_module_cxn(const std::string &block_name) {
    reset(block_name);
  }

  /**
   * @brief Destructor
   */

  ~next_chain_module_cxn() {
    client_.disconnect();
  }

  /**
   * @brief Reset block
   * Disconnect the current chain request client and connect to the new block
   * @param block_name Block name
   * @return Client protocol
   */

  std::shared_ptr<apache::thrift::protocol::TProtocol> reset(const std::string &block_name) {
    client_.disconnect();
    if (block_name != "nil") {
      auto bid = block_id_parser::parse(block_name);
      auto host = bid.host;
      auto port = bid.service_port;
      auto block_id = bid.id;
      client_.connect(host, port, block_id);
    }
    return client_.protocol();
  }

  /**
   * @brief Request an operation
   * @param seq Request sequence identifier
   * @param args Operation arguments
   */
  void request(const sequence_id &seq, const std::vector<std::string> &args) {
    client_.request(seq, args);
  }

  /**
   * @brief Run command on next block
   * @param result Result
   * @param args Command arguments
   */
  void run_command(std::vector<std::string> &result, const std::vector<std::string> &args) {
    client_.run_command(result, args);
  }

 private:
  /* Chain request client */
  chain_request_client client_;
};

/* Connection to previous chain module */
class prev_chain_module_cxn {
 public:
  prev_chain_module_cxn() = default;

  /**
   * @brief Constructor
   * @param prot Protocol
   */

  explicit prev_chain_module_cxn(std::shared_ptr<::apache::thrift::protocol::TProtocol> prot) {
    reset(std::move(prot));
  }

  /**
   * @brief Reset protocol
   * @param prot Protocol
   */

  void reset(std::shared_ptr<::apache::thrift::protocol::TProtocol> prot) {
    client_.reset_prot(std::move(prot));
  }

  /**
   * @brief Acknowledge previous block
   * @param seq Chain Sequence identifier
   */

  void ack(const sequence_id &seq) {
    client_.ack(seq);
  }

  /**
   * @brief Check if chain response client is set
   * @return Bool value, true if set
   */

  bool is_set() const {
    return client_.is_set();
  }

 private:
  /* Chain response client */
  chain_response_client client_;
};

/* Chain module class
 * Inherited from partition */
class chain_module : public partition {
 public:
  /* Class chain response handler
   * Inherited from chain response serviceIf class */
  class chain_response_handler : public block_response_serviceIf {
   public:
    /**
     * @brief Constructor
     * @param module Chain module
     */

    explicit chain_response_handler(chain_module *module) : module_(module) {}

    /**
     * @brief Chain acknowledgement
     * @param seq Operation sequence identifier
     */

    void chain_ack(const sequence_id &seq) override {
      module_->ack(seq);
    }

    /**
     * @brief Send response
     * @param seq Operation sequence identifier
     * @param result Output response.
     */
    void response(const sequence_id &, const std::vector<std::string> &) override {
      throw std::logic_error("Chain handler does not support query responses.");
    }
    void notification(const std::string &, const std::string &) override {
      throw std::logic_error("Chain handler does not support notification responses.");
    }
    void control(const response_type, const std::vector<std::string> &, const std::string &) override {
      throw std::logic_error("Chain handler does not support control responses.");
    }

   private:
    /* Chain module */
    chain_module *module_;
  };

  /**
   * @brief Constructor
   * @param name Partition name
   * @param metadata Partition metadata
   * @param supported_cmds Supported commands
   */
  chain_module(block_memory_manager *manager,
               const std::string &name,
               const std::string &metadata,
               const command_map &supported_cmds);

  /**
   * @brief Destructor
   */
  ~chain_module() override;

  /**
   * @brief Setup a chain module and start the processor thread
   * @param path Block path
   * @param chain Replica chain block names
   * @param role Chain module role
   * @param next_block_id Next block name
   */
  virtual void setup(const std::string &path,
                     const std::vector<std::string> &chain,
                     chain_role role,
                     const std::string &next_block_id);

  /**
   * @brief Set chain module role
   * @param role Role
   */
  void role(chain_role role) {
    role_ = role;
  }

  /**
   * @brief Fetch chain module role
   * @return Role
   */
  chain_role role() const {
    return role_;
  }

  /**
   * @brief Set replica chain block names
   * @param chain Chain block names
   */
  void chain(const std::vector<std::string> &chain) {
    chain_ = chain;
  }

  /**
   * @brief Fetch replica chain block names
   * @return Replica chain block names
   */
  const std::vector<std::string> &chain() {
    return chain_;
  }

  /**
   * @brief Check if chain module role is head
   * @return Bool value, true if chain role is head or singleton
   */
  bool is_head() const {
    return role() == chain_role::head || role() == chain_role::singleton;
  }

  /**
   * @brief Check if chain module role is tail
   * @return Bool value, true if chain role is tail or singleton
   */
  bool is_tail() const {
    return role() == chain_role::tail || role() == chain_role::singleton;
  }

  /**
   * @brief Check if previous chain module is set
   * @return Bool value, true if set
   */
  bool is_set_prev() const {
    return prev_->is_set(); // Does not require a lock since only one thread calls this at a time
  }

  /**
   * @brief Reset previous block
   * @param prot Protocol
   */
  void reset_prev(const std::shared_ptr<::apache::thrift::protocol::TProtocol> &prot) {
    prev_->reset(prot); // Does not require a lock since only one thread calls this at a time
  }

  /**
   * @brief Run command on next block
   * @param result Command result
   * @param args Command arguments
   */
  void run_command_on_next(response &result, const arg_list &args) {
    next_->run_command(result, args);
  }

  /**
   * @brief Add request to pending
   * @param seq Request sequence identifier
   * @param args Command arguments
   */

  void add_pending(const sequence_id &seq, const arg_list &args) {
    pending_.insert(seq.server_seq_no, chain_op{seq, args});
  }

  /**
   * @brief Remove a pending request
   * @param seq Sequence identifier
   */
  void remove_pending(const sequence_id &seq) {
    pending_.erase(seq.server_seq_no);
  }

  /**
   * @brief Resend the pending request
   */
  void resend_pending();

  /**
   * @brief Virtual function for forwarding all
   */
  virtual void forward_all() = 0;

  /**
   * @brief Request for the first time
   * @param seq Sequence identifier
   * @param args Command arguments
   */
  void request(sequence_id seq, const arg_list &args);

  /**
   * @brief Chain request
   * @param seq Sequence identifier
   * @param args Command arguments
   */
  void chain_request(const sequence_id &seq, const arg_list &args);

  /**
   * @brief Acknowledge the previous block
   * @param seq Sequence identifier
   */
  void ack(const sequence_id &seq);

 protected:
  /* Role of chain module */
  chain_role role_{singleton};
  /* Chain sequence number */
  int64_t chain_seq_no_{0};
  /* Next partition connection */
  std::unique_ptr<next_chain_module_cxn> next_{nullptr};
  /* Previous partition connection */
  std::unique_ptr<prev_chain_module_cxn> prev_{nullptr};
  /* Replica chain partition names */
  std::vector<std::string> chain_;
  /* Response processor thread */
  std::thread response_processor_;
  /* Pending operations */
  cuckoohash_map<int64_t, chain_op> pending_;
};

}
}

#endif //JIFFY_CHAIN_MODULE_H
