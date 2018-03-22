#ifndef ELASTICMEM_BLOCK_H
#define ELASTICMEM_BLOCK_H

#include <vector>
#include <string>
#include <cstring>
#include <algorithm>
#include <stdexcept>

#define MAX_BLOCK_OP_NAME_SIZE 63

namespace elasticmem {
namespace storage {

enum block_op_type : uint8_t {
  accessor = 0,
  mutator = 1
};

struct block_op {
  block_op_type type;
  char name[MAX_BLOCK_OP_NAME_SIZE];

  bool operator<(const block_op &other) const {
    return std::strcmp(name, other.name) < 0;
  }
};

class block {
 public:
  explicit block(const std::vector<block_op> &block_ops) : block_ops_(block_ops) {}

  virtual void run_command(std::vector<std::string> &_return, int oid, const std::vector<std::string> &args) = 0;

  void run_command(std::vector<std::string> &_return,
                   const std::string &op_name,
                   const std::vector<std::string> &args) {
    int oid = op_id(op_name);
    if (oid == -1) {
      throw std::invalid_argument("No such command " + op_name);
    }
    run_command(_return, oid, args);
  }

  bool is_accessor(int i) const {
    return block_ops_.at(static_cast<size_t>(i)).type == accessor;
  }

  bool is_mutator(int i) const {
    return block_ops_.at(static_cast<size_t>(i)).type == mutator;
  }

  int op_id(const std::string &op_name) const {
    return search_op_name(0, static_cast<int>(block_ops_.size() - 1), op_name.c_str());
  }

  std::string op_name(int op_id) {
    return block_ops_[op_id].name;
  }

  /** Management Operations **/
  virtual void load(const std::string &remote_storage_prefix, const std::string &path) = 0;

  virtual void flush(const std::string &remote_storage_prefix, const std::string &path) = 0;

  virtual std::size_t storage_capacity() = 0;

  virtual std::size_t storage_size() = 0;

  virtual void clear() = 0;

 private:
  int search_op_name(int l, int r, const char *name) const {
    if (r >= l) {
      int mid = l + (r - l) / 2;
      if (std::strcmp(block_ops_[mid].name, name) == 0)
        return mid;
      if (std::strcmp(block_ops_[mid].name, name) > 0)
        return search_op_name(l, mid - 1, name);
      return search_op_name(mid + 1, r, name);
    }
    return -1;
  }

  const std::vector<block_op> &block_ops_;
  std::string path_;
};

}
}
#endif //ELASTICMEM_BLOCK_H
