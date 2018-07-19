#ifndef MMUX_BLOCK_ALLOCATOR_H
#define MMUX_BLOCK_ALLOCATOR_H

#include <string>
#include <vector>

namespace mmux {
namespace directory {

class block_allocator {
 public:
  virtual std::vector<std::string> allocate(std::size_t count, const std::vector<std::string> &exclude_list) = 0;
  virtual void free(const std::vector<std::string> &block_name) = 0;
  virtual void add_blocks(const std::vector<std::string> &block_names) = 0;
  virtual void remove_blocks(const std::vector<std::string> &block_names) = 0;

  virtual std::size_t num_free_blocks() = 0;
  virtual std::size_t num_allocated_blocks() = 0;
  virtual std::size_t num_total_blocks() = 0;

  static std::string prefix(const std::string &block_name) const {
    auto pos = block_name.find_last_of(':');
    if (pos == std::string::npos) {
      throw std::logic_error("Malformed block name [" + block_name + "]");
    }
    return block_name.substr(0, pos);
  }
};

}
}

#endif //MMUX_BLOCK_ALLOCATOR_H
