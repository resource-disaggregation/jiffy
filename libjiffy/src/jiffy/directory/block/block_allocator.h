#ifndef JIFFY_BLOCK_ALLOCATOR_H
#define JIFFY_BLOCK_ALLOCATOR_H

#include <string>
#include <vector>

namespace jiffy {
namespace directory {
/* Block allocator virtual class */
class block_allocator {
 public:
  virtual ~block_allocator() = default;

  virtual std::vector<std::string> allocate(std::size_t count, const std::vector<std::string> &exclude_list, const std::string &tenant_id) = 0;
  virtual void free(const std::vector<std::string> &block_name, const std::string &tenant_id) = 0;
  virtual void add_blocks(const std::vector<std::string> &block_names) = 0;
  virtual void remove_blocks(const std::vector<std::string> &block_names) = 0;

  virtual std::size_t num_free_blocks() = 0;
  virtual std::size_t num_allocated_blocks() = 0;
  virtual std::size_t num_total_blocks() = 0;

  virtual void update_demand(const std::string &/*tenant_id*/, uint32_t /*demand*/, uint32_t /*oracle_demand*/) {
    // By default do nothing
    
  }
};

}
}

#endif //JIFFY_BLOCK_ALLOCATOR_H
