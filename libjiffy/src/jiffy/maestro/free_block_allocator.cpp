#include <iterator>
#include "free_block_allocator.h"
#include "jiffy/storage/manager/detail/block_id_parser.h"

namespace jiffy {
  namespace maestro {

    // TODO: 18/04/20 order the storage servers by free blocks size
    std::vector<std::string> free_block_allocator::allocate(std::size_t count, const std::vector<std::string> &) {
      std::unique_lock<std::mutex> lock(mtx_);
      std::vector<std::string> blocks;

      auto server = free_blocks_by_server_.begin();

      while(blocks.size() < count && server != free_blocks_by_server_.end()) {
        auto free_block_set = server->second;
        auto free_block_iterator = free_block_set.begin();

        while(blocks.size() < count && free_block_iterator != free_block_set.end()) {
          blocks.push_back(*free_block_iterator);
          free_block_set.erase(free_block_iterator++);
        }

        server++;
      }

      return blocks;
    }

    void free_block_allocator::free(const std::vector<std::string> &block_name) {
      add_blocks(block_name);
    }

    void free_block_allocator::add_blocks(const std::vector<std::string> &block_names) {
      std::unique_lock<std::mutex> lock(mtx_);

      for (const auto &block_name: block_names) {
        auto block = storage::block_id_parser::parse(block_name);
        auto storage_server_id = block.get_storage_server_id();
        free_blocks_by_server_[storage_server_id].insert(block_name);
      }
    }

    void free_block_allocator::remove_blocks(const std::vector<std::string> &block_names) {
      std::unique_lock<std::mutex> lock(mtx_);
      for (const auto &block_name: block_names) {
        auto block = storage::block_id_parser::parse(block_name);
        auto storage_server_id = block.get_storage_server_id();
        free_blocks_by_server_[storage_server_id].insert(block_name);
      }
    }

    std::size_t free_block_allocator::num_free_blocks() {
      std::unique_lock<std::mutex> lock(mtx_);

      unsigned int num_free_blocks = 0;

      for(auto &server: free_blocks_by_server_) {
        auto free_block_set = server.second;
        num_free_blocks += free_block_set.size();
      }

      return num_free_blocks;
    }

    std::size_t free_block_allocator::num_allocated_blocks() {
      throw std::logic_error("number of allocated blocks cannot be determined");
    }

    std::size_t free_block_allocator::num_total_blocks() {
      throw std::logic_error("number of total blocks cannot be determined");
    }
  }
}