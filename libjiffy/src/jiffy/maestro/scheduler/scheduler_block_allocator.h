#ifndef JIFFY_SCHEDULER_BLOCK_ALLOCATOR_H
#define JIFFY_SCHEDULER_BLOCK_ALLOCATOR_H


#include <jiffy/directory/block/block_allocator.h>
#include <mutex>
#include <jiffy/maestro/controller/maestro_allocator_client.h>

namespace jiffy {
  namespace maestro {

    class scheduler_block_allocator : public directory::block_allocator {

    public:

      explicit scheduler_block_allocator(std::string tenantID, std::shared_ptr<maestro_allocator_client> client);

      ~scheduler_block_allocator() override = default;

      std::vector<std::string> allocate(std::size_t count, const std::vector<std::string> &exclude_list) override;

      void free(const std::vector<std::string> &block_name) override;

      void add_blocks(const std::vector<std::string> &block_names) override;

      void remove_blocks(const std::vector<std::string> &block_names) override;

      std::size_t num_free_blocks() override;

      std::size_t num_allocated_blocks() override;

      std::size_t num_total_blocks() override;

    private:

      /* Operation mutex */
      std::mutex mtx_;

      std::string tenantID_;

      std::shared_ptr<jiffy::maestro::maestro_allocator_client> client_;
    };

  }
}

#endif //JIFFY_SCHEDULER_BLOCK_ALLOCATOR_H
