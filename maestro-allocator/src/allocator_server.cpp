#include <iostream>
#include <vector>
#include <thread>
#include <jiffy/maestro/free_block_allocator.h>
#include <jiffy/maestro/usage_tracker.h>
#include <jiffy/directory/block/block_registration_server.h>
#include <jiffy/directory/fs/directory_tree.h>
#include <jiffy/utils/logger.h>
#include <jiffy/maestro/allocator_service.h>
#include <jiffy/maestro/controller/maestro_alloactor_server.h>

using namespace ::jiffy::maestro;
using namespace ::jiffy::directory;
using namespace ::jiffy::storage;
using namespace ::jiffy::utils;

using namespace ::apache::thrift;

int main(int argc, char **argv) {

    std::string address = "0.0.0.0";
    int block_port = 9092;
    int service_port = 9090;
    std::unordered_map<std::string, std::float_t> capacityDistribution({
                                                                               { "A", 0.25 },
                                                                               { "B", 0.25 },
                                                                               { "C", 0.25 },
                                                                               { "D", 0.25 },
                                                                           });

    std::mutex failure_mtx;
    std::condition_variable failure_condition;
    std::atomic<int> failing_thread(-1); // alloc -> 0

    std::exception_ptr free_pool_exception = nullptr;
    auto free_block_pool = std::make_shared<free_block_allocator>();
    auto free_block_pool_server = block_registration_server::create(free_block_pool, address, block_port);
    std::thread alloc_serve_thread([&free_pool_exception, &free_block_pool_server, &failing_thread, &failure_condition] {
        try {
            free_block_pool_server->serve();
        } catch (...) {
            free_pool_exception = std::current_exception();
            failing_thread = 0;
            failure_condition.notify_all();
        }
    });
    LOG(log_level::info) << "Free pool server listening on " << address << ":" << block_port;


    std::exception_ptr allocator_exception = nullptr;
    auto tracker = std::make_shared<usage_tracker>(free_block_pool, capacityDistribution);
    auto reclaimer = std::make_shared<reclaim_service>(tracker);
    auto allocator = std::make_shared<allocator_service>(free_block_pool, tracker, reclaimer);
    auto maestro_alloactor_server = maestro_alloactor_server::create(allocator, address, service_port);
    std::thread directory_serve_thread([&allocator_exception, &maestro_alloactor_server, &failing_thread, &failure_condition] {
      try {
        maestro_alloactor_server->serve();
      } catch (...) {
        allocator_exception = std::current_exception();
        failing_thread = 1;
        failure_condition.notify_all();
      }
    });

    LOG(log_level::info) << "Allocator server listening on " << address << ":" << service_port;

    std::unique_lock<std::mutex> failure_condition_lock{failure_mtx};
    failure_condition.wait(failure_condition_lock, [&failing_thread] {
        return failing_thread != -1;
    });

    switch (failing_thread.load()) {
        case 0:
            if (free_pool_exception) {
                try {
                    std::rethrow_exception(free_pool_exception);
                } catch (std::exception &e) {
                    LOG(log_level::error) << "Free pool server failed: " << e.what();
                }
            }
            break;
      case 1:
        if (allocator_exception) {
          try {
            std::rethrow_exception(allocator_exception);
          } catch (std::exception &e) {
            LOG(log_level::error) << "Allocator server failed: " << e.what();
          }
        }
        break;
      default:break;
    }
    return -1;

}
